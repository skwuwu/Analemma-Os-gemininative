"""
Plan Briefing API Handler

워크플로우 실행 전 미리보기 및 Time Machine 디버깅을 위한 Lambda 핸들러입니다.
"""

import json
import logging
import os
import asyncio
from typing import Dict, Any, Optional

# 서비스 임포트
try:
    from src.services.plan_briefing_service import PlanBriefingService
    from src.services.checkpoint_service import CheckpointService
    from src.services.time_machine_service import TimeMachineService
    from src.common.auth_utils import extract_owner_id_from_event
except ImportError:
    from src.services.plan_briefing_service import PlanBriefingService
    from src.services.checkpoint_service import CheckpointService
    from src.services.time_machine_service import TimeMachineService
    from src.common.auth_utils import extract_owner_id_from_event

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def _response(status_code: int, body: Any, headers: Dict = None) -> Dict:
    """Lambda proxy response 생성"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            **(headers or {})
        },
        'body': json.dumps(body, ensure_ascii=False, default=str) if body else ''
    }


def _parse_body(event: Dict) -> tuple[Optional[Dict], Optional[str]]:
    """요청 바디 파싱"""
    raw_body = event.get('body')
    if not raw_body:
        return {}, None
    
    try:
        parsed = json.loads(raw_body)
        if not isinstance(parsed, dict):
            return None, 'Request body must be a JSON object'
        return parsed, None
    except json.JSONDecodeError as e:
        return None, f'Invalid JSON: {str(e)}'


def _get_query_param(event: Dict, param: str, default: Any = None) -> Any:
    """쿼리 파라미터 안전 추출"""
    query_params = event.get('queryStringParameters') or {}
    return query_params.get(param, default)


def _get_path_param(event: Dict, param: str) -> Optional[str]:
    """경로 파라미터 안전 추출"""
    path_params = event.get('pathParameters') or {}
    return path_params.get(param)


async def lambda_handler(event, context):
    """Plan Briefing API Lambda 핸들러"""
    
    # CORS preflight 처리
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    if http_method == 'OPTIONS':
        return _response(200, None)
    
    # 인증 확인
    owner_id = extract_owner_id_from_event(event)
    if not owner_id:
        return _response(401, {'error': 'Authentication required'})
    
    # 경로 및 메소드 추출
    path = event.get('path') or event.get('rawPath', '')
    
    logger.info(f"Plan Briefing API: {http_method} {path} (owner={owner_id[:8]}...)")
    
    try:
        # 라우팅
        if path.endswith('/preview') and 'detailed-draft' not in path:
            # POST /workflows/preview - 워크플로우 미리보기
            if http_method == 'POST':
                return await handle_generate_preview(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif path.endswith('/detailed-draft'):
            # POST /workflows/preview/detailed-draft - 상세 초안
            if http_method == 'POST':
                return await handle_detailed_draft(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif '/timeline' in path:
            # GET /executions/{id}/timeline - 실행 타임라인
            if http_method == 'GET':
                thread_id = _get_path_param(event, 'id')
                return await handle_get_timeline(owner_id, thread_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif '/checkpoints' in path and '/compare' not in path:
            thread_id = _get_path_param(event, 'id')
            checkpoint_id = _get_path_param(event, 'checkpoint_id')
            
            if checkpoint_id:
                # GET /executions/{id}/checkpoints/{checkpoint_id} - 체크포인트 상세
                if http_method == 'GET':
                    return await handle_get_checkpoint_detail(owner_id, thread_id, checkpoint_id, event)
                else:
                    return _response(405, {'error': f'Method {http_method} not allowed'})
            else:
                # GET /executions/{id}/checkpoints - 체크포인트 목록
                if http_method == 'GET':
                    return await handle_list_checkpoints(owner_id, thread_id, event)
                else:
                    return _response(405, {'error': f'Method {http_method} not allowed'})
                    
        elif '/rollback' in path:
            if 'preview' in path:
                # POST /executions/rollback/preview - 롤백 미리보기
                if http_method == 'POST':
                    return await handle_rollback_preview(owner_id, event)
                else:
                    return _response(405, {'error': f'Method {http_method} not allowed'})
            else:
                # POST /executions/rollback - 롤백 실행
                if http_method == 'POST':
                    return await handle_execute_rollback(owner_id, event)
                else:
                    return _response(405, {'error': f'Method {http_method} not allowed'})
                    
        elif '/compare' in path:
            # POST /executions/checkpoints/compare - 체크포인트 비교
            if http_method == 'POST':
                return await handle_compare_checkpoints(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif '/branches' in path:
            # GET /executions/{id}/branches - 분기 히스토리
            if http_method == 'GET':
                thread_id = _get_path_param(event, 'id')
                return await handle_get_branch_history(owner_id, thread_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif '/rollback-suggestions' in path:
            # GET /executions/{id}/rollback-suggestions - 롤백 추천
            if http_method == 'GET':
                thread_id = _get_path_param(event, 'id')
                return await handle_get_rollback_suggestions(owner_id, thread_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
        else:
            return _response(404, {'error': 'Endpoint not found'})
                
    except Exception as e:
        logger.exception(f"Plan Briefing API error: {e}")
        return _response(500, {'error': 'Internal server error'})


async def handle_generate_preview(owner_id: str, event: Dict) -> Dict:
    """워크플로우 미리보기 생성"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # 필수 필드 검증
    workflow_config = body.get('workflow_config')
    if not workflow_config:
        return _response(400, {'error': 'workflow_config is required'})
    
    try:
        async with PlanBriefingService() as service:
            briefing = await service.generate_briefing(
                workflow_config=workflow_config,
                initial_statebag=body.get('initial_statebag', {}),
                user_context=body.get('user_context'),
                use_llm=body.get('use_llm', True)
            )
        
        # Pydantic 모델을 dict로 변환
        briefing_dict = briefing.dict() if hasattr(briefing, 'dict') else briefing
        
        logger.info(f"Generated briefing for workflow {workflow_config.get('id', 'unknown')}")
        return _response(200, briefing_dict)
        
    except Exception as e:
        logger.error(f"Failed to generate briefing: {e}")
        return _response(500, {'error': 'Failed to generate briefing'})


async def handle_detailed_draft(owner_id: str, event: Dict) -> Dict:
    """상세 초안 생성"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # 필수 필드 검증
    node_config = body.get('node_config')
    if not node_config:
        return _response(400, {'error': 'node_config is required'})
    
    try:
        # 간단한 초안 생성 (실제로는 더 복잡한 로직 필요)
        draft = {
            'node_id': node_config.get('id', 'unknown'),
            'draft_content': f"Draft for node: {node_config.get('type', 'unknown')}",
            'confidence': 0.8,
            'estimated_tokens': 100
        }
        
        logger.info(f"Generated detailed draft for node {node_config.get('id', 'unknown')}")
        return _response(200, draft)
        
    except Exception as e:
        logger.error(f"Failed to generate detailed draft: {e}")
        return _response(500, {'error': 'Failed to generate detailed draft'})


async def handle_get_timeline(owner_id: str, thread_id: str, event: Dict) -> Dict:
    """실행 타임라인 조회"""
    include_state = _get_query_param(event, 'include_state', 'true').lower() == 'true'
    
    try:
        service = CheckpointService()
        timeline = await service.get_execution_timeline(thread_id, include_state)
        
        response = {
            'thread_id': thread_id,
            'timeline': timeline
        }
        
        logger.info(f"Retrieved timeline for execution {thread_id[:8]}...")
        return _response(200, response)
        
    except Exception as e:
        logger.error(f"Failed to get timeline: {e}")
        return _response(500, {'error': 'Failed to retrieve timeline'})


async def handle_list_checkpoints(owner_id: str, thread_id: str, event: Dict) -> Dict:
    """체크포인트 목록 조회"""
    limit = int(_get_query_param(event, 'limit', 50))
    limit = max(1, min(limit, 100))
    
    try:
        service = CheckpointService()
        checkpoints = await service.list_checkpoints(thread_id, limit)
        
        response = {
            'thread_id': thread_id,
            'checkpoints': checkpoints
        }
        
        logger.info(f"Retrieved {len(checkpoints)} checkpoints for execution {thread_id[:8]}...")
        return _response(200, response)
        
    except Exception as e:
        logger.error(f"Failed to list checkpoints: {e}")
        return _response(500, {'error': 'Failed to retrieve checkpoints'})


async def handle_get_checkpoint_detail(owner_id: str, thread_id: str, checkpoint_id: str, event: Dict) -> Dict:
    """체크포인트 상세 조회"""
    try:
        service = CheckpointService()
        checkpoint = await service.get_checkpoint_detail(thread_id, checkpoint_id)
        
        if not checkpoint:
            return _response(404, {'error': 'Checkpoint not found'})
        
        logger.info(f"Retrieved checkpoint detail {checkpoint_id[:8]}...")
        return _response(200, checkpoint)
        
    except Exception as e:
        logger.error(f"Failed to get checkpoint detail: {e}")
        return _response(500, {'error': 'Failed to retrieve checkpoint detail'})


async def handle_rollback_preview(owner_id: str, event: Dict) -> Dict:
    """롤백 미리보기"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    try:
        service = TimeMachineService()
        preview = await service.preview_rollback(body)
        
        logger.info(f"Generated rollback preview")
        return _response(200, preview)
        
    except Exception as e:
        logger.error(f"Failed to preview rollback: {e}")
        return _response(500, {'error': 'Failed to preview rollback'})


async def handle_execute_rollback(owner_id: str, event: Dict) -> Dict:
    """롤백 실행"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    try:
        service = TimeMachineService()
        branch_info = await service.execute_rollback(body)
        
        logger.info(f"Executed rollback")
        return _response(200, branch_info)
        
    except Exception as e:
        logger.error(f"Failed to execute rollback: {e}")
        return _response(500, {'error': 'Failed to execute rollback'})


async def handle_compare_checkpoints(owner_id: str, event: Dict) -> Dict:
    """체크포인트 비교"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    try:
        service = CheckpointService()
        comparison = await service.compare_checkpoints(
            body.get('thread_id'),
            body.get('checkpoint_id_a'),
            body.get('checkpoint_id_b')
        )
        
        logger.info(f"Compared checkpoints")
        return _response(200, comparison)
        
    except Exception as e:
        logger.error(f"Failed to compare checkpoints: {e}")
        return _response(500, {'error': 'Failed to compare checkpoints'})


async def handle_get_branch_history(owner_id: str, thread_id: str, event: Dict) -> Dict:
    """분기 히스토리 조회"""
    try:
        service = TimeMachineService()
        branches = await service.get_branch_history(thread_id)
        
        response = {
            'thread_id': thread_id,
            'branches': branches
        }
        
        logger.info(f"Retrieved branch history for execution {thread_id[:8]}...")
        return _response(200, response)
        
    except Exception as e:
        logger.error(f"Failed to get branch history: {e}")
        return _response(500, {'error': 'Failed to retrieve branch history'})


async def handle_get_rollback_suggestions(owner_id: str, thread_id: str, event: Dict) -> Dict:
    """롤백 추천 조회"""
    try:
        service = TimeMachineService()
        suggestions = await service.get_rollback_suggestions(thread_id)
        
        response = {
            'thread_id': thread_id,
            'suggestions': suggestions
        }
        
        logger.info(f"Retrieved rollback suggestions for execution {thread_id[:8]}...")
        return _response(200, response)
        
    except Exception as e:
        logger.error(f"Failed to get rollback suggestions: {e}")
        return _response(500, {'error': 'Failed to retrieve rollback suggestions'})


# 동기 래퍼 (Lambda는 비동기 핸들러를 직접 지원하지 않음)
def lambda_handler_sync(event, context):
    """동기 Lambda 핸들러 래퍼"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(lambda_handler(event, context))
    finally:
        loop.close()


# Lambda 엔트리포인트
def lambda_handler_entry(event, context):
    """Lambda entry point"""
    return lambda_handler_sync(event, context)