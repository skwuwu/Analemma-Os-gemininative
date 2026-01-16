"""
Co-design Assistant API Handler

양방향 협업 워크플로우 설계를 위한 스트리밍 API 핸들러입니다.
Canvas가 비어있을 때는 Agentic Designer 모드로 자동 전환됩니다.

[v2.0 개선사항]
- Lambda Function URL RESPONSE_STREAM 모드 지원 (진짜 스트리밍)
- 이벤트 루프 최적화 (전역 루프 재사용)
- UUID 기반 세션 ID 생성
- Gemini API 특화 에러 핸들링
"""

import json
import logging
import os
import asyncio
import uuid
from typing import Dict, Any, Optional, Generator, Callable
from functools import wraps

# ═══════════════════════════════════════════════════════════════════════════════
# 🚨 [Critical Fix] Import 경로 수정
# 기존: agentic_designer_handler (함수들이 존재하지 않음)
# 수정: 각 함수/상수가 실제로 정의된 모듈에서 직접 import
# ═══════════════════════════════════════════════════════════════════════════════

# 서비스 임포트
try:
    from src.services.design.codesign_assistant import (
        stream_codesign_response,
        explain_workflow,
        generate_suggestions,
        apply_suggestion,
        get_or_create_context
    )
    # LLM 클라이언트 (bedrock_client.py에서 import)
    from src.services.llm.bedrock_client import (
        invoke_bedrock_stream as invoke_bedrock_model_stream,
        MODEL_SONNET,
        is_mock_mode as _is_mock_mode,
        get_mock_workflow as _mock_workflow_json,
    )
    # 시스템 프롬프트 (prompts.py에서 import)
    from src.services.design.prompts import SYSTEM_PROMPT
    # 모델 라우터 및 인증
    from src.common.model_router import get_model_for_canvas_mode
    from src.common.auth_utils import extract_owner_id_from_event
    _IMPORTS_OK = True
except ImportError as e:
    import logging
    logging.getLogger(__name__).warning(f"Import fallback activated: {e}")
    _IMPORTS_OK = False
    
    # Fallback imports (개별적으로 시도)
    try:
        from src.services.design.codesign_assistant import (
            stream_codesign_response,
            explain_workflow,
            generate_suggestions,
            apply_suggestion,
            get_or_create_context
        )
    except ImportError:
        stream_codesign_response = None
        explain_workflow = None
        generate_suggestions = None
        apply_suggestion = None
        get_or_create_context = None
    
    # LLM 클라이언트 fallback
    invoke_bedrock_model_stream = None
    MODEL_SONNET = "anthropic.claude-3-sonnet-20240229-v1:0"
    _is_mock_mode = lambda: os.getenv("MOCK_MODE", "false").lower() in {"true", "1", "yes", "on"}
    _mock_workflow_json = lambda: {"nodes": [], "edges": []}
    
    # 시스템 프롬프트 fallback
    SYSTEM_PROMPT = "You are a workflow design assistant."
    
    try:
        from src.common.model_router import get_model_for_canvas_mode
    except ImportError:
        get_model_for_canvas_mode = None
    
    try:
        from src.common.auth_utils import extract_owner_id_from_event
    except ImportError:
        def extract_owner_id_from_event(*args, **kwargs):
            raise Exception("Unauthorized: auth_utils not available")

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ============================================================================
# 전역 이벤트 루프 관리 (Lambda 컨테이너 재사용 최적화)
# ============================================================================
_global_loop: Optional[asyncio.AbstractEventLoop] = None


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """
    Lambda 컨테이너 재사용을 위한 전역 이벤트 루프 관리.
    매 호출마다 new_event_loop()를 생성하는 오버헤드를 제거합니다.
    """
    global _global_loop
    
    try:
        # 현재 실행 중인 루프가 있으면 반환
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError:
        pass
    
    # 전역 루프가 있고 유효하면 재사용
    if _global_loop is not None and not _global_loop.is_closed():
        return _global_loop
    
    # 새 루프 생성 및 전역 저장
    _global_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_global_loop)
    return _global_loop


# ============================================================================
# 비즈니스 친화적 에러 분류 및 변환
# ============================================================================
class APIErrorCode:
    """API 에러 코드 상수"""
    SAFETY_FILTER_BLOCKED = "SAFETY_FILTER_BLOCKED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    MODEL_OVERLOADED = "MODEL_OVERLOADED"
    CONTEXT_TOO_LONG = "CONTEXT_TOO_LONG"
    INVALID_REQUEST = "INVALID_REQUEST"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    STREAMING_INTERRUPTED = "STREAMING_INTERRUPTED"


def classify_and_format_error(error: Exception) -> Dict[str, Any]:
    """
    LLM API 에러를 비즈니스 친화적 메시지로 변환.
    투자자/해커톤 심사위원이 볼 수 있는 사용자 메시지를 생성합니다.
    """
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # Gemini Safety Filter 차단
    if any(kw in error_str for kw in ["safety", "blocked", "harm", "dangerous"]):
        return {
            "error_code": APIErrorCode.SAFETY_FILTER_BLOCKED,
            "message": "요청 내용이 안전 정책에 의해 필터링되었습니다.",
            "user_action": "다른 표현으로 다시 시도해주세요.",
            "retryable": True,
            "status_code": 422
        }
    
    # Quota 초과
    if any(kw in error_str for kw in ["quota", "rate limit", "too many requests", "429"]):
        return {
            "error_code": APIErrorCode.QUOTA_EXCEEDED,
            "message": "현재 요청이 많아 잠시 후 다시 시도해주세요.",
            "user_action": "30초 후 다시 시도해주세요.",
            "retryable": True,
            "retry_after_seconds": 30,
            "status_code": 429
        }
    
    # 모델 과부하
    if any(kw in error_str for kw in ["overloaded", "capacity", "503", "service unavailable"]):
        return {
            "error_code": APIErrorCode.MODEL_OVERLOADED,
            "message": "AI 모델이 현재 바쁩니다. 잠시 후 다시 시도해주세요.",
            "user_action": "1분 후 다시 시도해주세요.",
            "retryable": True,
            "retry_after_seconds": 60,
            "status_code": 503
        }
    
    # 컨텍스트 길이 초과
    if any(kw in error_str for kw in ["context length", "token limit", "too long", "max tokens"]):
        return {
            "error_code": APIErrorCode.CONTEXT_TOO_LONG,
            "message": "워크플로우가 너무 복잡합니다. 일부 노드를 정리해주세요.",
            "user_action": "워크플로우를 단순화하거나 분할해주세요.",
            "retryable": False,
            "status_code": 413
        }
    
    # 잘못된 요청
    if any(kw in error_str for kw in ["invalid", "malformed", "bad request", "400"]):
        return {
            "error_code": APIErrorCode.INVALID_REQUEST,
            "message": "요청 형식이 올바르지 않습니다.",
            "user_action": "입력 내용을 확인해주세요.",
            "retryable": False,
            "status_code": 400
        }
    
    # 기타 내부 오류
    return {
        "error_code": APIErrorCode.INTERNAL_ERROR,
        "message": "일시적인 오류가 발생했습니다.",
        "user_action": "문제가 지속되면 관리자에게 문의해주세요.",
        "retryable": True,
        "status_code": 500,
        "debug_info": error_type if os.getenv("DEBUG_MODE") else None
    }


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


def _error_response(error: Exception) -> Dict:
    """
    에러를 비즈니스 친화적 응답으로 변환.
    Gemini API 특화 에러를 사용자가 이해할 수 있는 메시지로 변환합니다.
    """
    error_info = classify_and_format_error(error)
    return _response(
        error_info.get("status_code", 500),
        {
            "error": error_info["message"],
            "error_code": error_info["error_code"],
            "user_action": error_info.get("user_action"),
            "retryable": error_info.get("retryable", False),
            "retry_after_seconds": error_info.get("retry_after_seconds")
        }
    )


# ============================================================================
# Lambda Function URL RESPONSE_STREAM 지원 (진짜 스트리밍)
# ============================================================================

def _is_streaming_invocation(event: Dict) -> bool:
    """
    Lambda Function URL의 RESPONSE_STREAM 모드인지 확인.
    InvokeMode: RESPONSE_STREAM이 설정된 경우 True 반환.
    """
    request_context = event.get('requestContext', {})
    # Function URL의 스트리밍 모드 감지
    return (
        request_context.get('http', {}).get('method') is not None and  # Function URL 형식
        os.getenv('LAMBDA_STREAMING_ENABLED', 'false').lower() == 'true'
    )


async def _stream_to_response_stream(
    generator: Generator[str, None, None],
    response_stream: Any
) -> None:
    """
    Generator 출력을 Lambda Response Stream에 직접 쓰기.
    6MB 페이로드 제한을 우회하고 진짜 스트리밍을 구현합니다.
    
    Args:
        generator: JSONL 청크를 yield하는 제너레이터
        response_stream: awslambdaric.ResponseStream 객체
    """
    try:
        for chunk in generator:
            if chunk:
                # 청크를 즉시 클라이언트로 전송
                response_stream.write(chunk.encode('utf-8'))
                # 강제 플러시로 지연 없이 전송
                if hasattr(response_stream, 'flush'):
                    response_stream.flush()
    except Exception as e:
        logger.error(f"Stream write error: {e}")
        error_chunk = json.dumps({
            "type": "error",
            "error_code": APIErrorCode.STREAMING_INTERRUPTED,
            "data": "스트리밍이 중단되었습니다. 다시 시도해주세요."
        }) + "\n"
        response_stream.write(error_chunk.encode('utf-8'))
    finally:
        if hasattr(response_stream, 'close'):
            response_stream.close()


def _streaming_response(generator: Generator[str, None, None]) -> Dict:
    """
    스트리밍 응답 생성 (Fallback 모드).
    
    [주의] 이 함수는 Lambda Function URL의 RESPONSE_STREAM 모드가 
    비활성화된 경우에만 사용됩니다. 진짜 스트리밍이 아닌 
    청크 수집 후 일괄 반환 방식입니다.
    
    페이로드 크기 제한:
    - AWS Lambda 응답 제한: 6MB
    - 5MB 초과 시 경고 로깅
    """
    MAX_PAYLOAD_SIZE = 5 * 1024 * 1024  # 5MB 경고 임계값
    HARD_LIMIT = 6 * 1024 * 1024  # 6MB 하드 리밋
    
    chunks = []
    total_size = 0
    truncated = False
    
    try:
        for chunk in generator:
            chunk_size = len(chunk.encode('utf-8'))
            
            # 페이로드 크기 체크
            if total_size + chunk_size > HARD_LIMIT:
                logger.warning(f"Payload approaching 6MB limit, truncating response")
                truncated = True
                # 경고 메시지 추가
                chunks.append(json.dumps({
                    "type": "warning",
                    "data": "응답이 너무 길어 일부가 생략되었습니다."
                }) + "\n")
                break
            
            if total_size > MAX_PAYLOAD_SIZE:
                logger.warning(f"Payload exceeded 5MB: {total_size / 1024 / 1024:.2f}MB")
            
            chunks.append(chunk)
            total_size += chunk_size
            
    except Exception as e:
        logger.error(f"Streaming error: {e}")
        error_info = classify_and_format_error(e)
        chunks.append(json.dumps({
            "type": "error",
            "error_code": error_info["error_code"],
            "data": error_info["message"],
            "user_action": error_info.get("user_action")
        }) + "\n")
    
    response_body = ''.join(chunks)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/x-ndjson',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            'X-Payload-Truncated': 'true' if truncated else 'false',
            'X-Payload-Size': str(total_size),
        },
        'body': response_body
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


def _generate_session_id(owner_id: str) -> str:
    """
    UUID 기반 세션 ID 생성.
    
    기존 asyncio.get_event_loop().time() 방식의 문제점:
    - 단조 시간(Monotonic time)으로 시스템 재부팅 시 중복 가능
    - Lambda 컨테이너 재시작 시 충돌 위험
    
    UUID4는 122비트 랜덤으로 충돌 확률이 사실상 0입니다.
    """
    return f"session_{owner_id[:8]}_{uuid.uuid4().hex[:16]}"


async def lambda_handler(event, context):
    """Co-design Assistant API Lambda 핸들러"""
    
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
    
    logger.info(f"Co-design API: {http_method} {path} (owner={owner_id[:8]}...)")
    
    try:
        # 라우팅
        if path.endswith('/codesign') or path.endswith('/codesign/'):
            # POST /codesign - 협업 워크플로우 설계
            if http_method == 'POST':
                return await handle_codesign_stream(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif path.endswith('/explain'):
            # POST /codesign/explain - 워크플로우 설명
            if http_method == 'POST':
                return await handle_explain_workflow(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif path.endswith('/suggestions'):
            # POST /codesign/suggestions - 제안 생성
            if http_method == 'POST':
                return await handle_generate_suggestions(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
        elif '/apply-suggestion' in path:
            # POST /codesign/apply-suggestion - 제안 적용
            if http_method == 'POST':
                return await handle_apply_suggestion(owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
        else:
            return _response(404, {'error': 'Endpoint not found'})
                
    except Exception as e:
        logger.exception(f"Co-design API error: {e}")
        return _error_response(e)


def _generate_initial_workflow_stream(user_request: str, owner_id: str, session_id: str) -> Generator[str, None, None]:
    """
    빈 Canvas에서 초기 워크플로우를 생성하는 스트리밍 함수
    Agentic Designer 로직을 사용하여 완전한 워크플로우를 생성합니다.
    
    [출력 형식 일관성 보장]
    stream_codesign_response와 동일한 JSONL 형식을 사용합니다:
    - {"type": "node", "data": {...}}
    - {"type": "edge", "data": {...}}
    - {"type": "status", "data": "done"}
    - {"type": "error", "error_code": "...", "data": "..."}
    """
    try:
        # Mock 모드 처리
        if _is_mock_mode():
            logger.info("Mock mode: generating sample workflow")
            mock_workflow = _mock_workflow_json()
            
            # 노드들을 순차적으로 스트리밍
            for node in mock_workflow.get("nodes", []):
                yield json.dumps({"type": "node", "data": node}, ensure_ascii=False) + "\n"
            
            # 엣지들을 순차적으로 스트리밍
            for edge in mock_workflow.get("edges", []):
                yield json.dumps({"type": "edge", "data": edge}, ensure_ascii=False) + "\n"
            
            # 완료 신호
            yield json.dumps({"type": "status", "data": "done"}) + "\n"
            return
        
        # 동적 모델 선택 (Agentic Designer 모드)
        selected_model_id = get_model_for_canvas_mode(
            canvas_mode="agentic-designer",
            current_workflow={"nodes": [], "edges": []},
            user_request=user_request
        )
        
        logger.info(f"Using model for Agentic Designer: {selected_model_id}")
        
        # 실제 Bedrock 스트리밍 호출
        system_prompt = SYSTEM_PROMPT
        enhanced_prompt = f"""사용자 요청: {user_request}

위 요청을 분석하여 완전한 워크플로우를 생성해주세요. 
각 노드와 엣지를 JSONL 형식으로 순차적으로 출력하고, 
마지막에 {{"type": "status", "data": "done"}}으로 완료를 알려주세요.

레이아웃 규칙을 준수하여 X=150, Y는 50부터 시작해서 100씩 증가시켜주세요."""
        
        # 선택된 모델로 Bedrock 스트리밍 호출
        for chunk in invoke_bedrock_model_stream(system_prompt, enhanced_prompt):
            yield chunk
            
    except Exception as e:
        logger.error(f"Initial workflow generation failed: {e}")
        # 에러를 비즈니스 친화적 메시지로 변환
        error_info = classify_and_format_error(e)
        error_response = {
            "type": "error",
            "error_code": error_info["error_code"],
            "data": error_info["message"],
            "user_action": error_info.get("user_action"),
            "retryable": error_info.get("retryable", False)
        }
        yield json.dumps(error_response, ensure_ascii=False) + "\n"


async def handle_codesign_stream(owner_id: str, event: Dict) -> Dict:
    """협업 워크플로우 설계 스트리밍 (Canvas 상태에 따른 자동 모드 전환)"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # 필수 필드 검증
    user_request = body.get('user_request')
    current_workflow = body.get('current_workflow', {"nodes": [], "edges": []})
    
    if not user_request:
        return _response(400, {'error': 'user_request is required'})
    
    try:
        # Canvas 상태 확인 - 비어있는지 판단 (개선된 로직)
        nodes = current_workflow.get('nodes', [])
        edges = current_workflow.get('edges', [])
        recent_changes = body.get('recent_changes', [])
        
        # UUID 기반 세션 ID 생성 (기존 time() 방식 대체)
        session_id = body.get('session_id') or _generate_session_id(owner_id)
        
        # 세션 컨텍스트 확인 (대화 기록이 있는지)
        has_conversation_history = len(recent_changes) > 0
        is_physically_empty = len(nodes) == 0 and len(edges) == 0
        
        # 정교한 모드 결정 로직
        if is_physically_empty and not has_conversation_history:
            # 진짜 빈 Canvas - Agentic Designer 모드
            canvas_mode = "agentic-designer"
            logger.info("Empty canvas with no history - using Agentic Designer mode")
        elif is_physically_empty and has_conversation_history:
            # Canvas는 비어있지만 대화 기록 존재 - Co-design 모드 유지
            canvas_mode = "co-design"
            logger.info("Empty canvas but has conversation history - maintaining Co-design mode")
        else:
            # 기존 워크플로우 존재 - Co-design 모드
            canvas_mode = "co-design"
            logger.info("Existing workflow detected - using Co-design mode")
        
        logger.info(f"Canvas analysis: nodes={len(nodes)}, edges={len(edges)}, "
                   f"changes={len(recent_changes)}, mode={canvas_mode}")
        
        if canvas_mode == "agentic-designer":
            # Canvas가 비어있고 대화 기록도 없음 - Agentic Designer 모드로 전환
            logger.info(f"Empty canvas detected - switching to Agentic Designer mode")
            generator = _generate_initial_workflow_stream(
                user_request=user_request,
                owner_id=owner_id,
                session_id=session_id
            )
        else:
            # Canvas에 내용이 있거나 대화 기록이 있음 - Co-design 모드 사용
            logger.info(f"Using Co-design mode (canvas_mode={canvas_mode})")
            
            # Co-design 모드용 모델 선택
            selected_model_id = get_model_for_canvas_mode(
                canvas_mode="co-design",
                current_workflow=current_workflow,
                user_request=user_request,
                recent_changes=recent_changes
            )
            logger.info(f"Using model for Co-design: {selected_model_id}")
            
            generator = stream_codesign_response(
                user_request=user_request,
                current_workflow=current_workflow,
                recent_changes=recent_changes,
                session_id=session_id,
                connection_ids=None  # WebSocket 연결 ID는 별도 처리
            )
        
        logger.info(f"Started workflow streaming for session {session_id[:8]}...")
        return _streaming_response(generator)
        
    except Exception as e:
        logger.error(f"Failed to start workflow streaming: {e}")
        return _error_response(e)


async def handle_explain_workflow(owner_id: str, event: Dict) -> Dict:
    """워크플로우 설명 생성"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # 필수 필드 검증
    workflow = body.get('workflow')
    if not workflow:
        return _response(400, {'error': 'workflow is required'})
    
    try:
        explanation = explain_workflow(workflow)
        
        logger.info(f"Generated workflow explanation")
        return _response(200, explanation)
        
    except Exception as e:
        logger.error(f"Failed to explain workflow: {e}")
        return _error_response(e)


async def handle_generate_suggestions(owner_id: str, event: Dict) -> Dict:
    """워크플로우 최적화 제안 생성"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # 필수 필드 검증
    workflow = body.get('workflow')
    if not workflow:
        return _response(400, {'error': 'workflow is required'})
    
    try:
        max_suggestions = body.get('max_suggestions', 5)
        suggestions = []
        
        # 제안 생성 (Generator를 리스트로 변환)
        for suggestion in generate_suggestions(workflow, max_suggestions):
            suggestions.append(suggestion)
        
        response = {
            'suggestions': suggestions,
            'count': len(suggestions)
        }
        
        logger.info(f"Generated {len(suggestions)} suggestions")
        return _response(200, response)
        
    except Exception as e:
        logger.error(f"Failed to generate suggestions: {e}")
        return _error_response(e)


async def handle_apply_suggestion(owner_id: str, event: Dict) -> Dict:
    """제안 적용"""
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # 필수 필드 검증
    workflow = body.get('workflow')
    suggestion = body.get('suggestion')
    
    if not workflow or not suggestion:
        return _response(400, {'error': 'workflow and suggestion are required'})
    
    try:
        modified_workflow = apply_suggestion(workflow, suggestion)
        
        response = {
            'modified_workflow': modified_workflow,
            'applied_suggestion': suggestion
        }
        
        logger.info(f"Applied suggestion {suggestion.get('id', 'unknown')}")
        return _response(200, response)
        
    except Exception as e:
        logger.error(f"Failed to apply suggestion: {e}")
        return _error_response(e)


# ============================================================================
# Lambda 엔트리포인트 (최적화된 이벤트 루프 관리)
# ============================================================================

# 루프 상태 추적 (폴백 실행 모니터링)
_fallback_execution_count = 0
_FALLBACK_WARNING_THRESHOLD = 3


def lambda_handler_sync(event, context):
    """
    동기 Lambda 핸들러 래퍼 (최적화된 이벤트 루프 관리).
    
    [v2.0 개선사항]
    - 전역 이벤트 루프 재사용으로 컨테이너 재활용 시 오버헤드 제거
    - 매 호출마다 new_event_loop() 생성/삭제 비용 절감
    - 고부하 상황에서 리소스 누수(Resource Exhaustion) 방지
    
    [v2.1 안전성 강화]
    - 폴백 실행 횟수 모니터링 및 경고
    - 루프 상태 사전 검증으로 폴백 방지
    """
    global _fallback_execution_count
    
    # 사전 검증: 실행 중인 루프가 있는지 먼저 확인
    try:
        running_loop = asyncio.get_running_loop()
        # 이미 실행 중인 루프가 있으면 폴백 경로로 진입할 것임을 사전 감지
        logger.warning(
            f"⚠️ Running loop detected before execution - "
            f"this should not happen in Lambda. Loop: {running_loop}"
        )
    except RuntimeError:
        # 정상 경로: 실행 중인 루프 없음
        pass
    
    loop = get_or_create_event_loop()
    
    try:
        return loop.run_until_complete(lambda_handler(event, context))
    except RuntimeError as e:
        # 이미 실행 중인 루프가 있는 경우 (중첩 호출) - 폴백 경로
        if "This event loop is already running" in str(e):
            _fallback_execution_count += 1
            
            # 폴백 실행 횟수가 임계값을 넘으면 강력 경고
            if _fallback_execution_count >= _FALLBACK_WARNING_THRESHOLD:
                logger.error(
                    f"🚨 CRITICAL: ThreadPoolExecutor fallback executed {_fallback_execution_count} times! "
                    f"This indicates an event loop lifecycle issue that MUST be fixed before production. "
                    f"Risk: Latency spikes, OOM errors under high load."
                )
            else:
                logger.warning(
                    f"⚠️ ThreadPoolExecutor fallback triggered (count: {_fallback_execution_count}). "
                    f"Event loop was unexpectedly running."
                )
            
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    asyncio.run, 
                    lambda_handler(event, context)
                )
                return future.result(timeout=300)  # 5분 타임아웃
        raise


# ============================================================================
# Lambda Function URL RESPONSE_STREAM 핸들러 (AWS 공식 규격)
# ============================================================================

async def lambda_handler_streaming(event, response_stream, context):
    """
    AWS Lambda Function URL RESPONSE_STREAM 모드용 핸들러.
    
    [v2.1 수정사항 - AWS 공식 스트리밍 규격 준수]
    핸들러가 response_stream 객체를 직접 인자로 수신합니다.
    이 객체에 .write()로 청크를 즉시 전송하고 .close()로 종료합니다.
    
    설정 요구사항:
    1. Lambda Function URL: InvokeMode: RESPONSE_STREAM
    2. 환경변수: LAMBDA_STREAMING_ENABLED=true
    3. template.yaml:
        CodesignFunction:
          Type: AWS::Serverless::Function
          Properties:
            Handler: src.handlers.core.codesign_api_handler.lambda_handler_streaming
            FunctionUrlConfig:
              AuthType: AWS_IAM
              InvokeMode: RESPONSE_STREAM
    
    Args:
        event: Lambda 이벤트 객체
        response_stream: AWS Lambda ResponseStream 객체 (직접 write/close)
        context: Lambda 컨텍스트 객체
    """
    try:
        # 스트리밍 모드 확인 (환경변수 기반)
        if not _is_streaming_invocation(event):
            # 스트리밍 모드가 아닐 경우 기존 동기 로직으로 브릿지
            logger.info("Non-streaming invocation detected, bridging to sync handler")
            result = await lambda_handler(event, context)
            response_stream.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))
            response_stream.close()
            return
        
        # ── 스트리밍 메타데이터 설정 ──
        # awslambdaric의 StreamingResponse는 첫 write 전에 메타데이터 설정 가능
        if hasattr(response_stream, 'set_content_type'):
            response_stream.set_content_type('application/x-ndjson')
        
        # ── 요청 파싱 및 검증 ──
        http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
        
        # CORS preflight
        if http_method == 'OPTIONS':
            response_stream.write(json.dumps({"type": "cors", "data": "ok"}).encode('utf-8') + b"\n")
            response_stream.close()
            return
        
        # 인증 확인
        owner_id = extract_owner_id_from_event(event)
        if not owner_id:
            error_chunk = json.dumps({
                "type": "error",
                "error_code": APIErrorCode.INVALID_REQUEST,
                "data": "Authentication required",
                "user_action": "Please log in and try again."
            }, ensure_ascii=False).encode('utf-8') + b"\n"
            response_stream.write(error_chunk)
            response_stream.close()
            return
        
        # 경로 확인
        path = event.get('path') or event.get('rawPath', '')
        
        # ── /codesign 엔드포인트 스트리밍 처리 ──
        if (path.endswith('/codesign') or path.endswith('/codesign/')) and http_method == 'POST':
            body, error = _parse_body(event)
            if error:
                error_chunk = json.dumps({
                    "type": "error",
                    "error_code": APIErrorCode.INVALID_REQUEST,
                    "data": error
                }, ensure_ascii=False).encode('utf-8') + b"\n"
                response_stream.write(error_chunk)
                response_stream.close()
                return
            
            user_request = body.get('user_request')
            if not user_request:
                error_chunk = json.dumps({
                    "type": "error",
                    "error_code": APIErrorCode.INVALID_REQUEST,
                    "data": "user_request is required"
                }, ensure_ascii=False).encode('utf-8') + b"\n"
                response_stream.write(error_chunk)
                response_stream.close()
                return
            
            current_workflow = body.get('current_workflow', {"nodes": [], "edges": []})
            recent_changes = body.get('recent_changes', [])
            session_id = body.get('session_id') or _generate_session_id(owner_id)
            
            nodes = current_workflow.get('nodes', [])
            edges = current_workflow.get('edges', [])
            is_physically_empty = len(nodes) == 0 and len(edges) == 0
            has_conversation_history = len(recent_changes) > 0
            
            logger.info(f"🚀 Streaming started: session={session_id[:12]}, "
                       f"nodes={len(nodes)}, empty={is_physically_empty}, history={has_conversation_history}")
            
            # 모드 결정 및 제너레이터 선택
            if is_physically_empty and not has_conversation_history:
                generator = _generate_initial_workflow_stream(
                    user_request=user_request,
                    owner_id=owner_id,
                    session_id=session_id
                )
            else:
                generator = stream_codesign_response(
                    user_request=user_request,
                    current_workflow=current_workflow,
                    recent_changes=recent_changes,
                    session_id=session_id,
                    connection_ids=None
                )
            
            # ── 진짜 스트리밍: 청크를 즉시 response_stream에 write ──
            chunk_count = 0
            try:
                for chunk in generator:
                    if chunk:
                        response_stream.write(chunk.encode('utf-8'))
                        # 강제 플러시로 즉시 전송 (TTFT 최적화)
                        if hasattr(response_stream, 'flush'):
                            response_stream.flush()
                        chunk_count += 1
                
                logger.info(f"✅ Streaming completed: {chunk_count} chunks sent")
                
            except Exception as stream_error:
                logger.error(f"❌ Stream error after {chunk_count} chunks: {stream_error}")
                error_info = classify_and_format_error(stream_error)
                error_chunk = json.dumps({
                    "type": "error",
                    "error_code": error_info["error_code"],
                    "data": error_info["message"],
                    "user_action": error_info.get("user_action"),
                    "chunks_sent_before_error": chunk_count
                }, ensure_ascii=False).encode('utf-8') + b"\n"
                response_stream.write(error_chunk)
        
        else:
            # 스트리밍 미지원 엔드포인트는 기존 핸들러로 위임
            result = await lambda_handler(event, context)
            response_stream.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))
        
    except Exception as e:
        logger.exception(f"❌ Streaming handler fatal error: {e}")
        error_info = classify_and_format_error(e)
        try:
            error_chunk = json.dumps({
                "type": "error",
                "error_code": error_info["error_code"],
                "data": error_info["message"],
                "user_action": error_info.get("user_action")
            }, ensure_ascii=False).encode('utf-8') + b"\n"
            response_stream.write(error_chunk)
        except:
            pass  # response_stream 자체가 망가진 경우 무시
    
    finally:
        # 항상 스트림 종료
        try:
            response_stream.close()
        except:
            pass


def lambda_handler_streaming_sync(event, response_stream, context):
    """
    동기 래퍼: Lambda 런타임이 async를 직접 지원하지 않을 경우 사용.
    
    대부분의 경우 lambda_handler_streaming을 직접 사용하면 되지만,
    특정 Lambda 런타임 버전에서는 이 동기 래퍼가 필요할 수 있습니다.
    """
    loop = get_or_create_event_loop()
    return loop.run_until_complete(
        lambda_handler_streaming(event, response_stream, context)
    )


# Lambda 엔트리포인트 (기본: 동기 모드)
lambda_handler = lambda_handler_sync
