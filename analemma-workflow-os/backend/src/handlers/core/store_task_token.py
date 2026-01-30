import json
import os
import time
import logging
import boto3
from botocore.exceptions import ClientError
import uuid

# 공통 모듈에서 AWS 클라이언트 가져오기
try:
    from src.common.aws_clients import get_dynamodb_resource, get_stepfunctions_client
    from src.common.constants import DynamoDBConfig
    dynamodb = get_dynamodb_resource()
    sfn_client = get_stepfunctions_client()
except ImportError:
    dynamodb = boto3.resource('dynamodb')
    sfn_client = boto3.client('stepfunctions')

# [v3.17] Kernel Protocol 사용 - 안전한 state bag 추출
try:
    from src.common.kernel_protocol import open_state_bag
    KERNEL_PROTOCOL_AVAILABLE = True
except ImportError:
    KERNEL_PROTOCOL_AVAILABLE = False
    def open_state_bag(event):
        """Fallback: 기본 추출 로직"""
        if not isinstance(event, dict):
            return {}
        state_data = event.get('state_data', {})
        if isinstance(state_data, dict):
            bag = state_data.get('bag')
            if isinstance(bag, dict):
                return bag
            return state_data
        return event

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 환경 변수로 테이블명 지정
TASK_TOKENS_TABLE = DynamoDBConfig.TASK_TOKENS_TABLE

table = dynamodb.Table(TASK_TOKENS_TABLE)

# 기본 TTL: 1 day (상수 사용)
try:
    from src.common.constants import TTLConfig, EnvironmentVariables
    DEFAULT_TTL_SECONDS = int(os.environ.get(EnvironmentVariables.TASK_TOKEN_TTL_SECONDS, TTLConfig.TASK_TOKEN_DEFAULT))
except ImportError:
    DEFAULT_TTL_SECONDS = int(os.environ.get('TASK_TOKEN_TTL_SECONDS', 86400))


def get_user_connection_ids(owner_id):
    """
    DynamoDB GSI를 쿼리하여 ownerId에 연결된 모든 connectionId를 반환합니다.
    
    Returns:
        list: connectionId 목록, 비어있을 수 있음
    """
    table_name = DynamoDBConfig.WEBSOCKET_CONNECTIONS_TABLE
    gsi_name = DynamoDBConfig.WEBSOCKET_OWNER_ID_GSI
    
    if not table_name or not owner_id:
        return []
    
    try:
        conn_table = dynamodb.Table(table_name)
        from boto3.dynamodb.conditions import Key
        
        response = conn_table.query(
            IndexName=gsi_name,
            KeyConditionExpression=Key('ownerId').eq(owner_id)
        )
        
        return [item['connectionId'] for item in response.get('Items', []) if 'connectionId' in item]
    except Exception as e:
        import logging
        logger = logging.getLogger()
        logger.error(f'Failed to query connections for owner={owner_id}: {e}')
        return []


def _send_hitp_notification(owner_id, conversation_id, execution_id, context_info, payload):
    """
    HITP 발생 시 사용자에게 알림을 전송합니다.
    
    3가지 알림 채널:
    1. 실시간 Push (WebSocket) - 사용자가 접속 중일 때
    2. 영구적 알림 저장 (DynamoDB PendingNotifications) - 알림 센터용
    3. 이메일/SMS (옵션) - 완전 오프라인 사용자용
    
    Args:
        owner_id: 사용자 ID (테넌트 격리)
        conversation_id: 대화/실행 ID
        execution_id: Step Functions 실행 ID
        context_info: 워크플로우 컨텍스트 정보
        payload: 전체 payload (segment_to_run 등 추가 정보 포함)
    
    Returns:
        dict: 전송 결과 {'websocket': bool, 'persistent': bool, 'email': bool}
    """
    import logging
    logger = logging.getLogger()
    
    result = {
        'websocket': False,
        'persistent': False,
        'email': False
    }
    
    # 알림 페이로드 구성
    notification_payload = {
        'type': 'hitp_pause',
        'action': 'hitp_pause',
        'conversation_id': conversation_id,
        'execution_id': execution_id,
        'workflowId': context_info.get('workflowId') or payload.get('workflowId'),
        'workflow_name': context_info.get('workflow_name'),
        'segment_to_run': context_info.get('segment_to_run') or payload.get('segment_to_run'),
        'total_segments': context_info.get('total_segments') or payload.get('total_segments'),
        'status': 'PAUSED_FOR_HITP',
        'message': '워크플로우가 일시정지되었습니다. 사용자 입력을 기다립니다.',
        'timestamp': int(time.time()),
        'created_at': int(time.time())
    }
    
    # --- 1. 실시간 Push (WebSocket) ---
    try:
        connection_ids = get_user_connection_ids(owner_id)
        if connection_ids:
            websocket_endpoint = os.environ.get('WEBSOCKET_ENDPOINT_URL')
            if websocket_endpoint:
                apigw = boto3.client('apigatewaymanagementapi', endpoint_url=websocket_endpoint)
                payload_bytes = json.dumps({
                    'type': 'workflow_status',
                    'payload': notification_payload
                }, ensure_ascii=False).encode('utf-8')
                
                for conn_id in connection_ids:
                    try:
                        apigw.post_to_connection(ConnectionId=conn_id, Data=payload_bytes)
                        logger.info(f"📱 WebSocket HITP 알림 전송: owner={owner_id}, conn={conn_id}")
                        result['websocket'] = True
                    except apigw.exceptions.GoneException:
                        logger.info(f"Stale connection {conn_id}, will be cleaned up later")
                    except Exception as e:
                        logger.warning(f"Failed to send WebSocket to {conn_id}: {e}")
            else:
                logger.debug("WEBSOCKET_ENDPOINT_URL not configured, skipping WebSocket push")
    except Exception as e:
        logger.warning(f"WebSocket notification failed: {e}")
    
    # --- 2. 영구적 알림 저장 (Persistent Notification for Inbox) ---
    try:
        pending_table_name = os.environ.get('PENDING_NOTIFICATIONS_TABLE')
        if pending_table_name:
            pending_table = dynamodb.Table(pending_table_name)
            # 🚨 [Fix] timestamp를 밀리초(ms)로 저장하여 list_notifications.py와 일치시킴
            current_time = int(time.time())
            notification_item = {
                'ownerId': owner_id,
                'notificationId': str(uuid.uuid4()),
                'timestamp': current_time * 1000,  # 밀리초 단위 (프론트엔드 호환)
                'timestamp_seconds': current_time,  # 초 단위 (백엔드 호환)
                'notification': notification_payload,
                'status': 'pending',  # pending = 미읽음, sent = 읽음
                'type': 'hitp_pause',
                'conversation_id': conversation_id,
                'execution_id': execution_id,
                'ttl': current_time + TTLConfig.PENDING_NOTIFICATION  # 30일 보관
            }
            pending_table.put_item(Item=notification_item)
            logger.info(f"💾 영구 알림 저장 완료: owner={owner_id}, notification_id={notification_item['notificationId']}")
            result['persistent'] = True
        else:
            logger.debug("PENDING_NOTIFICATIONS_TABLE not configured, skipping persistent notification")
    except Exception as e:
        logger.warning(f"Persistent notification storage failed: {e}")
    
    # --- 3. 이메일/SMS 알림 (옵션, 환경변수로 제어) ---
    # 구현 예정: backend/backend_issues.md 참조
    if os.environ.get('ENABLE_EMAIL_NOTIFICATIONS', 'false').lower() == 'true':
        try:
            # [ISSUE] SES/SNS 이메일 알림 미구현 - backend_issues.md 참조
            logger.info(f"📧 이메일 알림은 향후 구현 예정: owner={owner_id}")
            result['email'] = False
        except Exception as e:
            logger.warning(f"Email notification failed: {e}")
    
    return result


from decimal import Decimal

def _convert_floats_to_decimals(obj):
    """Recursively convert float values to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_floats_to_decimals(v) for v in obj]
    return obj


def _mock_auto_resume(task_token: str, bag: dict, max_retries: int = 3) -> dict:
    """
    MOCK_MODE/AUTO_RESUME_HITP에서 HITP 자동 승인 - 테스트 시뮬레이터용.
    
    Step Functions의 waitForTaskToken 상태가 준비되기 전에
    send_task_success를 호출하면 InvalidToken 에러가 발생할 수 있으므로
    재시도 로직을 포함합니다.
    
    [v3.22] MergeCallbackResult가 기대하는 스키마로 수정:
    - user_response: HITL 응답 데이터
    - segment_to_run: None이면 다음 세그먼트로 진행 (_increment_segment=True)
    
    Args:
        task_token: Step Functions TaskToken
        bag: StateBag (open_state_bag으로 추출된 상태)
        max_retries: 최대 재시도 횟수 (레이스 컨디션 대응)
    
    Returns:
        dict: 자동 resume 결과
    """
    import logging
    logger = logging.getLogger()
    
    # [v3.22] MergeCallbackResult가 기대하는 스키마로 resume_output 구성
    # universal_sync_core.merge_callback 참조:
    #   - user_response -> delta['last_hitp_response']
    #   - segment_to_run = None -> _increment_segment = True (다음 세그먼트 진행)
    resume_output = {
        "status": "APPROVED",
        "user_response": {
            "action": "APPROVED",
            "comment": "Auto-approved by Simulator (AUTO_RESUME_HITP)",
            "approved_at": int(time.time()),
            "auto_resume": True
        },
        # segment_to_run을 None으로 설정하면 _increment_segment=True로 처리됨
        "segment_to_run": None
    }
    
    for attempt in range(max_retries):
        try:
            # 레이스 컨디션 방지: Step Functions가 대기 상태 진입할 시간 확보
            if attempt > 0:
                wait_time = 1.0 * attempt  # 1초, 2초 점진적 대기
                logger.info(f"⏳ Retry {attempt + 1}/{max_retries}: waiting {wait_time}s before send_task_success")
                time.sleep(wait_time)
            
            sfn_client.send_task_success(
                taskToken=task_token,
                output=json.dumps(resume_output, ensure_ascii=False, default=str)
            )
            
            logger.info(f"[v3.22] Auto-resumed HITP task successfully (attempt {attempt + 1})")
            return {
                "status": "AUTO_RESUMED",
                "attempt": attempt + 1,
                "user_response": resume_output.get("user_response")
            }
            
        except sfn_client.exceptions.InvalidToken as e:
            # 토큰이 아직 유효하지 않음 - Step Functions가 대기 상태 진입 전
            logger.warning(f"InvalidToken on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"[v3.22] Failed to auto-resume after {max_retries} attempts")
                return {
                    "status": "AUTO_RESUME_FAILED",
                    "error": "InvalidToken after max retries",
                    "attempts": max_retries
                }
        except sfn_client.exceptions.TaskTimedOut as e:
            # 이미 타임아웃됨
            logger.warning(f"Task already timed out: {e}")
            return {
                "status": "AUTO_RESUME_SKIPPED",
                "reason": "Task already timed out"
            }
        except Exception as e:
            logger.error(f"[v3.22] Unexpected error in auto-resume: {e}")
            return {
                "status": "AUTO_RESUME_FAILED",
                "error": str(e)
            }
    
    return {"status": "AUTO_RESUME_FAILED", "error": "Unknown error"}

def lambda_handler(event, context):
    """
    Step Functions WaitForCallback에서 호출되는 TaskToken 저장 Lambda.
    
    기능:
    - TaskToken을 DynamoDB에 저장
    - Step Functions 재시작을 위한 컨텍스트 보존
    - resume_handler에서 사용할 수 있도록 execution context 저장
    
    중요: Step Functions 내부 호출이므로 HTTP 응답이 아닌 순수 JSON 반환
    """
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    try:
        # event may already be a dict or a JSON string inside 'Payload' when invoked by Step Functions
        payload = event
        if isinstance(event, dict) and 'Payload' in event:
            # When invoked through Lambda Invoke, Step Functions wraps the original payload under 'Payload'
            payload = event['Payload']
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    payload = {}

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, ValueError):
                payload = {}

        try:
            # ========================================
            # [v3.17] Kernel Protocol로 안전한 State Bag 추출
            # ========================================
            # open_state_bag은 어떤 구조로 오든 실제 데이터를 추출:
            # - state_data.bag (v3.13 표준)
            # - state_data (평탄화)
            # - event 자체 (legacy)
            bag = open_state_bag(payload)
            
            logger.info(f"[v3.17] Kernel Protocol: bag keys={list(bag.keys())[:10]}")
            
            # [v3.18] current_state에서도 필드 추출 (깊은 탐색)
            current_state = bag.get('current_state', {})
            if not isinstance(current_state, dict):
                current_state = {}
            
            # TaskToken은 payload 최상위에서만 올 수 있음 (SFN이 직접 주입)
            task_token = payload.get('TaskToken') or payload.get('taskToken')
            
            # [v3.19] ASL v3에서는 execution_id를 사용 (conversation_id는 legacy)
            # execution_id가 있으면 그것을 conversation_id로도 사용
            execution_id = (
                payload.get('execution_id') or 
                payload.get('executionId') or 
                bag.get('execution_id') or
                bag.get('executionId') or
                current_state.get('execution_id')
            )
            
            # conversation_id 탐색 (execution_id fallback 포함)
            conversation_id = (
                payload.get('conversation_id') or 
                payload.get('conversationId') or
                bag.get('conversation_id') or
                bag.get('conversationId') or
                current_state.get('conversation_id') or
                current_state.get('conversationId') or
                execution_id  # v3에서는 execution_id를 conversation_id로 사용
            )
            
            # execution_id가 없으면 conversation_id로 설정
            if not execution_id:
                execution_id = conversation_id
            
            owner_id = (
                payload.get('ownerId') or 
                payload.get('owner_id') or
                bag.get('ownerId') or
                bag.get('owner_id') or
                current_state.get('ownerId') or
                current_state.get('owner_id')
            )
            
            # 필수 값 검증
            if not task_token or not conversation_id or not owner_id:
                logger.error(f"Missing required fields: task_token={bool(task_token)}, "
                           f"conversation_id={bool(conversation_id)}, owner_id={bool(owner_id)}, "
                           f"execution_id={bool(execution_id)}, bag_keys={list(bag.keys())[:15]}")
                raise ValueError('Missing TaskToken, conversation_id or ownerId')

            now = int(time.time())
            ttl = now + DEFAULT_TTL_SECONDS

            # Resume Handler가 사용할 수 있도록 추가 컨텍스트 정보 저장
            # Store the full workflow_config and other state so resume can reconstruct
            # the execution context reliably. Previously only 'workflow_name' was
            # stored which caused resume to miss required fields like workflow_config.
            # [v3.17] bag 사용 (Kernel Protocol)
            # Build context_info with None safety checks
            workflow_config_source = payload.get('workflow_config') or bag.get('workflow_config') or {}
            context_info = {
                'workflow_config': workflow_config_source if isinstance(workflow_config_source, dict) else None,
                'workflowId': payload.get('workflowId') or bag.get('workflowId'),
                'workflow_name': workflow_config_source.get('name') if isinstance(workflow_config_source, dict) else None,
                'segment_to_run': payload.get('segment_to_run') if payload.get('segment_to_run') is not None else bag.get('segment_to_run'),
                'total_segments': payload.get('total_segments') or bag.get('total_segments'),
                'partition_map': payload.get('partition_map') or bag.get('partition_map'),
                'current_state': payload.get('current_state') or bag.get('current_state'),
                # preserve state history if present so resume can reconstruct past snapshots
                'state_history': payload.get('state_history') or bag.get('state_history'),
                'state_s3_path': payload.get('state_s3_path') or bag.get('state_s3_path'),
                'idempotency_key': payload.get('idempotency_key') or bag.get('idempotency_key'),
                'execution_name': payload.get('execution_name'),
                # 🚨 [Critical Fix] 추가 필드 보존
                'ownerId': owner_id,
                'max_concurrency': payload.get('max_concurrency') or bag.get('max_concurrency'),
                'distributed_mode': payload.get('distributed_mode') or bag.get('distributed_mode'),
            }
            if context and hasattr(context, 'aws_request_id'):
                context_info['request_id'] = context.aws_request_id

            item = {
                # Scope the token to ownerId (tenant) as partition key and conversation_id as sort key
                'ownerId': owner_id,
                'conversation_id': conversation_id,
                # execution_id is unique per execution/run and should be returned to callers so
                # they can reference the execution directly when resuming.
                'execution_id': execution_id,
                'taskToken': task_token,
                'createdAt': now,
                'ttl': ttl,
                # Resume Handler가 사용할 추가 컨텍스트 저장
                'context': context_info
            }

            # [Fix] Convert floats to Decimals for DynamoDB
            item = _convert_floats_to_decimals(item)

            # Idempotent write: only create the item if conversation_id doesn't already exist.
            # If this Lambda is retried by Step Functions the conditional put will fail
            # with ConditionalCheckFailedException and we treat that as success (already stored).
            try:
                table.put_item(Item=item, ConditionExpression='attribute_not_exists(conversation_id)')
                logger.info(f"TaskToken stored successfully: conversation_id={conversation_id}, owner_id={owner_id}")
            except ClientError as e:
                # [Fix] None defense: e.response['Error']가 None일 수 있음
                code = (e.response.get('Error') or {}).get('Code')
                if code == 'ConditionalCheckFailedException':
                    # Already stored by a previous attempt - this is OK (idempotent)
                    logger.info(f"TaskToken already stored (retry detected): conversation_id={conversation_id}")
                else:
                    logger.exception(f"DynamoDB error storing TaskToken: {e}")
                    raise

            # --- 🆕 HITP 알림 전송 (실시간 Push + 영구 알림) ---
            notification_sent = _send_hitp_notification(
                owner_id=owner_id,
                conversation_id=conversation_id,
                execution_id=execution_id,
                context_info=context_info,
                payload=payload
            )
            logger.info(f"HITP notification sent: {notification_sent}")
            
            # --- 🆕 자동 Resume 로직 (MOCK_MODE 또는 AUTO_RESUME_HITP) ---
            # 1. MOCK_MODE=true: 즉시 자동 승인 (E2E 테스트용)
            # 2. AUTO_RESUME_HITP=true: 타임아웃 후 자동 승인 (시뮬레이터용)
            # [v3.21] AUTO_RESUME_HITP 플래그 추가 - LLM Simulator 무한대기 해결
            
            mock_mode_value = (
                payload.get('MOCK_MODE') or 
                bag.get('MOCK_MODE') or 
                os.environ.get('MOCK_MODE', 'false')
            )
            mock_mode = str(mock_mode_value).lower() == 'true'
            
            auto_resume_value = (
                payload.get('AUTO_RESUME_HITP') or 
                bag.get('AUTO_RESUME_HITP') or 
                os.environ.get('AUTO_RESUME_HITP', 'false')
            )
            auto_resume_hitp = str(auto_resume_value).lower() == 'true'
            
            # 자동 resume 타임아웃 (초) - 기본 5초, 환경변수로 조정 가능
            auto_resume_delay = int(
                payload.get('AUTO_RESUME_DELAY_SECONDS') or
                bag.get('AUTO_RESUME_DELAY_SECONDS') or
                os.environ.get('AUTO_RESUME_DELAY_SECONDS', '5')
            )
            
            mock_resume_result = None
            
            logger.info(f"🔍 Auto-resume check: MOCK_MODE={mock_mode}, AUTO_RESUME_HITP={auto_resume_hitp}, delay={auto_resume_delay}s")
            
            if mock_mode or auto_resume_hitp:
                if auto_resume_hitp and not mock_mode and auto_resume_delay > 0:
                    # 타임아웃 기반 자동 resume: Step Functions가 대기 상태 진입할 시간 확보
                    logger.info(f"⏳ AUTO_RESUME_HITP: waiting {auto_resume_delay}s before auto-resume")
                    time.sleep(auto_resume_delay)
                
                resume_mode = "MOCK_MODE" if mock_mode else "AUTO_RESUME_HITP"
                logger.info(f"🤖 {resume_mode} detected - initiating auto-resume for HITP")
                mock_resume_result = _mock_auto_resume(
                    task_token=task_token,
                    bag=bag,  # [v3.22] payload -> bag (StateBag)
                    max_retries=3
                )
                logger.info(f"🤖 Auto-resume result: {mock_resume_result}")

            # Step Functions expects pure JSON output (not HTTP response format)
            # Return the payload with additional context for next states
            # Important: Include state_data object for PrepareStateAfterPause to reference
            return {
                'message': 'TaskToken stored',
                'conversation_id': conversation_id,
                'ownerId': owner_id,
                'execution_id': execution_id,
                'taskToken': task_token,
                'stored_at': now,
                # MOCK_MODE 자동 resume 결과 포함 (디버깅용)
                'mock_resume_result': mock_resume_result,
                # Pass through important fields for next Step Functions states
                'workflowId': payload.get('workflowId'),
                'segment_to_run': payload.get('segment_to_run'),
                'workflow_config': payload.get('workflow_config'),
                'partition_map': payload.get('partition_map'),
                'total_segments': payload.get('total_segments'),
                'current_state': payload.get('current_state'),
                'state_s3_path': payload.get('state_s3_path'),
                # State bag pattern: wrap state data for consistent access pattern
                'state_data': {
                    'workflow_config': payload.get('workflow_config'),
                    'partition_map': payload.get('partition_map'),
                    'total_segments': payload.get('total_segments'),
                    'state_history': payload.get('state_history') or (payload.get('state_data') or {}).get('state_history'),
                    'ownerId': owner_id,
                    'workflowId': payload.get('workflowId'),
                    'segment_to_run': payload.get('segment_to_run'),
                    'current_state': payload.get('current_state'),
                    'state_s3_path': payload.get('state_s3_path'),
                    'idempotency_key': payload.get('idempotency_key')
                }
            }
        except Exception as e:
            logger.exception(f"Error in store_task_token: {e}")
            raise
    except Exception as e:
        logger.exception(f"Outer exception in store_task_token: {e}")
        raise
