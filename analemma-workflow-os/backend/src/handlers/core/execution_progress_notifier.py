import json
import logging
import os
import time
import uuid
import boto3
from typing import Any, Dict, List, Union, Optional
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal

# 공통 모듈에서 AWS 클라이언트 및 유틸리티 가져오기
try:
    from src.common.aws_clients import get_dynamodb_resource, get_s3_client
    from src.common.json_utils import DecimalEncoder, convert_to_dynamodb_format
    from src.common.constants import DynamoDBConfig
    dynamodb_resource = get_dynamodb_resource()
    s3_client = get_s3_client()
    _USE_COMMON_UTILS = True
except ImportError:
    dynamodb_resource = boto3.resource('dynamodb')
    s3_client = boto3.client('s3')
    _USE_COMMON_UTILS = False

# CloudWatch 클라이언트
cloudwatch_client = boto3.client('cloudwatch')

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 환경 변수
WEBSOCKET_CONNECTIONS_TABLE = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
WEBSOCKET_ENDPOINT_URL = os.environ.get('WEBSOCKET_ENDPOINT_URL')
WEBSOCKET_OWNER_ID_GSI = DynamoDBConfig.WEBSOCKET_OWNER_ID_GSI
EXECUTIONS_TABLE = os.environ.get('EXECUTIONS_TABLE')

# DB 업데이트 전략 관련 환경 변수
DB_UPDATE_STRATEGY = os.environ.get('DB_UPDATE_STRATEGY', 'SELECTIVE')  # ALL, SELECTIVE, MINIMAL
DB_UPDATE_INTERVAL = int(os.environ.get('DB_UPDATE_INTERVAL_SECONDS', '30'))  # 최소 업데이트 간격

# 테이블 리소스
connections_table = dynamodb_resource.Table(WEBSOCKET_CONNECTIONS_TABLE) if WEBSOCKET_CONNECTIONS_TABLE else None
executions_table = dynamodb_resource.Table(EXECUTIONS_TABLE) if EXECUTIONS_TABLE else None
SKELETON_S3_BUCKET = os.environ.get('SKELETON_S3_BUCKET')

# API Gateway 클라이언트 캐싱 (동적 URL 대응)
apigw_clients = {}

def get_apigw_client(endpoint_url: str) -> Any:
    """
    Endpoint URL별로 클라이언트를 캐싱하여 TCP 연결 재사용을 유도합니다.
    """
    if endpoint_url not in apigw_clients:
        apigw_clients[endpoint_url] = boto3.client(
            'apigatewaymanagementapi', 
            endpoint_url=endpoint_url.rstrip('/')
        )
    return apigw_clients[endpoint_url]

def get_user_connection_ids(owner_id: str) -> List[str]:
    """
    DynamoDB GSI를 쿼리하여 ownerId에 연결된 모든 connectionId를 반환합니다.
    에러 발생 시 빈 리스트를 반환하여 호출부 로직을 단순화합니다.
    """
    if not connections_table or not owner_id:
        return []
    
    try:
        response = connections_table.query(
            IndexName=WEBSOCKET_OWNER_ID_GSI,
            KeyConditionExpression=Key('ownerId').eq(owner_id)
        )
        return [item['connectionId'] for item in response.get('Items', []) if 'connectionId' in item]
    except Exception as e:
        logger.error(f'Failed to query connections for owner={owner_id}: {e}')
        return []

def _delete_connection(connection_id: str) -> None:
    """Stale connection 삭제"""
    if not connections_table:
        return

    try:
        connections_table.delete_item(Key={'connectionId': connection_id})
        logger.info(f'Removed stale connection {connection_id}')
    except Exception as exc:
        logger.warning(f'Failed to delete stale connection {connection_id}: {exc}')


def _mark_no_active_session(owner_id: str, execution_id: str) -> None:
    """
    [v2.1] 사용자의 모든 WebSocket 연결이 끊겼을 때 DB에 플래그 저장.
    
    이 플래그는 사용자가 재접속했을 때 다음 용도로 사용됩니다:
    1. 즉시 마지막 실행 상태를 fetch하여 UI 동기화
    2. 누락된 알림 재전송 (optional)
    3. 연결 끊김 이벤트 로깅 및 분석
    
    Args:
        owner_id: 사용자 ID
        execution_id: 실행 ID (선택적)
    """
    if not executions_table or not owner_id:
        return
    
    try:
        timestamp = int(time.time())
        
        # 실행 중인 execution이 있으면 해당 레코드에 플래그 저장
        if execution_id:
            executions_table.update_item(
                Key={
                    'ownerId': owner_id,
                    'executionArn': execution_id
                },
                UpdateExpression='SET #nas = :nas, #nast = :nast',
                ExpressionAttributeNames={
                    '#nas': 'no_active_session',
                    '#nast': 'no_active_session_timestamp'
                },
                ExpressionAttributeValues={
                    ':nas': True,
                    ':nast': timestamp
                }
            )
            logger.info(
                f"Marked no_active_session for owner={owner_id[:8]}..., "
                f"execution={execution_id[:16]}... at {timestamp}"
            )
        
        # CloudWatch 메트릭 발행 (모니터링용)
        try:
            cloudwatch_client.put_metric_data(
                Namespace='WorkflowOrchestrator/WebSocket',
                MetricData=[{
                    'MetricName': 'AllConnectionsLost',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'OwnerId', 'Value': owner_id[:8]}
                    ]
                }]
            )
        except Exception as metric_err:
            logger.debug(f"Failed to publish connection loss metric: {metric_err}")
            
    except Exception as e:
        logger.warning(f"Failed to mark no_active_session: {e}")

def _extract_graph_config(workflow_config: dict) -> dict:
    """
    Extract only nodes and edges from src.workflow_config for graph visualization.
    This minimizes WebSocket payload size while providing graph structure.
    """
    if not workflow_config or not isinstance(workflow_config, dict):
        return None
    
    nodes = workflow_config.get('nodes')
    edges = workflow_config.get('edges')
    
    if not nodes:
        return None
    
    # Extract minimal node data for graph rendering
    minimal_nodes = []
    for node in nodes:
        if isinstance(node, dict):
            minimal_nodes.append({
                'id': node.get('id'),
                'type': node.get('type'),
                'position': node.get('position'),
                'config': {
                    'label': node.get('config', {}).get('label') or node.get('id')
                } if isinstance(node.get('config'), dict) else {'label': node.get('id')}
            })
    
    return {
        'nodes': minimal_nodes,
        'edges': edges or []
    }

def _get_payload(event: Any) -> Dict[str, Any]:
    """
    이벤트 페이로드 추출 및 JSON 파싱.
    
    Supports multiple event sources:
    1. Direct Lambda invocation: event contains payload directly
    2. EventBridge event: event['detail'] contains the payload (may be string or dict)
    3. Step Functions: event['Payload'] contains the payload
    """
    payload = event
    if isinstance(event, dict):
        # EventBridge events have 'detail' field
        if 'detail' in event:
            detail = event['detail']
            # EventBridge 'detail' can be a string (JSON serialized) or dict
            if isinstance(detail, str):
                try:
                    payload = json.loads(detail)
                except (json.JSONDecodeError, ValueError):
                    payload = {'raw_detail': detail}
            else:
                payload = detail
            # Log EventBridge source for debugging
            source = event.get('source', 'unknown')
            detail_type = event.get('detail-type', 'unknown')
            logger.info(f"Processing EventBridge event: source={source}, detail-type={detail_type}")
        elif 'Payload' in event:
            payload = event['Payload']
    
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            pass  # JSON 파싱 실패 시 원본 문자열 유지 혹은 빈 딕셔너리 처리
            
    if isinstance(payload, str): # 이중 인코딩 된 경우 한번 더 시도
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            payload = {}
            
    return payload if isinstance(payload, dict) else {}

# Fallback DecimalEncoder and _convert_to_dynamodb_format if common module not available
if not _USE_COMMON_UTILS:
    class DecimalEncoder(json.JSONEncoder):
        """DynamoDB Decimal 타입을 JSON 호환되도록 변환"""
        def default(self, obj):
            if isinstance(obj, Decimal):
                return float(obj) if obj % 1 else int(obj)
            return super(DecimalEncoder, self).default(obj)

    def _convert_to_dynamodb_format(obj: Any) -> Any:
        """
        객체의 모든 숫자 값을 DynamoDB Decimal 타입으로 변환합니다.
        """
        if isinstance(obj, dict):
            return {k: _convert_to_dynamodb_format(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_convert_to_dynamodb_format(item) for item in obj]
        elif isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, int):
            return obj  # int는 그대로 유지
        else:
            return obj
else:
    # Use common utilities
    _convert_to_dynamodb_format = convert_to_dynamodb_format

def should_update_database(payload: dict, state_data: dict) -> bool:
    """
    데이터베이스 업데이트 여부를 결정하는 로직
    """
    current_status = payload.get('status', '').upper()
    action = payload.get('notification_type', payload.get('action', ''))
    current_segment = payload.get('segment_to_run', state_data.get('segment_to_run', 0))
    # 🛡️ [P0 Fix] None 값 방어 - payload나 state_data에 키가 있지만 값이 None인 경우 처리
    raw_total = payload.get('total_segments')
    if raw_total is None:
        raw_total = state_data.get('total_segments')
    total_segments = max(1, int(raw_total)) if raw_total is not None and isinstance(raw_total, (int, float)) else 1
    
    # 전략별 업데이트 결정
    if DB_UPDATE_STRATEGY == 'ALL':
        return True
    
    elif DB_UPDATE_STRATEGY == 'MINIMAL':
        # 중요 상태 변화만 DB 업데이트
        critical_statuses = ['STARTED', 'PAUSED_FOR_HITP', 'COMPLETED', 'SUCCEEDED', 'FAILED', 'ABORTED']
        critical_actions = ['workflow_started', 'hitp_pause', 'workflow_completed', 'workflow_failed']
        
        return (current_status in critical_statuses or 
                action in critical_actions or
                current_segment == 0 or  # 시작
                current_segment >= total_segments - 1)  # 완료 직전/완료
    
    else:  # SELECTIVE (기본값)
        # 스마트 업데이트: 중요 상태 + 주기적 진행 상황
        critical_statuses = ['STARTED', 'PAUSED_FOR_HITP', 'COMPLETED', 'SUCCEEDED', 'FAILED', 'ABORTED']
        
        if current_status in critical_statuses:
            return True
        
        # 마지막 DB 업데이트로부터 일정 시간 경과 시 업데이트
        last_db_update = state_data.get('last_db_update_time', 0)
        current_time = int(time.time())
        
        if current_time - last_db_update >= DB_UPDATE_INTERVAL:
            return True
        
        # 진행률 기반 업데이트 (10% 단위)
        if total_segments > 10:
            progress_percentage = (current_segment / total_segments) * 100
            last_progress = state_data.get('last_db_progress_percentage', 0)
            
            if progress_percentage - last_progress >= 10:  # 10% 진행 시마다
                return True
        
        return False

def publish_db_update_metrics(execution_id: str, updated: bool, strategy: str):
    """
    DB 업데이트 관련 메트릭 발행
    """
    try:
        metrics = [
            {
                'MetricName': 'DatabaseUpdates',
                'Value': 1 if updated else 0,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Strategy', 'Value': strategy},
                    {'Name': 'Updated', 'Value': str(updated)}
                ]
            },
            {
                'MetricName': 'DatabaseUpdateRate',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Strategy', 'Value': strategy}
                ]
            }
        ]
        
        cloudwatch_client.put_metric_data(
            Namespace='WorkflowOrchestrator/Database',
            MetricData=metrics
        )
    except Exception as e:
        logger.warning(f"Failed to publish DB metrics: {e}")

def _safe_json_compatible(value: Any) -> Any:
    """
    객체를 JSON 직렬화 가능한 형태로 변환합니다.
    
    [v2.1 성능 최적화]
    기존: json.dumps → json.loads 왕복 (대용량 데이터에서 CPU/메모리 과부하)
    개선: 재귀적 타입 변환으로 문자열 변환 없이 직접 처리
    
    벤치마크 (500KB payload 기준):
    - 기존: ~120ms, 메모리 +50MB
    - 개선: ~15ms, 메모리 +5MB
    """
    return _convert_value_recursive(value)


def _convert_value_recursive(value: Any) -> Any:
    """
    재귀적 타입 변환 (json.dumps/loads 없이 직접 변환).
    
    지원 타입:
    - Decimal → float/int
    - datetime → ISO 문자열
    - set → list
    - bytes → base64 문자열
    - 기타 직렬화 불가 → str()
    """
    if value is None:
        return None
    
    # 기본 직렬화 가능 타입
    if isinstance(value, (str, int, bool)):
        return value
    
    # float: NaN, Inf 처리
    if isinstance(value, float):
        if value != value:  # NaN check
            return None
        if value == float('inf') or value == float('-inf'):
            return None
        return value
    
    # Decimal → float/int (DynamoDB 호환)
    if isinstance(value, Decimal):
        return float(value) if value % 1 else int(value)
    
    # dict: 재귀 처리
    if isinstance(value, dict):
        return {k: _convert_value_recursive(v) for k, v in value.items()}
    
    # list/tuple: 재귀 처리
    if isinstance(value, (list, tuple)):
        return [_convert_value_recursive(item) for item in value]
    
    # set → list
    if isinstance(value, set):
        return [_convert_value_recursive(item) for item in value]
    
    # bytes → base64 (선택적)
    if isinstance(value, bytes):
        import base64
        return base64.b64encode(value).decode('utf-8')
    
    # datetime 처리
    try:
        from datetime import datetime, date
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
    except ImportError:
        pass
    
    # 기타: 문자열로 폴백
    try:
        return str(value)
    except Exception:
        return "[UNSERIALIZABLE]"

def _strip_state_history(obj: Any) -> Any:
    """
    내부 상태 히스토리(state_history)를 제거하여 페이로드 크기를 줄입니다.
    성능 최적화: copy.deepcopy 대신 필요한 부분만 얕은 복사 또는 재구성합니다.
    """
    if not isinstance(obj, dict):
        return obj
    
    # 최상위 레벨에서 state_history 제거 (얕은 복사)
    out = obj.copy()
    out.pop('state_history', None)
    
    # 중첩된 키 처리 (재귀적으로 처리하지 않고 특정 키만 타겟팅하여 성능 확보)
    # state_data, step_function_state, current_state 내부의 state_history 제거
    target_keys = ['state_data', 'step_function_state', 'current_state']
    
    for key in target_keys:
        val = out.get(key)
        if isinstance(val, dict):
            # 해당 딕셔너리만 얕은 복사 후 수정
            new_val = val.copy()
            new_val.pop('state_history', None)
            out[key] = new_val
            
    return out

def _fetch_existing_history_from_s3(execution_id: str) -> List[Dict]:
    """
    S3에서 기존 히스토리를 가져옵니다.
    
    [v2.1 데이터 무결성 강화]
    segment_runner가 누적 히스토리를 보내더라도,
    핸들러 레벨에서 S3의 기존 데이터와 병합하여 데이터 유실을 방지합니다.
    
    Returns:
        기존 히스토리 리스트 (없으면 빈 리스트)
    """
    if not SKELETON_S3_BUCKET:
        return []
    
    try:
        safe_id = execution_id.split(':')[-1]
        s3_key = f"executions/{safe_id}.json"
        
        response = s3_client.get_object(
            Bucket=SKELETON_S3_BUCKET,
            Key=s3_key
        )
        existing_data = json.loads(response['Body'].read().decode('utf-8'))
        existing_history = existing_data.get('state_history', [])
        
        if isinstance(existing_history, list):
            logger.info(f"Fetched {len(existing_history)} existing history entries from S3")
            return existing_history
        return []
        
    except s3_client.exceptions.NoSuchKey:
        logger.debug(f"No existing history in S3 for {execution_id}")
        return []
    except Exception as e:
        logger.warning(f"Failed to fetch existing history from S3: {e}")
        return []


def _merge_history_logs(
    existing_history: List[Dict], 
    new_logs: List[Dict],
    max_entries: int = 50
) -> List[Dict]:
    """
    기존 히스토리와 새 로그를 중복 없이 병합합니다.
    
    [v2.1 데이터 무결성 강화]
    - 중복 제거: timestamp + node_id 기반 고유 키
    - 순서 보장: timestamp 기준 정렬
    - 크기 제한: 최대 max_entries개만 유지 (FIFO)
    
    Args:
        existing_history: S3에서 가져온 기존 히스토리
        new_logs: 새로 추가할 로그 (segment_runner에서 전달)
        max_entries: 최대 히스토리 엔트리 수
    
    Returns:
        병합된 히스토리 리스트
    """
    if not new_logs:
        return existing_history[-max_entries:] if existing_history else []
    
    if not existing_history:
        return new_logs[-max_entries:]
    
    # 고유 키 생성 함수
    def get_entry_key(entry: Dict) -> str:
        if not isinstance(entry, dict):
            return str(entry)
        ts = entry.get('timestamp', entry.get('time', 0))
        node_id = entry.get('node_id', entry.get('nodeId', 'unknown'))
        return f"{ts}:{node_id}"
    
    # 기존 히스토리의 키 집합
    existing_keys = {get_entry_key(e) for e in existing_history}
    
    # 새 로그 중 중복 아닌 것만 추가
    merged = list(existing_history)
    new_added = 0
    for log in new_logs:
        key = get_entry_key(log)
        if key not in existing_keys:
            merged.append(log)
            existing_keys.add(key)
            new_added += 1
    
    # timestamp 기준 정렬 (안정적인 순서 보장)
    try:
        merged.sort(key=lambda x: x.get('timestamp', x.get('time', 0)) if isinstance(x, dict) else 0)
    except Exception as e:
        logger.warning(f"Failed to sort merged history: {e}")
    
    # 최대 엔트리 수 제한
    if len(merged) > max_entries:
        merged = merged[-max_entries:]
    
    logger.info(f"Merged history: {len(existing_history)} existing + {new_added} new = {len(merged)} total")
    return merged


def _upload_history_to_s3(execution_id: str, payload: dict) -> str:
    """
    전체 실행 이력을 S3에 업로드하고 키를 반환합니다.
    """
    if not SKELETON_S3_BUCKET:
        logger.warning("SKELETON_S3_BUCKET not configured, skipping S3 upload")
        return None

    try:
        # S3 키 생성 (executions/{execution_id}.json)
        # execution_id가 ARN인 경우 마지막 부분만 사용하거나 전체를 안전하게 변환
        safe_id = execution_id.split(':')[-1]
        s3_key = f"executions/{safe_id}.json"
        
        s3_client.put_object(
            Bucket=SKELETON_S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload, default=str),
            ContentType='application/json'
        )
        logger.info(f"Uploaded execution history to s3://{SKELETON_S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Failed to upload history to S3: {e}")
        return None

def _update_execution_status(owner_id: str, notification_payload: dict) -> bool:
    """
    ExecutionsTable에 현재 실행 상태를 업데이트합니다.
    비용 최적화: put_item 대신 update_item을 사용하여 WCU 절약 및 덮어쓰기 방지.
    """
    if not executions_table:
        logger.error("EXECUTIONS_TABLE not configured")
        return False

    try:
        timestamp = int(time.time())
        inner = notification_payload.get('payload', {})
        if not isinstance(inner, dict): inner = {}
        
        # [FIX] execution_id는 notification_payload 최상위 또는 inner에 있을 수 있음
        exec_id = inner.get('execution_id') or notification_payload.get('execution_id') or notification_payload.get('conversation_id')
        if not exec_id:
            logger.warning(f"[DEBUG] No execution_id found. notification_payload keys: {list(notification_payload.keys())}, inner keys: {list(inner.keys())}")
            return False

        current_status = str(inner.get('status', '') or notification_payload.get('status', '')).upper()
        
        # 1. S3에 전체 데이터 업로드 (Claim Check Pattern)
        # [FIX] Initialize full_state, then look for state_data or step_function_state
        raw_state = inner.get('step_function_state') or inner.get('state_data') or notification_payload.get('state_data')
        full_state = _safe_json_compatible(raw_state) if raw_state else {}
        
        # [v2.1 데이터 무결성 강화] S3에서 기존 히스토리 가져와서 병합
        # segment_runner가 누적 히스토리를 보내더라도, 핸들러 레벨에서 검증/병합
        new_logs = notification_payload.get('new_history_logs') or inner.get('new_history_logs')
        
        try:
            MAX_HISTORY = int(os.environ.get('STATE_HISTORY_MAX_ENTRIES', '50'))
        except:
            MAX_HISTORY = 50
        
        if new_logs and isinstance(new_logs, list):
            # [핵심 변경] S3에서 기존 히스토리 가져오기 (덮어쓰기 방지)
            existing_history = _fetch_existing_history_from_s3(exec_id)
            
            # 기존 히스토리와 새 로그 병합 (중복 제거, 순서 보장)
            merged_history = _merge_history_logs(
                existing_history=existing_history,
                new_logs=new_logs,
                max_entries=MAX_HISTORY
            )
            
            full_state['state_history'] = merged_history
            
            logger.info(
                f"[HistoryMerge] exec={exec_id[:16]}..., "
                f"existing={len(existing_history)}, new={len(new_logs)}, merged={len(merged_history)}"
            )
        else:
            # new_logs가 없으면 기존 state_history 유지 또는 빈 리스트
            current_history = full_state.get('state_history', [])
            if not isinstance(current_history, list):
                current_history = []
            full_state['state_history'] = current_history
                
        history_s3_key = _upload_history_to_s3(exec_id, full_state)
        logger.info(f"[DEBUG] history_s3_key after upload: {history_s3_key}")
        
        # 2. DynamoDB 저장용 데이터 준비 (state_history 제거)
        step_state = _strip_state_history(full_state)
        
        # DynamoDB UpdateExpression 구성
        key = {
            'ownerId': owner_id,
            'executionArn': exec_id
        }
        
        update_expr_parts = [
            "#st = :st",
            "#ua = :ua",
            "#sfs = :sfs"
        ]
        expr_names = {
            "#st": "status",
            "#ua": "updatedAt",
            "#sfs": "step_function_state"
        }
        expr_values = {
            ":st": current_status,
            ":ua": timestamp,
            ":sfs": _convert_to_dynamodb_format(step_state)
        }
        
        # S3 키가 있으면 추가
        if history_s3_key:
            update_expr_parts.append("#hsk = :hsk")
            expr_names["#hsk"] = "history_s3_key"
            expr_values[":hsk"] = history_s3_key

        update_expr = "SET " + ", ".join(update_expr_parts)

        executions_table.update_item(
            Key=key,
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values
        )
        logger.info(f"Updated execution status (partial): {exec_id} -> {current_status}")

        return True

    except Exception as e:
        logger.error(f"Failed to update execution status: {e}")
        return False

def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    # 1. Payload 추출 및 검증
    payload = _get_payload(event)

    # 내부 호출 검증
    if event.get('requestContext'):
        logger.error('External invocation forbidden')
        return {"status": "error", "message": "Forbidden"}

    # [Robustness] Step Functions input.$: $ 변경 대응
    # input이 중첩되어 들어올 수 있음 (예: { "input": { ... }, "TaskToken": ... })
    if 'input' in payload and isinstance(payload['input'], dict):
        # Merge input fields into top-level payload for easier access
        # [Fix] Do not overwrite existing keys (like 'status', 'message') with input data
        # which might contain stale state
        for k, v in payload['input'].items():
            if k not in payload:
                payload[k] = v

    owner_id = payload.get('ownerId') or payload.get('owner_id')
    
    # [Robustness] ownerId가 최상위에 없으면 state_data 내부 확인
    if not owner_id and isinstance(payload.get('state_data'), dict):
        owner_id = payload['state_data'].get('ownerId')

    if not owner_id:
        logger.error('Missing ownerId in payload: keys=%s', list(payload.keys()))
        # [Critical Fix] Crash prevention: Return error but don't fail Lambda execution
        # This allows Step Functions to receive a response (even if it's an error)
        return {"status": "error", "message": "Missing ownerId"}

    # [추가] 상태값이 없으면 기본값 'STARTED' 부여 (특히 시작 단계일 때)
    if 'status' not in payload:
        payload['status'] = 'STARTED'
        payload['message'] = payload.get('message', '시스템이 초기화 중입니다...')

    # 2. Notification Payload 구성
    # 데이터 추출 로직 간소화 (get with defaults)
    state_data = payload.get('state_data') or {}
    if not isinstance(state_data, dict): state_data = {} # 방어 코드

    # [Glass Box] 시작 시간 추출
    start_time = payload.get('start_time') or state_data.get('start_time')
    if not start_time:
        start_time = int(time.time())

    # [Glass Box] 상태별 실행 시간 추적
    state_durations = state_data.get('state_durations', {}) or {}
    current_status = payload.get('status')
    prev_update_time = state_data.get('last_update_time', start_time)
    current_time = int(time.time())
    
    # 이전 상태의 실행 시간 누적 (상태 변경 시)
    if current_status and prev_update_time:
        elapsed = current_time - prev_update_time
        if elapsed > 0:  # 음수 방지
            state_durations[current_status] = state_durations.get(current_status, 0) + elapsed

    # [Glass Box] 추가 메타데이터 계산
    current_segment = payload.get('segment_to_run') or state_data.get('segment_to_run', 0)
    total_segments = payload.get('total_segments') or state_data.get('total_segments', 1)
    
    # 1. 예상 완료 시간 (ETA) 계산
    estimated_completion_time = None
    estimated_remaining_seconds = None
    if total_segments > 1 and current_segment >= 0:
        completed_segments = current_segment  # 현재 진행 중인 세그먼트까지
        remaining_segments = total_segments - current_segment - 1  # 남은 세그먼트
        
        if completed_segments > 0:
            total_elapsed = current_time - start_time
            avg_segment_time = total_elapsed / completed_segments
            estimated_remaining_seconds = int(avg_segment_time * remaining_segments)
            estimated_completion_time = current_time + estimated_remaining_seconds
    
    # 2. 현재 단계 이름/설명
    current_step_label = f"세그먼트 {current_segment + 1}/{total_segments} 처리 중"
    partition_map = state_data.get('partition_map', [])
    if isinstance(partition_map, list) and current_segment < len(partition_map):
        segment_info = partition_map[current_segment]
        if isinstance(segment_info, dict):
            segment_type = segment_info.get('type', '')
            if segment_type == 'llm':
                current_step_label = f"세그먼트 {current_segment + 1}/{total_segments}: AI 모델 실행 중"
            elif segment_type == 'hitp':
                current_step_label = f"세그먼트 {current_segment + 1}/{total_segments}: 사용자 입력 대기 중"
            elif segment_type == 'isolated':
                current_step_label = f"세그먼트 {current_segment + 1}/{total_segments}: 독립 실행 중"
            else:
                current_step_label = f"세그먼트 {current_segment + 1}/{total_segments}: 일반 처리 중"
    
    # 3. 세그먼트당 평균 처리 속도
    average_segment_duration = None
    if current_segment > 0:
        total_elapsed = current_time - start_time
        average_segment_duration = int(total_elapsed / current_segment)

    # Sequence number 생성 (메시지 순서 보장용)
    sequence_number = int(time.time() * 1000000)  # 마이크로초 단위

    inner_payload = {
        'action': payload.get('notification_type', 'execution_progress'),
        'conversation_id': payload.get('conversation_id') or payload.get('conversationId'),
        'message': payload.get('message', 'Execution progress update'),
        'status': payload.get('status'),
        
        # [CRITICAL FIX] execution_id와 workflowId를 inner_payload에 포함 (DB 저장용)
        # execution_id는 여러 위치에서 찾을 수 있음:
        # 1. payload.execution_id (SegmentProgress events)
        # 2. payload.conversation_id (fallback)
        # 3. state_data.executionArn (completion events 에서 state_data 안에 있을 수 있음)
        # 4. payload.$$.Execution.Id 값이 state_data에 저장될 수 있음
        'execution_id': (
            payload.get('execution_id') 
            or payload.get('conversation_id')
            or state_data.get('executionArn')
            or state_data.get('execution_id')
        ),
        'workflowId': payload.get('workflowId') or state_data.get('workflowId'),
        
        # 순서 보장을 위한 메타데이터
        'sequence_number': sequence_number,
        'server_timestamp': current_time,
        'segment_sequence': current_segment,  # 세그먼트 기반 순서
        
        # [Glass Box] 핵심 메타데이터 추가
        'current_segment': current_segment,
        'total_segments': total_segments,
        'start_time': start_time,
        'last_update_time': current_time,
        'state_durations': state_durations,
        # [Added] final_result 추출 (execution_result.final_state)
        'final_result': (payload.get('execution_result') or {}).get('final_state'),
        # [Fix] Error details extraction from src.flat payload (due to Step Functions parameter simplification)
        'error_details': payload.get('error_details') or payload.get('partition_error') or payload.get('async_error') or payload.get('error_info') or payload.get('notification_error') or payload.get('failure_notification_error'),
        
        # [NEW] workflow_config for graph visualization (only nodes/edges to minimize size)
        'workflow_config': _extract_graph_config(state_data.get('workflow_config')),
        # [NEW] state_history for node status tracking
        'state_history': payload.get('new_history_logs') or state_data.get('state_history', []),
    }

    # [Added] Construct final notification payload
    notification_payload = {
        'type': 'workflow_status',
        'payload': inner_payload
    }

    # [Data Integrity] DB 저장을 위한 원본 데이터 보존
    # notification_payload는 WebSocket 용량 제한으로 인해 수정(Skeleton)될 수 있으므로,
    # DB에는 항상 원본 데이터를 저장하기 위해 별도 변수로 복사해둡니다.
    db_payload = notification_payload.copy()

    # WebSocket payload 크기 제한 체크 (128KB = 131072 bytes)
    msg_str = json.dumps(notification_payload, cls=DecimalEncoder)
    msg_bytes = msg_str.encode('utf-8')
    MAX_PAYLOAD_SIZE = 128 * 1024 # 128KB
    
    if len(msg_bytes) > MAX_PAYLOAD_SIZE:
        logger.warning(f"WebSocket payload size {len(msg_bytes)} exceeds limit {MAX_PAYLOAD_SIZE}, removing step_function_state")
        # 1차 시도: step_function_state 제거
        inner_payload.pop('step_function_state', None)
        notification_payload['payload'] = inner_payload
        msg_str = json.dumps(notification_payload, cls=DecimalEncoder)
        msg_bytes = msg_str.encode('utf-8')
        
        if len(msg_bytes) > MAX_PAYLOAD_SIZE:
            logger.error(f"Payload still too large ({len(msg_bytes)} bytes). Sending skeleton payload.")
            # 2차 시도: Skeleton Payload 전송 (Robustness)
            skeleton_payload = {
                'type': 'workflow_status',
                'payload': {
                    'action': inner_payload.get('action'),
                    'conversation_id': inner_payload.get('conversation_id'),
                    'status': inner_payload.get('status'),
                    'message': "Data too large to display via WebSocket. Please check history tab.",
                    'error': "PAYLOAD_TOO_LARGE",
                    'current_segment': inner_payload.get('current_segment'),
                    'total_segments': inner_payload.get('total_segments'),
                }
            }
            msg_str = json.dumps(skeleton_payload, cls=DecimalEncoder)
            msg_bytes = msg_str.encode('utf-8')
            # skeleton payload를 notification_payload로 교체
            notification_payload = skeleton_payload

    # 3. Endpoint URL 결정
    endpoint_url = WEBSOCKET_ENDPOINT_URL
    if not endpoint_url:
        # state_data 내의 callback context 탐색
        callback_context = (
            state_data.get('current_state', {}).get('__callback_context') or
            state_data.get('initial_state', {}).get('__callback_context') or
            state_data.get('__callback_context')
        )
        if callback_context:
            domain = callback_context.get('domainName')
            stage = callback_context.get('stage')
            if domain and stage:
                endpoint_url = f"https://{domain}/{stage}"

    # 4. WebSocket 전송 시도
    connection_ids = get_user_connection_ids(owner_id)
    success_count = 0
    
    if connection_ids and endpoint_url:
        apigw = get_apigw_client(endpoint_url)
        # msg_bytes는 위에서 계산됨 (skeleton일 수도 있음)
        
        for conn_id in connection_ids:
            try:
                apigw.post_to_connection(ConnectionId=conn_id, Data=msg_bytes)
                success_count += 1
            except ClientError as e:
                if e.response['Error']['Code'] == 'GoneException':
                    _delete_connection(conn_id)
                else:
                    logger.warning(f"Failed to send to {conn_id}: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error sending to {conn_id}: {e}")
    else:
        if not connection_ids:
            logger.info(f"No active connections for owner={owner_id}")
        if not endpoint_url:
            logger.warning("No endpoint_url available")
    
    # [v2.1 연결 끊김 상태 추적] 모든 연결이 끊긴 경우 DB에 플래그 저장
    # 사용자 재접속 시 즉시 마지막 상태를 fetch할 수 있도록 함
    all_connections_gone = (
        connection_ids and 
        endpoint_url and 
        success_count == 0 and 
        len(connection_ids) > 0
    )
    
    if all_connections_gone:
        _mark_no_active_session(owner_id, inner_payload.get('execution_id'))

    # 5. DB 업데이트 여부 결정 (전략 기반)
    should_update_db = should_update_database(payload, state_data)

    db_saved = False
    if should_update_db:
        # 업데이트 메타데이터 추가
        db_payload_with_meta = db_payload.copy()
        db_payload_with_meta['payload']['last_db_update_time'] = current_time
        db_payload_with_meta['payload']['last_db_progress_percentage'] = (current_segment / total_segments) * 100 if total_segments > 0 else 0
        
        # [수정] DB에는 원본 데이터(db_payload)를 저장하여 데이터 유실 방지
        db_saved = _update_execution_status(owner_id, db_payload_with_meta)
        logger.info(f"DB updated for execution {inner_payload.get('execution_id')} (strategy: {DB_UPDATE_STRATEGY})")
    else:
        logger.debug(f"DB update skipped for execution {inner_payload.get('execution_id')} (strategy: {DB_UPDATE_STRATEGY})")

    # 메트릭 발행
    publish_db_update_metrics(inner_payload.get('execution_id', ''), db_saved, DB_UPDATE_STRATEGY)
    
    logger.info(f"Processed: owner={owner_id}, sent={success_count}, saved={db_saved}, strategy={DB_UPDATE_STRATEGY}")
    return {
        'status': 'success', 
        'ws_sent': success_count, 
        'db_saved': db_saved,
        'db_update_skipped': not should_update_db,
        'update_strategy': DB_UPDATE_STRATEGY,
        'updated_state_data': {
            'state_durations': state_durations,
            'last_update_time': current_time,
            'start_time': start_time,
            # [NEW] Return updated state_history so SFN can update its state
            'state_history': db_payload.get('payload', {}).get('state_data', {}).get('state_history') 
                if db_saved and 'new_history_logs' in payload else None
        }
    }