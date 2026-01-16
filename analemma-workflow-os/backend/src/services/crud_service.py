"""
CRUD Service Layer

기존 Lambda 핸들러들의 로직을 재사용 가능한 서비스 레이어로 통합합니다.
FastAPI와 Lambda 핸들러 모두에서 사용 가능합니다.

통합 대상:
- Executions: list_my_executions, get_status, get_execution_history, delete_execution
- Workflows: get_workflow, get_workflow_by_name, save_workflow, delete_workflow
- Notifications: list_notifications, dismiss_notification
"""

import os
import json
import logging
import base64
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from botocore.exceptions import ClientError
from src.common.constants import DynamoDBConfig
from boto3.dynamodb.conditions import Key, Attr

# 통합된 공통 유틸리티 import
try:
    from src.common.json_utils import DecimalEncoder
    from src.common.pagination_utils import decode_pagination_token, encode_pagination_token
    from src.common.aws_clients import get_dynamodb_resource, get_s3_client
    _USE_COMMON = True
except ImportError:
    _USE_COMMON = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 환경변수에서 테이블 이름 로드
# 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
EXECUTIONS_TABLE = os.environ.get('EXECUTIONS_TABLE')
WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
# 🚨 [Critical Fix] 테이블별로 다른 GSI 사용
# ExecutionsTableV3: OwnerIdStartDateIndex
# WorkflowsTableV3: OwnerIdNameIndexV2
EXECUTIONS_OWNER_INDEX = os.environ.get('OWNER_INDEX', os.environ.get('OWNER_ID_START_DATE_INDEX', 'OwnerIdStartDateIndex'))
WORKFLOWS_OWNER_INDEX = os.environ.get('WORKFLOWS_OWNER_INDEX', os.environ.get('OWNER_ID_NAME_INDEX', 'OwnerIdNameIndexV2'))
NOTIFICATIONS_INDEX = os.environ.get('NOTIFICATIONS_INDEX', 'NotificationsIndex')
SKELETON_S3_BUCKET = os.environ.get('SKELETON_S3_BUCKET')
SKELETON_S3_PREFIX = os.environ.get('SKELETON_S3_PREFIX', '')

# 지연 초기화를 위한 모듈 레벨 변수
_dynamodb = None
_s3_client = None


def _get_dynamodb():
    """DynamoDB 리소스를 지연 초기화하여 반환"""
    global _dynamodb
    if _dynamodb is None:
        if _USE_COMMON:
            _dynamodb = get_dynamodb_resource()
        else:
            import boto3
            _dynamodb = boto3.resource('dynamodb')
    return _dynamodb


def _get_s3_client():
    """S3 클라이언트를 지연 초기화하여 반환"""
    global _s3_client
    if _s3_client is None:
        if _USE_COMMON:
            _s3_client = get_s3_client()
        else:
            import boto3
            _s3_client = boto3.client('s3')
    return _s3_client


# Fallback: 공통 모듈 import 실패 시에만 로컬 정의
if not _USE_COMMON:
    class DecimalEncoder(json.JSONEncoder):
        """DynamoDB Decimal 타입을 JSON 호환되도록 변환 (Fallback)"""
        def default(self, obj):
            if isinstance(obj, Decimal):
                return float(obj) if obj % 1 else int(obj)
            if isinstance(obj, set):
                return list(obj)
            return super().default(obj)

    def decode_pagination_token(token):
        if not token:
            return None
        try:
            return json.loads(base64.b64decode(token).decode('utf-8'))
        except Exception:
            return None

    def encode_pagination_token(obj):
        if not obj:
            return None
        try:
            return base64.b64encode(json.dumps(obj).encode('utf-8')).decode('utf-8')
        except Exception:
            return None


class ExecutionCRUDService:
    """Execution 관련 CRUD 작업 서비스"""

    def __init__(self):
        self._table = None

    @property
    def table(self):
        if self._table is None and EXECUTIONS_TABLE:
            self._table = _get_dynamodb().Table(EXECUTIONS_TABLE)
        return self._table

    def list_executions(
        self,
        owner_id: str,
        limit: int = 20,
        next_token: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """사용자의 실행 목록 조회"""
        if not self.table or not EXECUTIONS_OWNER_INDEX:
            raise ValueError("Server misconfigured: missing table or index")

        query_params = {
            'IndexName': EXECUTIONS_OWNER_INDEX,
            'KeyConditionExpression': Key('ownerId').eq(owner_id),
            'ScanIndexForward': False,  # 최신순
            'Limit': min(limit, 100)
        }

        exclusive_start_key = decode_pagination_token(next_token)
        if exclusive_start_key:
            query_params['ExclusiveStartKey'] = exclusive_start_key

        try:
            response = self.table.query(**query_params)
            items = response.get('Items', [])
            last_key = response.get('LastEvaluatedKey')
            
            return items, encode_pagination_token(last_key)
        except ClientError as e:
            logger.error(f"DynamoDB error listing executions: {e}")
            raise

    def get_status(self, owner_id: str, execution_arn: str) -> Optional[Dict]:
        """실행 상태 조회"""
        if not self.table:
            raise ValueError("Server misconfigured: missing table")

        try:
            response = self.table.get_item(
                Key={'executionArn': execution_arn}
            )
            item = response.get('Item')
            
            if not item:
                return None
                
            # 소유권 확인
            if item.get('ownerId') != owner_id:
                return None
                
            return {
                'executionArn': item.get('executionArn'),
                'status': item.get('status'),
                'startDate': item.get('startDate'),
                'stopDate': item.get('stopDate'),
                'workflowId': item.get('workflowId'),
                'name': item.get('name'),
                'input': item.get('input'),
                'output': item.get('output'),
                'error': item.get('error'),
                'cause': item.get('cause')
            }
        except ClientError as e:
            logger.error(f"DynamoDB error getting status: {e}")
            raise

    def get_execution_history(
        self,
        owner_id: str,
        execution_arn: str
    ) -> Optional[Dict]:
        """실행 히스토리 조회 (S3 Claim Check 패턴 지원)"""
        if not self.table:
            raise ValueError("Server misconfigured: missing table")

        try:
            response = self.table.get_item(
                Key={'executionArn': execution_arn}
            )
            item = response.get('Item')
            
            if not item:
                return None
                
            # 소유권 확인
            if item.get('ownerId') != owner_id:
                return None

            result = dict(item)
            
            # S3 Claim Check: 대용량 state_history 처리
            state_history = result.get('state_history')
            if isinstance(state_history, dict) and state_history.get('__s3_ref'):
                # S3에서 실제 데이터 로드
                s3_ref = state_history
                try:
                    s3_response = _get_s3_client().get_object(
                        Bucket=s3_ref['bucket'],
                        Key=s3_ref['key']
                    )
                    s3_data = json.loads(s3_response['Body'].read().decode('utf-8'))
                    result['state_history'] = s3_data.get('state_history', [])
                except ClientError as e:
                    logger.error(f"Failed to load state_history from src.S3: {e}")
                    result['state_history'] = []
                    result['state_history_error'] = 'Failed to load from src.storage'

            return result
        except ClientError as e:
            logger.error(f"DynamoDB error getting history: {e}")
            raise

    def delete_execution(self, owner_id: str, execution_arn: str) -> bool:
        """실행 삭제 (소유권 확인 포함)"""
        if not self.table:
            raise ValueError("Server misconfigured: missing table")

        try:
            # 소유권 확인과 삭제를 원자적으로 수행
            self.table.delete_item(
                Key={'executionArn': execution_arn},
                ConditionExpression=Attr('ownerId').eq(owner_id)
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False  # 소유권 불일치 또는 항목 없음
            logger.error(f"DynamoDB error deleting execution: {e}")
            raise


class WorkflowCRUDService:
    """Workflow 관련 CRUD 작업 서비스"""

    def __init__(self):
        self._table = None

    @property
    def table(self):
        if self._table is None and WORKFLOWS_TABLE:
            self._table = _get_dynamodb().Table(WORKFLOWS_TABLE)
        return self._table

    def get_workflow(
        self,
        owner_id: str,
        workflow_id: str,
        version: Optional[str] = None
    ) -> Optional[Dict]:
        """워크플로우 조회"""
        if not self.table:
            raise ValueError("Server misconfigured: missing table")

        try:
            # 버전이 지정되지 않으면 v0 (최신) 조회
            sk = version if version else 'v0'
            
            response = self.table.get_item(
                Key={
                    'pk': workflow_id,
                    'sk': sk
                }
            )
            item = response.get('Item')
            
            if not item:
                return None
                
            # 소유권 확인
            if item.get('ownerId') != owner_id:
                return None

            return item
        except ClientError as e:
            logger.error(f"DynamoDB error getting workflow: {e}")
            raise

    def get_workflow_by_name(
        self,
        owner_id: str,
        name: str
    ) -> Optional[Dict]:
        """이름으로 워크플로우 조회 (GSI 사용)"""
        if not self.table:
            raise ValueError("Server misconfigured: missing table")

        try:
            response = self.table.query(
                IndexName=DynamoDBConfig.OWNER_ID_NAME_INDEX,
                KeyConditionExpression=Key('ownerId').eq(owner_id) & Key('name').eq(name),
                Limit=1
            )
            items = response.get('Items', [])
            
            if not items:
                return None

            return items[0]
        except ClientError as e:
            logger.error(f"DynamoDB error getting workflow by name: {e}")
            raise

    def delete_workflow(
        self,
        owner_id: str,
        workflow_id: str,
        delete_all_versions: bool = False
    ) -> bool:
        """워크플로우 삭제"""
        if not self.table:
            raise ValueError("Server misconfigured: missing table")

        try:
            if delete_all_versions:
                # 모든 버전 조회 후 삭제
                response = self.table.query(
                    KeyConditionExpression=Key('pk').eq(workflow_id)
                )
                items = response.get('Items', [])
                
                for item in items:
                    if item.get('ownerId') != owner_id:
                        continue  # 소유권 불일치
                    self.table.delete_item(
                        Key={
                            'pk': item['pk'],
                            'sk': item['sk']
                        }
                    )
            else:
                # v0만 삭제 (소유권 확인)
                self.table.delete_item(
                    Key={
                        'pk': workflow_id,
                        'sk': 'v0'
                    },
                    ConditionExpression=Attr('ownerId').eq(owner_id)
                )
            
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            logger.error(f"DynamoDB error deleting workflow: {e}")
            raise


class NotificationCRUDService:
    """Notification 관련 CRUD 작업 서비스"""

    def __init__(self):
        self._executions_table = None

    @property
    def executions_table(self):
        if self._executions_table is None and EXECUTIONS_TABLE:
            self._executions_table = _get_dynamodb().Table(EXECUTIONS_TABLE)
        return self._executions_table

    def list_notifications(
        self,
        owner_id: str,
        status: Optional[str] = None,
        limit: int = 50,
        next_token: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """알림 목록 조회 (Sparse Index 패턴)"""
        if not self.executions_table:
            raise ValueError("Server misconfigured: missing table")

        notifications = []

        try:
            # 1. Active workflows (RUNNING, PAUSED_FOR_HITP 등)
            if status != 'dismissed':
                active_response = self.executions_table.query(
                    IndexName=EXECUTIONS_OWNER_INDEX,
                    KeyConditionExpression=Key('ownerId').eq(owner_id),
                    FilterExpression=Attr('status').is_in(['RUNNING', 'STARTED', 'PAUSED_FOR_HITP']),
                    Limit=limit
                )
                for item in active_response.get('Items', []):
                    notifications.append(self._map_execution_to_notification(item))

            # 2. Completed but not dismissed (NotificationsIndex GSI 사용)
            if NOTIFICATIONS_INDEX and status != 'dismissed':
                try:
                    completed_response = self.executions_table.query(
                        IndexName=NOTIFICATIONS_INDEX,
                        KeyConditionExpression=Key('ownerId').eq(owner_id),
                        Limit=limit
                    )
                    for item in completed_response.get('Items', []):
                        if not item.get('dismissed'):
                            notifications.append(self._map_execution_to_notification(item))
                except ClientError:
                    pass  # Index가 없는 경우 무시

            # 타임스탬프 기준 정렬 (최신순)
            notifications.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
            
            return notifications[:limit], None
        except ClientError as e:
            logger.error(f"DynamoDB error listing notifications: {e}")
            raise

    def dismiss_notification(
        self,
        owner_id: str,
        notification_id: str
    ) -> bool:
        """알림 무시 처리"""
        if not self.executions_table:
            raise ValueError("Server misconfigured: missing table")

        try:
            # notification_id는 executionArn
            self.executions_table.update_item(
                Key={'executionArn': notification_id},
                UpdateExpression='SET dismissed = :val, dismissedAt = :ts REMOVE notificationTime',
                ExpressionAttributeValues={
                    ':val': True,
                    ':ts': datetime.now().isoformat(),
                    ':owner': owner_id
                },
                ConditionExpression=Attr('ownerId').eq(owner_id)
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            logger.error(f"DynamoDB error dismissing notification: {e}")
            raise

    def _map_execution_to_notification(self, item: Dict) -> Dict:
        """ExecutionsTable 아이템을 NotificationItem 형식으로 변환"""
        ts = 0
        try:
            notification_time = item.get('notificationTime')
            if notification_time:
                dt = datetime.fromisoformat(notification_time.replace('Z', '+00:00'))
                ts = int(dt.timestamp() * 1000)
            else:
                ts = int(datetime.now().timestamp() * 1000)
        except Exception:
            ts = int(datetime.now().timestamp() * 1000)

        status = item.get('status')
        action = "workflow_status"
        if status in ['RUNNING', 'STARTED']:
            action = "execution_progress"
        elif status == 'PAUSED_FOR_HITP':
            action = "hitp_pause"

        return {
            "notificationId": item.get('executionArn'),
            "type": "workflow_status",
            "action": action,
            "status": "sent",
            "timestamp": ts,
            "notification": {
                "type": "workflow_status",
                "payload": {
                    "action": action,
                    "execution_id": item.get('executionArn'),
                    "status": status,
                    "workflowId": item.get('workflowId'),
                    "start_time": item.get('startDate'),
                    "stop_time": item.get('stopDate'),
                    "message": f"Workflow {status}",
                }
            }
        }
