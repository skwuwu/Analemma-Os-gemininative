"""
WebSocket $connect Handler.

[v2.1] 개선사항:
1. GSI 전파 지연 대응 (post_to_connection 재시도)
2. Pending 알림 배치 크기 제한 (max 20개)
3. lastSeen/connectedAt 필드 추가 (Stale 커넥션 정리용)
"""

import os
import json
import logging
import boto3
import time as _time
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =============================================================================
# [v2.1] 상수 정의
# =============================================================================
# Pending 알림 최대 전송 개수 (폭주 방지)
MAX_PENDING_NOTIFICATIONS = int(os.environ.get('MAX_PENDING_NOTIFICATIONS', '20'))

# 연결 저장 후 API Gateway 전파 대기 시간 (초)
CONNECTION_PROPAGATION_DELAY = float(os.environ.get('CONNECTION_PROPAGATION_DELAY', '0.1'))

# post_to_connection 재시도 횟수
POST_RETRY_COUNT = 2

# 공통 유틸리티 모듈 import (Lambda 환경에서는 상대 경로 import 불가)
try:
    from src.common.auth_utils import validate_token
    from src.common.constants import DynamoDBConfig
except ImportError:
    try:
        from src.common.auth_utils import validate_token
        from src.common.constants import DynamoDBConfig
    except ImportError:
        # Fallback: validate_token이 없으면 더미 함수 (개발/테스트용)
        def validate_token(*args, **kwargs):
            return None


dynamodb = boto3.resource('dynamodb')

# Import exec_status_helper with fallback pattern used by other Lambda functions
try:
    from src.common.exec_status_helper import (
        build_status_payload,
        ExecutionForbidden,
        ExecutionNotFound,
    )
except ImportError:
    try:
        from src.common.exec_status_helper import (
            build_status_payload,
            ExecutionForbidden,
            ExecutionNotFound,
        )
    except ImportError:
        # Last resort: define minimal fallbacks
        def build_status_payload(*args, **kwargs):
            return {"error": "exec_status_helper not available"}
        class ExecutionForbidden(Exception):
            pass
        class ExecutionNotFound(Exception):
            pass


def lambda_handler(event, context):
    """
    WebSocket $connect handler.

    Expects either:
      - ownerId provided as a query string parameter ?ownerId=..., or
      - API Gateway JWT authorizer injecting requestContext.authorizer.jwt.claims.sub

    Stores a mapping { connectionId -> ownerId } in the DynamoDB table specified
    by the WEBSOCKET_CONNECTIONS_TABLE env var.
    """
    # Helpful debug: log basic event keys so we can trace connect attempts
    try:
        logger.info('WebSocket $connect invoked; requestContext keys: %s', list(event.get('requestContext', {}).keys()))
        logger.debug('Full $connect event: %s', json.dumps(event, default=str)[:2000])
    except Exception:
        # defensive: ensure logging never raises
        logger.exception('Failed to log incoming connect event')

    table_name = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
    if not table_name:
        logger.error('WEBSOCKET_CONNECTIONS_TABLE not configured')
        return {'statusCode': 500, 'body': 'Server misconfiguration'}

    connection_id = event.get('requestContext', {}).get('connectionId')
    if not connection_id:
        logger.error('No connectionId in requestContext; event.requestContext=%s', event.get('requestContext'))
        return {'statusCode': 400, 'body': 'Missing connectionId'}

    # --- [Trust Model] Authorizer가 검증한 Identity 사용 ---
    # WebsocketAuthorizerFunction이 검증 후 principalId에 ownerId를 담아 전달함
    try:
        qs = event.get('queryStringParameters') or {}
        # executionArn is optional, used for connecting to a specific run
        execution_arn = qs.get('executionArn') or qs.get('execution_arn')

        authorizer_ctx = event.get('requestContext', {}).get('authorizer', {})
        # [FIX] Lambda Authorizer의 context 객체가 authorizer로 전달됨
        # principalId는 최상위에 있지만 API Gateway는 context만 전달함
        owner_id = authorizer_ctx.get('ownerId') or authorizer_ctx.get('principalId')
        
        # Fallback for local testing or misconfiguration
        if not owner_id:
            logger.warning("No principalId in requestContext.authorizer - check Authorizer configuration")
            # For strict security, we could reject here. 
            # But if Authorizer is disabled (e.g. dev), we might allow anonymous or fail naturally later.
            # Given we rely on Authorizer, if it's missing, it's an error.
            if not os.getenv('MOCK_MODE'):
                return {'statusCode': 401, 'body': 'Unauthorized: Missing identity'}
            
    except Exception as e:
        logger.error(f"Failed to retrieve identity from src.Authorizer context: {e}")
        return {'statusCode': 401, 'body': 'Unauthorized'}


    try:
        table = dynamodb.Table(table_name)
        
        # [v2.1] 현재 시간 (connectedAt, lastSeen 용)
        current_time = int(_time.time())
        
        item = {'connectionId': connection_id}
        if owner_id:
            item['ownerId'] = owner_id
        if execution_arn:
            item['executionArn'] = execution_arn
        
        # [v2.1] Stale 커넥션 정리를 위한 시간 필드
        item['connectedAt'] = current_time
        item['lastSeen'] = current_time  # 메시지 수신 시 업데이트 가능

        # TTL: 2 hours from src.now (using constant)
        try:
            from src.common.constants import TTLConfig
            item['ttl'] = current_time + TTLConfig.WEBSOCKET_CONNECTION
        except ImportError:
            item['ttl'] = current_time + 7200  # Fallback

        logger.info('Persisting websocket connection to DDB table=%s connectionId=%s ownerId=%s', table_name, connection_id, owner_id)
        table.put_item(Item=item)
        
        # [v2.1] API Gateway 연결 전파 대기
        # $connect 성공 응답 전에 post_to_connection 호출 시
        # API Gateway가 아직 연결을 완전히 인식하지 못할 수 있음
        if CONNECTION_PROPAGATION_DELAY > 0:
            _time.sleep(CONNECTION_PROPAGATION_DELAY)
        
        # 연결 성공 후 미전송 알림 전송 시도
        if owner_id:
            _send_pending_notifications(owner_id, connection_id, event)
            if execution_arn:
                _send_status_snapshot(owner_id, connection_id, execution_arn, event)
            
    except Exception as e:
        logger.exception('Failed to persist connection %s to table %s: %s', connection_id, table_name, e)
        return {'statusCode': 500, 'body': 'Failed to register connection'}

    return {
        'statusCode': 200,
        'body': 'Connected.'
    }


def _send_pending_notifications(owner_id: str, connection_id: str, event: dict):
    """
    새로운 WebSocket 연결이 성공했을 때, 해당 사용자의 미전송 알림을 전송합니다.
    
    [v2.1] 개선사항:
    - 최대 전송 개수 제한 (MAX_PENDING_NOTIFICATIONS)
    - 초과 시 요약 메시지 전송
    - post_to_connection 재시도 로직
    """
    pending_table_name = os.environ.get('PENDING_NOTIFICATIONS_TABLE', 'PendingNotifications')
    
    try:
        pending_table = dynamodb.Table(pending_table_name)
        
        # ownerId/status GSI로 'pending' 항목만 조회
        from boto3.dynamodb.conditions import Key
        index_name = DynamoDBConfig.OWNER_ID_STATUS_INDEX
        response = pending_table.query(
            IndexName=index_name,
            KeyConditionExpression=(
                Key('ownerId').eq(owner_id) & Key('status').eq('pending')
            ),
            # [v2.1] 최신 순으로 정렬, 제한보다 약간 더 가져와서 총 개수 확인
            Limit=MAX_PENDING_NOTIFICATIONS + 10,
            ScanIndexForward=False  # 최신 순
        )
        
        items = response.get('Items', [])
        if not items:
            logger.info(f'No pending notifications for owner={owner_id}')
            return
        
        total_pending = len(items)
        logger.info(f'Found {total_pending} pending notifications for owner={owner_id}')
        
        apigw_client = _create_apigw_client_from_event(event)
        if not apigw_client:
            logger.warning('Cannot send pending notifications: missing domainName')
            return
        
        # [v2.1] 최대 개수 제한
        items_to_send = items[:MAX_PENDING_NOTIFICATIONS]
        overflow_count = max(0, total_pending - MAX_PENDING_NOTIFICATIONS)
        
        # [v2.1] 초과 시 요약 메시지 먼저 전송
        if overflow_count > 0:
            summary_message = {
                'type': 'notification_summary',
                'payload': {
                    'total_pending': total_pending,
                    'sending': len(items_to_send),
                    'overflow': overflow_count,
                    'message': f'{overflow_count}개의 추가 알림이 있습니다. 알림 센터를 확인해 주세요.'
                }
            }
            _post_with_retry(apigw_client, connection_id, summary_message)
        
        # 각 미전송 알림 전송
        sent_count = 0
        for item in items_to_send:
            notification_id = item.get('notificationId')
            notification_payload = item.get('notification', {})
            
            try:
                # [v2.1] 재시도 로직 적용
                success = _post_with_retry(apigw_client, connection_id, notification_payload)
                
                if success:
                    # 전송 성공하면 상태 업데이트
                    pending_table.update_item(
                        Key={
                            'ownerId': owner_id,
                            'notificationId': notification_id
                        },
                        UpdateExpression='SET #status = :sent, sentAt = :sentAt',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={
                            ':sent': 'sent',
                            ':sentAt': int(_time.time())
                        }
                    )
                    sent_count += 1
                    logger.debug(f'Sent pending notification {notification_id} to connection {connection_id}')
                else:
                    logger.warning(f'Failed to send pending notification {notification_id} after retries')
                
            except Exception as e:
                logger.warning(f'Failed to send pending notification {notification_id}: {e}')
        
        logger.info(
            f'Successfully sent {sent_count}/{len(items_to_send)} pending notifications '
            f'for owner={owner_id} (overflow={overflow_count})'
        )
        
    except Exception as e:
        logger.error(f'Error processing pending notifications for owner={owner_id}: {e}')


def _post_with_retry(
    apigw_client, 
    connection_id: str, 
    payload: dict, 
    max_retries: int = POST_RETRY_COUNT
) -> bool:
    """
    [v2.1] post_to_connection with retry logic.
    
    API Gateway 연결 전파 지연으로 인한 실패 대응.
    """
    for attempt in range(max_retries + 1):
        try:
            apigw_client.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps(payload, ensure_ascii=False)
            )
            return True
        except apigw_client.exceptions.GoneException:
            # 연결이 이미 끊어짐 - 재시도 불필요
            logger.info(f'Connection {connection_id} already gone')
            return False
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'GoneException':
                return False
            
            if attempt < max_retries:
                delay = 0.1 * (attempt + 1)  # 0.1s, 0.2s, ...
                logger.debug(f'post_to_connection retry {attempt + 1}/{max_retries}: {e}')
                _time.sleep(delay)
            else:
                logger.warning(f'post_to_connection failed after {max_retries + 1} attempts: {e}')
                return False
        except Exception as e:
            if attempt < max_retries:
                _time.sleep(0.1)
            else:
                logger.warning(f'post_to_connection unexpected error: {e}')
                return False
    
    return False


def _create_apigw_client_from_event(event: dict):
    request_context = event.get('requestContext', {})
    domain_name = request_context.get('domainName')
    stage = request_context.get('stage', 'prod')
    if not domain_name:
        return None
    endpoint_url = f"https://{domain_name}/{stage}"
    return boto3.client('apigatewaymanagementapi', endpoint_url=endpoint_url)


def _send_status_snapshot(owner_id: str, connection_id: str, execution_arn: str, event: dict):
    apigw_client = _create_apigw_client_from_event(event)
    if not apigw_client:
        logger.warning('Cannot send status snapshot: missing domainName')
        return

    try:
        payload = build_status_payload(execution_arn, owner_id)
    except ExecutionNotFound:
        logger.info('Execution %s not found while sending status snapshot', execution_arn)
        return
    except ExecutionForbidden:
        logger.info('Execution %s not accessible for owner %s', execution_arn, owner_id)
        return
    except ClientError as exc:
        logger.exception('Failed to describe execution %s: %s', execution_arn, exc)
        return

    try:
        apigw_client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(payload, ensure_ascii=False)
        )
        logger.info('Sent status snapshot for execution %s to connection %s', execution_arn, connection_id)
    except apigw_client.exceptions.GoneException:
        logger.info('Connection %s gone while sending status snapshot', connection_id)
    except Exception:
        logger.exception('Failed to send status snapshot to connection %s', connection_id)
