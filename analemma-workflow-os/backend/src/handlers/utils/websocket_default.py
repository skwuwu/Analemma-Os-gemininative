"""
WebSocket $default route handler.

[v2.1] 개선사항:
1. Set(SS) 타입으로 구독 관리 (중복 방지 + 400KB 제한 대응)
2. unsubscribe 실제 구현 (DynamoDB에서 제거)
3. API Gateway 클라이언트 캐싱 (TCP 연결 재사용)
4. lastSeen 업데이트 (Stale 커넥션 정리용)

Handles all messages that don't match specific routes ($connect, $disconnect).
This is required for WebSocket API to process client messages.
"""
import os
import json
import logging
import boto3
import time as _time
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

# =============================================================================
# [v2.1] 전역 캐싱
# =============================================================================
# API Gateway Management API 클라이언트 캐시 (endpoint별)
_apigw_clients: Dict[str, Any] = {}

# 최대 구독 수 (List Growth Trap 방지)
MAX_SUBSCRIPTIONS = int(os.environ.get('MAX_SUBSCRIPTIONS_PER_CONNECTION', '50'))


def _get_apigw_client(domain: str, stage: str):
    """
    [v2.1] API Gateway Management API 클라이언트 캐싱.
    
    매 요청마다 boto3.client 생성 비용 절감.
    TCP 연결 재사용으로 레이턴시 감소.
    """
    global _apigw_clients
    
    endpoint_url = f'https://{domain}/{stage}'
    
    if endpoint_url not in _apigw_clients:
        _apigw_clients[endpoint_url] = boto3.client(
            'apigatewaymanagementapi',
            endpoint_url=endpoint_url
        )
        logger.debug(f'Created new API Gateway client for {endpoint_url}')
    
    return _apigw_clients[endpoint_url]


def _update_last_seen(connection_id: str):
    """
    [v2.1] lastSeen 타임스탬프 업데이트.
    
    메시지 수신 시마다 호출하여 활성 연결 추적.
    Stale 커넥션 정리 시 사용.
    """
    table_name = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
    if not table_name:
        return
    
    try:
        table = dynamodb.Table(table_name)
        table.update_item(
            Key={'connectionId': connection_id},
            UpdateExpression='SET lastSeen = :ts',
            ExpressionAttributeValues={':ts': int(_time.time())}
        )
    except Exception as e:
        # lastSeen 업데이트 실패는 치명적이지 않음
        logger.debug(f'Failed to update lastSeen for {connection_id}: {e}')


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    WebSocket $default handler.
    
    Processes incoming messages from connected WebSocket clients.
    Supported actions:
      - ping: Returns pong (health check)
      - subscribe: Subscribe to execution updates
      - unsubscribe: Unsubscribe from execution updates
      - Any other action: Returns acknowledgement
    """
    try:
        logger.info('WebSocket $default invoked')
        logger.debug('Event: %s', json.dumps(event, default=str)[:2000])
        
        connection_id = event.get('requestContext', {}).get('connectionId')
        if not connection_id:
            logger.error('No connectionId in requestContext')
            return {'statusCode': 400, 'body': 'Missing connectionId'}
        
        # [v2.1] lastSeen 업데이트 (활성 연결 추적)
        _update_last_seen(connection_id)
        
        # Parse message body
        body = event.get('body', '{}')
        try:
            message = json.loads(body) if body else {}
        except json.JSONDecodeError:
            logger.warning('Invalid JSON in message body: %s', body[:500])
            message = {}
        
        action = message.get('action', 'unknown')
        logger.info('Received action: %s from connection: %s', action, connection_id)
        
        # Handle different actions
        if action == 'ping':
            return _handle_ping(connection_id, event)
        elif action == 'subscribe':
            return _handle_subscribe(connection_id, message)
        elif action == 'unsubscribe':
            return _handle_unsubscribe(connection_id, message)
        else:
            # Default: acknowledge receipt
            return _handle_default(connection_id, action, message)
            
    except Exception as e:
        logger.exception('Error in $default handler: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}


def _handle_ping(connection_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle ping action - send pong response."""
    try:
        # [v2.1] 캐싱된 API Gateway 클라이언트 사용
        domain = event.get('requestContext', {}).get('domainName')
        stage = event.get('requestContext', {}).get('stage')
        
        if domain and stage:
            apigw_client = _get_apigw_client(domain, stage)
            
            apigw_client.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps({'action': 'pong', 'timestamp': _get_timestamp()}).encode('utf-8')
            )
            logger.info('Sent pong to connection: %s', connection_id)
        
        return {'statusCode': 200, 'body': 'pong'}
    except Exception as e:
        logger.warning('Failed to send pong: %s', str(e))
        return {'statusCode': 200, 'body': 'pong'}


def _handle_subscribe(connection_id: str, message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle subscribe action - subscribe to execution updates.
    
    [v2.1] 개선사항:
    - Set(SS) 타입 사용으로 중복 구독 방지
    - 최대 구독 수 제한 (MAX_SUBSCRIPTIONS)
    - 현재 구독 수 반환
    """
    execution_id = message.get('execution_id') or message.get('executionId')
    
    if not execution_id:
        logger.warning('Subscribe without execution_id from connection: %s', connection_id)
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing execution_id'})}
    
    table_name = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
    if not table_name:
        logger.error('WEBSOCKET_CONNECTIONS_TABLE not configured')
        return {'statusCode': 500, 'body': 'Server misconfiguration'}
    
    try:
        table = dynamodb.Table(table_name)
        
        # [v2.1] 먼저 현재 구독 수 확인
        try:
            response = table.get_item(
                Key={'connectionId': connection_id},
                ProjectionExpression='subscribed_executions'
            )
            current_subs = response.get('Item', {}).get('subscribed_executions', set())
            
            # DynamoDB Set은 Python set으로 변환됨
            if isinstance(current_subs, set):
                current_count = len(current_subs)
            else:
                current_count = len(current_subs) if current_subs else 0
                
        except Exception:
            current_count = 0
        
        # [v2.1] 최대 구독 수 체크
        if current_count >= MAX_SUBSCRIPTIONS:
            logger.warning(
                f'Connection {connection_id} reached max subscriptions ({MAX_SUBSCRIPTIONS})'
            )
            return {
                'statusCode': 400, 
                'body': json.dumps({
                    'error': 'Maximum subscriptions reached',
                    'max': MAX_SUBSCRIPTIONS,
                    'current': current_count
                })
            }
        
        # [v2.1] Set(SS) 타입으로 ADD (중복 자동 제거)
        table.update_item(
            Key={'connectionId': connection_id},
            UpdateExpression='ADD subscribed_executions :exec_id',
            ExpressionAttributeValues={
                ':exec_id': {execution_id}  # Set 타입
            }
        )
        
        logger.info('Connection %s subscribed to execution %s', connection_id, execution_id)
        return {
            'statusCode': 200, 
            'body': json.dumps({
                'subscribed': execution_id,
                'subscription_count': current_count + 1
            })
        }
        
    except Exception as e:
        logger.exception('Failed to subscribe: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}


def _handle_unsubscribe(connection_id: str, message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle unsubscribe action - unsubscribe from execution updates.
    
    [v2.1] 실제 DynamoDB에서 제거 구현.
    """
    execution_id = message.get('execution_id') or message.get('executionId')
    
    if not execution_id:
        logger.warning('Unsubscribe without execution_id from connection: %s', connection_id)
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing execution_id'})}
    
    table_name = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
    if not table_name:
        logger.error('WEBSOCKET_CONNECTIONS_TABLE not configured')
        return {'statusCode': 500, 'body': 'Server misconfiguration'}
    
    try:
        table = dynamodb.Table(table_name)
        
        # [v2.1] Set에서 DELETE로 제거
        table.update_item(
            Key={'connectionId': connection_id},
            UpdateExpression='DELETE subscribed_executions :exec_id',
            ExpressionAttributeValues={
                ':exec_id': {execution_id}  # Set 타입
            }
        )
        
        logger.info('Connection %s unsubscribed from execution %s', connection_id, execution_id)
        return {'statusCode': 200, 'body': json.dumps({'unsubscribed': execution_id})}
        
    except Exception as e:
        logger.exception('Failed to unsubscribe: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}


def _handle_default(connection_id: str, action: str, message: Dict[str, Any]) -> Dict[str, Any]:
    """Handle unknown/default actions - acknowledge receipt."""
    logger.info('Received unknown action "%s" from connection %s', action, connection_id)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'acknowledged': True,
            'action': action,
            'message': 'Action received'
        })
    }


def _get_timestamp() -> str:
    """Get current timestamp in ISO format."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
