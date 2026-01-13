"""
WebSocket 관련 공통 유틸리티 함수들
중복 코드를 제거하고 재사용성을 높이기 위한 모듈
DynamoDB Decimal 타입 직렬화 문제 해결 포함
"""

import os
import json
import logging
import boto3
from decimal import Decimal
from typing import List, Optional, Any, Union
from botocore.exceptions import ClientError

from .aws_clients import get_dynamodb_resource
from .json_utils import DecimalEncoder  # 통합된 DecimalEncoder 사용

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 글로벌 캐시: API Gateway 클라이언트 재사용
_apigw_clients = {}

# 글로벌 캐시: DynamoDB 테이블
_connections_table = None
_dynamodb_resource = None

# WebSocket 페이로드 제한 (실시간 데이터 경량화)
MAX_CURRENT_THOUGHT_LENGTH = 200  # 타이핑 애니메이션용 최대 글자 수
MAX_WEBSOCKET_PAYLOAD_BYTES = 32 * 1024  # 32KB (API Gateway 한도: 128KB)

# NOTE: DecimalEncoder는 common.json_utils에서 import됨 (중복 제거됨)


def get_connections_table():
    """WebSocket 연결 테이블 싱글톤"""
    global _connections_table
    if _connections_table is None:
        table_name = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
        if not table_name:
            logger.error("환경변수 WEBSOCKET_CONNECTIONS_TABLE이 설정되지 않았습니다.")
            return None

        try:
            _connections_table = get_dynamodb_resource().Table(table_name)
        except Exception as e:
            logger.error(f"WebSocket 연결 테이블 초기화 실패: {e}")
            return None
    return _connections_table


def get_websocket_gsi():
    """WebSocket GSI 이름"""
    return os.environ.get('WEBSOCKET_OWNER_ID_GSI', 'ownerId-index')


def get_apigateway_client(endpoint_url: Optional[str] = None) -> Optional[Any]:
    """
    API Gateway Management API 클라이언트 캐싱

    Args:
        endpoint_url: WebSocket 엔드포인트 URL (없으면 환경변수에서 가져옴)

    Returns:
        boto3 API Gateway 클라이언트
    """
    if not endpoint_url:
        endpoint_url = os.environ.get('WEBSOCKET_ENDPOINT_URL')

    if not endpoint_url:
        logger.warning("WebSocket 엔드포인트 URL이 설정되지 않음")
        return None

    if endpoint_url not in _apigw_clients:
        try:
            # endpoint_url 프로토콜 보정
            if not endpoint_url.startswith("https://") and not endpoint_url.startswith("http://"):
                formatted_url = "https://" + endpoint_url
            else:
                formatted_url = endpoint_url

            _apigw_clients[endpoint_url] = boto3.client(
                'apigatewaymanagementapi',
                endpoint_url=formatted_url.rstrip('/')
            )
            logger.debug(f"API Gateway 클라이언트 생성: {formatted_url}")
        except Exception as e:
            logger.error(f"API Gateway 클라이언트 생성 실패: {e}")
            return None

    return _apigw_clients[endpoint_url]


def get_connections_for_owner(owner_id: str) -> List[str]:
    """
    DynamoDB GSI를 쿼리하여 ownerId에 매핑된 모든 connectionId를 반환

    Args:
        owner_id: 사용자 ID

    Returns:
        활성 connection ID 리스트
    """
    table = get_connections_table()
    gsi_name = get_websocket_gsi()

    if not table or not owner_id:
        logger.debug(f"WebSocket 연결 조회 조건 불충분: table={bool(table)}, owner_id={bool(owner_id)}")
        return []

    try:
        from boto3.dynamodb.conditions import Key
        response = table.query(
            IndexName=gsi_name,
            KeyConditionExpression=Key('ownerId').eq(owner_id)
        )
        connection_ids = [item['connectionId'] for item in response.get('Items', []) if 'connectionId' in item]
        logger.debug(f"사용자 {owner_id}의 활성 연결 수: {len(connection_ids)}")
        return connection_ids
    except ClientError as e:
        logger.error(f"WebSocket 연결 조회 실패 (owner_id: {owner_id}): {e}")
        return []
    except Exception as e:
        logger.exception(f"WebSocket 연결 조회 중 예상치 못한 오류 (owner_id: {owner_id}): {e}")
        return []


def send_to_connection(connection_id: str, data: Any, endpoint_url: Optional[str] = None) -> bool:
    """
    단일 WebSocket 연결로 메시지 전송

    Args:
        connection_id: WebSocket 연결 ID
        data: 전송할 데이터 (dict 또는 str)
        endpoint_url: WebSocket 엔드포인트 URL

    Returns:
        전송 성공 여부
    """
    client = get_apigateway_client(endpoint_url)
    if not client or not connection_id:
        return False

    try:
        # 데이터가 dict면 실시간 필드 경량화 후 JSON 변환
        if isinstance(data, dict):
            data = _truncate_realtime_fields(data)
            data = json.dumps(data, cls=DecimalEncoder, ensure_ascii=False)
        
        # 페이로드 크기 검증
        payload_bytes = data.encode('utf-8') if isinstance(data, str) else data
        if len(payload_bytes) > MAX_WEBSOCKET_PAYLOAD_BYTES:
            logger.warning(
                f"WebSocket payload too large: {len(payload_bytes)} bytes > {MAX_WEBSOCKET_PAYLOAD_BYTES}. Truncating."
            )
            # 긴급 압축: 필수 필드만 유지
            data = _emergency_compress_payload(data)
            payload_bytes = data.encode('utf-8') if isinstance(data, str) else data

        client.post_to_connection(
            ConnectionId=connection_id,
            Data=payload_bytes
        )
        return True
    except client.exceptions.GoneException:
        logger.info(f"연결이 이미 종료됨: {connection_id}")
        # 여기서 연결 정리 로직을 호출할 수 있음
        cleanup_stale_connection(connection_id)
        return False
    except ClientError as e:
        logger.error(f"WebSocket 메시지 전송 실패 (connection_id: {connection_id}): {e}")
        return False
    except Exception as e:
        logger.exception(f"WebSocket 메시지 전송 중 예상치 못한 오류 (connection_id: {connection_id}): {e}")
        return False


def _truncate_realtime_fields(data: dict) -> dict:
    """
    실시간 전송 필드 경량화
    
    current_thought 등 실시간 업데이트 필드의 길이를 제한하여
    네트워크 부하를 줄이고 타이핑 애니메이션의 부드러움을 유지합니다.
    """
    result = dict(data)
    
    # current_thought 경량화
    if "current_thought" in result and isinstance(result["current_thought"], str):
        if len(result["current_thought"]) > MAX_CURRENT_THOUGHT_LENGTH:
            result["current_thought"] = result["current_thought"][:MAX_CURRENT_THOUGHT_LENGTH] + "..."
            result["_thought_truncated"] = True
    
    # thought_history에서 최신 3개만 전송 (전체 히스토리는 API 조회)
    if "thought_history" in result and isinstance(result["thought_history"], list):
        if len(result["thought_history"]) > 3:
            result["thought_history"] = result["thought_history"][-3:]
            result["_history_truncated"] = True
    
    # artifacts에서 preview_content 제거 (썸네일만 유지)
    if "artifacts" in result and isinstance(result["artifacts"], list):
        for artifact in result["artifacts"]:
            if isinstance(artifact, dict) and "preview_content" in artifact:
                if artifact["preview_content"] and len(artifact["preview_content"]) > 100:
                    artifact["preview_content"] = artifact["preview_content"][:100] + "..."
    
    return result


def _emergency_compress_payload(data: Union[str, dict]) -> str:
    """
    긴급 페이로드 압축
    
    페이로드가 너무 클 때 필수 필드만 유지하여 전송합니다.
    """
    try:
        if isinstance(data, str):
            data = json.loads(data)
        
        # 필수 필드만 추출
        essential_fields = {
            "task_id": data.get("task_id"),
            "status": data.get("status"),
            "progress_percentage": data.get("progress_percentage"),
            "current_thought": data.get("current_thought", "")[:100] + "..." if data.get("current_thought") else "",
            "updated_at": data.get("updated_at"),
            "_payload_compressed": True,
        }
        
        return json.dumps(essential_fields, cls=DecimalEncoder, ensure_ascii=False)
        
    except Exception as e:
        logger.error(f"Emergency compression failed: {e}")
        return json.dumps({"error": "payload_too_large", "_compressed": True})


def broadcast_to_connections(connection_ids: List[str], data: Any, endpoint_url: Optional[str] = None) -> int:
    """
    여러 WebSocket 연결로 메시지 브로드캐스트

    Args:
        connection_ids: WebSocket 연결 ID 리스트
        data: 전송할 데이터
        endpoint_url: WebSocket 엔드포인트 URL

    Returns:
        성공적으로 전송된 메시지 수
    """
    if not connection_ids:
        return 0

    # JSON 변환을 루프 밖에서 한 번만 수행 (성능 최적화)
    if not isinstance(data, str):
        try:
            data = json.dumps(data, cls=DecimalEncoder, ensure_ascii=False)
        except Exception as e:
            logger.error(f"데이터 JSON 변환 실패: {e}")
            return 0

    success_count = 0
    for connection_id in connection_ids:
        if send_to_connection(connection_id, data, endpoint_url):
            success_count += 1

    logger.info(f"WebSocket 브로드캐스트 완료: {success_count}/{len(connection_ids)} 성공")
    return success_count


def cleanup_stale_connection(connection_id: str):
    """
    끊긴 WebSocket 연결 정리

    Args:
        connection_id: 정리할 연결 ID
    """
    table = get_connections_table()
    if not table or not connection_id:
        return

    try:
        table.delete_item(Key={'connectionId': connection_id})
        logger.info(f"오래된 연결 정리 완료: {connection_id}")
    except Exception as e:
        logger.warning(f"오래된 연결 정리 실패 (connection_id: {connection_id}): {e}")


def notify_user(owner_id: str, data: Any, endpoint_url: Optional[str] = None) -> bool:
    """
    특정 사용자에게 WebSocket 알림 전송

    Args:
        owner_id: 사용자 ID
        data: 전송할 데이터
        endpoint_url: WebSocket 엔드포인트 URL

    Returns:
        알림 전송 성공 여부 (적어도 하나의 연결에 성공)
    """
    connection_ids = get_connections_for_owner(owner_id)
    if not connection_ids:
        logger.debug(f"사용자 {owner_id}의 활성 WebSocket 연결이 없음")
        return False

    success_count = broadcast_to_connections(connection_ids, data, endpoint_url)
    return success_count > 0