# -*- coding: utf-8 -*-
"""
Pagination Utility Functions

Production-grade pagination token encoding/decoding utility.
Used for pagination of DynamoDB query results.

Features:
    - Automatic handling of DynamoDB Decimal types
    - URL-Safe Base64 encoding (query parameter safe)
    - Token expiration support
    - HMAC integrity verification (tamper prevention)

Usage:
    from src.common.pagination_utils import encode_pagination_token, decode_pagination_token
    
    # Basic usage (no expiration/integrity verification)
    token = encode_pagination_token(last_evaluated_key)
    key = decode_pagination_token(token)
    
    # Enhanced security usage (expiration + integrity verification)
    token = encode_pagination_token(last_evaluated_key, ttl_seconds=3600)
    key = decode_pagination_token(token, verify_integrity=True)
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# 환경 변수에서 HMAC 시크릿 로드 (없으면 기본값 사용, 프로덕션에서는 반드시 설정)
_HMAC_SECRET_KEY = os.environ.get("PAGINATION_HMAC_SECRET", "analemma-default-pagination-secret").encode('utf-8')

# 토큰 기본 TTL (초) - 0이면 만료 없음
DEFAULT_TOKEN_TTL_SECONDS = 0


class DynamoDBEncoder(json.JSONEncoder):
    """
    DynamoDB 타입을 JSON 직렬화하기 위한 커스텀 인코더.
    
    DynamoDB는 숫자를 Decimal 타입으로 반환하며,
    표준 json.dumps는 이를 처리하지 못합니다.
    """
    
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            # 정수인 경우 int로, 아니면 float로 변환
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode('utf-8')
        return super().default(obj)


def _generate_hmac_signature(payload: str) -> str:
    """페이로드에 대한 HMAC-SHA256 서명 생성"""
    signature = hmac.new(
        _HMAC_SECRET_KEY,
        payload.encode('utf-8'),
        hashlib.sha256
    ).digest()
    # URL-safe base64로 인코딩하고 앞 8바이트만 사용 (충분한 보안 + 토큰 길이 최적화)
    return base64.urlsafe_b64encode(signature[:8]).decode('utf-8').rstrip('=')


def _verify_hmac_signature(payload: str, signature: str) -> bool:
    """HMAC 서명 검증"""
    expected = _generate_hmac_signature(payload)
    return hmac.compare_digest(expected, signature)


def encode_pagination_token(
    obj: Optional[Dict[str, Any]],
    ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
    include_integrity: bool = True
) -> Optional[str]:
    """
    페이지네이션 토큰을 URL-Safe Base64로 인코딩
    
    Args:
        obj: DynamoDB LastEvaluatedKey 딕셔너리 또는 None
        ttl_seconds: 토큰 만료 시간 (초). 0이면 만료 없음.
        include_integrity: HMAC 무결성 서명 포함 여부
        
    Returns:
        URL-Safe Base64 인코딩된 문자열 또는 None
        
    Example:
        # 기본 사용 (무결성 검증 포함, 만료 없음)
        last_key = response.get("LastEvaluatedKey")
        next_token = encode_pagination_token(last_key)
        
        # 1시간 후 만료되는 토큰
        next_token = encode_pagination_token(last_key, ttl_seconds=3600)
        
        # 레거시 호환 (무결성 검증 없음)
        next_token = encode_pagination_token(last_key, include_integrity=False)
    """
    if not obj:
        return None
    
    try:
        # 토큰 페이로드 구성
        token_payload: Dict[str, Any] = {
            "k": obj,  # key (LastEvaluatedKey)
            "v": 2,    # version (토큰 포맷 버전)
        }
        
        # TTL이 설정된 경우 만료 시간 추가
        if ttl_seconds > 0:
            token_payload["e"] = int(time.time()) + ttl_seconds  # expiration timestamp
        
        # DynamoDB Decimal 타입 처리를 위해 커스텀 인코더 사용
        json_str = json.dumps(token_payload, cls=DynamoDBEncoder, ensure_ascii=False, separators=(',', ':'))
        
        # URL-Safe Base64 인코딩 (+ -> -, / -> _, 패딩 제거)
        encoded = base64.urlsafe_b64encode(json_str.encode('utf-8')).decode('utf-8').rstrip('=')
        
        # 무결성 서명 추가
        if include_integrity:
            signature = _generate_hmac_signature(encoded)
            return f"{encoded}.{signature}"
        
        return encoded
        
    except (TypeError, ValueError) as e:
        logger.debug(f"Failed to encode pagination token: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error encoding pagination token: {e}")
        return None


def decode_pagination_token(
    token: Optional[str],
    verify_integrity: bool = False,
    allow_expired: bool = False
) -> Optional[Dict[str, Any]]:
    """
    URL-Safe Base64 인코딩된 페이지네이션 토큰을 디코딩
    
    Args:
        token: URL-Safe Base64 인코딩된 문자열 또는 None
        verify_integrity: HMAC 무결성 검증 수행 여부 (기본 False - 레거시 호환)
        allow_expired: 만료된 토큰 허용 여부
        
    Returns:
        디코딩된 딕셔너리 (DynamoDB LastEvaluatedKey) 또는 None
        
    Example:
        token = request.query_params.get("nextToken")
        exclusive_start_key = decode_pagination_token(token)
        if exclusive_start_key:
            query_params["ExclusiveStartKey"] = exclusive_start_key
            
        # 보안 강화 모드 (무결성 검증)
        key = decode_pagination_token(token, verify_integrity=True)
        
        # 만료된 토큰도 허용 (디버깅용)
        key = decode_pagination_token(token, allow_expired=True)
    """
    if not token:
        return None
    
    try:
        payload = token
        
        # 무결성 서명 검증 (토큰에 '.'이 포함된 경우)
        if '.' in token:
            parts = token.rsplit('.', 1)
            if len(parts) == 2:
                payload, signature = parts
                if verify_integrity and not _verify_hmac_signature(payload, signature):
                    logger.warning("Pagination token integrity check failed - possible tampering")
                    return None
        elif verify_integrity:
            # 서명이 없는데 검증이 필요한 경우 - 레거시 토큰일 수 있음
            logger.debug("Token has no integrity signature, attempting legacy decode")
        
        # URL-Safe Base64 디코딩 (패딩 복원)
        padding = 4 - (len(payload) % 4)
        if padding != 4:
            payload += '=' * padding
        
        decoded_bytes = base64.urlsafe_b64decode(payload)
        decoded_str = decoded_bytes.decode('utf-8')
        token_data = json.loads(decoded_str)
        
        # 토큰 버전 확인
        version = token_data.get("v", 1)
        
        if version >= 2:
            # V2+ 토큰: 만료 시간 검증
            expiration = token_data.get("e")
            if expiration and not allow_expired:
                if int(time.time()) > expiration:
                    logger.info(f"Pagination token expired at {expiration}")
                    return None
            
            # LastEvaluatedKey 반환
            return token_data.get("k")
        else:
            # V1 레거시 토큰: 전체 페이로드가 LastEvaluatedKey
            return token_data
            
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.debug(f"Failed to decode pagination token: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error decoding pagination token: {e}")
        return None


# ============================================================================
# 레거시 호환성 함수 (Deprecated - 기존 코드 호환용)
# ============================================================================

def encode_pagination_token_legacy(obj: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    [Deprecated] 레거시 Base64 인코딩 (URL-unsafe, 무결성 없음)
    
    기존 코드 호환성을 위해 유지. 새 코드에서는 encode_pagination_token 사용 권장.
    """
    if not obj:
        return None
    
    try:
        json_str = json.dumps(obj, cls=DynamoDBEncoder, ensure_ascii=False)
        encoded_bytes = base64.b64encode(json_str.encode('utf-8'))
        return encoded_bytes.decode('utf-8')
    except Exception as e:
        logger.debug(f"Failed to encode legacy pagination token: {e}")
        return None


def decode_pagination_token_legacy(token: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    [Deprecated] 레거시 Base64 디코딩
    
    기존 코드 호환성을 위해 유지. 새 코드에서는 decode_pagination_token 사용 권장.
    """
    if not token:
        return None
    
    try:
        decoded_bytes = base64.b64decode(token)
        decoded_str = decoded_bytes.decode('utf-8')
        return json.loads(decoded_str)
    except Exception as e:
        logger.debug(f"Failed to decode legacy pagination token: {e}")
        return None
