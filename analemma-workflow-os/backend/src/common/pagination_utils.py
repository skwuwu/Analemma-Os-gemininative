# -*- coding: utf-8 -*-
"""
Pagination Utility Functions

페이지네이션 토큰 인코딩/디코딩 공통 유틸리티입니다.
DynamoDB 쿼리 결과의 페이지네이션에 사용됩니다.

Usage:
    from src.common.pagination_utils import encode_pagination_token, decode_pagination_token
"""

import base64
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def decode_pagination_token(token: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Base64 인코딩된 페이지네이션 토큰을 디코딩
    
    Args:
        token: Base64 인코딩된 문자열 또는 None
        
    Returns:
        디코딩된 딕셔너리 (DynamoDB LastEvaluatedKey) 또는 None
        
    Example:
        token = request.query_params.get("nextToken")
        exclusive_start_key = decode_pagination_token(token)
        if exclusive_start_key:
            query_params["ExclusiveStartKey"] = exclusive_start_key
    """
    if not token:
        return None
    
    try:
        decoded_bytes = base64.b64decode(token)
        decoded_str = decoded_bytes.decode('utf-8')
        return json.loads(decoded_str)
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.debug(f"Failed to decode pagination token: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error decoding pagination token: {e}")
        return None


def encode_pagination_token(obj: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    페이지네이션 토큰을 Base64로 인코딩
    
    Args:
        obj: DynamoDB LastEvaluatedKey 딕셔너리 또는 None
        
    Returns:
        Base64 인코딩된 문자열 또는 None
        
    Example:
        last_key = response.get("LastEvaluatedKey")
        next_token = encode_pagination_token(last_key)
        return {"items": items, "nextToken": next_token}
    """
    if not obj:
        return None
    
    try:
        json_str = json.dumps(obj, ensure_ascii=False)
        encoded_bytes = base64.b64encode(json_str.encode('utf-8'))
        return encoded_bytes.decode('utf-8')
    except (TypeError, ValueError) as e:
        logger.debug(f"Failed to encode pagination token: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error encoding pagination token: {e}")
        return None
