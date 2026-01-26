"""
JSON serialization utilities for DynamoDB and API responses.

Consolidates DecimalEncoder and related utilities used across Lambda handlers.
"""

import json
from decimal import Decimal
from typing import Any


class DecimalEncoder(json.JSONEncoder):
    """
    JSON Encoder that handles DynamoDB Decimal types and other special types.
    
    Converts:
    - Decimal with no fractional part -> int
    - Decimal with fractional part -> float
    - set -> list
    - Other non-serializable types -> str (fallback)
    
    Usage:
        json.dumps(data, cls=DecimalEncoder)
    """
    
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            # Integer if no fractional part, otherwise float
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, set):
            return list(obj)
        # Try str() as fallback for unknown types
        try:
            return str(obj)
        except Exception:
            return super().default(obj)


def convert_decimals(obj: Any) -> Any:
    """
    Recursively convert Decimal objects returned by DynamoDB into native Python types.
    
    Args:
        obj: Any object that may contain Decimal values
        
    Returns:
        Object with all Decimals converted to int or float
    """
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def convert_to_dynamodb_format(obj: Any) -> Any:
    """
    Convert Python objects to DynamoDB-compatible format.
    
    Converts:
    - float -> Decimal (via string to preserve precision)
    - int -> kept as-is
    - dict/list -> recursively converted
    
    Args:
        obj: Any object to convert
        
    Returns:
        Object with floats converted to Decimal
    """
    if isinstance(obj, dict):
        return {k: convert_to_dynamodb_format(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_dynamodb_format(item) for item in obj]
    elif isinstance(obj, float):
        return Decimal(str(obj))
    return obj


def dumps_decimal(obj: Any, **kwargs) -> str:
    """
    Convenience wrapper for json.dumps with DecimalEncoder.
    
    Args:
        obj: Object to serialize
        **kwargs: Additional arguments passed to json.dumps
        
    Returns:
        JSON string
    """
    return json.dumps(obj, cls=DecimalEncoder, **kwargs)


def get_ms_timestamp(value: Any) -> int:
    """
    명칭이나 형식에 상관없이 밀리초 숫자로 변환 (방어적 코딩).
    
    지원하는 형식:
    - int/float: 그대로 정수 변환
    - ISO 8601 문자열: "2026-01-26T08:12:53.732Z"
    - Unix timestamp 문자열: "1769415175"
    - 잘못된 형식: 0 반환
    
    Args:
        value: 변환할 타임스탬프 값
        
    Returns:
        밀리초 단위의 정수 타임스탬프
    """
    from datetime import datetime
    
    if value is None:
        return 0
    
    if isinstance(value, (int, float)):
        # 이미 숫자인 경우
        ts = int(value)
        # 초 단위인 경우 밀리초로 변환 (2000년 이후 기준)
        if ts < 10_000_000_000:  # 10자리 미만 = 초 단위
            ts *= 1000
        return ts
    
    if isinstance(value, str):
        # 빈 문자열이나 placeholder 문자열 처리
        if not value or value in ('epoch_ms', 'unix_ms', 'iso', 'N/A', 'null', 'None'):
            return 0
        
        try:
            # ISO 형식 문자열인 경우 (T 또는 날짜 구분자 포함)
            if 'T' in value or (len(value) > 10 and '-' in value[:10]):
                # Z를 +00:00으로 변환
                normalized = value.replace('Z', '+00:00')
                dt = datetime.fromisoformat(normalized)
                return int(dt.timestamp() * 1000)
            
            # 숫자 형태의 문자열인 경우
            ts = int(float(value))
            if ts < 10_000_000_000:
                ts *= 1000
            return ts
        except (ValueError, TypeError, OSError):
            return 0
    
    return 0
