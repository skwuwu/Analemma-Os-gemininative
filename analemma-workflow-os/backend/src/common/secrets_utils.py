# -*- coding: utf-8 -*-
"""
Secrets Manager Utility Functions

AWS Secrets Manager에서 API 키 및 자격 증명을 조회하는 공통 유틸리티입니다.
각 서비스에서 중복으로 정의되던 비밀 조회 로직을 통합합니다.

Usage:
    from src.common.secrets_utils import get_secret_value, get_api_key
"""

import os
import json
import logging
from functools import lru_cache
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Lazy-initialized Secrets Manager client
_secrets_client = None


def _get_secrets_client():
    """Secrets Manager 클라이언트 싱글톤"""
    global _secrets_client
    if _secrets_client is None:
        try:
            from .aws_clients import get_secrets_client
            _secrets_client = get_secrets_client()
        except (ImportError, AttributeError):
            import boto3
            region = os.environ.get("AWS_REGION", "us-east-1")
            _secrets_client = boto3.client("secretsmanager", region_name=region)
            logger.debug("Secrets Manager client initialized (fallback)")
    return _secrets_client


@lru_cache(maxsize=16)
def get_secret_value(secret_name: str) -> Optional[Dict[str, Any]]:
    """
    Secrets Manager에서 시크릿 값을 조회 (캐싱됨)
    
    Args:
        secret_name: 시크릿 이름 또는 ARN
        
    Returns:
        파싱된 시크릿 딕셔너리 또는 None
        
    Note:
        결과가 lru_cache로 캐싱되므로 Lambda 수명 동안 재사용됩니다.
        시크릿 로테이션 시 Lambda를 재시작해야 합니다.
    """
    try:
        client = _get_secrets_client()
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get("SecretString", "{}")
        return json.loads(secret_string)
    except json.JSONDecodeError as e:
        logger.warning(f"Secret {secret_name} is not valid JSON: {e}")
        return None
    except Exception as e:
        logger.warning(f"Failed to get secret {secret_name}: {e}")
        return None


def get_api_key(
    secret_name: str,
    key_field: str = "api_key",
    env_var_override: Optional[str] = None
) -> Optional[str]:
    """
    API 키 조회 (환경변수 우선, Secrets Manager 폴백)
    
    Args:
        secret_name: Secrets Manager 시크릿 이름
        key_field: 시크릿 JSON 내의 키 필드명 (기본: "api_key")
        env_var_override: 환경변수 우선 조회할 변수명 (옵션)
        
    Returns:
        API 키 문자열 또는 None
        
    Usage:
        # Gemini API 키
        api_key = get_api_key(
            "backend-workflow-dev-gemini_api_key",
            env_var_override="GOOGLE_API_KEY"
        )
        
        # OpenAI API 키
        api_key = get_api_key(
            "backend-workflow-dev-openai_api_key",
            env_var_override="OPENAI_API_KEY"
        )
    """
    # 1. 환경변수에서 우선 조회
    if env_var_override:
        env_value = os.environ.get(env_var_override)
        if env_value:
            return env_value
    
    # 2. Secrets Manager에서 조회
    secret = get_secret_value(secret_name)
    if secret:
        return secret.get(key_field)
    
    return None


def get_gemini_api_key() -> Optional[str]:
    """
    Gemini API 키 조회 (편의 함수)
    
    환경변수 GOOGLE_API_KEY 우선, 없으면 Secrets Manager에서 조회
    """
    secret_name = os.environ.get(
        "GEMINI_SECRET_NAME",
        "backend-workflow-dev-gemini_api_key"
    )
    return get_api_key(
        secret_name=secret_name,
        key_field="api_key",
        env_var_override="GOOGLE_API_KEY"
    )


def clear_cache():
    """시크릿 캐시 초기화 (테스트용)"""
    get_secret_value.cache_clear()
