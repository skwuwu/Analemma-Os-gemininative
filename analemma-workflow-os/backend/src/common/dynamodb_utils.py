# -*- coding: utf-8 -*-
"""
DynamoDB Utility Functions

⚠️ DEPRECATED: 이 모듈은 레거시입니다.
common/aws_clients.py의 get_dynamodb_resource(), get_dynamodb_table()을 사용하세요.

마이그레이션 가이드:
    # Before (deprecated)
    from src.common.dynamodb_utils import get_dynamodb_resource
    
    # After (recommended)
    from src.common.aws_clients import get_dynamodb_resource, get_dynamodb_table

공통 DynamoDB 리소스 관리를 위한 유틸리티입니다.
각 서비스에서 중복으로 정의되던 함수들을 통합합니다.
"""

import warnings
import boto3
from functools import lru_cache

# Deprecation 경고 발생
warnings.warn(
    "dynamodb_utils 모듈은 deprecated입니다. "
    "common/aws_clients.py의 get_dynamodb_resource()를 사용하세요.",
    DeprecationWarning,
    stacklevel=2
)


@lru_cache(maxsize=1)
def get_dynamodb_resource():
    """
    지연 초기화된 DynamoDB 리소스 반환
    
    ⚠️ DEPRECATED: aws_clients.get_dynamodb_resource()를 사용하세요.
    
    싱글톤 패턴으로 리소스를 캐싱하여 재사용합니다.
    """
    warnings.warn(
        "dynamodb_utils.get_dynamodb_resource()는 deprecated입니다. "
        "common/aws_clients.py의 get_dynamodb_resource()를 사용하세요.",
        DeprecationWarning,
        stacklevel=2
    )
    return boto3.resource('dynamodb')


def get_dynamodb_table(table_name: str):
    """
    지정된 테이블 이름으로 DynamoDB Table 객체 반환
    
    ⚠️ DEPRECATED: aws_clients.get_dynamodb_table()을 사용하세요.
    
    Args:
        table_name: DynamoDB 테이블 이름
        
    Returns:
        boto3 DynamoDB Table 객체
    """
    warnings.warn(
        "dynamodb_utils.get_dynamodb_table()은 deprecated입니다. "
        "common/aws_clients.py의 get_dynamodb_table()을 사용하세요.",
        DeprecationWarning,
        stacklevel=2
    )
    return get_dynamodb_resource().Table(table_name)
