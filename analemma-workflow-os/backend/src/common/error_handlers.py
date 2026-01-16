"""
외부 서비스 예외 처리 유틸리티
AWS 서비스 및 외부 API 호출 시 발생하는 예외를 세분화하여 처리

사용법:
    from src.common.error_handlers import handle_dynamodb_error, handle_s3_error
    
    try:
        table.put_item(Item=item)
    except ClientError as e:
        raise handle_dynamodb_error(e, operation="put_item", table_name="MyTable")
"""

import json
from typing import Any, Dict, Optional
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from src.common.exceptions import (
    ExternalServiceError, LLMServiceError, S3OperationError,
    ValidationError, AuthenticationError, RateLimitExceededError,
    QuotaExceededError
)
from src.common.logging_utils import get_logger

logger = get_logger(__name__)


def handle_dynamodb_error(
    error: ClientError, 
    operation: str, 
    table_name: str = None,
    item_key: Dict = None
) -> Exception:
    """
    DynamoDB ClientError를 구체적인 예외로 변환
    
    Args:
        error: boto3 ClientError
        operation: 수행하려던 작업 (예: "put_item", "query")
        table_name: 테이블 이름
        item_key: 아이템 키 (선택적)
    
    Returns:
        Exception: 적절한 커스텀 예외
    """
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']
    
    # 컨텍스트 정보 구성
    context = {
        "service": "dynamodb",
        "operation": operation,
        "error_code": error_code,
        "table_name": table_name,
        "item_key": item_key
    }
    
    logger.error(f"DynamoDB {operation} failed", extra=context)
    
    # 에러 코드별 세분화 처리
    if error_code == 'ThrottlingException':
        return RateLimitExceededError(
            f"DynamoDB throttling on {operation}",
            retry_after=5
        )
    elif error_code == 'ProvisionedThroughputExceededException':
        return RateLimitExceededError(
            f"DynamoDB throughput exceeded on {table_name}",
            retry_after=10
        )
    elif error_code == 'ValidationException':
        return ValidationError(f"DynamoDB validation error: {error_message}")
    elif error_code == 'ConditionalCheckFailedException':
        return ValidationError(f"DynamoDB conditional check failed on {operation}")
    elif error_code == 'ResourceNotFoundException':
        return ValidationError(f"DynamoDB table not found: {table_name}")
    elif error_code == 'AccessDeniedException':
        return AuthenticationError(f"DynamoDB access denied for {operation}")
    else:
        return ExternalServiceError("dynamodb", f"{error_code}: {error_message}")


def handle_s3_error(
    error: ClientError,
    operation: str,
    bucket: str = None,
    key: str = None
) -> Exception:
    """
    S3 ClientError를 구체적인 예외로 변환
    
    Args:
        error: boto3 ClientError
        operation: 수행하려던 작업 (예: "get_object", "put_object")
        bucket: S3 버킷 이름
        key: S3 객체 키
    
    Returns:
        Exception: 적절한 커스텀 예외
    """
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']
    
    context = {
        "service": "s3",
        "operation": operation,
        "error_code": error_code,
        "bucket": bucket,
        "key": key
    }
    
    logger.error(f"S3 {operation} failed", extra=context)
    
    if error_code == 'NoSuchBucket':
        return S3OperationError(operation, bucket, key, f"Bucket not found: {bucket}")
    elif error_code == 'NoSuchKey':
        return S3OperationError(operation, bucket, key, f"Object not found: {key}")
    elif error_code == 'AccessDenied':
        return AuthenticationError(f"S3 access denied for {operation}")
    elif error_code == 'SlowDown':
        return RateLimitExceededError(f"S3 rate limit exceeded", retry_after=5)
    elif error_code == 'ServiceUnavailable':
        return ExternalServiceError("s3", f"S3 service unavailable: {error_message}")
    else:
        return S3OperationError(operation, bucket, key, f"{error_code}: {error_message}")


def handle_stepfunctions_error(
    error: ClientError,
    operation: str,
    execution_arn: str = None
) -> Exception:
    """
    Step Functions ClientError를 구체적인 예외로 변환
    """
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']
    
    context = {
        "service": "stepfunctions",
        "operation": operation,
        "error_code": error_code,
        "execution_arn": execution_arn
    }
    
    logger.error(f"Step Functions {operation} failed", extra=context)
    
    if error_code == 'ExecutionDoesNotExist':
        return ValidationError(f"Execution not found: {execution_arn}")
    elif error_code == 'ExecutionAlreadyExists':
        return ValidationError(f"Execution already exists: {execution_arn}")
    elif error_code == 'InvalidArn':
        return ValidationError(f"Invalid execution ARN: {execution_arn}")
    elif error_code == 'AccessDeniedException':
        return AuthenticationError(f"Step Functions access denied for {operation}")
    else:
        return ExternalServiceError("stepfunctions", f"{error_code}: {error_message}")


def handle_bedrock_error(
    error: ClientError,
    model: str = None,
    operation: str = "invoke_model"
) -> Exception:
    """
    Bedrock ClientError를 구체적인 예외로 변환
    """
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']
    
    context = {
        "service": "bedrock",
        "operation": operation,
        "error_code": error_code,
        "model": model
    }
    
    logger.error(f"Bedrock {operation} failed", extra=context)
    
    if error_code == 'ThrottlingException':
        return RateLimitExceededError(f"Bedrock rate limit exceeded for {model}")
    elif error_code == 'ValidationException':
        return ValidationError(f"Bedrock validation error: {error_message}")
    elif error_code == 'AccessDeniedException':
        return AuthenticationError(f"Bedrock access denied for model {model}")
    elif error_code == 'ResourceNotFoundException':
        return ValidationError(f"Bedrock model not found: {model}")
    elif error_code == 'ServiceQuotaExceededException':
        return QuotaExceededError("bedrock", f"Service quota exceeded for {model}")
    else:
        return LLMServiceError("bedrock", f"{error_code}: {error_message}")


def handle_llm_api_error(
    error: Exception,
    provider: str,
    model: str = None,
    operation: str = "api_call"
) -> Exception:
    """
    외부 LLM API (OpenAI, Anthropic 등) 에러를 처리
    
    Args:
        error: 원본 예외
        provider: LLM 제공자 ("openai", "anthropic", "google")
        model: 모델 이름
        operation: 작업 이름
    """
    context = {
        "service": f"llm_{provider}",
        "operation": operation,
        "model": model,
        "error_type": type(error).__name__,
        "error_message": str(error)
    }
    
    logger.error(f"LLM API {operation} failed", extra=context)
    
    # OpenAI 에러 처리
    if provider == "openai":
        error_str = str(error).lower()
        if "rate limit" in error_str or "429" in error_str:
            return RateLimitExceededError(f"OpenAI rate limit exceeded for {model}")
        elif "quota" in error_str or "insufficient" in error_str:
            return QuotaExceededError("openai", f"OpenAI quota exceeded for {model}")
        elif "invalid" in error_str or "400" in error_str:
            return ValidationError(f"OpenAI validation error: {error}")
        elif "unauthorized" in error_str or "401" in error_str:
            return AuthenticationError(f"OpenAI authentication failed")
    
    # Anthropic 에러 처리
    elif provider == "anthropic":
        error_str = str(error).lower()
        if "rate_limit" in error_str or "429" in error_str:
            return RateLimitExceededError(f"Anthropic rate limit exceeded for {model}")
        elif "overloaded" in error_str or "529" in error_str:
            return ExternalServiceError("anthropic", "Service overloaded")
        elif "invalid" in error_str or "400" in error_str:
            return ValidationError(f"Anthropic validation error: {error}")
    
    # 일반적인 LLM 서비스 에러
    return LLMServiceError(provider, str(error))


def handle_network_error(error: Exception, service: str, operation: str) -> Exception:
    """
    네트워크 관련 에러 처리 (연결 실패, 타임아웃 등)
    """
    context = {
        "service": service,
        "operation": operation,
        "error_type": type(error).__name__,
        "error_message": str(error)
    }
    
    logger.error(f"Network error for {service}", extra=context)
    
    if isinstance(error, EndpointConnectionError):
        return ExternalServiceError(service, f"Connection failed: {error}")
    elif isinstance(error, NoCredentialsError):
        return AuthenticationError(f"AWS credentials not found for {service}")
    elif "timeout" in str(error).lower():
        return ExternalServiceError(service, f"Request timeout: {error}")
    elif "connection" in str(error).lower():
        return ExternalServiceError(service, f"Connection error: {error}")
    else:
        return ExternalServiceError(service, f"Network error: {error}")


def safe_external_call(func, *args, **kwargs):
    """
    외부 서비스 호출을 안전하게 래핑하는 헬퍼 함수
    
    Usage:
        result = safe_external_call(
            lambda: table.put_item(Item=item),
            error_handler=lambda e: handle_dynamodb_error(e, "put_item", "MyTable")
        )
    """
    error_handler = kwargs.pop('error_handler', None)
    
    try:
        return func(*args, **kwargs)
    except ClientError as e:
        if error_handler:
            raise error_handler(e)
        else:
            raise ExternalServiceError("unknown", str(e))
    except (EndpointConnectionError, NoCredentialsError) as e:
        raise handle_network_error(e, "unknown", "unknown")
    except Exception as e:
        if error_handler:
            raise error_handler(e)
        else:
            raise


def handle_lambda_error(error: Exception) -> Dict[str, Any]:
    """
    Lambda 예외를 표준화된 API 응답으로 변환

    Args:
        error: 발생한 예외

    Returns:
        API Gateway 호환 응답 딕셔너리
    """
    from .http_utils import build_response
    from .exceptions import BaseAnalemmaError

    if isinstance(error, BaseAnalemmaError):
        return build_response(error.status_code, {"error": error.message})

    # 예상치 못한 에러는 500으로 처리
    logger.error(f"Unexpected error in Lambda handler: {error}", exc_info=True)
    return build_response(500, {"error": "Internal server error"})
