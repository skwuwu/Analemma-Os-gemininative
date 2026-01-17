"""
Structured logging utility module
JSON structured logging and correlation ID tracking using AWS Lambda Powertools

Usage:
    from src.common.logging_utils import get_logger, log_execution_context
    
    logger = get_logger(__name__)
    
    @log_execution_context
    def lambda_handler(event, context):
        logger.info("Processing request", extra={"user_id": "123"})
"""

import os
import functools
from typing import Any, Dict, Optional
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit


# 글로벌 인스턴스 (Lambda 컨테이너 재사용 최적화)
_logger_instances: Dict[str, Logger] = {}
_tracer = Tracer()
_metrics = Metrics()


def get_logger(name: str = None, level: str = None) -> Logger:
    """
    구조화된 Logger 인스턴스를 반환합니다.
    
    Args:
        name: 로거 이름 (기본값: 호출 모듈명)
        level: 로그 레벨 (기본값: 환경변수 LOG_LEVEL 또는 INFO)
    
    Returns:
        Logger: AWS Lambda Powertools Logger 인스턴스
    """
    if name is None:
        name = __name__
    
    # 캐시된 로거 반환 (성능 최적화)
    if name in _logger_instances:
        return _logger_instances[name]
    
    # 새 로거 생성
    log_level = level or os.getenv("LOG_LEVEL", "INFO")
    logger = Logger(
        service=os.getenv("AWS_LAMBDA_FUNCTION_NAME", "analemma-backend"),
        level=log_level,
        child=True if name != __name__ else False
    )
    
    _logger_instances[name] = logger
    return logger


def get_tracer() -> Tracer:
    """X-Ray 트레이서 인스턴스를 반환합니다."""
    return _tracer


def get_metrics() -> Metrics:
    """CloudWatch 메트릭 인스턴스를 반환합니다."""
    return _metrics


def log_execution_context(func):
    """
    Lambda 핸들러에 구조화된 로깅 컨텍스트를 주입하는 데코레이터
    
    자동으로 correlation_id, request_id 등을 로그에 포함시킵니다.
    
    Usage:
        @log_execution_context
        def lambda_handler(event, context):
            logger = get_logger(__name__)
            logger.info("Processing request")
    """
    @functools.wraps(func)
    def wrapper(event, context):
        logger = get_logger(func.__module__)
        
        # Lambda 컨텍스트 정보 자동 주입
        with logger.inject_lambda_context(
            correlation_id_path=correlation_paths.API_GATEWAY_HTTP,
            log_event=True
        ):
            # 추가 컨텍스트 정보
            logger.append_keys(
                function_name=context.function_name if context else "unknown",
                function_version=context.function_version if context else "unknown",
                memory_limit=context.memory_limit_in_mb if context else "unknown"
            )
            
            return func(event, context)
    
    return wrapper


def log_external_service_call(service_name: str, operation: str):
    """
    외부 서비스 호출을 로깅하는 데코레이터
    
    Args:
        service_name: 서비스 이름 (예: "bedrock", "s3", "dynamodb")
        operation: 작업 이름 (예: "invoke_model", "put_object")
    
    Usage:
        @log_external_service_call("bedrock", "invoke_model")
        def call_bedrock_api(model, prompt):
            # API 호출 로직
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            metrics = get_metrics()
            
            # 호출 시작 로그
            logger.info(
                f"Starting {service_name} {operation}",
                extra={
                    "service": service_name,
                    "operation": operation,
                    "function": func.__name__
                }
            )
            
            try:
                result = func(*args, **kwargs)
                
                # 성공 로그 및 메트릭
                logger.info(
                    f"Completed {service_name} {operation}",
                    extra={
                        "service": service_name,
                        "operation": operation,
                        "status": "success"
                    }
                )
                metrics.add_metric(
                    name=f"{service_name}_{operation}_success",
                    unit=MetricUnit.Count,
                    value=1
                )
                
                return result
                
            except Exception as e:
                # 실패 로그 및 메트릭
                logger.error(
                    f"Failed {service_name} {operation}",
                    extra={
                        "service": service_name,
                        "operation": operation,
                        "status": "error",
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    }
                )
                metrics.add_metric(
                    name=f"{service_name}_{operation}_error",
                    unit=MetricUnit.Count,
                    value=1
                )
                raise
        
        return wrapper
    return decorator


def log_business_event(event_type: str, **context):
    """
    비즈니스 이벤트를 구조화된 형태로 로깅합니다.
    
    Args:
        event_type: 이벤트 타입 (예: "workflow_started", "execution_completed")
        **context: 추가 컨텍스트 정보
    
    Usage:
        log_business_event(
            "workflow_started",
            workflow_id="wf-123",
            owner_id="user-456",
            execution_arn="arn:aws:states:..."
        )
    """
    logger = get_logger("business_events")
    
    logger.info(
        f"Business event: {event_type}",
        extra={
            "event_type": event_type,
            "event_category": "business",
            **context
        }
    )


def log_security_event(event_type: str, severity: str = "INFO", **context):
    """
    보안 관련 이벤트를 로깅합니다.
    
    Args:
        event_type: 보안 이벤트 타입 (예: "auth_failure", "unauthorized_access")
        severity: 심각도 ("INFO", "WARN", "ERROR", "CRITICAL")
        **context: 추가 컨텍스트 정보
    """
    logger = get_logger("security_events")
    
    log_method = getattr(logger, severity.lower(), logger.info)
    log_method(
        f"Security event: {event_type}",
        extra={
            "event_type": event_type,
            "event_category": "security",
            "severity": severity,
            **context
        }
    )


# 편의 함수들
def log_workflow_event(event_type: str, workflow_id: str, owner_id: str, **context):
    """워크플로우 관련 이벤트 로깅"""
    log_business_event(
        event_type,
        workflow_id=workflow_id,
        owner_id=owner_id,
        **context
    )


def log_execution_event(event_type: str, execution_arn: str, owner_id: str, **context):
    """실행 관련 이벤트 로깅"""
    log_business_event(
        event_type,
        execution_arn=execution_arn,
        owner_id=owner_id,
        **context
    )