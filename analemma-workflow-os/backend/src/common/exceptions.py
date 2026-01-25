"""
공통 예외 클래스 모듈
백엔드 전체에서 일관된 예외 처리를 위한 커스텀 예외 클래스들

사용법:
    from src.common.exceptions import (
        ExecutionNotFound, ExecutionForbidden, WorkflowNotFound,
        AuthenticationError, ValidationError
    )
"""


class BaseAnalemmaError(Exception):
    """모든 Analemma 커스텀 예외의 기본 클래스"""
    
    def __init__(self, message: str = None, status_code: int = 500):
        self.message = message or self.__class__.__doc__ or "An error occurred"
        self.status_code = status_code
        super().__init__(self.message)
    
    def to_dict(self):
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "statusCode": self.status_code
        }


# ============================================================
# 실행(Execution) 관련 예외
# ============================================================

class ExecutionNotFound(BaseAnalemmaError):
    """실행을 찾을 수 없습니다"""
    
    def __init__(self, execution_id: str = None):
        message = f"Execution not found: {execution_id}" if execution_id else "Execution not found"
        super().__init__(message, status_code=404)
        self.execution_id = execution_id


class ExecutionForbidden(BaseAnalemmaError):
    """해당 실행에 대한 접근 권한이 없습니다"""
    
    def __init__(self, execution_id: str = None, owner_id: str = None):
        message = "Access denied to this execution"
        if execution_id:
            message = f"Access denied to execution: {execution_id}"
        super().__init__(message, status_code=403)
        self.execution_id = execution_id
        self.owner_id = owner_id


class ExecutionAlreadyExists(BaseAnalemmaError):
    """동일한 실행이 이미 존재합니다 (멱등성 충돌)"""
    
    def __init__(self, execution_id: str = None):
        message = f"Execution already exists: {execution_id}" if execution_id else "Execution already exists"
        super().__init__(message, status_code=409)
        self.execution_id = execution_id


# ============================================================
# 워크플로우(Workflow) 관련 예외
# ============================================================

class WorkflowNotFound(BaseAnalemmaError):
    """워크플로우를 찾을 수 없습니다"""
    
    def __init__(self, workflow_id: str = None, workflow_name: str = None):
        if workflow_id:
            message = f"Workflow not found: {workflow_id}"
        elif workflow_name:
            message = f"Workflow not found with name: {workflow_name}"
        else:
            message = "Workflow not found"
        super().__init__(message, status_code=404)
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name


class WorkflowValidationError(BaseAnalemmaError):
    """워크플로우 구조가 유효하지 않습니다"""
    
    def __init__(self, message: str = None, details: dict = None):
        super().__init__(message or "Invalid workflow structure", status_code=400)
        self.details = details or {}


# ============================================================
# 인증/인가 관련 예외
# ============================================================

class AuthenticationError(BaseAnalemmaError):
    """인증에 실패했습니다"""
    
    def __init__(self, message: str = None):
        super().__init__(message or "Authentication failed", status_code=401)


class AuthorizationError(BaseAnalemmaError):
    """권한이 없습니다"""
    
    def __init__(self, message: str = None, resource: str = None):
        msg = message or "Access denied"
        if resource:
            msg = f"Access denied to resource: {resource}"
        super().__init__(msg, status_code=403)
        self.resource = resource


class TokenExpiredError(AuthenticationError):
    """토큰이 만료되었습니다"""
    
    def __init__(self):
        super().__init__("Token has expired")


class InvalidTokenError(AuthenticationError):
    """토큰이 유효하지 않습니다"""
    
    def __init__(self, reason: str = None):
        message = f"Invalid token: {reason}" if reason else "Invalid token"
        super().__init__(message)


# ============================================================
# 입력 검증 관련 예외
# ============================================================

class ValidationError(BaseAnalemmaError):
    """입력 데이터가 유효하지 않습니다"""
    
    def __init__(self, message: str = None, field: str = None, details: dict = None):
        if field:
            msg = f"Validation error on field '{field}': {message}"
        else:
            msg = message or "Validation error"
        super().__init__(msg, status_code=400)
        self.field = field
        self.details = details or {}


class MissingRequiredFieldError(ValidationError):
    """필수 필드가 누락되었습니다"""
    
    def __init__(self, field: str):
        super().__init__(f"Required field '{field}' is missing", field=field)


# ============================================================
# 외부 서비스 관련 예외
# ============================================================

class ExternalServiceError(BaseAnalemmaError):
    """외부 서비스 호출에 실패했습니다"""
    
    def __init__(self, service_name: str, message: str = None):
        msg = f"External service error ({service_name}): {message}" if message else f"External service error: {service_name}"
        super().__init__(msg, status_code=502)
        self.service_name = service_name


class LLMServiceError(ExternalServiceError):
    """LLM 서비스 호출에 실패했습니다"""
    
    def __init__(self, provider: str = "unknown", message: str = None):
        super().__init__(f"LLM/{provider}", message)
        self.provider = provider


class S3OperationError(ExternalServiceError):
    """S3 작업에 실패했습니다"""
    
    def __init__(self, operation: str = None, bucket: str = None, key: str = None, message: str = None):
        details = []
        if operation:
            details.append(f"operation={operation}")
        if bucket:
            details.append(f"bucket={bucket}")
        if key:
            details.append(f"key={key}")
        detail_str = ", ".join(details) if details else ""
        msg = f"{message} ({detail_str})" if detail_str else message
        super().__init__("S3", msg)
        self.operation = operation
        self.bucket = bucket
        self.key = key


# ============================================================
# 리소스 제한 관련 예외
# ============================================================

class RateLimitExceededError(BaseAnalemmaError):
    """요청 속도 제한을 초과했습니다"""
    
    def __init__(self, message: str = None, retry_after: int = None):
        super().__init__(message or "Rate limit exceeded", status_code=429)
        self.retry_after = retry_after


class QuotaExceededError(BaseAnalemmaError):
    """사용량 한도를 초과했습니다"""
    
    def __init__(self, quota_type: str = None, message: str = None):
        msg = f"Quota exceeded: {quota_type}" if quota_type else "Quota exceeded"
        if message:
            msg = f"{msg} - {message}"
        super().__init__(msg, status_code=402)
        self.quota_type = quota_type


# ============================================================
# LLM 서비스 관련 예외
# ============================================================

class LLMServiceError(BaseAnalemmaError):
    """LLM 서비스 호출 중 오류가 발생했습니다"""
    
    def __init__(
        self, 
        message: str = None, 
        original_error: Exception = None,
        provider: str = None,
        node_id: str = None,
        attempts: int = None,
        error_category: str = None
    ):
        msg = message or "LLM service error occurred"
        super().__init__(msg, status_code=502)  # Bad Gateway
        
        self.original_error = original_error
        self.provider = provider
        self.node_id = node_id
        self.attempts = attempts
        self.error_category = error_category
    
    def to_dict(self):
        base_dict = super().to_dict()
        base_dict.update({
            "provider": self.provider,
            "nodeId": self.node_id,
            "attempts": self.attempts,
            "errorCategory": self.error_category,
            "originalError": str(self.original_error) if self.original_error else None
        })
        return base_dict
