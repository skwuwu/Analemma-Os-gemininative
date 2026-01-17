"""
Common utility module
Common functions to eliminate duplication in backend code and increase reusability
"""

from src.common.websocket_utils import (
    get_connections_for_owner,
    send_to_connection,
    broadcast_to_connections,
    notify_user,
    cleanup_stale_connection
)
from src.common.auth_utils import (
    validate_token,
    extract_owner_id_from_token,
    extract_owner_id_from_event,
    require_authentication
)
from src.common.aws_clients import (
    get_dynamodb_resource,
    get_dynamodb_table,
    get_s3_client,
    get_stepfunctions_client,
    get_ssm_client,
    get_ecs_client,
    get_lambda_client,
    get_kinesis_client
)
from src.common.exceptions import (
    BaseAnalemmaError,
    ExecutionNotFound,
    ExecutionForbidden,
    ExecutionAlreadyExists,
    WorkflowNotFound,
    WorkflowValidationError,
    AuthenticationError,
    AuthorizationError,
    TokenExpiredError,
    InvalidTokenError,
    ValidationError,
    MissingRequiredFieldError,
    ExternalServiceError,
    LLMServiceError,
    S3OperationError,
    RateLimitExceededError,
    QuotaExceededError
)
from src.common.json_utils import (
    DecimalEncoder,
    convert_decimals,
    convert_to_dynamodb_format,
    dumps_decimal
)
from src.common.http_utils import (
    get_cors_headers,
    get_json_headers,
    JSON_HEADERS,
    CORS_HEADERS,
    build_response,
    success_response,
    created_response,
    bad_request_response,
    unauthorized_response,
    forbidden_response,
    not_found_response,
    internal_error_response
)
from src.common.logging_utils import (
    get_logger,
    get_tracer,
    get_metrics,
    log_execution_context,
    log_external_service_call,
    log_business_event,
    log_security_event,
    log_workflow_event,
    log_execution_event
)
from src.common.error_handlers import (
    handle_dynamodb_error,
    handle_s3_error,
    handle_stepfunctions_error,
    handle_bedrock_error,
    handle_llm_api_error,
    handle_network_error,
    safe_external_call,
    handle_lambda_error
)
from src.common.constants import (
    TTLConfig,
    QuotaLimits,
    ModelPricing,
    HTTPStatusCodes,
    RetryConfig,
    WorkflowConfig,
    LoggingConfig,
    SecurityConfig,
    EnvironmentVariables,
    DynamoDBConfig,
    get_env_var,
    get_table_name,
    is_mock_mode,
    get_stage_name,
    get_log_level,
    get_inline_threshold,
    get_messages_window
)

__all__ = [
    # WebSocket utilities
    'get_connections_for_owner',
    'send_to_connection',
    'broadcast_to_connections',
    'notify_user',
    'cleanup_stale_connection',

    # Authentication utilities
    'validate_token',
    'extract_owner_id_from_token',
    'extract_owner_id_from_event',
    'require_authentication',
    
    # AWS Client utilities
    'get_dynamodb_resource',
    'get_dynamodb_table',
    'get_s3_client',
    'get_stepfunctions_client',
    'get_ssm_client',
    'get_ecs_client',
    'get_lambda_client',
    'get_kinesis_client',
    
    # Exception classes
    'BaseAnalemmaError',
    'ExecutionNotFound',
    'ExecutionForbidden',
    'ExecutionAlreadyExists',
    'WorkflowNotFound',
    'WorkflowValidationError',
    'AuthenticationError',
    'AuthorizationError',
    'TokenExpiredError',
    'InvalidTokenError',
    'ValidationError',
    'MissingRequiredFieldError',
    'ExternalServiceError',
    'LLMServiceError',
    'S3OperationError',
    'RateLimitExceededError',
    'QuotaExceededError',
    
    # JSON utilities
    'DecimalEncoder',
    'convert_decimals',
    'convert_to_dynamodb_format',
    'dumps_decimal',
    
    # HTTP utilities
    'get_cors_headers',
    'get_json_headers',
    'JSON_HEADERS',
    'CORS_HEADERS',
    'build_response',
    'success_response',
    'created_response',
    'bad_request_response',
    'unauthorized_response',
    'forbidden_response',
    'not_found_response',
    'internal_error_response',
    
    # Logging utilities
    'get_logger',
    'get_tracer',
    'get_metrics',
    'log_execution_context',
    'log_external_service_call',
    'log_business_event',
    'log_security_event',
    'log_workflow_event',
    'log_execution_event',
    
    # Error handling utilities
    'handle_dynamodb_error',
    'handle_s3_error',
    'handle_stepfunctions_error',
    'handle_bedrock_error',
    'handle_llm_api_error',
    'handle_network_error',
    'safe_external_call',
    
    # Constants and configuration
    'TTLConfig',
    'QuotaLimits',
    'ModelPricing',
    'HTTPStatusCodes',
    'RetryConfig',
    'WorkflowConfig',
    'LoggingConfig',
    'SecurityConfig',
    'EnvironmentVariables',
    'DynamoDBConfig',
    'get_env_var',
    'get_table_name',
    'is_mock_mode',
    'get_stage_name',
    'get_log_level',
    'get_inline_threshold',
    'get_messages_window',
]