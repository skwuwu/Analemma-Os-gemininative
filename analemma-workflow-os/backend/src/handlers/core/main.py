import json
import time
import re
import os
import copy
import operator
import concurrent.futures
import logging
import random
from typing import TypedDict, Dict, Any, List, Optional, Annotated, Union, Callable, Tuple, Literal
from functools import partial
import socket
import ipaddress
from collections import ChainMap
from collections.abc import Mapping
from urllib.parse import urlparse

from pydantic import BaseModel, Field, conlist, constr, ValidationError, field_validator, ConfigDict, model_validator

import boto3
from botocore.config import Config
from botocore.exceptions import ReadTimeoutError
from src.langchain_core_custom.outputs import LLMResult, Generation
from .token_utils import (
    extract_token_usage,
    aggregate_tokens_from_branches,
    aggregate_tokens_from_iterations,
    aggregate_tokens_from_nested,
    accumulate_tokens_in_state,
    calculate_cost_usd
)

# -----------------------------------------------------------------------------
# 1. Imports & Constants
# -----------------------------------------------------------------------------

# LangGraph imports for state management
try:
    from langgraph.graph.message import add_messages
except ImportError:
    # Fallback logic mainly for basic testing without full deps
    def add_messages(left, right):
        if not isinstance(left, list): left = [left] if left else []
        if not isinstance(right, list): right = [right] if right else []
        return left + right

# 커스텀 예외: Step Functions가 Error 필드로 쉽게 감지 가능 (비동기 처리용)
class AsyncLLMRequiredException(Exception):
    """Exception raised when async LLM processing is required"""
    pass

# HITP (Human in the Loop) 엣지 타입들
HITP_EDGE_TYPES = {"hitp", "human_in_the_loop", "pause"}

# Configure basic logging
logger = logging.getLogger("workflow")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Check for LangGraph availability and warn if using fallback
try:
    from langgraph.graph.message import add_messages
except ImportError:
    logger.warning("LangGraph not available, using fallback message reducer. This may cause issues with complex workflows.")

# ============================================================================
# [Configuration] Model Mapping and Timeouts
# ============================================================================

# Gemini to Bedrock model fallback mapping
# Can be overridden via environment variable BEDROCK_MODEL_MAP (JSON format)
DEFAULT_BEDROCK_MODEL_MAP = {
    "gemini-2.0-flash": "anthropic.claude-3-haiku-20240307-v1:0",
    "gemini-1.5-pro": "anthropic.claude-3-sonnet-20240229-v1:0",
    "gemini-1.5-flash": "anthropic.claude-3-haiku-20240307-v1:0",
    "gemini-1.5-flash-8b": "anthropic.claude-3-haiku-20240307-v1:0",
}

def get_bedrock_model_map() -> Dict[str, str]:
    """
    Get Bedrock model mapping with environment variable override support.
    
    Returns:
        Dictionary mapping Gemini model names to Bedrock model IDs
    """
    try:
        env_map = os.environ.get('BEDROCK_MODEL_MAP')
        if env_map:
            custom_map = json.loads(env_map)
            logger.info(f"Using custom Bedrock model map from environment: {custom_map}")
            return custom_map
    except Exception as e:
        logger.warning(f"Failed to parse BEDROCK_MODEL_MAP from environment: {e}")
    
    return DEFAULT_BEDROCK_MODEL_MAP.copy()

# ============================================================================
# [Cancellation] Execution Cancellation Support
# ============================================================================

def check_execution_cancelled(execution_arn: str) -> bool:
    """
    Check if the Step Functions execution has been cancelled/stopped.
    
    Args:
        execution_arn: The ARN of the Step Functions execution
        
    Returns:
        True if execution is cancelled/stopped, False otherwise
    """
    try:
        # Get Step Functions client
        sfn_client = boto3.client('stepfunctions', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        
        # Describe execution to get current status
        response = sfn_client.describe_execution(executionArn=execution_arn)
        status = response.get('status', 'RUNNING')
        
        # Check if execution is in a terminal state (not running)
        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            logger.warning(f"Execution {execution_arn} is in terminal state: {status}")
            return True
            
        return False
    except Exception as e:
        logger.warning(f"Failed to check execution status for {execution_arn}: {e}")
        # If we can't check, assume not cancelled to avoid false positives
        return False

# Lambda early exit threshold (milliseconds)
# If remaining Lambda execution time < this value, trigger AsyncLLMRequiredException
LAMBDA_EARLY_EXIT_THRESHOLD_MS = int(os.environ.get('LAMBDA_EARLY_EXIT_MS', '10000'))  # 10 seconds default

# LLM default configuration values (environment variable overrides)
# [Fix] Changed default from gpt-3.5-turbo to gemini-2.0-flash (Bedrock compatible via fallback mapping)
DEFAULT_LLM_MODEL = os.environ.get('DEFAULT_LLM_MODEL', 'gemini-2.0-flash')
DEFAULT_MAX_TOKENS = int(os.environ.get('DEFAULT_MAX_TOKENS', '1024'))
DEFAULT_TEMPERATURE = float(os.environ.get('DEFAULT_TEMPERATURE', '0.7'))

# Retry configuration defaults
DEFAULT_RETRY_BASE_DELAY = float(os.environ.get('DEFAULT_RETRY_BASE_DELAY', '1.0'))  # seconds
DEFAULT_RETRY_MAX_DELAY = float(os.environ.get('DEFAULT_RETRY_MAX_DELAY', '30.0'))  # seconds

# Bedrock timeout configuration
DEFAULT_BEDROCK_TIMEOUT = int(os.environ.get('DEFAULT_BEDROCK_TIMEOUT', '90'))  # seconds
LAMBDA_TIMEOUT_BUFFER_MS = int(os.environ.get('LAMBDA_TIMEOUT_BUFFER_MS', '5000'))  # milliseconds

# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# 2. State Schema Definition
# -----------------------------------------------------------------------------

# NOTE: WorkflowState TypedDict는 타입 힌트/문서화 목적으로만 유지됩니다.
# 실제 LangGraph 1.0+ 실행 시에는 DynamicWorkflowBuilder에서 
# Annotated[Dict[str, Any], merge_state_dict] 스키마를 사용하여
# 동적 키를 완벽하게 지원합니다.
#
# 이 TypedDict는 IDE 자동완성 및 정적 분석에 활용됩니다.
class WorkflowState(TypedDict, total=False):
    user_query: str
    user_api_keys: Dict[str, str]
    step_history: List[str]
    # Messages list that accumulates instead of overwriting
    messages: Annotated[List[Dict[str, Any]], add_messages]
    # Common dynamic fields
    item: Any  # For for_each operations
    result: Any  # General result storage
    
    # --- Skills Integration ---
    injected_skills: List[str]
    active_context_ref: str
    active_skills: Dict[str, Any]
    skill_execution_log: Annotated[List[Dict[str, Any]], operator.add]


# -----------------------------------------------------------------------------
# 3. Core Helper Functions (Template, S3 Check, Bedrock)
# -----------------------------------------------------------------------------

# --- Pydantic Schemas for workflow config validation ---

# 🛡️ [P2] 허용된 노드 타입 목록 - NODE_REGISTRY에 핸들러가 등록된 실행 가능한 타입들만 포함
# ⚠️ 주의: branch, router, join, hitp, pause 등은 Edge로 처리되므로 노드 타입에서 제외
ALLOWED_NODE_TYPES = {
    # Core execution types (NODE_REGISTRY에 핸들러 등록됨)
    "operator", "operator_custom", "operator_official",
    "llm_chat",
    # Flow control (노드로 실행됨)
    "parallel_group", "aggregator", "for_each", "nested_for_each",
    # Subgraph (재귀적 워크플로우 실행)
    "subgraph",
    # Infrastructure & Data
    "api_call", "db_query",
    # Multimodal & Skills
    "vision", "video_chunker", "skill_executor",
}

# 🔗 Edge로 처리되는 타입들 - 노드가 아닌 엣지 속성으로 정의됨
# conditional_edge.router_func, edge.type="hitp" 등으로 처리
EDGE_HANDLED_TYPES = {
    "branch", "router", "join",  # conditional_edge로 처리
    "hitp", "pause",              # edge.type으로 처리 (HITP_EDGE_TYPES)
}

# 📌 UI 전용 마커 노드 - 실행되지 않음 (프론트엔드에서만 사용)
# 이 타입들은 partition_service에서 무시되거나 passthrough됨
UI_MARKER_TYPES = {
    "input", "output", "start", "end",
}

#  별칭(Alias) 매핑 - field_validator에서 정규 타입으로 변환됨
NODE_TYPE_ALIASES = {
    "code": "operator",      # 'code'는 'operator'의 별칭
    "aimodel": "llm_chat",   # [Fix] map to canonical 'llm_chat'
    "aiModel": "llm_chat",   # [Fix] map to canonical 'llm_chat'
    "llm": "llm_chat",       # [Fix] legacy support
    "chat": "llm_chat",
    "genai": "llm_chat",
    "gpt": "llm_chat",
    "claude": "llm_chat",
    "gemini": "llm_chat",
    "function": "operator",
    "lambda": "operator",
    "task": "operator",
    "parallel": "parallel_group",
    "map": "for_each",
    "foreach": "for_each",
    "loop": "for_each",
    "image_analysis": "vision",
    "chunker": "video_chunker",
    "group": "subgraph",
    "map_in_map": "nested_for_each",
}

class EdgeModel(BaseModel):
    source: constr(min_length=1, max_length=128)
    target: constr(min_length=1, max_length=128)
    type: constr(min_length=1, max_length=64) = "edge"
    # conditional_edge 지원 필드
    router_func: Optional[str] = None        # 라우터 함수명 (NODE_REGISTRY에 등록된)
    mapping: Optional[Dict[str, str]] = None  # 라우터 반환값 -> 타겟 노드 매핑
    condition: Optional[str] = None           # 조건 표현식 (partition_service에서 사용)
    
    class Config:
        extra = "ignore"


class NodeModel(BaseModel):
    id: constr(min_length=1, max_length=128)
    type: constr(min_length=1, max_length=64)
    label: Optional[constr(min_length=0, max_length=256)] = None
    action: Optional[constr(min_length=0, max_length=256)] = None
    hitp: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None
    next: Optional[str] = None
    # [Fix] parallel_group support - branches와 resource_policy 필드 추가
    # extra="ignore"로 인해 이 필드들이 누락되어 NoneType 에러 발생했음
    branches: Optional[List[Dict[str, Any]]] = None
    resource_policy: Optional[Dict[str, Any]] = None
    # [Fix] subgraph support
    subgraph_ref: Optional[str] = None
    subgraph_inline: Optional[Dict[str, Any]] = None
    
    @field_validator('type', mode='before')
    @classmethod
    def alias_and_validate_node_type(cls, v):
        """
        🛡️ [P2] Validate and alias node types.
        - Aliases are converted to canonical types (e.g., 'code' -> 'operator')
        - Unknown types are rejected with clear error message
        """
        if not isinstance(v, str):
            raise ValueError(f"Node type must be string, got {type(v).__name__}")
        
        v = v.strip().lower()
        
        # Apply alias mapping first
        if v in NODE_TYPE_ALIASES:
            return NODE_TYPE_ALIASES[v]
        
        # 🛡️ All accepted types: executable nodes + UI markers (passthrough)
        all_valid_types = ALLOWED_NODE_TYPES | UI_MARKER_TYPES
        
        # Validate against allowed types
        if v not in all_valid_types:
            raise ValueError(
                f"Unknown node type: '{v}'. "
                f"Allowed types: {sorted(all_valid_types)}. "
                f"Aliases: {NODE_TYPE_ALIASES}"
            )
        
        return v
    
    class Config:
        extra = "ignore"


class WorkflowConfigModel(BaseModel):
    workflow_name: Optional[constr(min_length=0, max_length=256)] = None
    description: Optional[constr(min_length=0, max_length=512)] = None
    nodes: conlist(NodeModel, min_length=0, max_length=500)
    edges: conlist(EdgeModel, min_length=0, max_length=1000)
    start_node: Optional[constr(min_length=1, max_length=128)] = None


# -----------------------------------------------------------------------------
# PII Masking Helpers for Glass-Box logging
# -----------------------------------------------------------------------------
PII_REGEX_PATTERNS = [
    (r"\bsk-[a-zA-Z0-9]{20,}\b", "[API_KEY_REDACTED]"),
    (r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "[EMAIL_REDACTED]"),
    (r"\d{3}-\d{3,4}-\d{4}", "[PHONE_REDACTED]"),
]

# -----------------------------------------------------------------------------
# 🛡️ State Pollution Safeguards - Kernel Protection Layer
# -----------------------------------------------------------------------------
RESERVED_STATE_KEYS = {
    # System Context (Both Case Styles) - ownerId 제외 (Step Functions JSONPath 호환성)
    "workflowId", "workflow_id", "owner_id", 
    "execution_id", "user_id", "idempotency_key",
    
    # Flow Control (Most dangerous manipulation points - Loop/Segment control)
    "loop_counter", "max_loop_iterations", "segment_id", 
    "segment_to_run", "total_segments", "segment_type",
    
    # State & Infrastructure (S3 offloading integrity protection)
    "current_state", "final_state", "state_s3_path", "final_state_s3_path",
    "partition_map", "partition_map_s3_path", "__s3_offloaded", "__s3_path",
    
    # Telemetry & Logs (Traceability protection)
    "step_history", "execution_logs", "__new_history_logs", 
    "skill_execution_log", "__kernel_actions",
    
    # Scheduling & Guardrails (Prevent scheduler/guardrail bypass)
    "scheduling_metadata", "__scheduling_metadata", 
    "guardrail_verified", "__guardrail_verified",
    "batch_count_actual", "state_size_threshold",
    
    # Sensitive Credentials (Prevent credential exposure)
    "user_api_keys", "aws_credentials",
    
    # Response Envelope (Step Functions JSONPath 정합성 유지)
    "status", "error_info"
}

def _validate_output_keys(output: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    """
    🛡️ [Guard] Validate and filter output keys to prevent state pollution.

    This function completes the isolation layer of 'user mode vs kernel mode'
    to prevent user-defined code (Operators) from invading the kernel's domain.

    Especially when MOCK_MODE is turned off and real LLM is deployed, this prevents
    accidents where the model generates arbitrary JSON keys and overwrites system metadata.

    Blocking targets:
    - Flow Control variables (loop_counter, segment_id, etc.) → Prevent infinite loops/wrong jumps
    - State Infrastructure (state_s3_path, __s3_offloaded, etc.) → Protect S3 integrity
    - Telemetry (step_history, execution_logs, etc.) → Protect traceability
    - Response Envelope (status, error_info) → Maintain Step Functions JSONPath integrity

    Args:
        output: Dictionary returned by the node
        node_id: Node identifier (for logging)

    Returns:
        Safe dictionary with system reserved keys removed
    """
    if not isinstance(output, dict):
        return output
        
    # 🛡️ [Guard] Kernel domain intrusion check
    forbidden_attempts = [k for k in output.keys() if k in RESERVED_STATE_KEYS]
    
    if forbidden_attempts:
        logger.warning(
            f"🚨 [Pollution Blocked] Node '{node_id}' tried to overwrite system keys: {forbidden_attempts}. "
            f"These keys are in the kernel domain and access by user code is prohibited."
        )
        
        # Force data diet: Filter only safe data excluding system keys
        safe_output = {k: v for k, v in output.items() if k not in RESERVED_STATE_KEYS}
        
        # [Telemetry] 위반 시도 기록 (선택적)
        # safe_output["__safeguard_violations"] = {
        #     "node_id": node_id,
        #     "blocked_keys": forbidden_attempts,
        #     "timestamp": time.time()
        # }
        
        return safe_output
            
    return output



# -----------------------------------------------------------------------------
# 🛡️ Pydantic Schema Validation Layer - Type Safety for State
# -----------------------------------------------------------------------------
class SafeStateOutput(BaseModel):
    """
    🛡️ [Guard] Pydantic 모델 기반 스키마 검증 레이어
    
    노드 출력값의 타입 안전성을 보장하고, 예약 키 침범을 이중으로 차단합니다.
    이 레이어는 _validate_output_keys의 "백업 가드"로 작동합니다.
    
    장점:
    1. 타입 안전성: 잘못된 타입의 값이 state에 유입되는 것을 방지
    2. 스키마 강제: 시스템 필수 필드의 구조 검증
    3. 자동 변환: Pydantic의 coercion 기능으로 호환 가능한 타입 자동 변환
    
    아키텍처 개선 (2단계 방어):
    - model_validator(mode='before'): extra 필드를 포함한 전체 입력 스캔
    - 예약 키 발견 시 None 반환 대신 딕셔너리에서 제거 (상태 오염 방지)
    """
    model_config = ConfigDict(
        extra='allow',  # 사용자 정의 키는 허용
        validate_assignment=True,  # 할당 시마다 검증
        arbitrary_types_allowed=True
    )
    
    # 시스템 필수 필드 (읽기 전용, 노드가 설정 불가)
    workflowId: Optional[str] = Field(None, frozen=True)
    ownerId: Optional[str] = Field(None, frozen=True)
    execution_id: Optional[str] = Field(None, frozen=True)
    
    # Flow Control (노드가 절대 변경하면 안 되는 필드)
    loop_counter: Optional[int] = Field(None, frozen=True, ge=0)
    max_loop_iterations: Optional[int] = Field(None, frozen=True, ge=1)
    segment_id: Optional[int] = Field(None, frozen=True)
    segment_to_run: Optional[int] = Field(None, frozen=True)
    
    @model_validator(mode='before')
    @classmethod
    def block_reserved_keys_globally(cls, data: Any) -> Any:
        """
        🛡️ [Critical Fix] 전역 예약 키 차단 (extra 필드 포함)
        
        field_validator('*')는 명시적으로 정의된 필드에만 적용되므로,
        model_validator를 사용하여 extra 필드까지 포함한 전체 입력을 스캔합니다.
        
        중요: return None이 아닌 키 삭제(pop)를 통해 상태 오염 방지
        - None 반환 시: loop_counter=5 → None으로 덮어씀 (오염 발생)
        - 키 삭제 시: loop_counter는 아예 출력에서 제외 (기존 값 유지)
        """
        if not isinstance(data, dict):
            return data
            
        # 예약 키 탐지
        forbidden_keys = [k for k in data.keys() if k in RESERVED_STATE_KEYS]
        
        if forbidden_keys:
            logger.warning(
                f"🚨 [Pydantic Model Guard] Detected reserved keys in extra fields: {forbidden_keys}. "
                f"이 키들은 딕셔너리에서 제거되어 커널 상태를 보호합니다."
            )
            
            # 🛡️ [Critical Fix] None 반환이 아닌 키 삭제 (상태 오염 방지)
            # 예약 키를 딕셔너리에서 제거하여 상태 병합 시 기존 값이 유지되도록 함
            cleaned_data = {k: v for k, v in data.items() if k not in RESERVED_STATE_KEYS}
            return cleaned_data
            
        return data


def validate_state_with_schema(output: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    """
    🛡️ [Guard] Pydantic 스키마를 사용한 추가 검증 레이어
    
    _validate_output_keys 이후 실행되어 타입 안전성과 스키마 정합성을 보장합니다.
    
    2단계 방어 시스템:
    1. Layer 1 (_validate_output_keys): 예약 키 필터링 (기본 방어선)
    2. Layer 2 (validate_state_with_schema): Pydantic 타입 검증 + extra 필드 스캔 (백업 방어선)
    
    Args:
        output: 노드가 반환한 출력 딕셔너리 (Layer 1 통과 후)
        node_id: 노드 식별자
        
    Returns:
        스키마 검증을 통과한 안전한 딕셔너리
    """
    try:
        # Pydantic 모델로 변환하여 검증
        # model_validator에서 예약 키가 제거되고 타입 검증이 수행됨
        validated = SafeStateOutput(**output)
        
        # 검증된 데이터만 추출 (exclude_none으로 None 필드는 제외)
        safe_dict = validated.model_dump(
            exclude_none=True,  # None 값 제외 (상태 오염 방지)
            exclude_unset=True,  # 설정되지 않은 필드 제외
            mode='python'  # Python 네이티브 타입으로 변환
        )
        
        # 원본 output에 있던 사용자 정의 키는 보존 (예약 키가 아닌 경우만)
        for key, value in output.items():
            if key not in RESERVED_STATE_KEYS and key not in safe_dict:
                safe_dict[key] = value
                
        return safe_dict
        
    except ValidationError as e:
        logger.error(
            f"🚨 [Schema Validation Failed] Node '{node_id}' output failed Pydantic validation: {e}"
        )
        # 검증 실패 시 원본 반환 (이미 _validate_output_keys를 통과했으므로)
        return output
    except Exception as e:
        logger.error(
            f"🚨 [Schema Validation Error] Unexpected error in Pydantic validation for node '{node_id}': {e}"
        )
        return output


def mask_pii(text: Any) -> Any:
    if not isinstance(text, str):
        return text
    masked = text
    for pattern, repl in PII_REGEX_PATTERNS:
        masked = re.sub(pattern, repl, masked)
    return masked


def humanize_llm_error(error: Exception, provider: str, node_id: str) -> str:
    """
    🛡️ [User-Friendly Error Messages] Convert technical errors to user-friendly messages

    Convert LLM API errors to messages that general users can understand.
    Sensitive information is masked for security.

    Args:
        error: Original exception object
        provider: "gemini" or "bedrock"
        node_id: Node identifier

    Returns:
        User-friendly error message
    """
    error_msg = str(error).lower()
    error_type = type(error).__name__

    # Rate Limit 에러
    if any(keyword in error_msg for keyword in ["429", "quota", "rate limit", "resource exhausted", "throttling"]):
        return (
            f"🚦 AI service is currently busy. Please try again in a moment. "
            f"(Rate limit exceeded on {provider})"
        )

    # Authentication/Authorization 에러
    elif any(keyword in error_msg for keyword in ["403", "forbidden", "unauthorized", "access denied", "permission"]):
        return (
            f"🔐 You don't have access to the AI service. Please contact your administrator. "
            f"(Authentication error on {provider})"
        )

    # Timeout 에러
    elif any(keyword in error_msg for keyword in ["timeout", "deadline", "read timeout"]):
        return (
            f"⏱️ AI service response is delayed. Please try again. "
            f"(Timeout on {provider})"
        )

    # Content Safety 에러
    elif any(keyword in error_msg for keyword in ["blocked", "safety", "content", "inappropriate"]):
        return (
            f"🚫 Your request violates AI safety policies. Please modify your content. "
            f"(Content safety filter on {provider})"
        )

    # Model Not Found 에러
    elif any(keyword in error_msg for keyword in ["not found", "unavailable", "model", "resource not found"]):
        return (
            f"🤖 The requested AI model is not available. Please select a different model. "
            f"(Model unavailable on {provider})"
        )

    # Network/Connection 에러
    elif any(keyword in error_msg for keyword in ["connection", "network", "dns", "unreachable"]):
        return (
            f"🌐 There is a network connection issue. Please check your internet connection. "
            f"(Network error on {provider})"
        )

    # Validation 에러
    elif any(keyword in error_msg for keyword in ["validation", "invalid", "malformed"]):
        return (
            f"📝 The input data format is incorrect. Please check your content. "
            f"(Validation error on {provider})"
        )

    # 기타 에러
    else:
        return (
            f"⚠️ A temporary error occurred in the AI service. Please try again in a moment. "
            f"(Unexpected error on {provider})"
        )


def _get_nested_value(state: Dict[str, Any], path: str, default: Any = "") -> Any:
    """Retrieve nested value from src.state using dot-separated path."""
    if not path: return default
    parts = path.split('.')
    cur: Any = state
    try:
        for p in parts:
            if isinstance(cur, Mapping) and p in cur:
                cur = cur[p]
            else:
                return default
        return cur
    except Exception:
        return default


def _render_template(template: Any, state: Dict[str, Any]) -> Any:
    """Render {{variable}} templates against the provided state. Support basic Jinja2 conditionals."""
    if template is None: return None
    if isinstance(template, str):
        # 1. Handle Basic Jinja2 Conditionals {% if ... %} ... {% else %} ... {% endif %} (Lightweight)
        # Note: This is not a full Jinja2 parser, but supports common patterns used in test definitions.
        while "{% if" in template and "{% endif %}" in template:
            match = re.search(r"\{%\s*if\s+(.+?)\s*%\}(.+?)(?:\{%\s*else\s*%\}(.+?))?\{%\s*endif\s*%\}", template, re.DOTALL)
            if not match: break
            
            full_block = match.group(0)
            condition = match.group(1).strip()
            true_block = match.group(2)
            false_block = match.group(3) or ""
            
            # Evaluate condition
            result = False
            try:
                # Check for 'is undefined' / 'is defined'
                if " is undefined" in condition:
                    var_name = condition.split(" is undefined")[0].strip()
                    val = _get_nested_value(state, var_name, None)
                    result = (val is None)
                elif " is defined" in condition:
                    var_name = condition.split(" is defined")[0].strip()
                    val = _get_nested_value(state, var_name, None)
                    result = (val is not None)
                else:
                    # Comparison logic (e.g., attempt_count < 2)
                    # Safe eval: Only allow simple comparison
                    # Replace variable names with values
                    eval_cond = condition
                    # Match variable names (simple alphanumeric & dots)
                    for var_match in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_.]*)\b", condition):
                        var_name = var_match.group(1)
                        if var_name in ("and", "or", "not", "True", "False", "None"): continue
                        val = _get_nested_value(state, var_name, None)
                        if val is None: val = 0 # Default for numeric comparison
                        if isinstance(val, str): val = f"'{val}'"
                        eval_cond = eval_cond.replace(var_name, str(val), 1)
                    
                    # Very restricted eval
                    allowed_chars = set("0123456789.+-*/()<>=! '\"andornotTrueFalse")
                    if set(eval_cond).issubset(allowed_chars):
                        result = eval(eval_cond)
            except Exception as e:
                logger.warning(f"Template condition eval failed: {condition} -> {e}")
                result = False
            
            replacement = true_block if result else false_block
            template = template.replace(full_block, replacement, 1)

        # 2. Variable Substitution {{ var }}
        def _repl(m):
            key = m.group(1).strip()
            # Support basic filters (ignore them for now except | tojson)
            if "|" in key:
                parts = key.split("|")
                key = parts[0].strip()
                filter_name = parts[1].strip()
            else:
                filter_name = None

            if key == "__state_json":
                try: return json.dumps(state, ensure_ascii=False)
                except: return str(state)
            
            val = _get_nested_value(state, key, "")
            
            # Simple handling for | tojson
            if filter_name == "tojson":
                if isinstance(val, (dict, list)):
                    return json.dumps(val, ensure_ascii=False)
            
            if isinstance(val, (dict, list)):
                try: return json.dumps(val, ensure_ascii=False)
                except: return str(val)
            return str(val)
            
        return re.sub(r"\{\{\s*(.+?)\s*\}\}", _repl, template)
        
    if isinstance(template, dict):
        return {k: _render_template(v, state) for k, v in template.items()}
    if isinstance(template, list):
        return [_render_template(v, state) for v in template]
    return template


# --- Bedrock & LLM Helpers ---
_bedrock_client = None
# Base retry/timeout config for reuse (reduces cold-start overhead)
_bedrock_base_config = Config(
    retries={"max_attempts": 3, "mode": "standard"},
    read_timeout=int(os.environ.get("BEDROCK_READ_TIMEOUT_SECONDS", "60")),
    connect_timeout=int(os.environ.get("BEDROCK_CONNECT_TIMEOUT_SECONDS", "5")),
)

def get_bedrock_client():
    global _bedrock_client
    if _bedrock_client:
        return _bedrock_client
    try:
        region = os.environ.get("AWS_REGION", "us-east-1")
        _bedrock_client = boto3.client("bedrock-runtime", region_name=region, config=_bedrock_base_config)
        return _bedrock_client
    except Exception:
        logger.warning("Failed to create Bedrock client")
        return None

def _is_mock_mode() -> bool:
    return os.getenv("MOCK_MODE", "false").strip().lower() in {"true", "1", "yes", "on"}

def _build_mock_llm_text(model_id: str, prompt: str) -> str:
    return f"[MOCK_MODE] Response from {model_id}. Prompt: {prompt[:50]}..."

def invoke_bedrock_model(model_id: str, system_prompt: str | None, user_prompt: str, max_tokens: int | None = None, temperature: float | None = None, read_timeout_seconds: int | None = None) -> Any:
    if _is_mock_mode():
        logger.info(f"MOCK_MODE: Skipping Bedrock call for {model_id}")
        # Mock realistic token usage for testing cost guardrails
        mock_input_tokens = min(500, len(user_prompt) // 4)  # Rough estimate: ~4 chars per token
        mock_output_tokens = 200  # Typical response length
        return {
            "content": [{"text": _build_mock_llm_text(model_id, user_prompt)}],
            "usage": {
                "input_tokens": mock_input_tokens,
                "output_tokens": mock_output_tokens
            }
        }

    try:
        # Prefer shared client; only create ad-hoc client when caller explicitly needs longer timeout
        if read_timeout_seconds and read_timeout_seconds != _bedrock_base_config.read_timeout:
            client = boto3.client("bedrock-runtime", config=_bedrock_base_config.merge(Config(read_timeout=read_timeout_seconds)))
        else:
            client = get_bedrock_client()

        if not client:
            # Fallback if client creation failed
            return {"content": [{"text": "[Error] Bedrock client unavailable"}]}

        messages = [{"role": "user", "content": [{"type": "text", "text": user_prompt}]}]
        payload = {"messages": messages}
        
        if "gemini" not in (model_id or "").lower():
            payload["anthropic_version"] = "bedrock-2023-05-31"
        if system_prompt: payload["system"] = system_prompt
        if max_tokens: payload["max_tokens"] = int(max_tokens)
        if temperature: payload["temperature"] = float(temperature)

        resp = client.invoke_model(body=json.dumps(payload), modelId=model_id)
        return json.loads(resp['body'].read())

    except ReadTimeoutError:
        logger.warning(f"Bedrock read timeout for {model_id}")
        raise AsyncLLMRequiredException("SDK read timeout")
    except Exception as e:
        logger.exception(f"Bedrock invocation failed for {model_id}")
        raise e

def extract_text_from_bedrock_response(resp: Any) -> str:
    """Extract text from src.standard Bedrock response format."""
    try:
        if isinstance(resp, dict):
            c = resp.get("content")
            if isinstance(c, list) and c:
                if "text" in c[0]: return c[0]["text"]
        return str(resp)
    except Exception:
        return str(resp)


def clean_llm_json_response(text: str) -> str:
    """
    Clean LLM response to extract valid JSON.
    
    LLMs often wrap JSON in markdown code blocks or add explanatory text.
    This function strips common artifacts:
    - Markdown code fences: ```json ... ```
    - Leading/trailing whitespace
    - Text before first { or [
    - Text after last } or ]
    
    Returns:
        Cleaned JSON string ready for parsing
    """
    if not isinstance(text, str):
        return str(text)
    
    # Remove markdown code fences
    text = text.strip()
    
    # Pattern 1: ```json\n{...}\n```
    if text.startswith("```json"):
        text = text[7:]  # Remove ```json
        if text.endswith("```"):
            text = text[:-3]  # Remove closing ```
    elif text.startswith("```"):
        text = text[3:]  # Remove generic ```
        if text.endswith("```"):
            text = text[:-3]
    
    text = text.strip()
    
    # Pattern 2: Text before/after JSON object or array
    # Find first { or [ and last } or ]
    start_obj = text.find('{')
    start_arr = text.find('[')
    
    # Determine actual start (whichever comes first, or -1 if neither found)
    if start_obj == -1 and start_arr == -1:
        return text  # No JSON structure found, return as-is
    elif start_obj == -1:
        start = start_arr
    elif start_arr == -1:
        start = start_obj
    else:
        start = min(start_obj, start_arr)
    
    # Find corresponding end
    if text[start] == '{':
        end = text.rfind('}')
    else:
        end = text.rfind(']')
    
    if end != -1 and end > start:
        text = text[start:end+1]
    
    return text.strip()


def parse_llm_json_response(text: str, fallback_value: Any = None) -> Any:
    """
    Attempt to parse LLM response as JSON with robust error handling.
    
    Args:
        text: Raw LLM response text
        fallback_value: Value to return if parsing fails (default: original text)
    
    Returns:
        Parsed JSON object, or fallback_value if parsing fails
    """
    if not isinstance(text, str):
        return fallback_value if fallback_value is not None else text
    
    try:
        # First attempt: direct parsing
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    try:
        # Second attempt: clean markdown artifacts
        cleaned = clean_llm_json_response(text)
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse LLM response as JSON: {e}. Returning raw text.")
        return fallback_value if fallback_value is not None else text
    except Exception as e:
        logger.error(f"Unexpected error parsing LLM JSON: {e}")
        return fallback_value if fallback_value is not None else text

def normalize_llm_usage(usage: Dict[str, Any], provider: str) -> Dict[str, Any]:
    """
    Normalize token usage statistics from different LLM providers.
    
    Analemma 대시보드에서 일관된 비용 차트를 표시하기 위해 
    Gemini와 Bedrock의 다른 필드명을 표준 인터페이스로 통합합니다.
    
    Args:
        usage: Raw usage dictionary from provider
        provider: Provider name ("gemini" or "bedrock")
    
    Returns:
        Normalized usage dict with standard keys for dashboard compatibility:
        {
            "input_tokens": int,      # 입력 토큰 수
            "output_tokens": int,     # 출력 토큰 수
            "total_tokens": int,      # 총 토큰 수
            "cached_tokens": int,     # Context Cache로 절감된 토큰 (Gemini only)
            "estimated_cost_usd": float,  # 예상 비용 (USD)
            "provider": str,          # 제공자 이름
            "cost_saved_usd": float,  # 캐싱으로 절감된 비용 (Gemini only)
        }
    """
    normalized = {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cached_tokens": 0,
        "estimated_cost_usd": 0.0,
        "cost_saved_usd": 0.0,
        "provider": provider,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    
    try:
        if provider == "gemini":
            # Gemini structure: {input_tokens, output_tokens, cached_tokens, total_tokens, estimated_cost_usd}
            normalized["input_tokens"] = usage.get("input_tokens", 0)
            normalized["output_tokens"] = usage.get("output_tokens", 0)
            normalized["cached_tokens"] = usage.get("cached_tokens", 0)
            # [FIX] Always calculate total_tokens to handle Mock responses with total_tokens=0
            raw_total = usage.get("total_tokens", 0)
            calculated_total = normalized["input_tokens"] + normalized["output_tokens"]
            normalized["total_tokens"] = raw_total if raw_total > 0 else calculated_total
            normalized["estimated_cost_usd"] = usage.get("estimated_cost_usd", 0.0)
            
            # Calculate cost savings from caching (configurable reduction for cached tokens)
            cached = normalized["cached_tokens"]
            if cached > 0:
                # Gemini cached tokens cost reduction (default 75% less)
                normalized["cost_saved_usd"] = round(cached * CACHE_COST_REDUCTION * APPROXIMATE_TOKEN_COST_USD, 6)
                
        elif provider == "bedrock":
            # Bedrock structure: {inputTokens, outputTokens} or {input_tokens, output_tokens}
            normalized["input_tokens"] = usage.get("inputTokens") or usage.get("input_tokens", 0)
            normalized["output_tokens"] = usage.get("outputTokens") or usage.get("output_tokens", 0)
            normalized["total_tokens"] = normalized["input_tokens"] + normalized["output_tokens"]
            
            # Bedrock Claude model pricing (approximate)
            # Claude 3 Sonnet: $3/1M input, $15/1M output
            input_cost = (normalized["input_tokens"] / 1_000_000) * 3.0
            output_cost = (normalized["output_tokens"] / 1_000_000) * 15.0
            normalized["estimated_cost_usd"] = round(input_cost + output_cost, 6)
            
    except Exception as e:
        logger.debug(f"Failed to normalize usage from {provider}: {e}")
    
    return normalized

def prepare_multimodal_content(prompt: str, state: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Extract S3 URIs from prompt and prepare multimodal content for Gemini Vision API.
    
    Gemini Vision API는 텍스트 + 이미지/비디오를 contents 리스트로 받아야 합니다.
    이 함수는 프롬프트에서 S3 URI를 추출하고 invoke_with_images용 데이터를 준비합니다.
    
    Args:
        prompt: User prompt that may contain S3 URIs (e.g., "이 이미지 분석해줘 s3://bucket/image.jpg")
        state: Execution state with potential hydrated binary data
    
    Returns:
        Tuple of (cleaned_prompt, multimodal_parts)
        - cleaned_prompt: Prompt with S3 URIs replaced with placeholder
        - multimodal_parts: List of dicts for invoke_with_images:
            [{"source": "s3://..." or bytes, "mime_type": "image/jpeg", "source_uri": "s3://..."}]
    """
    import re
    
    multimodal_parts = []
    
    # Pattern to detect S3 URIs in prompt
    s3_uri_pattern = r's3://[a-zA-Z0-9\-_.]+/[a-zA-Z0-9\-_./]+'
    s3_uris = re.findall(s3_uri_pattern, prompt)
    
    if not s3_uris:
        return prompt, multimodal_parts
    
    # MIME type mapping (Gemini 지원 형식)
    MIME_TYPE_MAP = {
        # Images
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".heic": "image/heic",
        ".heif": "image/heif",
        ".bmp": "image/bmp",
        # Videos
        ".mp4": "video/mp4",
        ".mov": "video/quicktime",
        ".avi": "video/x-msvideo",
        ".webm": "video/webm",
        ".mkv": "video/x-matroska",
        # Documents (Gemini 1.5+)
        ".pdf": "application/pdf",
    }
    
    def get_mime_type(uri: str) -> str:
        """확장자에서 MIME 타입 추출"""
        uri_lower = uri.lower()
        for ext, mime in MIME_TYPE_MAP.items():
            if uri_lower.endswith(ext):
                return mime
        return "image/jpeg"  # default
    
    # Extract multimodal data
    for s3_uri in s3_uris:
        try:
            mime_type = get_mime_type(s3_uri)
            
            # Check if hydrated data exists in state
            s3_key = f"hydrated_{s3_uri.replace('s3://', '').replace('/', '_')}"
            
            if s3_key in state:
                # Binary data already hydrated - use directly
                binary_data = state[s3_key]
                multimodal_parts.append({
                    "source": binary_data,  # bytes for invoke_with_images
                    "data": binary_data,    # backward compat
                    "mime_type": mime_type,
                    "source_uri": s3_uri,
                    "hydrated": True
                })
                logger.info(f"Prepared hydrated multimodal part: {s3_uri} ({mime_type})")
            else:
                # Not hydrated - pass S3 URI directly (GeminiService will download)
                multimodal_parts.append({
                    "source": s3_uri,       # S3 URI for invoke_with_images
                    "mime_type": mime_type,
                    "source_uri": s3_uri,
                    "hydrated": False
                })
                logger.info(f"Prepared S3 URI multimodal part: {s3_uri} ({mime_type})")
                
        except Exception as e:
            logger.warning(f"Failed to prepare multimodal data for {s3_uri}: {e}")
    
    # Remove S3 URIs from prompt if we extracted them
    if multimodal_parts:
        cleaned_prompt = re.sub(s3_uri_pattern, "[미디어 첨부됨]", prompt)
        return cleaned_prompt, multimodal_parts
    
    return prompt, multimodal_parts
    
    # Remove S3 URIs from prompt if we extracted them
    if multimodal_parts:
        cleaned_prompt = re.sub(s3_uri_pattern, "[Image/Video attached]", prompt)
        return cleaned_prompt, multimodal_parts
    
    return prompt, multimodal_parts

# Async processing threshold (configurable via environment variable)
ASYNC_TOKEN_THRESHOLD = int(os.getenv('ASYNC_TOKEN_THRESHOLD', '2000'))
ASYNC_HEAVY_MODELS = os.getenv('ASYNC_HEAVY_MODELS', 'claude-3-opus,gpt-4').split(',')

# HTTP request timeout configuration
DEFAULT_HTTP_TIMEOUT = float(os.environ.get('DEFAULT_HTTP_TIMEOUT', '10.0'))  # seconds
MAX_HTTP_TIMEOUT = float(os.environ.get('MAX_HTTP_TIMEOUT', '30.0'))  # seconds

# Token cost estimation (USD per million tokens)
CACHE_COST_REDUCTION = float(os.environ.get('CACHE_COST_REDUCTION', '0.75'))  # 75% savings
APPROXIMATE_TOKEN_COST_USD = float(os.environ.get('APPROXIMATE_TOKEN_COST_USD', '0.000001'))  # $1 per 1M tokens

def should_use_async_llm(config: Dict[str, Any]) -> bool:
    """Heuristic to check if async processing is needed."""
    max_tokens = config.get("max_tokens", 0)
    model = config.get("model", "")
    force_async = config.get("force_async", False)
    
    high_token_count = max_tokens > ASYNC_TOKEN_THRESHOLD
    heavy_model = any(heavy in model for heavy in ASYNC_HEAVY_MODELS)
    
    if high_token_count or heavy_model or force_async:
        logger.info(f"Async required: tokens={max_tokens}, model={model}, force={force_async}")
        return True
    return False


# -----------------------------------------------------------------------------
# 4. Node Runners Implementation
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# S3 Hydration Helper
# -----------------------------------------------------------------------------
def _hydrate_s3_value(value: Any) -> Any:
    """
    If value is an S3 pointer string (s3://bucket/key), download and return content.
    Otherwise return value as-is.
    """
    if not isinstance(value, str) or not value.startswith("s3://"):
        return value
    
    try:
        bucket, key = value.replace("s3://", "").split("/", 1)
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return content
    except Exception as e:
        logger.warning(f"Failed to hydrate S3 value {value}: {e}")
        return value

def _hydrate_state_for_config(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Hydrate keys referenced in input_variables if present."""
    hydrated_state = state.copy()
    input_vars = config.get("input_variables", [])
    if isinstance(input_vars, list):
        for key in input_vars:
            val = _get_nested_value(hydrated_state, key)
            hydrated_val = _hydrate_s3_value(val)
            if hydrated_val != val:
                # Update nested state not supported easily here, so we just update top level or 
                # strictly mapped keys. For now, we update the top-level key if it matches.
                # Ideally we should use a set_nested_value, but input_variables usually refer to top level.
                if key in hydrated_state:
                    hydrated_state[key] = hydrated_val
    return hydrated_state

# -----------------------------------------------------------------------------
# 4. Node Runners Implementation
# -----------------------------------------------------------------------------

def llm_chat_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Standard LLM Chat Runner with Async detection and Retry/Hydration support."""
    
    # 0. Hydrate Data (Pre-execution)
    # Ensure S3 pointers defined in input_variables are downloaded
    exec_state = _hydrate_state_for_config(state, config)
    
    # [FIX] Override MOCK_MODE from state if present (payload takes precedence over Lambda env var)
    # This allows LLM Simulator to force MOCK_MODE=false even when Lambda default is true
    if "MOCK_MODE" in exec_state:
        # [CRITICAL] Convert boolean False to lowercase "false" string
        # Python's str(False) -> "False" which is truthy, need "false" for is_mock_mode()
        mock_mode_value = exec_state["MOCK_MODE"]
        if isinstance(mock_mode_value, bool):
            mock_mode_str = "true" if mock_mode_value else "false"
        else:
            mock_mode_str = str(mock_mode_value).lower()
        
        os.environ["MOCK_MODE"] = mock_mode_str
        logger.info(f"🔄 MOCK_MODE overridden from state: {exec_state['MOCK_MODE']} -> {mock_mode_str}")
    
    # [Cancellation] Check if execution has been cancelled before starting LLM call
    execution_arn = exec_state.get("execution_arn") or exec_state.get("ExecutionArn")
    if execution_arn and check_execution_cancelled(execution_arn):
        logger.warning(f"🚨 Execution cancelled, aborting LLM call for node {config.get('id', 'llm')}")
        raise Exception("Execution cancelled by user")
    
    # 1. Get Retry Config
    # [Fix] None defense: config['retry_config']가 None일 수 있음
    retry_config = config.get("retry_config") or {}
    max_retries = retry_config.get("max_retries", 0)  # Default 0 means single attempt
    base_delay = retry_config.get("base_delay", DEFAULT_RETRY_BASE_DELAY)
    
    # Initialize retry loop variables
    attempt = 0
    last_error = None
    
    # [Critical] Get Lambda context for timeout management
    # Lambda context is available via global _lambda_context if set by handler
    lambda_context = globals().get('_lambda_context')
    
    while attempt <= max_retries:
        # [Cancellation] Check if execution has been cancelled before each attempt
        if execution_arn and check_execution_cancelled(execution_arn):
            logger.warning(f"🚨 Execution cancelled during retry attempt {attempt+1}, aborting LLM call for node {config.get('id', 'llm')}")
            raise Exception("Execution cancelled by user")
        
        try:
            # [Critical] Lambda Early Exit: Check remaining execution time
            if lambda_context and hasattr(lambda_context, 'get_remaining_time_in_millis'):
                remaining_ms = lambda_context.get_remaining_time_in_millis()
                if remaining_ms < LAMBDA_EARLY_EXIT_THRESHOLD_MS:
                    logger.warning(f"🚨 Lambda timeout imminent ({remaining_ms}ms remaining). "
                                 f"Triggering async mode to prevent forceful termination.")
                    raise AsyncLLMRequiredException(
                        f"Insufficient Lambda time ({remaining_ms}ms < {LAMBDA_EARLY_EXIT_THRESHOLD_MS}ms)"
                    )
            
            # [Critical] State Isolation: Deep copy to prevent attempt_count pollution
            # Each retry attempt should have clean state to avoid template rendering contamination
            current_attempt_state = exec_state.copy()
            current_attempt_state["attempt_count"] = attempt + 1
            
            # Render prompts with current attempt count
            prompt_template = config.get("prompt_content") or config.get("user_prompt_template", "")
            prompt = _render_template(prompt_template, current_attempt_state)
            
            # [DEBUG] Log rendered prompt for troubleshooting empty prompt issues
            if not prompt or len(prompt.strip()) < 10:
                logger.error(f"⚠️ [PROMPT DEBUG] Empty or very short prompt detected for node {config.get('id', 'llm')}")
                logger.error(f"⚠️ [PROMPT DEBUG] prompt_template: {prompt_template[:200] if prompt_template else 'NONE'}...")
                logger.error(f"⚠️ [PROMPT DEBUG] rendered prompt: {prompt[:200] if prompt else 'EMPTY'}")
                logger.error(f"⚠️ [PROMPT DEBUG] state keys: {list(current_attempt_state.keys())}")
                # Check for input_text specifically
                input_text_val = current_attempt_state.get('input_text', '__NOT_FOUND__')
                logger.error(f"⚠️ [PROMPT DEBUG] input_text value: {str(input_text_val)[:100] if input_text_val != '__NOT_FOUND__' else 'NOT IN STATE'}")
            
            system_prompt_tmpl = config.get("system_prompt", "")
            system_prompt = _render_template(system_prompt_tmpl, current_attempt_state)
            
            node_id = config.get("id", "llm")

            # [Test Logic] Simulate Rate Limit Error for retry testing
            if prompt and "SIMULATE_RATE_LIMIT_ERROR" in prompt:
                logger.warning(f"🧪 Simulation triggered: Raising Rate Limit Error for node {node_id}")
                # Check provider for appropriate exception type
                provider = config.get("provider", "gemini")
                if provider == "gemini":
                    # Simulate Gemini rate limit (will be caught and retried)
                    raise Exception("429 Resource Exhausted: Quota exceeded for Gemini API")
                else:
                    # Simulate Bedrock ThrottlingException
                    from botocore.exceptions import ClientError
                    raise ClientError(
                        {"Error": {"Code": "ThrottlingException", "Message": "Simulated Rate Limit"}},
                        "InvokeModel"
                    )
            
            # 2. Check Async Conditions
            # node_id already defined above check async
            # [Fix] None defense: config['llm_config']가 None일 수 있음
            model = config.get("model") or (config.get("llm_config") or {}).get("model_id") or DEFAULT_LLM_MODEL
            max_tokens = config.get("max_tokens") or (config.get("llm_config") or {}).get("max_tokens", DEFAULT_MAX_TOKENS)
            temperature = config.get("temperature") or (config.get("llm_config") or {}).get("temperature", DEFAULT_TEMPERATURE)
            
            if should_use_async_llm(config):
                logger.warning(f"🚨 Async required by heuristic for node {node_id}")
                raise AsyncLLMRequiredException("Resource-intensive processing required")

            # 3. Invoke - Provider Selection (Gemini or Bedrock)
            meta = {"model": model, "max_tokens": max_tokens, "attempt": attempt + 1, "provider": "gemini"}
            
            # [Fix] Manually trigger callbacks since we are using Boto3 directly
            callbacks = config.get("callbacks", [])
            if callbacks:
                for cb in callbacks:
                    if hasattr(cb, 'on_llm_start'):
                        try:
                            cb.on_llm_start(serialized={"name": node_id}, prompts=[prompt])
                        except Exception:
                            pass

            # Provider selection: Gemini (default) or Bedrock (fallback)
            provider = config.get("provider", "gemini")
            
            if provider == "gemini":
                # Use Gemini Service (Native SDK)
                try:
                    from src.services.llm.gemini_service import GeminiService, GeminiConfig, GeminiModel
                    
                    # Map model string to GeminiModel enum
                    model_mapping = {
                        "gemini-2.0-flash": GeminiModel.GEMINI_2_0_FLASH,
                        "gemini-1.5-pro": GeminiModel.GEMINI_1_5_PRO,
                        "gemini-1.5-flash": GeminiModel.GEMINI_1_5_FLASH,
                        "gemini-1.5-flash-8b": GeminiModel.GEMINI_1_5_FLASH_8B,
                    }
                    
                    gemini_model = model_mapping.get(model, GeminiModel.GEMINI_1_5_FLASH)
                    
                    gemini_config = GeminiConfig(
                        model=gemini_model,
                        max_output_tokens=max_tokens,
                        temperature=temperature,
                        system_instruction=system_prompt
                    )
                    
                    # Prepare multimodal content if S3 URIs present
                    cleaned_prompt, multimodal_parts = prepare_multimodal_content(prompt, exec_state)
                    
                    service = GeminiService(config=gemini_config)
                    
                    # Use multimodal invocation if images/videos detected
                    if multimodal_parts:
                        logger.info(f"Invoking Gemini Vision API with {len(multimodal_parts)} multimodal parts")
                        
                        # Extract sources from multimodal_parts using unified 'source' key
                        # source can be: bytes (hydrated) or S3 URI string
                        image_sources = [p["source"] for p in multimodal_parts]
                        mime_types = [p.get("mime_type", "image/jpeg") for p in multimodal_parts]
                        
                        # Log multimodal details for debugging
                        for i, part in enumerate(multimodal_parts):
                            hydrated = part.get("hydrated", False)
                            source_type = "bytes" if hydrated else "S3 URI"
                            logger.debug(f"  Part {i+1}: {part['mime_type']} ({source_type})")
                        
                        # Call invoke_with_images for multimodal processing
                        # [Cancellation] Check before expensive multimodal call
                        if execution_arn and check_execution_cancelled(execution_arn):
                            logger.warning(f"🚨 Execution cancelled before multimodal LLM call for node {node_id}")
                            raise Exception("Execution cancelled by user")
                        
                        resp = service.invoke_with_images(
                            user_prompt=cleaned_prompt,
                            image_sources=image_sources,
                            mime_types=mime_types,
                            system_instruction=system_prompt,
                            max_output_tokens=max_tokens,
                            temperature=temperature
                        )
                    else:
                        # Standard text-only invocation
                        # [Cancellation] Check before LLM call
                        if execution_arn and check_execution_cancelled(execution_arn):
                            logger.warning(f"🚨 Execution cancelled before LLM call for node {node_id}")
                            raise Exception("Execution cancelled by user")
                        
                        resp = service.invoke_model(
                            user_prompt=prompt,
                            system_instruction=system_prompt,
                            max_output_tokens=max_tokens,
                            temperature=temperature
                        )
                    
                    # Extract text from Gemini response structure
                    text = ""
                    if "content" in resp and isinstance(resp["content"], list) and resp["content"]:
                        text = resp["content"][0].get("text", "")
                    elif "text" in resp:
                        text = resp["text"]
                    else:
                        # [FIX] JSON Serialization: Convert Python dict to proper JSON string
                        # This ensures json_parse operator can process the output
                        text = json.dumps(resp, default=str)
                    
                    # Normalize usage statistics
                    # [Fix] None defense: resp['metadata']가 None일 수 있음
                    raw_usage = (resp.get("metadata") or {}).get("token_usage", {})
                    usage = normalize_llm_usage(raw_usage, "gemini")
                    meta["provider"] = "gemini"
                    meta["multimodal"] = len(multimodal_parts) > 0
                    
                except Exception as gemini_error:
                    # 🛡️ [Enhanced Fallback Logging] Gemini 실패 시 상세한 폴백 로깅
                    error_type = type(gemini_error).__name__
                    error_msg = str(gemini_error).lower()
                    
                    # 에러 카테고리 분류
                    if any(keyword in error_msg for keyword in ["429", "quota", "rate limit", "resource exhausted"]):
                        error_category = "RATE_LIMIT"
                        log_level = logger.warning
                        fallback_reason = "Rate limit exceeded"
                    elif any(keyword in error_msg for keyword in ["403", "forbidden", "permission", "unauthorized"]):
                        error_category = "AUTHENTICATION"
                        log_level = logger.error
                        fallback_reason = "Authentication/permission error"
                    elif any(keyword in error_msg for keyword in ["timeout", "deadline", "unavailable"]):
                        error_category = "TIMEOUT"
                        log_level = logger.warning
                        fallback_reason = "Timeout/network error"
                    elif any(keyword in error_msg for keyword in ["empty", "must not be empty", "required"]):
                        # Input validation error - distinct from content safety
                        error_category = "VALIDATION_ERROR"
                        log_level = logger.error
                        fallback_reason = "Empty or invalid input"
                    elif any(keyword in error_msg for keyword in ["blocked", "safety", "content filter", "harm"]):
                        # More specific patterns for actual content safety issues
                        error_category = "CONTENT_SAFETY"
                        log_level = logger.warning
                        fallback_reason = "Content safety filter"
                    else:
                        error_category = "UNKNOWN"
                        log_level = logger.warning
                        fallback_reason = "Unknown error"
                    
                    log_level(
                        f"🚨 [Gemini Fallback Triggered] Node: {node_id}, "
                        f"Category: {error_category}, Reason: {fallback_reason}, "
                        f"Error: {gemini_error}, Falling back to Bedrock"
                    )
                    
                    # 재시도 가능한 에러인지 확인
                    is_retryable = any(keyword in error_msg for keyword in [
                        "429", "quota", "rate limit", "resource exhausted",
                        "timeout", "deadline exceeded", "unavailable"
                    ])
                    
                    if is_retryable:
                        # 재시도 가능한 에러 - 재시도 루프에 전달
                        logger.info(f"Retryable Gemini error, propagating to retry loop: {gemini_error}")
                        raise gemini_error
                    else:
                        # 비재시도 에러 - Bedrock 폴백
                        logger.warning(
                            f"Non-retryable Gemini error, falling back to Bedrock: {gemini_error}"
                        )
                        provider = "bedrock"
            
            if provider == "bedrock":
                # Bedrock fallback (existing logic)
                meta["provider"] = "bedrock"
                
                # [Critical] Use configurable model mapping instead of hardcoded map
                bedrock_model_map = get_bedrock_model_map()
                bedrock_model_id = bedrock_model_map.get(model, model)  # Use original if not in map
                
                # Log model mapping if applied
                if bedrock_model_id != model:
                    logger.info(f"Model mapped for Bedrock: {model} -> {bedrock_model_id}")
                
                # [Critical] Calculate safe timeout: remaining Lambda time - safety buffer
                read_timeout = DEFAULT_BEDROCK_TIMEOUT
                if lambda_context and hasattr(lambda_context, 'get_remaining_time_in_millis'):
                    remaining_ms = lambda_context.get_remaining_time_in_millis()
                    # Set timeout to remaining time minus buffer (in seconds)
                    max_safe_timeout = max(10, (remaining_ms - LAMBDA_TIMEOUT_BUFFER_MS) / 1000)
                    read_timeout = min(read_timeout, max_safe_timeout)
                    logger.info(f"Adjusted Bedrock timeout: {read_timeout}s (Lambda remaining: {remaining_ms}ms)")
                
                # [Cancellation] Check before Bedrock call
                if execution_arn and check_execution_cancelled(execution_arn):
                    logger.warning(f"🚨 Execution cancelled before Bedrock LLM call for node {node_id}")
                    raise Exception("Execution cancelled by user")
                
                resp = invoke_bedrock_model(
                    model_id=bedrock_model_id,
                    system_prompt=system_prompt,
                    user_prompt=prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    read_timeout_seconds=read_timeout
                )
                text = extract_text_from_bedrock_response(resp)
                
                # Extract and normalize usage stats
                raw_usage = {}
                if isinstance(resp, dict) and "usage" in resp:
                    raw_usage = resp["usage"]
                usage = normalize_llm_usage(raw_usage, "bedrock")
            
            # [Fix] Manually trigger on_llm_end
            if callbacks:
                llm_result = LLMResult(generations=[[Generation(text=text)]], llm_output={"usage": usage})
                for cb in callbacks:
                    if hasattr(cb, 'on_llm_end'):
                        try:
                            cb.on_llm_end(response=llm_result)
                        except Exception:
                            pass
            
            # Update history
            current_history = state.get("step_history", [])
            new_history = current_history + [f"{node_id}:llm_call"]
            
            # [Critical] Parse JSON response if parse_json flag is set
            # Enables structured output validation and schema enforcement
            parse_json = config.get("parse_json", False)
            output_value = text
            
            if parse_json:
                logger.info(f"[LLM Response] Parsing JSON for node {node_id}")
                output_value = parse_llm_json_response(text, fallback_value=text)
                
                # Log parsing result
                if isinstance(output_value, (dict, list)):
                    logger.info(f"[LLM Response] Successfully parsed JSON ({type(output_value).__name__})")
                else:
                    logger.warning(f"[LLM Response] JSON parsing failed, using raw text")
            
            # [FIX] Output Key Priority: Use user-specified output_key first, fall back to node_id pattern
            # This ensures test workflows can verify results using exact key names
            out_key = config.get("output_key") or config.get("writes_state_key") or f"{node_id}_output"
            logger.debug(f"[LLM Response] Output key resolved: {out_key} (from config: {config.get('output_key')})")
            # 🛡️ [Guard] Layer 1: Validate output keys (Reserved key check)
            raw_output = {out_key: output_value, f"{node_id}_meta": meta, "step_history": new_history, "usage": usage}
            validated_output = _validate_output_keys(raw_output, node_id)
            # 🛡️ [Guard] Layer 2: Schema validation (Type safety)
            return validate_state_with_schema(validated_output, node_id)
            
        except Exception as e:
            last_error = e
            logger.warning(f"LLM execution attempt {attempt+1}/{max_retries+1} failed: {e}")
            
            # [Fix] Manually trigger on_llm_error
            callbacks = config.get("callbacks", [])
            if callbacks:
                for cb in callbacks:
                    if hasattr(cb, 'on_llm_error'):
                        try:
                            cb.on_llm_error(error=e)
                        except Exception:
                            pass

            if isinstance(e, AsyncLLMRequiredException):
                # Bubble up to let orchestrator pause
                raise
            
            # Retry logic
            if attempt < max_retries:
                # Calculate backoff with jitter
                delay = min(DEFAULT_RETRY_MAX_DELAY, base_delay * (2 ** attempt)) * (0.5 + random.random())
                time.sleep(delay)
                attempt += 1
                continue
            else:
                # 🛡️ [Enhanced Final Error Handling] 모든 재시도 실패 시 사용자 친화적 에러
                human_readable_error = humanize_llm_error(last_error, meta.get("provider", "unknown"), node_id)
                
                logger.error(
                    f"🚨 [LLM Execution Failed] Node: {node_id}, "
                    f"Attempts: {max_retries+1}, Final error: {last_error}, "
                    f"Human readable: {human_readable_error}"
                )
                
                # 사용자 친화적인 메시지를 포함한 새로운 예외 발생
                from src.common.exceptions import LLMServiceError
                raise LLMServiceError(
                    message=human_readable_error,
                    original_error=last_error,
                    provider=meta.get("provider", "unknown"),
                    node_id=node_id,
                    attempts=max_retries+1
                ) from last_error

    # Should not reach here
    raise last_error if last_error else RuntimeError("LLM execution failed unexpectedly")

    # Should not reach here
    raise last_error if last_error else RuntimeError("LLM execution failed unexpectedly")


def aggregator_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Aggregates results from parallel branches or iterations, with special token usage aggregation."""
    node_id = config.get("id", "aggregator")
    
    # [Token Aggregation] 여러 소스의 토큰 사용량 합산
    total_input_tokens = 0
    total_output_tokens = 0
    aggregation_sources = []
    
    # 1. 병렬 그룹 결과 합산
    # [FIX] Support flattened structure from parallel_group_runner (payload size optimization)
    # Strategy 1: Use pre-computed branch_token_details if available
    if 'branch_token_details' in state:
        branch_token_details = state['branch_token_details']
        logger.info(f"Aggregator using pre-computed branch_token_details: {len(branch_token_details)} branches")
        for detail in branch_token_details:
            total_input_tokens += detail.get('input_tokens', 0)
            total_output_tokens += detail.get('output_tokens', 0)
            aggregation_sources.append({
                'source': 'parallel_branch',
                'branch_id': detail.get('branch_id', 'unknown'),
                'input_tokens': detail['input_tokens'],
                'output_tokens': detail['output_tokens'],
                'total_tokens': detail['total_tokens']
            })
    else:
        # Strategy 2: Scan for flattened branch results (branch_* keys)
        logger.info(f"Aggregator scanning for flattened branch results (branch_* keys)")
        for key, value in state.items():
            if key.startswith('branch_') and isinstance(value, dict):
                # Extract branch_id from key (e.g., 'branch_doc_summarize' -> 'branch_doc_summarize')
                branch_id = key
                usage = extract_token_usage(value)
                input_tokens = usage['input_tokens']
                output_tokens = usage['output_tokens']
                
                if input_tokens > 0 or output_tokens > 0:  # Only count branches with actual token usage
                    total_input_tokens += input_tokens
                    total_output_tokens += output_tokens
                    
                    aggregation_sources.append({
                        'source': 'parallel_branch',
                        'branch_id': branch_id,
                        'input_tokens': input_tokens,
                        'output_tokens': output_tokens,
                        'total_tokens': input_tokens + output_tokens
                    })
        
        if aggregation_sources:
            logger.info(f"Aggregator found {len(aggregation_sources)} branches in flattened structure")
        else:
            logger.warning(f"Aggregator {node_id}: No parallel branch results found in state")
    
    # 2. ForEach/Map 결과 합산
    foreach_result = state.get('for_each_result') or state.get('map_result')
    if foreach_result and isinstance(foreach_result, list):
        for i, item_result in enumerate(foreach_result):
            if isinstance(item_result, dict):
                usage = extract_token_usage(item_result)
                input_tokens = usage['input_tokens']
                output_tokens = usage['output_tokens']
                
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                
                aggregation_sources.append({
                    'source': 'iteration',
                    'iteration': i,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'total_tokens': input_tokens + output_tokens
                })
    
    # 3. 중첩 ForEach 결과 합산
    nested_result = state.get('nested_for_each_result')
    if nested_result and isinstance(nested_result, list):
        usage = aggregate_tokens_from_nested(nested_result)
        total_input_tokens += usage['input_tokens']
        total_output_tokens += usage['output_tokens']
        
        # Add aggregation sources for nested results
        for outer_result in nested_result:
            if isinstance(outer_result, dict):
                outer_id = outer_result.get('outer_id', 'unknown')
                inner_results = outer_result.get('inner_results', [])
                
                for inner_result in inner_results:
                    if isinstance(inner_result, dict):
                        inner_usage = extract_token_usage(inner_result)
                        
                        aggregation_sources.append({
                            'source': 'nested_iteration',
                            'outer_id': outer_id,
                            'input_tokens': inner_usage['input_tokens'],
                            'output_tokens': inner_usage['output_tokens'],
                            'total_tokens': inner_usage['total_tokens']
                        })
    
    # 4. 직접적인 토큰 사용량 필드 합산 제거 (token_utils.accumulate_tokens_in_state로 대체)
    # accumulate_tokens_in_state가 이전 상태의 토큰을 자동으로 누적하므로 수동 합산 불필요
    
    total_tokens = total_input_tokens + total_output_tokens
    
    # 결과 업데이트
    result = {
        'total_input_tokens': total_input_tokens,
        'total_output_tokens': total_output_tokens,
        'total_tokens': total_tokens,
        'aggregation_sources': aggregation_sources,
        'aggregated_at': node_id
    }
    
    # [Accumulation] 이전 상태의 토큰 값과 누적
    result = accumulate_tokens_in_state(result, state)
    
    # 비용 계산 추가
    estimated_cost = calculate_cost_usd({
        'input_tokens': result['total_input_tokens'],
        'output_tokens': result['total_output_tokens']
    })
    result['estimated_cost_usd'] = estimated_cost
    
    logger.info(f"Aggregator {node_id}: Aggregated {len(aggregation_sources)} sources, "
                f"total tokens: {result['total_tokens']} "
                f"({result['total_input_tokens']} input + {result['total_output_tokens']} output), "
                f"cost: ${estimated_cost:.6f}")
    
    return result


def operator_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Runs arbitrary python code (sandboxed)."""
    # 🛡️ [Guard] Input Validation
    if config is None:
        config = {}
    
    node_id = config.get("id", "operator")
    code = config.get("code") or (config.get("config") or {}).get("code")
    result_updates = {}
    mock_mode_enabled = _is_mock_mode()
    
    # 1. 'sets' shorthand
    sets = config.get("sets")
    if isinstance(sets, dict):
        result_updates.update(sets)

    # 2. Execute code with security restrictions
    if code:
        # Only allow exec in MOCK_MODE to avoid RCE in production paths
        if not mock_mode_enabled:
            raise PermissionError(f"Operator code execution is disabled outside MOCK_MODE (node={node_id})")
        try:
            # Security: Restrict builtins
            # [FIX v3.2] Pre-import safe standard library modules
            import time as _time
            import json as _json
            import re as _re
            import uuid as _uuid
            import math as _math
            import random as _random
            import sys as _sys
            import collections as _collections
            from datetime import datetime as _datetime, timezone as _timezone, timedelta as _timedelta
            from collections import Counter as _Counter
            
            # [FIX v3.2] Whitelisted __import__ wrapper
            SAFE_MODULES = {
                'time': _time,
                'json': _json,
                're': _re,
                'uuid': _uuid,
                'math': _math,
                'random': _random,
                'datetime': __import__('datetime'),
                'collections': _collections,
                'sys': _sys,
            }
            
            def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
                if name not in SAFE_MODULES:
                    raise ImportError(f"Module '{name}' is not allowed in workflow code. Allowed: {list(SAFE_MODULES.keys())}")
                return SAFE_MODULES[name]
            
            safe_builtins = {
                "print": print,
                "len": len,
                "list": list,
                "dict": dict,
                "range": range,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "sum": sum,
                "min": min,
                "max": max,
                "abs": abs,
                "round": round,
                "enumerate": enumerate,
                "zip": zip,
                "sorted": sorted,
                "reversed": reversed,
                "all": all,
                "any": any,
                "filter": filter,
                "map": map,
                "isinstance": isinstance,
                "type": type,
                "tuple": tuple,
                "set": set,
                "frozenset": frozenset,
                "getattr": getattr,
                "setattr": setattr,
                "hasattr": hasattr,
                "open": None,
                "eval": None,
                "exec": None,
                "compile": None,
                "Exception": Exception,
                "ValueError": ValueError,
                "TypeError": TypeError,
                "KeyError": KeyError,
                "RuntimeError": RuntimeError,
                "IndexError": IndexError,
                "AttributeError": AttributeError,
                "StopIteration": StopIteration,
                "ImportError": ImportError,
                "True": True,
                "False": False,
                "None": None,
                "time": _time,
                "json": _json,
                "re": _re,
                "uuid": _uuid,
                "math": _math,
                "random": _random,
                "datetime": _datetime,
                "timezone": _timezone,
                "timedelta": _timedelta,
                "Counter": _Counter,
                "collections": _collections,
                "__import__": _safe_import,
            }
            
            # [Guard] Safety check for state
            if state is None:
                logger.warning(f"[Operator] {node_id} received None state! Initializing empty dict.")
                exec_state = {}
            else:
                exec_state = dict(state)
                
            local_vars = {"state": exec_state, "result": None}
            # Execute with restricted builtins
            exec(code, {"__builtins__": safe_builtins}, local_vars)
            code_result = local_vars.get("result")
            
            # Check if state was modified during execution
            if exec_state != state:
                # State was modified, merge changes (but be careful with security)
                for key, value in exec_state.items():
                    if key not in state or state[key] != value:
                        result_updates[key] = value
            
            if code_result is not None:
                if isinstance(code_result, dict): result_updates.update(code_result)
                else: result_updates[f"{node_id}_result"] = code_result
                    
        except Exception as e:
            logger.exception(f"Operator {node_id} failed")
            raise e
    
    # 3. Handle output_key if specified (for backward compatibility)
    output_key = config.get("output_key")
    if output_key and output_key in result_updates:
        # output_key is already handled above
        pass
            
    if not result_updates:
        result_updates = {f"{node_id}_status": "ok"}
        
    return result_updates

def api_call_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    node_id = config.get("id", "api_call")
    # Hydrate potential S3 inputs first
    exec_state = _hydrate_state_for_config(state, config)
    
    url_template = config.get("url")
    if not url_template: raise ValueError("api_call requires 'url'")

    def _validate_outbound_url(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Only http/https schemes are allowed")
        host = parsed.hostname
        if not host:
            raise ValueError("URL must include hostname")
        # Restrict ports to common web ports to avoid hitting infra/admin ports
        if parsed.port not in (None, 80, 443):
            raise ValueError(f"Port {parsed.port} is not allowed (only 80/443)")
        try:
            infos = socket.getaddrinfo(host, parsed.port or 80, proto=socket.IPPROTO_TCP)
        except socket.gaierror:
            raise ValueError("Invalid hostname")
        for info in infos:
            ip_str = info[4][0]
            ip_obj = ipaddress.ip_address(ip_str)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_reserved or ip_obj.is_multicast:
                raise ValueError(f"Access to internal/private IP is blocked ({ip_str})")
        return None

    url = _render_template(url_template, exec_state)
    method = (_render_template(config.get("method") or "GET", exec_state)).upper()
    headers = _render_template(config.get("headers"), exec_state) or {}
    params = _render_template(config.get("params"), exec_state)
    json_body = _render_template(config.get("json"), exec_state)
    timeout = _render_template(config.get("timeout", 10), exec_state)

    allowed_methods = {"GET", "POST", "PUT", "PATCH", "DELETE"}
    if method not in allowed_methods:
        raise ValueError(f"Method {method} not allowed; allowed: {sorted(allowed_methods)}")

    try:
        _validate_outbound_url(url)
    except ValueError as ve:
        return {f"{node_id}_status": "error", f"{node_id}_error": str(ve)}

    # Clamp timeout to a safe upper bound
    try:
        timeout_val = float(timeout) if timeout is not None else DEFAULT_HTTP_TIMEOUT
    except Exception:
        timeout_val = DEFAULT_HTTP_TIMEOUT
    timeout_val = max(1.0, min(timeout_val, MAX_HTTP_TIMEOUT))

    try:
        import requests
        resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout_val, allow_redirects=False)
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return {f"{node_id}_status": resp.status_code, f"{node_id}_response": body}
    except requests.exceptions.Timeout:
        return {f"{node_id}_status": "error", f"{node_id}_error": f"Request timed out (limit: {timeout_val}s)"}
    except requests.exceptions.ConnectionError:
        return {f"{node_id}_status": "error", f"{node_id}_error": "Connection refused or DNS failure"}
    except Exception as e:
        return {f"{node_id}_status": "error", f"{node_id}_error": str(e)}

def db_query_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a database query.
    
    SECURITY NOTE: This runner is disabled in production environments.
    Only allowed when ALLOW_DB_QUERY=true environment variable is set.
    """
    node_id = config.get("id", "db_query")
    
    # Security: Block db_query in production unless explicitly allowed
    allow_db_query = os.getenv("ALLOW_DB_QUERY", "false").lower() in ("true", "1", "yes")
    is_production = os.getenv("APP_ENV", "").lower() == "production"
    
    if is_production and not allow_db_query:
        logger.warning(f"db_query node {node_id} blocked in production environment")
        return {f"{node_id}_error": "db_query is not allowed in production"}
    
    query = config.get("query")
    conn_str = _render_template(config.get("connection_string"), state)
    if not query or not conn_str: raise ValueError("db_query requires 'query' and 'connection_string'")

    try:
        from src.sqlalchemy import create_engine, text
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute(text(query), config.get("params", {}))
            fetch = config.get("fetch", "all")
            if fetch == "one":
                row = result.fetchone()
                res = dict(row) if row else None
            elif fetch == "all":
                res = [dict(r) for r in result.fetchall()]
            else:
                res = result.rowcount
            return {f"{node_id}_result": res}
    except Exception as e:
        return {f"{node_id}_error": str(e)}


# -----------------------------------------------------------------------------
# Skill Executor Runner - Execute skills from src.active_skills context
# -----------------------------------------------------------------------------

def skill_executor_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a skill's tool from src.the hydrated active_skills context.
    """
    import time
    from datetime import datetime, timezone
    
    node_id = config.get("id", "skill_executor")
    skill_ref = config.get("skill_ref")
    tool_call = config.get("tool_call")
    input_mapping = config.get("input_mapping", {})
    output_key = config.get("output_key", f"{node_id}_result")
    error_handling = config.get("error_handling", "fail")
    
    # Hydrate potential inputs
    exec_state = _hydrate_state_for_config(state, config)
    
    # Validate required config
    if not skill_ref:
        raise ValueError(f"skill_executor node '{node_id}' requires 'skill_ref'")
    if not tool_call:
        raise ValueError(f"skill_executor node '{node_id}' requires 'tool_call'")
    
    # Get active skills from src.state
    active_skills = exec_state.get("active_skills", {})
    
    # Look up the skill
    skill = active_skills.get(skill_ref)
    if not skill:
        error_msg = f"Skill '{skill_ref}' not found in active_skills. Available: {list(active_skills.keys())}"
        if error_handling == "skip":
            logger.warning(f"[skill_executor] {error_msg} - Skipping")
            return {output_key: None, f"{node_id}_skipped": True}
        raise ValueError(error_msg)
    
    # Find the tool definition
    tool_definitions = skill.get("tool_definitions", [])
    tool_def = None
    for td in tool_definitions:
        if td.get("name") == tool_call:
            tool_def = td
            break
    
    if not tool_def:
        error_msg = f"Tool '{tool_call}' not found in skill '{skill_ref}'. Available: {[t.get('name') for t in tool_definitions]}"
        if error_handling == "skip":
            logger.warning(f"[skill_executor] {error_msg} - Skipping")
            return {output_key: None, f"{node_id}_skipped": True}
        raise ValueError(error_msg)
    
    # Render input mappings
    rendered_inputs = {}
    for key, template in input_mapping.items():
        rendered_inputs[key] = _render_template(template, exec_state)
    
    # Determine handler type and dispatch
    handler_type = tool_def.get("handler_type", "operator")
    handler_config = tool_def.get("handler_config", {})
    
    # Merge skill system instructions if this is an LLM call
    if handler_type == "llm_chat" and skill.get("system_instructions"):
        handler_config = {**handler_config}
        existing_system = handler_config.get("system_prompt", "")
        skill_instructions = skill.get("system_instructions", "")
        handler_config["system_prompt"] = f"{skill_instructions}\n\n{existing_system}".strip()
    
    # Build execution config
    exec_config = {
        "id": f"{node_id}_{tool_call}",
        **handler_config,
        **rendered_inputs,
        "callbacks": config.get("callbacks", [])
    }
    
    # Track execution time
    start_time = time.time()
    result = {}
    error = None
    
    try:
        # Dispatch to appropriate handler
        handler = NODE_REGISTRY.get(handler_type)
        if not handler:
            raise ValueError(f"Unknown handler_type '{handler_type}' for tool '{tool_call}'")
        
        result = handler(exec_state, exec_config)
        
    except Exception as e:
        error = str(e)
        logger.exception(f"[skill_executor] Tool '{tool_call}' in skill '{skill_ref}' failed")
        
        if error_handling == "skip":
            result = {output_key: None, f"{node_id}_error": error}
        elif error_handling == "retry":
            # For now, just fail on retry - could implement actual retry logic
            raise
        else:  # fail
            raise
    
    execution_time_ms = int((time.time() - start_time) * 1000)
    
    # Build execution log entry
    log_entry = {
        "skill_id": skill_ref,
        "node_id": node_id,
        "tool_name": tool_call,
        "input_params": rendered_inputs,
        "execution_time_ms": execution_time_ms,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        log_entry["error"] = error
    
    # Prepare output
    output = {output_key: result}
    
    # Append to skill execution log (uses Annotated accumulator)
    current_log = exec_state.get("skill_execution_log", [])
    output["skill_execution_log"] = current_log + [log_entry]
    
    # Update step history
    current_history = exec_state.get("step_history", [])
    output["step_history"] = current_history + [f"{node_id}:skill_executor:{skill_ref}.{tool_call}"]
    
    # 🛡️ [Guard] Layer 1: Validate output keys (Reserved key check)
    validated_output = _validate_output_keys(output, node_id)
    # 🛡️ [Guard] Layer 2: Schema validation (Type safety)
    return validate_state_with_schema(validated_output, node_id)

def for_each_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Executes sub-node for each item in list concurrently."""
    
    # [Cancellation] Check if execution has been cancelled before starting parallel processing
    execution_arn = state.get("execution_arn") or state.get("ExecutionArn")
    if execution_arn and check_execution_cancelled(execution_arn):
        logger.warning(f"🚨 Execution cancelled, aborting for_each processing for node {config.get('id', 'for_each')}")
        return {config.get("output_key", "for_each_results"): []}
    
    # [Fix] Support both flat config and nested config (node_def structure)
    # When called from builder, config is the full node_def: {id, type, config: {...}}
    # Extract the inner config if present
    inner_config = config.get("config", {}) if config.get("type") == "for_each" else {}
    
    # Try inner config first, then fall back to top-level config
    # [Fix v2] Also support items_path (alternative to input_list_key)
    input_list_key = (
        inner_config.get("input_list_key") or 
        inner_config.get("items_path") or  # Alternative key name
        config.get("input_list_key") or 
        config.get("items_path") or  # Alternative key name
        (config.get("foreach_config") or {}).get("items_source", "").replace("$.", "")
    )
    
    # [Fix v2] Support body_nodes as alternative to sub_node_config
    # body_nodes is a list of node IDs to execute - we create a simple operator wrapper
    body_nodes = inner_config.get("body_nodes") or config.get("body_nodes")
    item_key = inner_config.get("item_key") or config.get("item_key") or "item"
    
    sub_node_config = (
        inner_config.get("sub_node_config") or 
        config.get("sub_node_config") or 
        (config.get("foreach_config") or {}).get("item_processor")
    )
    
    # [Critical Fix] Support sub_workflow structure (used in test configs)
    # sub_workflow contains nodes array - extract first node as sub_node_config
    if not sub_node_config:
        sub_workflow = inner_config.get("sub_workflow") or config.get("sub_workflow")
        if sub_workflow and isinstance(sub_workflow, dict):
            workflow_nodes = sub_workflow.get("nodes", [])
            if workflow_nodes and len(workflow_nodes) > 0:
                # Use first node as the iteration processor
                sub_node_config = workflow_nodes[0]
                logger.info(f"[ForEach] Extracted sub_node_config from sub_workflow.nodes[0]")
    
    # [Critical Fix] Support subgraph_inline structure (builder transformation)
    # Builder may convert sub_workflow to subgraph_inline
    if not sub_node_config:
        subgraph_inline = config.get("subgraph_inline")
        if subgraph_inline and isinstance(subgraph_inline, dict):
            subgraph_nodes = subgraph_inline.get("nodes", [])
            if subgraph_nodes and len(subgraph_nodes) > 0:
                sub_node_config = subgraph_nodes[0]
                logger.info(f"[ForEach] Extracted sub_node_config from subgraph_inline.nodes[0]")
    
    # [Fix v2] If body_nodes is specified but no sub_node_config, create a passthrough operator
    if body_nodes and not sub_node_config:
        # Create a simple operator that just logs the item
        sub_node_config = {
            "type": "operator",
            "config": {
                "code": f"state['{item_key}'] = state.get('item')\nprint(f'Processing {{state.get(\"item\")}}')\nstate['processed'] = True",
                "output_key": "iteration_result"
            }
        }
    
    output_key = inner_config.get("output_key") or config.get("output_key") or "for_each_results"
    max_iterations = (
        inner_config.get("max_iterations") or 
        config.get("max_iterations") or 
        (config.get("foreach_config") or {}).get("max_iterations", 10)
    )
    metadata = inner_config.get("metadata", {}) or config.get("metadata", {})
    segmentation_policy = metadata.get("segmentation_policy")
    
    # Handle None input_list_key
    if not input_list_key:
        logger.warning(f"for_each config missing input_list_key/items_path: {config.keys()}")
        return {output_key: []}
    
    if "." in input_list_key:
        input_list_key = input_list_key.split(".")[-1] # Simple extraction for now
        
    # [Critical Guard] Validate essential config before proceeding
    if not input_list_key:
        logger.error(f"[ForEach] Missing input_list_key. Config keys: {config.keys()}")
        return {output_key: []}
    
    if not sub_node_config:
        logger.error(f"[ForEach] Missing sub_node_config. Config keys: {config.keys()}")
        return {output_key: []}
    
    if not isinstance(sub_node_config, dict):
        logger.error(f"[ForEach] sub_node_config must be dict, got {type(sub_node_config)}")
        return {output_key: []}
        
    input_list = _get_nested_value(state, input_list_key, [])
    if not isinstance(input_list, list):
        logger.warning(f"for_each input {input_list_key} is not a list")
        input_list = []

    # [Check] Segmentation Policy
    if segmentation_policy == "force_split":
        # Divide iterations into chunks
        # This is a simulation: In a real distributed map, the state machine handles this.
        # Here we just log and potentially enforce the limit strictly.
        logger.info(f"🔄 Segmentation Policy 'force_split' active. Processing first {max_iterations} items.")
        
    if len(input_list) > max_iterations:
        logger.warning(f"for_each truncated {len(input_list)} -> {max_iterations}")
        input_list = input_list[:max_iterations]
    
    sub_node_type = sub_node_config.get("type", "llm")
    sub_node_func = NODE_REGISTRY.get(sub_node_type)
    if not sub_node_func: 
        # Fallback for testing: if type is llm but no func found (unlikely), default to llm_chat_runner
        if sub_node_type == "llm":
            sub_node_func = llm_chat_runner
        else:
            raise ValueError(f"Unknown sub-node: {sub_node_type}")

    def worker(item):
        # [Cancellation] Check if execution has been cancelled before processing each item
        if execution_arn and check_execution_cancelled(execution_arn):
            logger.warning(f"🚨 Execution cancelled during parallel processing, skipping item {item}")
            return {"error": "Execution cancelled", "item": item}
        
        # [Optimization] Use ChainMap for zero-copy state view
        item_state = ChainMap({"item": item}, state)
        # Deep copy only the messages list if it exists (to avoid reducer conflicts)
        if "messages" in item_state and isinstance(item_state["messages"], list):
            item_state["messages"] = item_state["messages"].copy()
        
        # Merge sub_node_config into config-like structure for the runner
        # Ensure ID is unique per item if needed, but runner might not care
        c = sub_node_config.copy()
        c["id"] = f"{config.get('id', 'foreach')}_sub"
        
        # Render any templates in sub_node_config against item_state
        # Note: sub_node_config might be nested, _render_template handles dicts
        rendered_sub = _render_template(c, item_state)
        return sub_node_func(item_state, rendered_sub)

    # Handle empty list early
    if not input_list:
        return {output_key: []}
    
    # Dynamic worker count based on CPU cores and list size
    cpu_count = os.cpu_count() or 2
    max_workers = min(len(input_list), max(1, cpu_count // 2))  # Conservative: half of CPU cores
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(worker, input_list))
    
    # [Token Aggregation] for_each iteration들의 토큰 사용량 합산
    total_input_tokens = 0
    total_output_tokens = 0
    iteration_token_details = []
    
    for i, result in enumerate(results):
        # 각 iteration 결과에서 토큰 사용량 추출 (usage 또는 token_usage 키 지원)
        if isinstance(result, dict):
            usage = extract_token_usage(result)
            input_tokens = usage['input_tokens']
            output_tokens = usage['output_tokens']
            
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            
            iteration_token_details.append({
                'iteration': i,
                'input_tokens': input_tokens,
                'output_tokens': output_tokens,
                'total_tokens': input_tokens + output_tokens
            })
    
    # 합산된 토큰 정보를 결과에 추가
    result_updates = {output_key: results}
    result_updates['total_input_tokens'] = total_input_tokens
    result_updates['total_output_tokens'] = total_output_tokens
    result_updates['total_tokens'] = total_input_tokens + total_output_tokens
    result_updates['iteration_token_details'] = iteration_token_details
    
    # [Accumulation] 이전 상태의 토큰 값과 누적
    temp_result = {
        'total_input_tokens': result_updates['total_input_tokens'],
        'total_output_tokens': result_updates['total_output_tokens'],
        'total_tokens': result_updates['total_tokens']
    }
    accumulated_result = accumulate_tokens_in_state(temp_result, state)
    result_updates.update(accumulated_result)
    
    # 비용 계산 추가
    estimated_cost = calculate_cost_usd({
        'input_tokens': result_updates['total_input_tokens'],
        'output_tokens': result_updates['total_output_tokens']
    })
    result_updates['estimated_cost_usd'] = estimated_cost
    
    logger.info(f"ForEach {config.get('id', 'for_each')}: Processed {len(results)} iterations, "
                f"total tokens: {result_updates['total_tokens']} "
                f"({total_input_tokens} input + {total_output_tokens} output), "
                f"cost: ${estimated_cost:.6f}")
    
    return result_updates


def nested_for_each_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Nested Map-in-Map 처리를 위한 재귀적 ForEach Runner.
    
    V3 하이퍼-스트레스 시나리오 지원:
    - 10개 국가 × 5개 산업군 = 50개 병렬 태스크
    - 중첩된 Map 구조 자동 처리
    
    Config 예시:
    {
        "type": "nested_for_each",
        "input_list_key": "countries",          # 외부 리스트
        "nested_config": {
            "input_list_key": "industries",     # 내부 리스트 (item 내 속성)
            "sub_node_config": {...}            # 최종 처리 노드
        },
        "output_key": "analysis_results",
        "max_outer_iterations": 10,
        "max_inner_iterations": 5
    }
    """
    node_id = config.get("id", "nested_foreach")
    input_list_key = config.get("input_list_key", "")
    # [Fix] None defense: config['nested_config'], config['metadata']가 None일 수 있음
    nested_config = config.get("nested_config") or {}
    output_key = config.get("output_key", "nested_results")
    max_outer = config.get("max_outer_iterations", 10)
    max_inner = config.get("max_inner_iterations", 5)
    metadata = config.get("metadata") or {}
    
    logger.info(f"🔄 Nested ForEach starting: {node_id}")
    
    # 외부 리스트 가져오기
    if "." in input_list_key:
        input_list_key = input_list_key.split(".")[-1]
    
    outer_list = _get_nested_value(state, input_list_key, [])
    if not isinstance(outer_list, list):
        logger.warning(f"Nested ForEach: {input_list_key} is not a list")
        return {output_key: []}
    
    # 외부 제한 적용
    if len(outer_list) > max_outer:
        logger.warning(f"Nested ForEach: truncating outer {len(outer_list)} -> {max_outer}")
        outer_list = outer_list[:max_outer]
    
    # 내부 설정 추출
    inner_list_key = nested_config.get("input_list_key", "")
    # [Fix] None defense: nested_config['sub_node_config']가 None일 수 있음
    sub_node_config = nested_config.get("sub_node_config") or {}
    
    if not sub_node_config:
        logger.error(f"Nested ForEach: missing sub_node_config")
        return {output_key: []}
    
    sub_node_type = sub_node_config.get("type", "llm")
    sub_node_func = NODE_REGISTRY.get(sub_node_type)
    
    if not sub_node_func:
        if sub_node_type == "llm":
            sub_node_func = llm_chat_runner
        else:
            raise ValueError(f"Unknown nested sub-node: {sub_node_type}")
    
    def process_outer_item(outer_idx: int, outer_item: Any) -> Dict[str, Any]:
        """외부 아이템 처리 (내부 리스트 포함)"""
        outer_item_id = outer_item.get("id", f"outer_{outer_idx}") if isinstance(outer_item, dict) else f"outer_{outer_idx}"
        
        # 내부 리스트 추출
        if isinstance(outer_item, dict) and inner_list_key:
            # inner_list_key에서 $. 제거
            clean_inner_key = inner_list_key.replace("$.", "").replace("$.item.", "")
            inner_list = outer_item.get(clean_inner_key, [])
        else:
            inner_list = []
        
        if not isinstance(inner_list, list):
            inner_list = [inner_list] if inner_list else []
        
        # 내부 제한 적용
        if len(inner_list) > max_inner:
            logger.debug(f"Nested ForEach [{outer_item_id}]: truncating inner {len(inner_list)} -> {max_inner}")
            inner_list = inner_list[:max_inner]
        
        inner_results = []
        
        def process_inner_item(inner_item: Any) -> Dict[str, Any]:
            """내부 아이템 처리"""
            # 상태 준비
            # [Optimization] Use ChainMap for zero-copy state view
            # Writes updates to the first dict, keeping 'state' pristine and avoiding deep/shallow copy overhead
            item_state = ChainMap({}, state)
            item_state["outer_item"] = outer_item
            item_state["inner_item"] = inner_item
            item_state["item"] = inner_item  # 기존 호환성 유지
            item_state["parent"] = outer_item
            
            # 메시지 복사 (레이스 컨디션 방지)
            if "messages" in item_state and isinstance(item_state["messages"], list):
                item_state["messages"] = item_state["messages"].copy()
            
            # 서브노드 설정 렌더링
            c = sub_node_config.copy()
            c["id"] = f"{node_id}_{outer_item_id}_sub"
            rendered_sub = _render_template(c, item_state)
            
            try:
                return sub_node_func(item_state, rendered_sub)
            except Exception as e:
                logger.error(f"Nested ForEach inner error [{outer_item_id}]: {e}")
                return {"error": str(e), "outer": outer_item_id}
        
        # 내부 리스트 병렬 처리
        if inner_list:
            cpu_count = os.cpu_count() or 2
            inner_workers = min(len(inner_list), max(1, cpu_count // 4))  # 보수적: 1/4 코어
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=inner_workers) as inner_executor:
                inner_results = list(inner_executor.map(process_inner_item, inner_list))
        
        return {
            "outer_id": outer_item_id,
            "outer_item": outer_item if isinstance(outer_item, dict) else {"value": outer_item},
            "inner_count": len(inner_list),
            "inner_results": inner_results
        }
    
    # 외부 리스트 병렬 처리
    all_results = []
    if outer_list:
        cpu_count = os.cpu_count() or 2
        outer_workers = min(len(outer_list), max(1, cpu_count // 2))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=outer_workers) as outer_executor:
            futures = [
                outer_executor.submit(process_outer_item, idx, item)
                for idx, item in enumerate(outer_list)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    logger.error(f"Nested ForEach outer error: {e}")
                    all_results.append({"error": str(e)})
    
    # 결과 집계
    total_inner_processed = sum(r.get("inner_count", 0) for r in all_results)
    logger.info(f"✅ Nested ForEach complete: {len(all_results)} outer × {total_inner_processed} inner tasks")
    
    # [Token Aggregation] 중첩 for_each의 토큰 사용량 합산 (재귀적 처리)
    usage = aggregate_tokens_from_nested(all_results)
    total_input_tokens = usage['input_tokens']
    total_output_tokens = usage['output_tokens']
    total_tokens = usage['total_tokens']
    
    # 상세 토큰 정보 수집 (디버깅용, 합산과 별도)
    nested_token_details = []
    for outer_result in all_results:
        if isinstance(outer_result, dict):
            outer_id = outer_result.get('outer_id', 'unknown')
            inner_results = outer_result.get('inner_results', [])
            
            for inner_result in inner_results:
                if isinstance(inner_result, dict):
                    inner_usage = extract_token_usage(inner_result)
                    
                    nested_token_details.append({
                        'outer_id': outer_id,
                        'input_tokens': inner_usage['input_tokens'],
                        'output_tokens': inner_usage['output_tokens'],
                        'total_tokens': inner_usage['total_tokens']
                    })
    
    # 합산된 토큰 정보를 결과에 추가
    result_updates = {
        output_key: all_results,
        f"{output_key}_summary": {
            "outer_count": len(all_results),
            "total_inner_count": total_inner_processed,
            "node_id": node_id
        }
    }
    result_updates['total_input_tokens'] = total_input_tokens
    result_updates['total_output_tokens'] = total_output_tokens
    result_updates['total_tokens'] = total_tokens
    result_updates['nested_token_details'] = nested_token_details
    
    # [Accumulation] 이전 상태의 토큰 값과 누적
    temp_result = {
        'total_input_tokens': result_updates['total_input_tokens'],
        'total_output_tokens': result_updates['total_output_tokens'],
        'total_tokens': result_updates['total_tokens']
    }
    accumulated_result = accumulate_tokens_in_state(temp_result, state)
    result_updates.update(accumulated_result)
    
    # 비용 계산 추가
    estimated_cost = calculate_cost_usd({
        'input_tokens': result_updates['total_input_tokens'],
        'output_tokens': result_updates['total_output_tokens']
    })
    result_updates['estimated_cost_usd'] = estimated_cost
    
    logger.info(f"Nested ForEach {node_id}: Processed {len(nested_token_details)} nested iterations, "
                f"total tokens: {result_updates['total_tokens']} "
                f"({total_input_tokens} input + {total_output_tokens} output), "
                f"cost: ${estimated_cost:.6f}")
    
    return result_updates


def route_draft_quality(state: Dict[str, Any]) -> str:
    draft = state.get("gemini_draft")
    if not isinstance(draft, dict) or not draft.get("is_complete"):
        return "reviser"
    return "send_email"


def parallel_group_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Executes branches in parallel and merges results."""
    node_id = config.get("id", "parallel_group")
    # [Fix] Support both nested config and direct config
    branches = config.get("branches", (config.get("config") or {}).get("branches", []))
    
    if not branches:
        return {}

    def run_branch(branch):
        branch_id = branch.get("branch_id", "unknown")
        nodes = branch.get("nodes", [])
        
        # Branch execution uses a copy of the state
        branch_state = state.copy()
        branch_updates = {}
        
        for node_def in nodes:
            node_type = node_def.get("type")
            handler = NODE_REGISTRY.get(node_type)
            if not handler:
                logger.error(f"Unknown node type in branch {branch_id}: {node_type}")
                continue
                
            # Execute node
            try:
                # Note: handlers usually return a dict of updates, not the full state
                updates = handler(branch_state, node_def)
                if isinstance(updates, dict):
                    branch_state.update(updates)
                    branch_updates.update(updates)
            except Exception as e:
                logger.error(f"Node execution failed in branch {branch_id}: {e}")
                raise e
                
        return branch_id, branch_updates

    # Execute branches in parallel
    combined_updates = {}
    branch_results = {}  # [Fix] Track branch results explicitly
    
    # Use ThreadPoolExecutor for concurrency
    # Note: Be careful with state conflicts if branches write to same keys
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_branch = {executor.submit(run_branch, b): b for b in branches}
        for future in concurrent.futures.as_completed(future_to_branch):
            branch = future_to_branch[future]
            try:
                branch_id, updates = future.result()
                # [Fix] Store branch results explicitly for verification
                branch_results[branch_id] = updates
                # [Fix] Namespace results to prevent race conditions (Last-Write-Wins)
                # Downstream nodes must access results via branch_id
                combined_updates[branch_id] = updates
                # [Optimized] Flattening removed to prevent conflicts: combined_updates.update(updates)
            except Exception as e:
                logger.error(f"Branch execution failed: {e}")
                raise e
    
    # [Fix] Add explicit branch execution markers for test verification
    for branch_id in branch_results.keys():
        combined_updates[f"{branch_id}_executed"] = True
    
    # [Token Aggregation] 병렬 브랜치들의 토큰 사용량 합산
    total_input_tokens = 0
    total_output_tokens = 0
    branch_token_details = []
    
    for branch_id, updates in branch_results.items():
        # 각 브랜치의 토큰 사용량 추출 (usage 또는 token_usage 키 지원)
        usage = extract_token_usage(updates)
        input_tokens = usage['input_tokens']
        output_tokens = usage['output_tokens']
        
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        
        branch_token_details.append({
            'branch_id': branch_id,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'total_tokens': input_tokens + output_tokens
        })
    
    # 합산된 토큰 정보를 state에 기록
    combined_updates['total_input_tokens'] = total_input_tokens
    combined_updates['total_output_tokens'] = total_output_tokens
    combined_updates['total_tokens'] = total_input_tokens + total_output_tokens
    combined_updates['branch_token_details'] = branch_token_details
    
    # [Accumulation] 이전 상태의 토큰 값과 누적
    temp_result = {
        'total_input_tokens': combined_updates['total_input_tokens'],
        'total_output_tokens': combined_updates['total_output_tokens'],
        'total_tokens': combined_updates['total_tokens']
    }
    accumulated_result = accumulate_tokens_in_state(temp_result, state)
    combined_updates.update(accumulated_result)
    
    # 비용 계산 추가
    estimated_cost = calculate_cost_usd({
        'input_tokens': combined_updates['total_input_tokens'],
        'output_tokens': combined_updates['total_output_tokens']
    })
    combined_updates['estimated_cost_usd'] = estimated_cost
    
    logger.info(f"Parallel group {node_id}: Aggregated {len(branch_results)} branches, "
                f"total tokens: {combined_updates['total_tokens']} "
                f"({total_input_tokens} input + {total_output_tokens} output), "
                f"cost: ${estimated_cost:.6f}")
    
    # [Debug] Log the keys being returned
    logger.info(f"Parallel group {node_id} returning keys: {list(combined_updates.keys())}")
    
    # [Payload Optimization] Keep flattened structure to avoid 256KB Step Functions limit
    # Aggregators must query individual branch IDs directly from state
    return combined_updates


# -----------------------------------------------------------------------------
# Vision Runner - Gemini Vision Multimodal Analysis
# -----------------------------------------------------------------------------

def vision_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gemini Vision을 활용한 이미지/비디오 분석 Runner.
    
    워크플로우 노드에서 멀티모달 분석을 수행합니다:
    - 이미지에서 텍스트/스펙 추출 (OCR)
    - 제품 이미지 분석
    - 스크린샷 해석
    - 다이어그램/차트 분석
    
    Config Options:
        image_inputs: List[str] - 이미지 소스 리스트 (S3 URI, HTTP URL, state 키)
        prompt_content: str - 분석 프롬프트 (템플릿 지원)
        system_prompt: str - 시스템 지침
        output_key: str - 결과 저장 키
        max_tokens: int - 최대 출력 토큰
        temperature: float - 샘플링 온도
        
    Example Config:
        {
            "type": "vision",
            "config": {
                "image_inputs": ["{{product_image_s3_uri}}", "{{spec_sheet_url}}"],
                "prompt_content": "이 제품 이미지들에서 스펙을 JSON으로 추출해주세요",
                "output_key": "extracted_specs"
            }
        }
    """
    # 1. Hydrate state
    exec_state = _hydrate_state_for_config(state, config)
    node_id = config.get("id", "vision")
    
    # [Fix] Config Extraction: Support nested 'config' dict (Workflow JSON standard) vs Flat dict (Test/Legacy)
    # Prioritize inner 'config' if present, otherwise fall back to root config
    vision_config = config.get("config", config) if isinstance(config.get("config"), dict) else config
    
    # 2. Resolve media sources (Images & Videos)
    media_inputs = []
    
    # process image_inputs
    raw_image_inputs = vision_config.get("image_inputs", [])
    if isinstance(raw_image_inputs, str): raw_image_inputs = [raw_image_inputs]
    
    for img_input in raw_image_inputs:
        resolved = _render_template(img_input, exec_state)
        # Check if state key reference
        if resolved and not resolved.startswith(("s3://", "gs://", "http://", "https://", "data:")):
            state_val = exec_state.get(resolved)
            if state_val: resolved = state_val
        
        if resolved:
            media_inputs.append({"type": "image", "source": resolved})
            
    # process video_inputs
    raw_video_inputs = vision_config.get("video_inputs", [])
    if isinstance(raw_video_inputs, str): raw_video_inputs = [raw_video_inputs]
    
    for vid_input in raw_video_inputs:
        resolved = _render_template(vid_input, exec_state)
        # Check if state key reference
        if resolved and not resolved.startswith(("s3://", "gs://", "http://", "https://")):
             state_val = exec_state.get(resolved)
             if state_val: resolved = state_val
             
        if resolved:
            media_inputs.append({"type": "video", "source": resolved})
    
    if not media_inputs:
        logger.warning(f"No media sources resolved for vision node {node_id}")
        out_key = vision_config.get("output_key", f"{node_id}_output")
        return {out_key: "[Error: No media provided]", "step_history": state.get("step_history", []) + [f"{node_id}:no_media"]}
    
    # 3. Render prompt
    prompt_template = vision_config.get("prompt_content") or vision_config.get("user_prompt_template", "이 컨텐츠를 분석해주세요.")
    prompt = _render_template(prompt_template, exec_state)
    
    system_prompt_tmpl = vision_config.get("system_prompt", "")
    system_prompt = _render_template(system_prompt_tmpl, exec_state) if system_prompt_tmpl else None
    
    # 4. Get model config
    max_tokens = vision_config.get("max_tokens", DEFAULT_MAX_TOKENS)
    temperature = vision_config.get("temperature", DEFAULT_TEMPERATURE)
    
    # 5. Invoke Gemini Vision (Multimodal)
    try:
        from src.services.llm.gemini_service import GeminiService, GeminiConfig, GeminiModel
        
        # Vision 지원 모델 사용
        model_name = vision_config.get("model", "gemini-1.5-flash")
        # Model selection logic... (simplified mapping)
        model_enum = GeminiModel.GEMINI_1_5_FLASH
        if "pro" in model_name: model_enum = GeminiModel.GEMINI_1_5_PRO
        elif "2.0" in model_name: model_enum = GeminiModel.GEMINI_2_0_FLASH
        
        gemini_config = GeminiConfig(
            model=model_enum,
            max_output_tokens=max_tokens,
            temperature=temperature,
            system_instruction=system_prompt
        )
        
        service = GeminiService(config=gemini_config)
        
        logger.info(f"Vision runner invoking Multimodal Gemini with {len(media_inputs)} inputs")
        
        result = service.invoke_multimodal(
            user_prompt=prompt,
            media_inputs=media_inputs,
            system_instruction=system_prompt
        )
        
        # Extract text from response
        text = ""
        if "content" in result and result["content"]:
            # Handle list or dict content
            content_data = result["content"]
            if isinstance(content_data, list) and content_data:
                text = content_data[0].get("text", "")
            elif isinstance(content_data, dict):
                text = content_data.get("text", "")
        
        metadata = result.get("metadata", {})
        
        # Count images and videos separately
        image_count = sum(1 for m in media_inputs if m.get("type") == "image")
        video_count = sum(1 for m in media_inputs if m.get("type") == "video")
        
        # Update history
        current_history = state.get("step_history", [])
        new_history = current_history + [f"{node_id}:vision_analysis"]
        
        out_key = vision_config.get("output_key", f"{node_id}_output")
        
        raw_output = {
            out_key: text,
            f"{node_id}_meta": {
                "model": model_name,
                "image_count": image_count,
                "video_count": video_count,
                "total_media": len(media_inputs),
                "token_usage": metadata.get("token_usage", {}),
                "latency_ms": metadata.get("latency_ms", 0)
            },
            "step_history": new_history
        }
        
        # 🛡️ [Guard] Layer 1: Validate output keys (Reserved key check)
        validated_output = _validate_output_keys(raw_output, node_id)
        # 🛡️ [Guard] Layer 2: Schema validation (Type safety)
        return validate_state_with_schema(validated_output, node_id)
        
    except Exception as e:
        logger.exception(f"Vision runner failed for node {node_id}: {e}")
        out_key = vision_config.get("output_key", f"{node_id}_output")
        return {
            out_key: f"[Vision Error: {str(e)}]",
            "step_history": state.get("step_history", []) + [f"{node_id}:error"]
        }


def video_chunker_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Splits video into chunks using VideoChunkerService.
    """
    from src.services.media.video_chunker import VideoChunkerService
    
    node_id = config.get("id", "chunker")
    exec_state = _hydrate_state_for_config(state, config)
    
    video_uri = _render_template(config.get("video_uri", ""), exec_state)
    segment_min = config.get("segment_length_min", 5)
    output_key = config.get("output_key", "video_chunks")
    
    if not video_uri:
        raise ValueError("video_chunker requires 'video_uri'")
        
    service = VideoChunkerService()
    chunks = service.chunk_video(video_uri, segment_length_min=segment_min)
    
    return {output_key: chunks, f"{node_id}_status": "done"}


# -----------------------------------------------------------------------------
# 5. Registry & Orchestration
# -----------------------------------------------------------------------------

NODE_REGISTRY: Dict[str, Callable] = {}

def register_node(name: str, func: Callable) -> None:
    NODE_REGISTRY[name] = func

# Register Nodes
register_node("operator", operator_runner)
register_node("operator_custom", operator_runner)  # 사용자 정의 코드/sets 전용 (MOCK_MODE에서만 exec 허용)

# -----------------------------------------------------------------------------
# Safe Operator Official Runner - Production-ready built-in transformations
# -----------------------------------------------------------------------------
def operator_official_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute safe, built-in transformation strategies without exec().
    
    Production-ready operator that provides 50+ transformation strategies:
    - JSON/Object: json_parse, deep_get, pick_fields, merge_objects, etc.
    - List: list_filter, list_map, list_reduce, list_sort, etc.
    - String: string_template, regex_extract, string_case, etc.
    - Type: to_int, to_date, coerce_type, etc.
    - Control: if_else, switch_case, default_value, etc.
    - Encoding: base64_encode, url_encode, hash_sha256, uuid_generate, etc.
    - Math: math_round, math_clamp, math_expression, etc.
    
    Config schema:
    {
        "type": "operator_official",
        "id": "my_transform",
        "config": {
            "strategy": "list_filter",       # Required: strategy name
            "input": "{{items}}",            # Optional: input template (defaults to state)
            "input_key": "items",            # Alternative: direct state key
            "params": {                       # Strategy-specific parameters
                "condition": "$.active == true"
            },
            "output_key": "filtered_items"   # Optional: output key (defaults to {node_id}_result)
        }
    }
    """
    from src.services.operators.operator_strategies import execute_strategy, get_available_strategies
    
    node_id = config.get("id", "operator_official")
    
    # Extract config (support both flat and nested config)
    # [Fix] None defense: config['config']가 None일 수 있음
    inner_config = config.get("config") or {}
    strategy = inner_config.get("strategy") or config.get("strategy")
    
    if not strategy:
        raise ValueError(
            f"operator_official node '{node_id}' requires 'strategy'. "
            f"Available: {get_available_strategies()}"
        )
    
    # Resolve input
    input_template = inner_config.get("input") or config.get("input")
    input_key = inner_config.get("input_key") or config.get("input_key")
    
    if input_template:
        # Render template against state
        input_value = _render_template(input_template, state)
    elif input_key:
        # Direct state key access
        input_value = _get_nested_value(state, input_key)
    else:
        # Use entire state as input
        input_value = state
    
    # Get strategy parameters
    params = inner_config.get("params", {}) or config.get("params", {})
    
    # Render any templates in params
    if isinstance(params, dict):
        params = _render_template(params, state)
    
    # Execute strategy
    try:
        result = execute_strategy(strategy, input_value, params, state)
    except AssertionError as e:
        # Assert strategy failed - propagate as error
        raise ValueError(f"Assertion failed in node '{node_id}': {e}")
    except Exception as e:
        # Check for fallback handling
        error_handling = inner_config.get("error_handling") or config.get("error_handling", "fail")
        fallback = inner_config.get("fallback") or config.get("fallback")
        
        if error_handling == "fallback" and fallback is not None:
            logger.warning(f"[operator_official] {node_id} failed, using fallback: {e}")
            result = _render_template(fallback, state) if isinstance(fallback, str) else fallback
        elif error_handling == "skip":
            logger.warning(f"[operator_official] {node_id} failed, skipping: {e}")
            return {f"{node_id}_skipped": True, f"{node_id}_error": str(e)}
        else:
            raise
    
    # Build output
    output_key = inner_config.get("output_key") or config.get("output_key") or f"{node_id}_result"
    
    output = {output_key: result}
    
    # Update step history
    current_history = state.get("step_history", [])
    output["step_history"] = current_history + [f"{node_id}:operator_official:{strategy}"]
    
    # 🛡️ [Guard] Layer 1: Validate output keys (Reserved key check)
    validated_output = _validate_output_keys(output, node_id)
    # 🛡️ [Guard] Layer 2: Schema validation (Type safety)
    return validate_state_with_schema(validated_output, node_id)

register_node("operator_official", operator_official_runner)
register_node("safe_operator", operator_official_runner)  # Alias for operator_official
register_node("llm_chat", llm_chat_runner)
register_node("video_chunker", video_chunker_runner)
register_node("aiModel", llm_chat_runner)  # aiModel은 llm_chat과 동일하게 처리
register_node("api_call", api_call_runner)
register_node("db_query", db_query_runner)
register_node("for_each", for_each_runner)
register_node("route_draft_quality", route_draft_quality)
register_node("parallel_group", parallel_group_runner)
register_node("aggregator", aggregator_runner)
register_node("aiModel", llm_chat_runner)  # aiModel은 llm_chat과 동일하게 처리
register_node("skill_executor", skill_executor_runner)  # Skills integration
register_node("nested_for_each", nested_for_each_runner)  # V3 Hyper-Stress: Nested Map-in-Map support
register_node("vision", vision_runner)  # Gemini Vision multimodal analysis
register_node("image_analysis", vision_runner)  # Alias for vision
# Note: 'code' 타입은 NODE_REGISTRY에 추가하지 않음
# Pydantic field_validator에서 'code' -> 'operator'로 변환되므로 여기 도달 불가
# 만약 'code' 타입이 여기 도달하면 검증 단계를 우회한 것이므로 에러가 맞음

# SubGraph/Group 노드 러너 - DynamicWorkflowBuilder에서 재귀적으로 처리
def subgraph_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    SubGraph/Group 노드 실행 핸들러.
    
    실제 서브그래프 컴파일 및 실행은 DynamicWorkflowBuilder에서 처리됩니다.
    이 핸들러는 세그먼트 러너에서 직접 호출될 때를 위한 폴백입니다.
    
    Config 옵션:
    - subgraph_ref: 참조할 서브그래프 ID
    - subgraph_inline: 인라인 서브그래프 정의
    - skill_ref: 참조할 Skill ID
    - input_mapping: 부모→자식 상태 매핑
    - output_mapping: 자식→부모 상태 매핑
    """
    node_id = config.get("id", "subgraph")
    logger.info(f"📦 SubGraph 노드 실행: {node_id}")
    
    try:
        # DynamicWorkflowBuilder import
        from src.services.workflow.builder import DynamicWorkflowBuilder
        
        # 서브그래프 정의 해석
        subgraph_def = None
        
        if config.get("subgraph_inline"):
            subgraph_def = config["subgraph_inline"]
        elif config.get("subgraph_ref"):
            # subgraph_ref는 워크플로우 컨텍스트에서 해석되어야 함
            # 여기서는 state에서 subgraphs를 찾음
            subgraphs = state.get("_workflow_subgraphs", {})
            ref = config["subgraph_ref"]
            if ref in subgraphs:
                subgraph_def = subgraphs[ref]
            else:
                logger.warning(f"SubGraph 참조 '{ref}'를 찾을 수 없습니다.")
                return {"subgraph_error": f"SubGraph ref not found: {ref}"}
        elif config.get("skill_ref"):
            # Skill 기반 서브그래프
            try:
                from src.services.skill_repository import get_skill_repository
                repo = get_skill_repository()
                skill = repo.get_latest_skill(config["skill_ref"])
                if skill and skill.get("skill_type") == "subgraph_based":
                    subgraph_def = skill.get("subgraph_config")
            except ImportError:
                logger.warning("SkillRepository를 사용할 수 없습니다.")
        
        if not subgraph_def:
            logger.warning(f"SubGraph 정의를 찾을 수 없습니다: {node_id}")
            return {"subgraph_status": "skipped", "reason": "no_definition"}
        
        # 입력 매핑 적용
        input_mapping = config.get("input_mapping", {})
        child_state = {}
        for parent_key, child_key in input_mapping.items():
            if parent_key in state:
                child_state[child_key] = state[parent_key]
        
        # 기본 필드 상속
        for key in ["execution_id", "workflow_id", "owner_id"]:
            if key in state and key not in child_state:
                child_state[key] = state[key]
        
        # 서브그래프 빌드 및 실행
        builder = DynamicWorkflowBuilder(subgraph_def, use_lightweight_state=True)
        compiled = builder.build()
        child_output = compiled.invoke(child_state)
        
        # 출력 매핑 적용
        output_mapping = config.get("output_mapping", {})
        result = {}
        for child_key, parent_key in output_mapping.items():
            if child_key in child_output:
                result[parent_key] = child_output[child_key]
        
        # step_history 병합
        if "step_history" in child_output:
            current_history = state.get("step_history", [])
            result["step_history"] = current_history + child_output["step_history"]
        
        logger.info(f"✅ SubGraph 노드 완료: {node_id}")
        return result
        
    except Exception as e:
        logger.exception(f"❌ SubGraph 노드 실행 실패: {node_id}")
        error_handling = config.get("error_handling", "fail")
        if error_handling == "ignore":
            return {"subgraph_status": "error_ignored", "error": str(e)}
        elif error_handling == "fallback":
            return {"subgraph_status": "fallback", "error": str(e)}
        else:
            raise

register_node("group", subgraph_runner)  # SubGraph 노드 (group 타입)
register_node("subgraph", subgraph_runner)  # SubGraph 노드 (subgraph 타입)


def _get_mock_config(mock_behavior: str) -> Dict[str, Any]:
    """Returns test configurations for mock behaviors."""
    if mock_behavior == "E2E_S3_LARGE_DATA":
        return {
            "nodes": [{
                "id": "large_data_generator", "type": "operator",
                "config": { "code": "state['res'] = 'X'*300000", "output_key": "res" }
            }],
            "edges": [], "start_node": "large_data_generator"
        }
    elif mock_behavior == "CONTINUE":
        return {
            "nodes": [
                {"id": "step1", "type": "operator", "config": {"sets": {"step": 1}}},
                {"id": "step2", "type": "operator", "config": {"sets": {"step": 2}}}
            ],
            "edges": [{"source": "step1", "target": "step2"}], "start_node": "step1"
        }
    # Add other mock configs as needed (FAIL, PAUSE, etc.)
    return {"nodes": [], "edges": []} # Default empty


def run_workflow(config_json: str | Dict[str, Any], initial_state: Dict[str, Any] | None = None, 
                 user_api_keys: Dict[str, str] | None = None, 
                 use_cache: bool = True, 
                 conversation_id: str | None = None, 
                 ddb_table_name: str | None = None,
                 run_config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Main entry point using Dynamic Builder architecture.
    Supports both Real and Mock execution paths.
    """
    initial_state = dict(initial_state) if initial_state else {}
    
    # 1. Check for Mock/Test Request
    mock_behavior = initial_state.get("mock_behavior")
    if mock_behavior:
        logger.info(f"🧪 Mock behavior detected: {mock_behavior}")
        mock_config = _get_mock_config(mock_behavior)
        config_json = json.dumps(mock_config)
        
        # Mock Response Simulation for HITP
        if mock_behavior == "PAUSED_FOR_HITP":
            return {"status": "PAUSED_FOR_HITP", "next_segment_to_run": 1}

    # 2. Config Validation (JSON parse + Pydantic schema)
    if isinstance(config_json, dict):
        raw_config = config_json
    else:
        try:
            raw_config = json.loads(config_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON config: {str(e)}. Please check the config_json format.")
        except Exception as e:
            raise ValueError(f"Failed to parse workflow config: {str(e)}")

    try:
        validated_config = WorkflowConfigModel.model_validate(raw_config)
        # Use validated dict for downstream (keeps types constrained)
        workflow_config = validated_config.model_dump()
    except ValidationError as ve:
        raise ValueError(f"Invalid workflow config: {ve}") from ve

    # 3. Dynamic Build (No S3, No Pickle)
    # Lazy import to avoid circular ref with NODE_REGISTRY
    from src.services.workflow.builder import DynamicWorkflowBuilder
    
    logger.info("🏗️ Building workflow dynamically...")
    builder = DynamicWorkflowBuilder(workflow_config)
    app = builder.build()

    # 4. Apply Checkpointer if needed
    if ddb_table_name:
        try:
            from langgraph_checkpoint_dynamodb import DynamoDBSaver
            saver = DynamoDBSaver(table_name=ddb_table_name)
            app = app.with_checkpointer(saver)
        except ImportError:
            logger.warning("DynamoDBSaver not found, skipping persistence")

    # 5. Execution
    # [수정] run_config가 없으면 빈 딕셔너리로 초기화
    final_config = run_config.copy() if run_config else {}
    
    # configurable이 없으면 생성
    if "configurable" not in final_config:
        final_config["configurable"] = {}
    
    # thread_id 및 conversation_id 보정
    configurable = final_config["configurable"]
    if not configurable.get("thread_id"):
        configurable["thread_id"] = conversation_id or "default_thread"
    if conversation_id and not configurable.get("conversation_id"):
        configurable["conversation_id"] = conversation_id

    # Setup API Keys
    if user_api_keys:
        initial_state.setdefault("user_api_keys", {}).update(user_api_keys)
    initial_state.setdefault("step_history", [])

    # 6. Setup Callbacks (Glass Box)
    from src.langchain_core_custom.callbacks import BaseCallbackHandler
    import uuid
    
    class StateHistoryCallback(BaseCallbackHandler):
        """
        Capture AI thoughts and tool usage with PII masking for Glass-Box UI.
        """
        def __init__(self):
            self.logs = []
            
        def on_chain_start(self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any) -> None:
            pass
            
        def on_llm_start(self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any) -> None:
            prompt_safe = mask_pii(prompts[0]) if prompts else ""
            self.logs.append({
                "id": str(uuid.uuid4()),
                "type": "ai_thought",
                "name": serialized.get("name", "LLM Thinking"),
                "node_id": serialized.get("name", "llm_node"),
                "content": "Thinking...",
                "timestamp": int(time.time()),
                "status": "RUNNING",
                "details": {
                    "prompts": [prompt_safe] if prompt_safe else []
                }
            })
            
        def on_llm_end(self, response: Any, **kwargs: Any) -> None:
            if self.logs and self.logs[-1]["type"] == "ai_thought":
                self.logs[-1]["status"] = "COMPLETED"
                
                if getattr(response, "llm_output", None) and "usage" in response.llm_output:
                    self.logs[-1]["usage"] = response.llm_output["usage"]
                
                try:
                    text = response.generations[0][0].text
                    self.logs[-1]["content"] = mask_pii(text)
                except Exception:
                    pass
                    
        def on_llm_error(self, error: BaseException, **kwargs: Any) -> None:
            if self.logs and self.logs[-1]["type"] == "ai_thought":
                self.logs[-1]["status"] = "FAILED"
                self.logs[-1]["error"] = {
                    "message": str(error),
                    "type": type(error).__name__
                }

        def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs: Any) -> None:
            self.logs.append({
                "id": str(uuid.uuid4()),
                "type": "tool_usage",
                "name": serialized.get("name", "Tool"),
                "node_id": serialized.get("name", "tool_node"),
                "status": "RUNNING",
                "timestamp": int(time.time()),
                "input": mask_pii(input_str)
            })

        def on_tool_end(self, output: str, **kwargs: Any) -> None:
            if self.logs and self.logs[-1]["type"] == "tool_usage":
                self.logs[-1]["status"] = "COMPLETED"
                self.logs[-1]["output"] = mask_pii(str(output))

        def on_tool_error(self, error: BaseException, **kwargs: Any) -> None:
            if self.logs and self.logs[-1]["type"] == "tool_usage":
                self.logs[-1]["status"] = "FAILED"
                self.logs[-1]["error"] = str(error)

    history_callback = StateHistoryCallback()
    final_config.setdefault("callbacks", []).append(history_callback)

    # Run!
    logger.info("🚀 Invoking workflow...")
    try:
        # [수정] config 전체를 넘겨야 metadata 등이 함께 전달됨
        result = app.invoke(initial_state, config=final_config) 
        
        # [NEW] Attach collected logs to the result (if result is a dict)
        if isinstance(result, dict):
            # Legacy field (kept for backward compatibility)
            result["__new_history_logs"] = history_callback.logs
            # Glass-box logs for UI
            prev_logs = result.get("execution_logs", [])
            result["execution_logs"] = prev_logs + history_callback.logs
        return result
    except AsyncLLMRequiredException:
        # Signal orchestrator to pause for async LLM / HITP handling
        return {"status": "PAUSED_FOR_ASYNC_LLM"}
    except Exception as e:
        logger.exception("Workflow execution failed")
        raise e


# -----------------------------------------------------------------------------
# Partition Workflow Functions (for Lambda compatibility)
# -----------------------------------------------------------------------------

def partition_workflow(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    워크플로우를 세그먼트로 분할하는 함수.
    partition_workflow_advanced의 alias로, Lambda 호환성을 위해 유지.
    """
    from src.services.workflow.partition_service import partition_workflow_advanced
    
    # partition_workflow_advanced는 {"partition_map": [...], ...} 형태로 반환하므로
    # partition_map 리스트만 추출
    result = partition_workflow_advanced(config)
    return result.get("partition_map", [])


def _build_segment_config(segment: Dict[str, Any]) -> Dict[str, Any]:
    """
    세그먼트 객체를 실행 가능한 워크플로우 config로 변환.
    
    세그먼트는 {"id": str, "nodes": [...], "edges": [...], "type": str, "node_ids": [...]} 형태.
    이를 run_workflow에 전달할 수 있는 {"nodes": [...], "edges": [...]} 형태로 변환.
    """
    return {
        "nodes": segment.get("nodes", []),
        "edges": segment.get("edges", [])
    }


def run_workflow_from_dynamodb(table_name: str, key_name: str, key_value: str, initial_state: Optional[Dict[str, Any]] = None, user_api_keys: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    DynamoDB에서 워크플로우 config를 가져와서 실행.
    
    Args:
        table_name: DynamoDB 테이블 이름
        key_name: 파티션 키 이름
        key_value: 파티션 키 값
        initial_state: 초기 상태 (옵션)
        user_api_keys: 사용자 API 키 (옵션)
    
    Returns:
        워크플로우 실행 결과
    """
    # DynamoDB에서 config 가져오기 - 리전을 환경변수에서 가져옴
    region = os.environ.get('AWS_REGION', 'ap-northeast-2')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    response = table.get_item(Key={key_name: key_value})
    
    if 'Item' not in response:
        raise ValueError(f"Workflow config not found in DynamoDB table {table_name} with key {key_name}={key_value}")
    
    item = response['Item']
    
    # [Hybrid Storage] S3 Hydration Check
    config_s3_ref = item.get('config_s3_ref')
    if config_s3_ref:
        logger.info(f"🔄 Hybrid Storage: Hydrating config from {config_s3_ref}")
        try:
            # Parse s3://bucket/key
            if config_s3_ref.startswith("s3://"):
                parts = config_s3_ref[5:].split("/", 1)
                bucket = parts[0]
                key = parts[1]
                
                s3_client = boto3.client('s3', region_name=region)
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                config_json = obj['Body'].read().decode('utf-8')
            else:
                logger.warning(f"Invalid S3 ref format: {config_s3_ref}, falling back to item config")
                config_json = item.get('config_json') or item.get('config')
        except Exception as e:
            logger.error(f"❌ Failed to hydrate config from S3: {e}")
            raise ValueError(f"Failed to load offloaded config: {e}")
    else:
        # Standard load
        config_json = item.get('config_json') or item.get('config')
    
    if not config_json:
        raise ValueError(f"No config_json found in DynamoDB item")
    
    # JSON 파싱 (필요한 경우)
    if isinstance(config_json, str):
        config_json = json.loads(config_json)
    
    # 워크플로우 실행
    return run_workflow(config_json, initial_state, user_api_keys)
