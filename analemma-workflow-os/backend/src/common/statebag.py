"""State-bag normalization helpers.

Analemma OS 커널 수준의 상태 관리 유틸리티입니다.
Lambda/Step Functions 이벤트를 정규화하여 핸들러가 레거시 최상위 필드와
새로운 `state_data` 구조를 모두 처리할 수 있게 합니다.

Features:
    - Shallow/Deep merge 지원
    - Read-only 커널 상태 보호
    - JIT 스키마 검증
    - Step Functions 페이로드 최적화

Usage:
    from src.common.statebag import normalize_event, normalize_inplace, deep_normalize
    
    # 기본 사용 (shallow merge)
    event = normalize_event(raw_event)
    
    # Deep merge (중첩 딕셔너리 병합)
    event = deep_normalize(raw_event)
    
    # 스키마 검증과 함께
    event = normalize_event(raw_event, validate_schema=True, required_fields=['workflow_id'])
"""
from typing import Any, Dict, FrozenSet, List, Optional, Set, Union
import logging
import copy

logger = logging.getLogger(__name__)

# ============================================================================
# 커널 보호 필드 (Read-only State Protection)
# ============================================================================
# 이 필드들은 사용자/워크플로우에서 덮어쓸 수 없는 커널 수준 상태입니다.
# state_data에서 이 필드들이 존재해도 최상위 값이 우선합니다.

KERNEL_PROTECTED_FIELDS: FrozenSet[str] = frozenset({
    # 실행 식별자
    "execution_id",
    "execution_arn",
    "workflow_id",
    "owner_id",
    "user_id",
    
    # 커널 메타데이터
    "_kernel_version",
    "_created_at",
    "_started_at",
    "_task_token",
    
    # 보안 관련
    "auth_context",
    "permissions",
    "tenant_id",
})

# 절대 병합하면 안 되는 내부 필드 (완전 무시)
INTERNAL_FIELDS: FrozenSet[str] = frozenset({
    "__trace_id",
    "__span_id", 
    "__debug_mode",
})


# ============================================================================
# Deep Merge 유틸리티
# ============================================================================

def _is_mergeable_dict(obj: Any) -> bool:
    """병합 가능한 딕셔너리인지 확인 (리스트/세트 등 제외)"""
    return isinstance(obj, dict) and not hasattr(obj, '_asdict')  # namedtuple 제외


def deep_merge(base: Dict[str, Any], overlay: Dict[str, Any], 
               protect_kernel_fields: bool = True) -> Dict[str, Any]:
    """
    두 딕셔너리를 재귀적으로 깊은 병합(Deep Merge)합니다.
    
    Args:
        base: 기본 딕셔너리 (우선순위 높음)
        overlay: 오버레이 딕셔너리 (base에 없는 값만 병합)
        protect_kernel_fields: True면 커널 보호 필드는 base 값 유지
        
    Returns:
        병합된 새 딕셔너리
        
    Example:
        base = {"config": {"timeout": 30}, "name": "test"}
        overlay = {"config": {"retries": 3, "timeout": 60}, "debug": True}
        result = deep_merge(base, overlay)
        # {"config": {"timeout": 30, "retries": 3}, "name": "test", "debug": True}
    """
    result = copy.copy(base)
    
    for key, overlay_value in overlay.items():
        # 내부 필드는 완전 무시
        if key in INTERNAL_FIELDS:
            continue
            
        # 커널 보호 필드는 base 값 유지
        if protect_kernel_fields and key in KERNEL_PROTECTED_FIELDS:
            if key in result:
                continue  # base 값 유지
        
        if key not in result:
            # base에 없으면 overlay 값 사용
            result[key] = copy.deepcopy(overlay_value) if _is_mergeable_dict(overlay_value) else overlay_value
        elif _is_mergeable_dict(result[key]) and _is_mergeable_dict(overlay_value):
            # 둘 다 딕셔너리면 재귀적으로 병합
            result[key] = deep_merge(result[key], overlay_value, protect_kernel_fields)
        # else: base 값 유지 (기존 동작)
    
    return result


# ============================================================================
# 스키마 검증 (JIT Validation)
# ============================================================================

class StateBagValidationError(ValueError):
    """상태 검증 실패 시 발생하는 예외"""
    
    def __init__(self, message: str, missing_fields: Optional[List[str]] = None,
                 invalid_fields: Optional[Dict[str, str]] = None):
        super().__init__(message)
        self.missing_fields = missing_fields or []
        self.invalid_fields = invalid_fields or {}


def validate_state(
    event: Dict[str, Any],
    required_fields: Optional[List[str]] = None,
    field_types: Optional[Dict[str, type]] = None,
    raise_on_error: bool = False
) -> tuple[bool, List[str]]:
    """
    상태 데이터의 JIT(Just-In-Time) 스키마 검증을 수행합니다.
    
    Args:
        event: 검증할 이벤트 딕셔너리
        required_fields: 필수 필드 목록
        field_types: 필드별 예상 타입 {field_name: expected_type}
        raise_on_error: True면 검증 실패 시 예외 발생
        
    Returns:
        (is_valid, error_messages) 튜플
        
    Example:
        is_valid, errors = validate_state(
            event,
            required_fields=['workflow_id', 'owner_id'],
            field_types={'max_retries': int, 'timeout': (int, float)}
        )
    """
    errors: List[str] = []
    
    # 필수 필드 검증
    if required_fields:
        for field in required_fields:
            if field not in event or event[field] is None:
                errors.append(f"Missing required field: '{field}'")
    
    # 타입 검증
    if field_types:
        for field, expected_type in field_types.items():
            if field in event and event[field] is not None:
                if not isinstance(event[field], expected_type):
                    actual_type = type(event[field]).__name__
                    if isinstance(expected_type, tuple):
                        expected_names = '/'.join(t.__name__ for t in expected_type)
                    else:
                        expected_names = expected_type.__name__
                    errors.append(
                        f"Field '{field}' has wrong type: expected {expected_names}, got {actual_type}"
                    )
    
    is_valid = len(errors) == 0
    
    if not is_valid and raise_on_error:
        raise StateBagValidationError(
            f"State validation failed with {len(errors)} error(s)",
            missing_fields=[f for f in (required_fields or []) if f not in event],
            invalid_fields={e.split("'")[1]: e for e in errors if "wrong type" in e}
        )
    
    return is_valid, errors


# ============================================================================
# 메인 정규화 함수들
# ============================================================================

def normalize_event(
    event: Union[Dict[str, Any], Any], 
    remove_state_data: bool = False,
    deep: bool = False,
    protect_kernel_fields: bool = True,
    validate_schema: bool = False,
    required_fields: Optional[List[str]] = None,
    field_types: Optional[Dict[str, type]] = None
) -> Union[Dict[str, Any], Any]:
    """
    이벤트를 정규화하여 state_data의 키들을 최상위로 병합합니다.
    
    Args:
        event: 정규화할 이벤트 딕셔너리
        remove_state_data: True면 병합 후 state_data 키 제거 (페이로드 최적화)
        deep: True면 중첩 딕셔너리도 재귀적으로 병합 (Deep Merge)
        protect_kernel_fields: True면 커널 보호 필드는 최상위 값 유지
        validate_schema: True면 JIT 스키마 검증 수행
        required_fields: 필수 필드 목록 (validate_schema=True일 때)
        field_types: 필드별 예상 타입 (validate_schema=True일 때)
        
    Returns:
        정규화된 새 딕셔너리 (원본 불변)
        
    Example:
        # 기본 사용
        event = normalize_event(raw_event)
        
        # Deep merge + 검증
        event = normalize_event(
            raw_event, 
            deep=True,
            validate_schema=True,
            required_fields=['workflow_id']
        )
    """
    if not isinstance(event, dict):
        return event

    sd = event.get("state_data")
    if not isinstance(sd, dict):
        # state_data가 없어도 검증은 수행
        if validate_schema:
            validate_state(event, required_fields, field_types, raise_on_error=True)
        return event

    # 병합 수행
    if deep:
        # 먼저 event에서 state_data 제외한 복사본 생성
        base = {k: v for k, v in event.items() if k != "state_data"}
        out = deep_merge(base, sd, protect_kernel_fields)
    else:
        # Shallow merge (기존 동작)
        out = dict(event)
        for k, v in sd.items():
            # 내부 필드 무시
            if k in INTERNAL_FIELDS:
                continue
            # 커널 보호 필드는 기존 값 유지
            if protect_kernel_fields and k in KERNEL_PROTECTED_FIELDS and k in out:
                continue
            if k not in out:
                out[k] = v
    
    # Step Functions Payload 최적화
    if remove_state_data:
        out.pop("state_data", None)
    
    # JIT 스키마 검증
    if validate_schema:
        validate_state(out, required_fields, field_types, raise_on_error=True)
    
    return out


def normalize_inplace(
    event: Union[Dict[str, Any], Any], 
    remove_state_data: bool = False,
    protect_kernel_fields: bool = True
) -> Union[Dict[str, Any], Any]:
    """
    이벤트를 제자리에서(in-place) 정규화합니다.
    동일한 딕셔너리 객체를 유지해야 할 때 사용합니다.

    Args:
        event: 정규화할 이벤트 (직접 수정됨)
        remove_state_data: True면 병합 후 state_data 키 제거
        protect_kernel_fields: True면 커널 보호 필드는 기존 값 유지
        
    Note:
        Deep merge는 지원하지 않습니다. 깊은 병합이 필요하면 
        normalize_event(deep=True)를 사용하세요.
    """
    if not isinstance(event, dict):
        return event
        
    sd = event.get("state_data")
    if not isinstance(sd, dict):
        return event
    
    # Pythonic setdefault 활용: C 레벨 최적화
    for k, v in sd.items():
        # 내부 필드 무시
        if k in INTERNAL_FIELDS:
            continue
        # 커널 보호 필드 체크
        if protect_kernel_fields and k in KERNEL_PROTECTED_FIELDS and k in event:
            continue
        event.setdefault(k, v)
    
    # Step Functions Payload 최적화
    if remove_state_data:
        event.pop("state_data", None)
    
    return event


def deep_normalize(
    event: Union[Dict[str, Any], Any],
    remove_state_data: bool = False,
    validate_schema: bool = False,
    required_fields: Optional[List[str]] = None
) -> Union[Dict[str, Any], Any]:
    """
    Deep merge를 사용하는 정규화의 편의 함수입니다.
    
    normalize_event(event, deep=True, ...)와 동일합니다.
    """
    return normalize_event(
        event, 
        remove_state_data=remove_state_data,
        deep=True,
        protect_kernel_fields=True,
        validate_schema=validate_schema,
        required_fields=required_fields
    )


# ============================================================================
# 유틸리티 함수들
# ============================================================================

def extract_kernel_state(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    이벤트에서 커널 보호 필드만 추출합니다.
    
    디버깅이나 감사(audit) 로깅에 유용합니다.
    """
    return {k: v for k, v in event.items() if k in KERNEL_PROTECTED_FIELDS}


def is_kernel_field(field_name: str) -> bool:
    """필드가 커널 보호 필드인지 확인합니다."""
    return field_name in KERNEL_PROTECTED_FIELDS


def add_protected_field(field_name: str) -> None:
    """
    런타임에 커널 보호 필드를 추가합니다.
    
    Warning: 이 함수는 전역 상태를 변경합니다. 신중히 사용하세요.
    """
    global KERNEL_PROTECTED_FIELDS
    KERNEL_PROTECTED_FIELDS = KERNEL_PROTECTED_FIELDS | frozenset({field_name})
    logger.info(f"Added kernel protected field: {field_name}")
