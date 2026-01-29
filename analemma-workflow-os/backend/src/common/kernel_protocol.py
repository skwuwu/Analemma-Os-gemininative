"""
🎒 Kernel Protocol - Lambda ↔ ASL 통신 표준 규약

v3.13 - "The Great Seal" Pattern

모든 Lambda는 이 프로토콜을 통해 ASL과 통신합니다:
    - 입구: open_state_bag(event) → 실제 데이터(bag) 추출
    - 출구: seal_state_bag(base, delta, action) → 표준 응답 포맷

ASL 계약:
    Lambda 반환: { "state_data": {...}, "next_action": "CONTINUE" }
    ASL ResultSelector: { "bag.$": "$.Payload.state_data", "next_action.$": "$.Payload.next_action" }
    ASL ResultPath: "$.state_data"
    
    → SFN 상태: { "state_data": { "bag": {...}, "next_action": "..." } }
    → 다음 Lambda Payload: "$.state_data.bag"

이 설계가 해결하는 문제:
    1. 이중 래핑 방지: Lambda가 state_data만 반환, SFN Payload wrapper와 겹치지 않음
    2. 256KB 선제 방어: seal_state_bag 내부에서 USC가 S3 오프로딩 수행
    3. 참조 일관성: open_state_bag 한 줄로 데이터 추출, ASL 경로 변경에도 코드 수정 최소화
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Universal Sync Core import (lazy)
_universal_sync_core = None
_USC_AVAILABLE = None


def _get_universal_sync_core():
    """Lazy import to avoid circular dependencies"""
    global _universal_sync_core, _USC_AVAILABLE
    
    if _USC_AVAILABLE is None:
        try:
            from src.handlers.utils.universal_sync_core import universal_sync_core
            _universal_sync_core = universal_sync_core
            _USC_AVAILABLE = True
        except ImportError:
            try:
                from handlers.utils.universal_sync_core import universal_sync_core
                _universal_sync_core = universal_sync_core
                _USC_AVAILABLE = True
            except ImportError:
                _USC_AVAILABLE = False
                logger.warning("⚠️ universal_sync_core not available")
    
    return _universal_sync_core if _USC_AVAILABLE else None


def seal_state_bag(
    base_state: Dict[str, Any],
    result_delta: Any,
    action: str = 'sync',
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    📤 Lambda → ASL 반환용 표준 포맷터 (The Great Seal)
    
    모든 Lambda는 리턴 시 이 함수를 호출하여 '완성된 가방'을 반환합니다.
    
    Args:
        base_state: 기존 상태 (event에서 open_state_bag으로 추출한 것)
        result_delta: 실행 결과 (새로운 데이터)
        action: USC 액션 ('init', 'sync', 'aggregate_branches', etc.)
        context: 추가 컨텍스트 (idempotency_key, segment_id 등)
    
    Returns:
        {
            "state_data": { ... merged & optimized ... },
            "next_action": "CONTINUE" | "COMPLETE" | "FAILED" | ...
        }
        
    Note:
        - SFN이 자동으로 Payload wrapper를 씌우므로 최종적으로:
          $.Payload = { "state_data": ..., "next_action": ... }
        - ASL ResultSelector가 $.Payload.state_data를 $.state_data.bag으로 저장
    """
    usc = _get_universal_sync_core()
    
    if usc is None:
        # USC 미사용 폴백
        logger.warning("⚠️ [seal_state_bag] USC not available, using simple merge")
        merged = dict(base_state or {})
        if isinstance(result_delta, dict):
            merged.update(result_delta)
        return {
            "state_data": merged,
            "next_action": result_delta.get('status', 'CONTINUE') if isinstance(result_delta, dict) else 'CONTINUE'
        }
    
    # USC 컨텍스트 구성
    usc_context = {'action': action}
    if context:
        usc_context.update(context)
    
    # 1. USC를 통해 상태 병합 및 S3 오프로딩(30KB+) 즉시 실행
    usc_result = usc(
        base_state=base_state or {},
        new_result=result_delta,
        context=usc_context
    )
    
    # 2. 🚨 이중 래핑 방지를 위한 표준 구조 강제
    # SFN의 Payload 밑에 바로 state_data가 오도록 구성
    sealed = {
        "state_data": usc_result.get('state_data', {}),
        "next_action": usc_result.get('next_action', 'CONTINUE')
    }
    
    logger.debug(f"[seal_state_bag] Sealed: action={action}, next={sealed['next_action']}")
    
    return sealed


def open_state_bag(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    📥 ASL → Lambda 입구용 표준 추출기
    
    어떤 경로로 들어오든 실제 데이터(Bag)만 쏙 뽑아줍니다.
    
    탐색 순서:
        1. event.state_data.bag (표준 v3.13 경로)
        2. event.state_data (평탄화된 경우)
        3. event 자체 (루트가 데이터인 경우)
    
    Args:
        event: Lambda 이벤트 (Step Functions에서 전달)
    
    Returns:
        실제 상태 데이터 (bag 내용물)
        
    Example:
        # Lambda 핸들러 시작부
        def lambda_handler(event, context):
            bag = open_state_bag(event)
            workflow_config = bag.get('workflow_config')
            ...
    """
    if not isinstance(event, dict):
        logger.warning(f"[open_state_bag] event is not dict: {type(event)}")
        return {}
    
    # 1. 표준 경로: state_data.bag
    state_data = event.get('state_data')
    if isinstance(state_data, dict):
        bag = state_data.get('bag')
        if isinstance(bag, dict):
            logger.debug(f"[open_state_bag] Found bag at state_data.bag, keys={list(bag.keys())[:5]}")
            return bag
        
        # 2. 평탄화된 경우: state_data 자체가 bag
        if state_data:
            logger.debug(f"[open_state_bag] Using state_data as bag, keys={list(state_data.keys())[:5]}")
            return state_data
    
    # 3. 루트가 데이터인 경우 (legacy 또는 direct invocation)
    logger.debug(f"[open_state_bag] Using event root as bag, keys={list(event.keys())[:5]}")
    return event


def get_from_bag(
    event: Dict[str, Any],
    key: str,
    default: Any = None
) -> Any:
    """
    📥 Bag에서 특정 키 안전 추출 (open_state_bag + get 편의 함수)
    
    Args:
        event: Lambda 이벤트
        key: 추출할 키
        default: 기본값
    
    Returns:
        bag[key] 또는 default
        
    Example:
        workflow_config = get_from_bag(event, 'workflow_config')
        partition_map = get_from_bag(event, 'partition_map', [])
    """
    bag = open_state_bag(event)
    return bag.get(key, default)
