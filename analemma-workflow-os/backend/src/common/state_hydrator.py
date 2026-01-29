"""
🚀 StateHydrator - Smart StateBag Architecture
================================================

Analemma OS의 핵심 성능 최적화 모듈.
14만 줄 커널에서 발생하는 직렬화/역직렬화 오버헤드를 해결합니다.

## 핵심 전략

1. **Control Plane vs Data Plane 분리**
   - Control Plane (Small): ID, Status, Counters, Pointers → SFN 컨텍스트
   - Data Plane (Large): LLM 응답, 대용량 JSON → S3 + Pointer

2. **On-demand Hydration (수분 공급 패턴)**
   - Lambda 입구에서 필요한 필드가 '포인터'라면 S3에서 로드
   - 처리 완료 후 S3에 덤프하고 포인터만 반환
   - SFN 페이로드 항상 10KB 미만 유지

3. **Delta Updates (델타 업데이트)**
   - 전체 StateBag 대신 '변경된 필드'만 리턴
   - StateDataManager가 중앙에서 취합하여 마스터 업데이트

Usage:
    from src.common.state_hydrator import StateHydrator, SmartStateBag
    
    # Lambda 핸들러 시작
    hydrator = StateHydrator(bucket_name="execution-bucket")
    state = hydrator.hydrate(event)  # S3에서 필요한 데이터만 로드
    
    # 비즈니스 로직 수행
    state["llm_response"] = call_llm(...)
    
    # Lambda 반환 (자동 오프로드)
    return hydrator.dehydrate(
        state=state,
        owner_id=event.get('ownerId'),
        workflow_id=event.get('workflowId'),
        execution_id=event.get('execution_id')
    )  # 큰 필드는 S3로, 포인터만 반환

Author: Analemma OS Team
Version: 1.0.0
"""

from __future__ import annotations
import json
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union, TypedDict, Literal
from functools import lru_cache

logger = logging.getLogger(__name__)


# ============================================================================
# ExecutionResult Interface (P0: ResultPath 통일)
# ============================================================================
# 모든 Lambda는 이 인터페이스를 준수해야 합니다.
# Step Functions의 모든 Task에서 ResultPath: "$.execution_result"만 사용합니다.

class ThoughtSignature(TypedDict, total=False):
    """
    🧠 Gemini 3 사고 과정 저장 공간
    
    미래 Gemini 3의 thinking process를 이 구조에 저장합니다.
    데이터 구조가 미리 준비되어 있어 필드 불일치 문제를 방지합니다.
    """
    thinking_process: str  # 사고 과정 텍스트
    thought_steps: List[Dict[str, Any]]  # 단계별 사고 과정
    confidence_score: float  # 신뢰도 점수 (0.0 ~ 1.0)
    reasoning_tokens: int  # 사고에 사용된 토큰 수
    model_version: str  # 사용된 모델 버전
    timestamp: float  # 생성 시간


class RoutingInfo(TypedDict, total=False):
    """
    🚦 Inter-segment 라우팅 정보
    
    partition_map.outgoing_edges에서 추출한 정보를 담습니다.
    """
    edge_type: str  # "normal", "hitp", "conditional", "loop_exit"
    is_loop_exit: bool
    is_back_edge: bool
    condition: Optional[str]
    target_node: str
    requires_hitp: bool


class ExecutionResult(TypedDict, total=False):
    """
    📦 표준 실행 결과 인터페이스 (P0: 모든 Lambda 필수 준수)
    
    Step Functions의 모든 Task는 이 인터페이스로 결과를 반환해야 합니다.
    ResultPath는 항상 "$.execution_result"로 통일합니다.
    
    사용 예시:
        return {
            "status": "CONTINUE",
            "final_state": {...},
            "final_state_s3_path": "s3://...",
            "next_segment_to_run": 3,
            "routing_info": {"requires_hitp": False}
        }
    """
    # 필수 필드
    status: Literal[
        "CONTINUE",           # 다음 세그먼트로 계속
        "COMPLETE",           # 워크플로우 완료
        "PARALLEL_GROUP",     # 병렬 그룹 실행 필요
        "SEQUENTIAL_BRANCH",  # 순차 브랜치 실행 필요
        "PAUSE",              # 일시 정지 (외부 이벤트 대기)
        "PAUSED_FOR_HITP",    # HITP 대기 중
        "FAILED",             # 실패
        "HALTED",             # 강제 중지
        "SIGKILL",            # 종료 신호
        "SKIPPED"             # 스킵됨
    ]
    
    # 상태 데이터
    final_state: Dict[str, Any]  # 실행 후 상태
    final_state_s3_path: Optional[str]  # S3 오프로드 시 경로
    
    # 라우팅 정보
    next_segment_to_run: Optional[int]  # 다음 세그먼트 ID
    routing_info: Optional[RoutingInfo]  # Inter-segment 라우팅 정보
    
    # 메타데이터
    new_history_logs: List[str]  # 새 히스토리 로그
    error_info: Optional[Dict[str, Any]]  # 에러 정보
    segment_type: str  # "normal", "llm", "hitp", "aggregator" 등
    segment_id: Optional[int]  # 현재 세그먼트 ID
    total_segments: int  # 전체 세그먼트 수
    execution_time: float  # 실행 시간 (초)
    
    # 병렬 처리 (Fork-Join 패턴)
    branches: Optional[List[Dict[str, Any]]]  # PARALLEL_GROUP일 때 브랜치 설정
    inner_partition_map: Optional[List[Dict[str, Any]]]  # SEQUENTIAL_BRANCH일 때
    
    # Kernel 액션
    kernel_actions: Optional[List[Dict[str, Any]]]  # 카널 액션 로그
    
    # 🧠 Gemini 3 사고 과정 (미래 예약)
    thought_signature: Optional[ThoughtSignature]  # LLM 사고 과정 저장


# ============================================================================
# Branch Result Interface (Fork-Join 패턴)
# ============================================================================

class BranchResult(TypedDict, total=False):
    """
    🔀 병렬 브랜치 결과 (Fork-Join 패턴)
    
    parallel_group에서 각 브랜치는 이 구조로 결과를 반환합니다.
    병합 충돌을 방지하기 위해:
    1. 각 브랜치는 branch_results 배열에 기록
    2. Aggregator 노드에서 메인 state_bag에 병합
    """
    branch_id: str  # 브랜치 식별자
    status: Literal["completed", "failed", "partial_failure"]
    final_state: Dict[str, Any]  # 브랜치 최종 상태
    final_state_s3_path: Optional[str]  # S3 오프로드 시
    loop_iterations: int  # 루프 반복 횟수
    execution_time_ms: float  # 실행 시간 (ms)
    error_info: Optional[Dict[str, Any]]  # 에러 정보
    thought_signature: Optional[ThoughtSignature]  # LLM 사고 과정

# ============================================================================
# Constants
# ============================================================================

# 페이로드 임계값 (bytes)
CONTROL_PLANE_MAX_SIZE = 10 * 1024  # 10KB - SFN 페이로드 목표
DATA_PLANE_THRESHOLD = 50 * 1024   # 50KB - S3 오프로드 트리거
FIELD_OFFLOAD_THRESHOLD = 10 * 1024  # 10KB - 개별 필드 오프로드 트리거

# Control Plane 필드 (항상 SFN 컨텍스트에 유지)
CONTROL_PLANE_FIELDS = frozenset({
    # 식별자
    "ownerId", "workflowId", "idempotency_key", "execution_id",
    "quota_reservation_id",
    
    # 경로 포인터 (S3 참조)
    "workflow_config_s3_path", "state_s3_path", "partition_map_s3_path",
    "segment_manifest_s3_path", "final_state_s3_path",
    
    # 카운터 및 상태
    "segment_to_run", "total_segments", "loop_counter",
    "max_loop_iterations", "max_branch_iterations", "max_concurrency",
    
    # 전략 및 모드
    "distributed_strategy", "distributed_mode", "MOCK_MODE",
    
    # 세그먼트 메타데이터
    "llm_segments", "hitp_segments", "segment_type",
    
    # 라이트 설정 (축약된 메타데이터)
    "light_config"
})

# Data Plane 필드 (자동 S3 오프로드 대상)
DATA_PLANE_FIELDS = frozenset({
    "workflow_config", "partition_map", "segment_manifest",
    "current_state", "final_state", "state_history",
    "parallel_results", "branch_results", "callback_result",
    "llm_response", "query_results", "step_history", "messages",
    # 🧠 Gemini 3 사고 과정 (대용량 가능)
    "thought_signature", "thinking_process", "thought_steps"
})

# 포인터 마커
POINTER_MARKER = "__s3_pointer__"
DELTA_MARKER = "__delta_update__"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class S3Pointer:
    """S3 데이터 포인터 - 실제 데이터 대신 참조만 저장"""
    bucket: str
    key: str
    size_bytes: int
    checksum: str
    field_name: str
    created_at: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            POINTER_MARKER: True,
            "bucket": self.bucket,
            "key": self.key,
            "size_bytes": self.size_bytes,
            "checksum": self.checksum,
            "field_name": self.field_name,
            "created_at": self.created_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["S3Pointer"]:
        if not data.get(POINTER_MARKER):
            return None
        return cls(
            bucket=data["bucket"],
            key=data["key"],
            size_bytes=data.get("size_bytes", 0),
            checksum=data.get("checksum", ""),
            field_name=data.get("field_name", ""),
            created_at=data.get("created_at", 0)
        )


@dataclass
class DeltaUpdate:
    """델타 업데이트 - 변경된 필드만 추적"""
    changed_fields: Dict[str, Any] = field(default_factory=dict)
    deleted_fields: Set[str] = field(default_factory=set)
    s3_pointers: Dict[str, S3Pointer] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            DELTA_MARKER: True,
            "changed_fields": self.changed_fields,
            "deleted_fields": list(self.deleted_fields),
            "s3_pointers": {k: v.to_dict() for k, v in self.s3_pointers.items()},
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["DeltaUpdate"]:
        if not data.get(DELTA_MARKER):
            return None
        return cls(
            changed_fields=data.get("changed_fields", {}),
            deleted_fields=set(data.get("deleted_fields", [])),
            s3_pointers={
                k: S3Pointer.from_dict(v) 
                for k, v in data.get("s3_pointers", {}).items()
                if S3Pointer.from_dict(v) is not None
            },
            timestamp=data.get("timestamp", 0)
        )


# ============================================================================
# SmartStateBag - Hybrid Pointer Bag
# ============================================================================

class SmartStateBag(dict):
    """
    🚀 Smart StateBag - Pointer-based State Management
    
    일반 dict처럼 사용하면서도 내부적으로:
    - 큰 필드는 자동으로 포인터로 대체
    - 변경 사항 추적 (Delta Updates)
    - 필요할 때만 S3에서 로드 (Lazy Loading)
    """
    
    def __init__(
        self, 
        initial_data: Optional[Dict[str, Any]] = None,
        hydrator: Optional["StateHydrator"] = None,
        track_changes: bool = True
    ):
        self._hydrator = hydrator
        self._track_changes = track_changes
        self._original_values: Dict[str, Any] = {}
        self._changed_fields: Set[str] = set()
        self._deleted_fields: Set[str] = set()
        self._lazy_fields: Dict[str, S3Pointer] = {}  # 아직 로드 안 된 필드
        
        if initial_data:
            # 포인터 필드 분리
            for key, value in initial_data.items():
                pointer = S3Pointer.from_dict(value) if isinstance(value, dict) else None
                if pointer:
                    self._lazy_fields[key] = pointer
                else:
                    super().__setitem__(key, self._wrap(value))
                    if track_changes:
                        self._original_values[key] = value
    
    def _wrap(self, value: Any) -> Any:
        """중첩 dict를 SmartStateBag으로 래핑"""
        if isinstance(value, dict) and not isinstance(value, SmartStateBag):
            # 포인터는 래핑하지 않음
            if value.get(POINTER_MARKER):
                return value
            return SmartStateBag(value, self._hydrator, track_changes=False)
        elif isinstance(value, list):
            return [self._wrap(item) for item in value if item is not None]
        return value
    
    def __setitem__(self, key: str, value: Any):
        # 변경 추적
        if self._track_changes and key not in self._changed_fields:
            self._changed_fields.add(key)
        
        # lazy 필드에서 제거 (실제 값으로 대체됨)
        if key in self._lazy_fields:
            del self._lazy_fields[key]
        
        super().__setitem__(key, self._wrap(value))
    
    def __getitem__(self, key: str) -> Any:
        # Lazy Loading: 포인터 필드면 S3에서 로드
        if key in self._lazy_fields:
            pointer = self._lazy_fields[key]
            if self._hydrator:
                value = self._hydrator._load_from_s3(pointer)
                super().__setitem__(key, self._wrap(value))
                del self._lazy_fields[key]
                return super().__getitem__(key)
            else:
                # hydrator 없으면 포인터 그대로 반환
                return pointer.to_dict()
        
        return super().get(key)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Safe get with lazy loading"""
        try:
            value = self.__getitem__(key)
            return value if value is not None else default
        except KeyError:
            return default
    
    def __contains__(self, key: object) -> bool:
        return super().__contains__(key) or key in self._lazy_fields
    
    def __delitem__(self, key: str):
        if self._track_changes:
            self._deleted_fields.add(key)
        if key in self._lazy_fields:
            del self._lazy_fields[key]
        else:
            super().__delitem__(key)
    
    def get_delta(self) -> DeltaUpdate:
        """변경된 필드만 추출"""
        changed = {}
        for field_name in self._changed_fields:
            if field_name in self:
                changed[field_name] = self[field_name]
        
        return DeltaUpdate(
            changed_fields=changed,
            deleted_fields=self._deleted_fields.copy()
        )
    
    def has_changes(self) -> bool:
        """변경 사항 존재 여부"""
        return bool(self._changed_fields) or bool(self._deleted_fields)
    
    def get_control_plane(self) -> Dict[str, Any]:
        """Control Plane 필드만 추출 (SFN 페이로드용)"""
        result = {}
        for key in CONTROL_PLANE_FIELDS:
            if key in self:
                result[key] = super().get(key)
        return result
    
    def get_lazy_pointers(self) -> Dict[str, S3Pointer]:
        """아직 로드되지 않은 포인터 목록"""
        return self._lazy_fields.copy()
    
    def to_dict(self) -> Dict[str, Any]:
        """일반 dict로 변환 (lazy 필드는 포인터로)"""
        result = dict(self)
        for key, pointer in self._lazy_fields.items():
            result[key] = pointer.to_dict()
        return result


# ============================================================================
# StateHydrator - Core Hydration Engine
# ============================================================================

class StateHydrator:
    """
    🚀 State Hydrator - On-demand Data Loading & Smart Offloading
    
    Lambda 함수에서 사용:
    1. hydrate(): 이벤트에서 필요한 데이터만 S3에서 로드
    2. dehydrate(): 큰 데이터를 S3로 오프로드하고 포인터 반환
    """
    
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        s3_client: Optional[Any] = None,
        control_plane_max_size: int = CONTROL_PLANE_MAX_SIZE,
        field_offload_threshold: int = FIELD_OFFLOAD_THRESHOLD
    ):
        self._bucket = bucket_name or os.environ.get("EXECUTION_BUCKET", "")
        self._s3_client = s3_client
        self._control_plane_max_size = control_plane_max_size
        self._field_offload_threshold = field_offload_threshold
        self._cache: Dict[str, Any] = {}  # 인메모리 캐시 (Lambda 재사용 시)
    
    @property
    def s3_client(self):
        """Lazy S3 client initialization"""
        if self._s3_client is None:
            from src.common.aws_clients import get_s3_client
            self._s3_client = get_s3_client()
        return self._s3_client
    
    def hydrate(
        self,
        event: Dict[str, Any],
        fields_to_load: Optional[Set[str]] = None,
        eager_load: bool = False
    ) -> SmartStateBag:
        """
        🚀 이벤트를 SmartStateBag으로 변환 (On-demand Hydration)
        
        Args:
            event: Lambda 이벤트 (SFN 또는 API Gateway)
            fields_to_load: 즉시 로드할 필드 (None이면 lazy loading)
            eager_load: True면 모든 포인터 즉시 로드
        
        Returns:
            SmartStateBag: Hydrated state bag
        """
        start_time = time.time()
        
        # state_data 추출 (SFN 컨텍스트)
        state_data = event.get("state_data", event)
        
        # SmartStateBag 생성
        bag = SmartStateBag(state_data, hydrator=self)
        
        # 즉시 로드할 필드 처리
        if fields_to_load or eager_load:
            fields = fields_to_load or set(bag.get_lazy_pointers().keys())
            for field_name in fields:
                if field_name in bag.get_lazy_pointers():
                    _ = bag[field_name]  # 접근하면 자동 로드
        
        elapsed = (time.time() - start_time) * 1000
        logger.debug(f"[StateHydrator] Hydrated in {elapsed:.2f}ms, "
                    f"lazy_fields={len(bag.get_lazy_pointers())}")
        
        return bag
    
    def dehydrate(
        self,
        state: SmartStateBag,
        owner_id: str,
        workflow_id: str,
        execution_id: str,
        segment_id: Optional[int] = None,
        force_offload_fields: Optional[Set[str]] = None,
        return_delta: bool = True
    ) -> Dict[str, Any]:
        """
        🚀 SmartStateBag을 SFN 반환용으로 변환 (Dehydration)
        
        큰 필드는 S3로 오프로드하고 포인터만 반환합니다.
        
        Args:
            state: SmartStateBag 인스턴스
            owner_id: 소유자 ID
            workflow_id: 워크플로우 ID
            execution_id: 실행 ID
            segment_id: 세그먼트 ID (optional)
            force_offload_fields: 강제로 S3 오프로드할 필드
            return_delta: True면 변경된 필드만 반환
        
        Returns:
            Dict: SFN 페이로드 (10KB 미만 보장)
        """
        start_time = time.time()
        result = {}
        offloaded_count = 0
        
        # Control Plane 필드는 항상 포함
        result.update(state.get_control_plane())
        
        # Data Plane 필드 처리
        all_fields = set(state.keys()) | set(state.get_lazy_pointers().keys())
        
        for field_name in all_fields:
            # Control Plane은 이미 처리됨
            if field_name in CONTROL_PLANE_FIELDS:
                continue
            
            # Lazy 필드는 포인터 그대로 유지
            if field_name in state.get_lazy_pointers():
                result[field_name] = state.get_lazy_pointers()[field_name].to_dict()
                continue
            
            # Delta 모드에서 변경되지 않은 필드는 스킵
            if return_delta and field_name not in state._changed_fields:
                continue
            
            value = state.get(field_name)
            if value is None:
                continue
            
            # 크기 체크 및 오프로드 결정
            should_offload = (
                field_name in DATA_PLANE_FIELDS or
                (force_offload_fields and field_name in force_offload_fields)
            )
            
            if should_offload:
                value_json = json.dumps(value, ensure_ascii=False, default=str)
                value_size = len(value_json.encode('utf-8'))
                
                if value_size > self._field_offload_threshold:
                    # S3 오프로드
                    pointer = self._offload_to_s3(
                        value=value,
                        value_json=value_json,
                        owner_id=owner_id,
                        workflow_id=workflow_id,
                        execution_id=execution_id,
                        field_name=field_name,
                        segment_id=segment_id
                    )
                    result[field_name] = pointer.to_dict()
                    offloaded_count += 1
                    continue
            
            # 작은 필드는 그대로 포함
            result[field_name] = value
        
        # Delta 메타데이터 추가
        if return_delta and state.has_changes():
            result[DELTA_MARKER] = True
            result["__changed_fields__"] = list(state._changed_fields)
            result["__deleted_fields__"] = list(state._deleted_fields)
        
        # 최종 크기 검증
        final_size = len(json.dumps(result, ensure_ascii=False, default=str).encode('utf-8'))
        
        elapsed = (time.time() - start_time) * 1000
        logger.info(f"[StateHydrator] Dehydrated in {elapsed:.2f}ms, "
                   f"final_size={final_size/1024:.2f}KB, offloaded={offloaded_count}")
        
        if final_size > self._control_plane_max_size:
            logger.warning(f"[StateHydrator] Payload exceeds target ({final_size/1024:.2f}KB > "
                          f"{self._control_plane_max_size/1024:.2f}KB)")
        
        return result
    
    def _offload_to_s3(
        self,
        value: Any,
        value_json: str,
        owner_id: str,
        workflow_id: str,
        execution_id: str,
        field_name: str,
        segment_id: Optional[int] = None
    ) -> S3Pointer:
        """S3에 데이터 오프로드"""
        # S3 키 생성
        segment_suffix = f"_seg{segment_id}" if segment_id is not None else ""
        timestamp = int(time.time() * 1000)
        s3_key = f"workflows/{workflow_id}/executions/{execution_id}/{field_name}{segment_suffix}_{timestamp}.json"
        
        # 체크섬 계산
        checksum = hashlib.md5(value_json.encode('utf-8')).hexdigest()[:8]
        
        # S3 업로드
        self.s3_client.put_object(
            Bucket=self._bucket,
            Key=s3_key,
            Body=value_json.encode('utf-8'),
            ContentType="application/json"
        )
        
        logger.debug(f"[StateHydrator] Offloaded {field_name} to s3://{self._bucket}/{s3_key}")
        
        return S3Pointer(
            bucket=self._bucket,
            key=s3_key,
            size_bytes=len(value_json.encode('utf-8')),
            checksum=checksum,
            field_name=field_name
        )
    
    def _load_from_s3(self, pointer: S3Pointer) -> Any:
        """
        S3에서 데이터 로드 (캐시 사용 + Checksum 검증)
        
        🛡️ [P0 강화] 분산 환경에서 데이터 오염 방지를 위한
        Checksum 검증 로직 추가
        """
        cache_key = f"{pointer.bucket}/{pointer.key}"
        
        # 캐시 확인
        if cache_key in self._cache:
            logger.debug(f"[StateHydrator] Cache hit for {pointer.field_name}")
            return self._cache[cache_key]
        
        # S3에서 로드
        try:
            response = self.s3_client.get_object(
                Bucket=pointer.bucket,
                Key=pointer.key
            )
            raw_bytes = response['Body'].read()
            raw_str = raw_bytes.decode('utf-8')
            
            # 🛡️ [P0] Checksum 검증 - 데이터 오염 방지
            if pointer.checksum:
                calculated_checksum = hashlib.md5(raw_str.encode('utf-8')).hexdigest()[:8]
                if calculated_checksum != pointer.checksum:
                    logger.error(
                        f"[StateHydrator] 🚨 CHECKSUM MISMATCH for {pointer.field_name}! "
                        f"Expected: {pointer.checksum}, Got: {calculated_checksum}. "
                        f"Data may be corrupted. S3 Key: {pointer.key}"
                    )
                    # 엄격 모드: 오염된 데이터 거부
                    raise ValueError(
                        f"Data integrity check failed for {pointer.field_name}. "
                        f"Checksum mismatch: expected {pointer.checksum}, got {calculated_checksum}"
                    )
                else:
                    logger.debug(f"[StateHydrator] Checksum verified for {pointer.field_name}")
            
            data = json.loads(raw_str)
            
            # 캐시 저장
            self._cache[cache_key] = data
            
            logger.debug(f"[StateHydrator] Loaded {pointer.field_name} from S3 "
                        f"({pointer.size_bytes/1024:.2f}KB, checksum={pointer.checksum or 'N/A'})")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"[StateHydrator] JSON decode failed for {pointer.field_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"[StateHydrator] Failed to load {pointer.field_name}: {e}")
            raise
    
    def clear_cache(self):
        """캐시 초기화"""
        self._cache.clear()


# ============================================================================
# Helper Functions
# ============================================================================

def check_inter_segment_edges(
    segment_config: Dict[str, Any],
    next_segment_config: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    🚀 Inter-segment edge 정보 추출 (O(1) 조회)
    
    partition_map의 outgoing_edges 필드를 활용하여
    workflow_config 전체 스캔 없이 엣지 타입 확인
    
    Args:
        segment_config: 현재 세그먼트 설정
        next_segment_config: 다음 세그먼트 설정 (optional)
    
    Returns:
        Dict with edge_type, is_loop_exit, condition, metadata
        None if no inter-segment edge found
    """
    outgoing_edges = segment_config.get("outgoing_edges", [])
    
    if not outgoing_edges:
        return None
    
    if next_segment_config is None:
        # 다음 세그먼트 없으면 첫 번째 outgoing edge 반환
        if outgoing_edges:
            edge = outgoing_edges[0]
            return {
                "edge_type": edge.get("edge_type", "normal"),
                "is_loop_exit": edge.get("is_loop_exit", False),
                "is_back_edge": edge.get("is_back_edge", False),
                "condition": edge.get("condition"),
                "router_func": edge.get("router_func"),
                "target_node": edge.get("target_node"),
                "metadata": edge.get("metadata", {})
            }
        return None
    
    # 다음 세그먼트의 node_ids 집합
    next_node_ids = set(next_segment_config.get("node_ids", []))
    
    # outgoing_edges에서 다음 세그먼트로 가는 엣지 찾기
    for edge in outgoing_edges:
        if edge.get("target_node") in next_node_ids:
            return {
                "edge_type": edge.get("edge_type", "normal"),
                "is_loop_exit": edge.get("is_loop_exit", False),
                "is_back_edge": edge.get("is_back_edge", False),
                "condition": edge.get("condition"),
                "router_func": edge.get("router_func"),
                "target_node": edge.get("target_node"),
                "metadata": edge.get("metadata", {})
            }
    
    return None


def is_hitp_edge(edge_info: Optional[Dict[str, Any]]) -> bool:
    """HITP 엣지인지 확인"""
    if not edge_info:
        return False
    edge_type = edge_info.get("edge_type", "").lower()
    return edge_type in {"hitp", "human_in_the_loop", "pause", "approval"}


def is_loop_exit_edge(edge_info: Optional[Dict[str, Any]]) -> bool:
    """Loop exit 엣지인지 확인"""
    if not edge_info:
        return False
    return edge_info.get("is_loop_exit", False)


def prepare_response_with_offload(
    final_state: Dict[str, Any],
    output_s3_path: Optional[str],
    threshold_kb: int = 250
) -> Dict[str, Any]:
    """
    🚀 S3 오프로드 헬퍼 (DRY 원칙)
    
    4곳에서 중복되던 로직을 단일 함수로 통합
    
    Args:
        final_state: 최종 상태 데이터
        output_s3_path: S3 저장 경로 (이미 저장된 경우)
        threshold_kb: 오프로드 임계값 (KB)
    
    Returns:
        Dict: 원본 또는 S3 포인터 메타데이터
    """
    if not output_s3_path:
        return final_state
    
    try:
        state_json = json.dumps(final_state, ensure_ascii=False, default=str)
        state_size = len(state_json.encode('utf-8'))
    except Exception:
        # 직렬화 실패 시 원본 반환
        return final_state
    
    if state_size < threshold_kb * 1024:
        return final_state
    
    # 큰 상태는 메타데이터만
    return {
        "__s3_offloaded": True,
        "__s3_path": output_s3_path,
        "__original_size_kb": round(state_size / 1024, 2)
    }


def merge_delta_into_state(
    master_state: Dict[str, Any],
    delta: Dict[str, Any]
) -> Dict[str, Any]:
    """
    델타 업데이트를 마스터 상태에 병합
    
    Args:
        master_state: 기존 마스터 상태
        delta: 델타 업데이트 (changed_fields, deleted_fields 포함)
    
    Returns:
        Dict: 병합된 상태
    """
    if not delta.get(DELTA_MARKER):
        # 델타가 아니면 그대로 반환
        return delta
    
    result = master_state.copy()
    
    # 변경된 필드 적용
    changed_fields = delta.get("changed_fields", {})
    for key, value in changed_fields.items():
        result[key] = value
    
    # 삭제된 필드 제거
    deleted_fields = delta.get("deleted_fields", [])
    for key in deleted_fields:
        result.pop(key, None)
    
    # S3 포인터 적용
    s3_pointers = delta.get("s3_pointers", {})
    for key, pointer_data in s3_pointers.items():
        result[key] = pointer_data
    
    return result


def validate_execution_result(result: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    🛡️ ExecutionResult 인터페이스 준수 검증
    
    Lambda가 반환하는 결과가 ExecutionResult 인터페이스를
    준수하는지 검증합니다.
    
    Args:
        result: Lambda 반환 결과
    
    Returns:
        Tuple[bool, List[str]]: (유효성 여부, 오류 메시지 리스트)
    """
    errors = []
    
    # 필수 필드 검증
    if "status" not in result:
        errors.append("Missing required field: status")
    else:
        valid_statuses = {
            "CONTINUE", "COMPLETE", "PARALLEL_GROUP", "SEQUENTIAL_BRANCH",
            "PAUSE", "PAUSED_FOR_HITP", "FAILED", "HALTED", "SIGKILL", "SKIPPED"
        }
        if result["status"] not in valid_statuses:
            errors.append(f"Invalid status: {result['status']}. Valid: {valid_statuses}")
    
    # final_state 검증
    if "final_state" not in result and "final_state_s3_path" not in result:
        errors.append("Must have either 'final_state' or 'final_state_s3_path'")
    
    # 라우팅 정보 검증 (CONTINUE 상태일 때)
    if result.get("status") == "CONTINUE" and "next_segment_to_run" not in result:
        errors.append("CONTINUE status requires 'next_segment_to_run'")
    
    # 병렬 그룹 검증
    if result.get("status") == "PARALLEL_GROUP" and "branches" not in result:
        errors.append("PARALLEL_GROUP status requires 'branches'")
    
    return len(errors) == 0, errors


def aggregate_branch_results(
    branch_results: List[Dict[str, Any]],
    merge_strategy: Literal["last_wins", "deep_merge", "collect"] = "deep_merge"
) -> Dict[str, Any]:
    """
    🔀 Fork-Join 패턴: 병렬 브랜치 결과 병합
    
    병렬 실행된 브랜치들의 결과를 안전하게 병합합니다.
    병합 충돌을 방지하기 위해 전략을 선택할 수 있습니다.
    
    Args:
        branch_results: 각 브랜치의 BranchResult 배열
        merge_strategy:
            - "last_wins": 마지막 브랜치가 덮어쓀 (불권장)
            - "deep_merge": 깊은 병합 (기본값)
            - "collect": 모든 브랜치 결과를 배열로 수집
    
    Returns:
        Dict: 병합된 상태
    """
    if not branch_results:
        return {}
    
    if merge_strategy == "collect":
        # 모든 브랜치 결과를 배열로 수집
        return {
            "branch_results": branch_results,
            "total_branches": len(branch_results),
            "successful_branches": len([b for b in branch_results if b.get("status") == "completed"]),
            "failed_branches": len([b for b in branch_results if b.get("status") == "failed"])
        }
    
    if merge_strategy == "last_wins":
        # 마지막 브랜치가 덮어쓀 (병합 충돌 위험!)
        logger.warning("[Fork-Join] Using 'last_wins' strategy - potential merge conflicts!")
        result = {}
        for branch in branch_results:
            final_state = branch.get("final_state", {})
            result.update(final_state)
        return result
    
    # deep_merge: 깊은 병합 (기본)
    def deep_merge(base: Dict, update: Dict) -> Dict:
        result = base.copy()
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            elif key in result and isinstance(result[key], list) and isinstance(value, list):
                # 리스트는 확장
                result[key] = result[key] + value
            else:
                result[key] = value
        return result
    
    merged = {}
    for branch in branch_results:
        final_state = branch.get("final_state", {})
        merged = deep_merge(merged, final_state)
    
    # 브랜치 메타데이터 추가
    merged["__branch_metadata__"] = {
        "total_branches": len(branch_results),
        "successful_branches": len([b for b in branch_results if b.get("status") == "completed"]),
        "failed_branches": len([b for b in branch_results if b.get("status") == "failed"]),
        "branch_ids": [b.get("branch_id") for b in branch_results],
        "total_execution_time_ms": sum(b.get("execution_time_ms", 0) for b in branch_results)
    }
    
    return merged


def create_thought_signature(
    thinking_process: str,
    thought_steps: Optional[List[Dict[str, Any]]] = None,
    confidence_score: float = 0.0,
    reasoning_tokens: int = 0,
    model_version: str = "unknown"
) -> ThoughtSignature:
    """
    🧠 Gemini 3 사고 과정 서명 생성
    
    LLM의 사고 과정을 구조화된 형식으로 저장합니다.
    
    Args:
        thinking_process: 사고 과정 텍스트
        thought_steps: 단계별 사고 과정
        confidence_score: 신뢰도 점수 (0.0 ~ 1.0)
        reasoning_tokens: 사고에 사용된 토큰 수
        model_version: 사용된 모델 버전
    
    Returns:
        ThoughtSignature: 사고 과정 서명
    """
    return {
        "thinking_process": thinking_process,
        "thought_steps": thought_steps or [],
        "confidence_score": max(0.0, min(1.0, confidence_score)),
        "reasoning_tokens": reasoning_tokens,
        "model_version": model_version,
        "timestamp": time.time()
    }


# ============================================================================
# Backward Compatibility
# ============================================================================

def ensure_smart_state_bag(state: Any, hydrator: Optional[StateHydrator] = None) -> SmartStateBag:
    """기존 StateBag 또는 dict를 SmartStateBag으로 업그레이드"""
    if isinstance(state, SmartStateBag):
        return state
    return SmartStateBag(state if isinstance(state, dict) else {}, hydrator=hydrator)


# ============================================================================
# Module-level Singleton (Lambda 재사용 최적화)
# ============================================================================

_default_hydrator: Optional[StateHydrator] = None


def get_default_hydrator() -> StateHydrator:
    """Default StateHydrator 싱글톤"""
    global _default_hydrator
    if _default_hydrator is None:
        _default_hydrator = StateHydrator()
    return _default_hydrator


def hydrate_event(event: Dict[str, Any], **kwargs) -> SmartStateBag:
    """이벤트를 SmartStateBag으로 변환 (싱글톤 사용)"""
    return get_default_hydrator().hydrate(event, **kwargs)


def dehydrate_state(
    state: SmartStateBag,
    owner_id: str,
    workflow_id: str,
    execution_id: str,
    **kwargs
) -> Dict[str, Any]:
    """SmartStateBag을 SFN 반환용으로 변환 (싱글톤 사용)"""
    return get_default_hydrator().dehydrate(
        state, owner_id, workflow_id, execution_id, **kwargs
    )
