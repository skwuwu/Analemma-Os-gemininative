"""
🎯 Universal Sync Core - Function-Agnostic 데이터 파이프라인

v3.3 - "Unified Pipe: 탄생부터 소멸까지"

핵심 원칙:
    "함수가 무엇이든 상관없이, 데이터가 흐르는 파이프 자체를 표준화"
    
    모든 액션 함수는 이제 "3줄짜리 래퍼"입니다:
        1. 입력 전처리 (액션별 특수 필드 추출)
        2. universal_sync_core() 호출
        3. 응답 포맷팅

모든 StateDataManager 액션은 이 코어를 통과합니다:
    1. flatten_result() - 입력 정규화 (액션별 스마트 추출)
    2. merge_logic() - 상태 병합 (Shallow Merge + Copy-on-Write)
    3. optimize_and_offload() - 자동 최적화 (P0~P2 자동 해결)

데이터 생애 주기 (Unified Pipe):
    - 탄생 (Init): {} → Universal Sync → StateBag v0
    - 성장 (Sync): StateBag vN + Result → Universal Sync → StateBag vN+1
    - 협업 (Aggregate): StateBag vN + Branches → Universal Sync → StateBag vFinal

성능 최적화:
    - ① Copy-on-Write: 전체 deepcopy 대신 변경된 서브트리만 복사
    - ② Shallow Merge: 불필요한 중첩 복사 방지
    - ③ Checksum 검증: S3 로드 시 데이터 무결성 확인

P0~P2 자동 해결:
    - P0: 어떤 경로로 들어왔든 크면 S3로 간다 (T=0 가드레일 포함)
    - P1: 포인터 비대화 방지 (모든 액션에 자동 적용)
    - P2: 스냅샷도 코어 통과 → 중복 저장 방지

v3.3 - 2026-01-29 (Unified Pipe: Day-Zero Sync)
"""

import json
import hashlib
import time
from typing import Dict, Any, Optional, List, Callable, TypedDict, Literal
from datetime import datetime, timezone
from abc import ABC, abstractmethod

# Lazy imports to avoid circular dependencies
_logger = None
_s3_client = None
_S3_BUCKET = None

def _get_logger():
    global _logger
    if _logger is None:
        from aws_lambda_powertools import Logger
        import os
        _logger = Logger(
            service=os.getenv("AWS_LAMBDA_FUNCTION_NAME", "universal-sync-core"),
            level=os.getenv("LOG_LEVEL", "INFO"),
            child=True
        )
    return _logger

def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client('s3')
    return _s3_client

def _get_s3_bucket():
    global _S3_BUCKET
    if _S3_BUCKET is None:
        import os
        # 🛡️ [Constraint] Bucket Name Consistency
        # Prioritize unified WORKFLOW_STATE_BUCKET, then legacy fallbacks
        _S3_BUCKET = (
            os.environ.get('WORKFLOW_STATE_BUCKET') or 
            os.environ.get('S3_BUCKET') or 
            os.environ.get('STATE_STORAGE_BUCKET') or
            ''
        )
        # 🛡️ [Guard] Fail-fast if no bucket configured
        if not _S3_BUCKET:
            _get_logger().error(
                "[CRITICAL] No S3 bucket configured! "
                "Set WORKFLOW_STATE_BUCKET, S3_BUCKET, or STATE_STORAGE_BUCKET env var."
            )
    return _S3_BUCKET


# ============================================
# Type Definitions
# ============================================

class MergeStrategy(TypedDict, total=False):
    """필드별 병합 전략"""
    list_strategy: Literal['append', 'replace', 'dedupe_append', 'set_union']
    conflict_resolution: Literal['latest', 'base', 'delta']
    deep_merge_fields: List[str]


class SyncContext(TypedDict, total=False):
    """동기화 컨텍스트"""
    execution_id: str
    action: str
    merge_strategy: MergeStrategy
    idempotency_key: str


# ============================================
# Constants
# ============================================

# 오프로딩 제외 제어 필드
CONTROL_FIELDS_NEVER_OFFLOAD = frozenset({
    'execution_id',
    'segment_to_run', 
    'segment_id',  # 🛡️ [Fix] Routing safety
    'loop_counter',
    'next_action',
    'status',
    'idempotency_key',
    'state_s3_path',
    'pre_snapshot_s3_path',
    'post_snapshot_s3_path',
    'last_update_time',
    'payload_size_kb'
})

# 리스트 필드 기본 병합 전략
LIST_FIELD_STRATEGIES: Dict[str, str] = {
    'state_history': 'dedupe_append',      # 중복 제거 후 추가
    'new_history_logs': 'dedupe_append',
    'failed_branches': 'append',           # 그냥 추가
    'distributed_outputs': 'append',
    'branches': 'replace',                 # 교체 (최신 브랜치 정보)
    'chunk_results': 'replace',
}

# 크기 임계값 (KB)
FIELD_OFFLOAD_THRESHOLD_KB = 30
FULL_STATE_OFFLOAD_THRESHOLD_KB = 100
MAX_PAYLOAD_SIZE_KB = 200
POINTER_BLOAT_WARNING_THRESHOLD_KB = 10


# ============================================
# Retry Strategy (Abstract + Concrete)
# ============================================

class RetryStrategy(ABC):
    """재시도 전략 추상 클래스"""
    
    @abstractmethod
    def execute(self, func: Callable, fallback: Any = None) -> Any:
        """재시도를 적용하여 함수 실행"""
        pass


class ExponentialBackoffRetry(RetryStrategy):
    """Exponential Backoff 재시도 전략"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 8.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def execute(self, func: Callable, fallback: Any = None) -> Any:
        logger = _get_logger()
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.warning(f"Retry {attempt+1}/{self.max_retries} after {delay:.2f}s: {e}")
                    time.sleep(delay)
        
        logger.error(f"Failed after {self.max_retries} attempts: {last_exception}")
        return fallback


class NoRetry(RetryStrategy):
    """재시도 없는 전략 (테스트용)"""
    
    def execute(self, func: Callable, fallback: Any = None) -> Any:
        try:
            return func()
        except Exception:
            return fallback


# ============================================
# StateHydrator - S3 복구 전담 클래스
# ============================================

class StateHydrator:
    """
    상태 복구 전담 클래스 (Control Plane)
    
    v3.1 개선사항:
        - Retry Strategy 주입 가능
        - Checksum 검증 지원
        - 캐시 통합
    """
    
    def __init__(
        self, 
        retry_strategy: Optional[RetryStrategy] = None,
        validate_checksum: bool = True
    ):
        self.retry_strategy = retry_strategy or ExponentialBackoffRetry()
        self.validate_checksum = validate_checksum
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self._cache_ttl = 300  # 5분
        self._max_cache_size = 20
    
    def load_from_s3(
        self, 
        s3_path: str, 
        expected_checksum: Optional[str] = None,
        use_cache: bool = True
    ) -> Any:
        """
        재시도 + 체크섬 검증이 통합된 S3 로딩
        
        Args:
            s3_path: S3 경로 (s3://bucket/key)
            expected_checksum: 예상 MD5 해시 (검증용)
            use_cache: 캐시 사용 여부
        
        Returns:
            로드된 데이터 또는 None (실패 시)
        """
        if not s3_path or not s3_path.startswith('s3://'):
            return None
        
        # 캐시 확인
        if use_cache:
            cached = self._get_from_cache(s3_path)
            if cached is not None:
                return cached
        
        # 재시도 전략 적용
        result = self.retry_strategy.execute(
            func=lambda: self._load_and_validate(s3_path, expected_checksum),
            fallback=None
        )
        
        # 캐시 저장
        if result is not None and use_cache:
            self._put_to_cache(s3_path, result)
        
        return result
    
    def _load_and_validate(self, s3_path: str, expected_checksum: Optional[str]) -> Any:
        """단일 시도: S3 로드 + 체크섬 검증"""
        logger = _get_logger()
        s3_client = _get_s3_client()
        
        # Parse s3://bucket/key
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        content_str = content.decode('utf-8')
        
        # ③ Checksum 검증 - 데이터가 깨졌다면 에러를 내고 재시도
        if self.validate_checksum and expected_checksum:
            actual_checksum = hashlib.md5(content).hexdigest()
            if actual_checksum != expected_checksum:
                raise ValueError(
                    f"Checksum mismatch! Expected {expected_checksum}, got {actual_checksum}. "
                    "Data corrupted - triggering retry."
                )
        
        return json.loads(content_str)
    
    def _get_from_cache(self, s3_path: str) -> Optional[Any]:
        """캐시에서 데이터 조회 (TTL 체크)"""
        if s3_path not in self._cache:
            return None
        
        cache_time = self._cache_timestamps.get(s3_path, 0)
        if time.time() - cache_time >= self._cache_ttl:
            # TTL 만료
            del self._cache[s3_path]
            del self._cache_timestamps[s3_path]
            return None
        
        _get_logger().debug(f"Cache hit: {s3_path}")
        return self._cache[s3_path]
    
    def _put_to_cache(self, s3_path: str, data: Any) -> None:
        """캐시에 데이터 저장 (LRU 정책)"""
        if len(self._cache) >= self._max_cache_size:
            # 가장 오래된 항목 제거
            oldest = min(self._cache_timestamps, key=self._cache_timestamps.get)
            del self._cache[oldest]
            del self._cache_timestamps[oldest]
        
        self._cache[s3_path] = data
        self._cache_timestamps[s3_path] = time.time()
    
    def clear_cache(self) -> None:
        """캐시 초기화"""
        self._cache.clear()
        self._cache_timestamps.clear()


# 모듈 레벨 싱글턴
_default_hydrator: Optional[StateHydrator] = None

def get_default_hydrator() -> StateHydrator:
    """기본 StateHydrator 인스턴스 반환"""
    global _default_hydrator
    if _default_hydrator is None:
        _default_hydrator = StateHydrator()
    return _default_hydrator


# ============================================
# Universal Sync Core - 핵심 함수
# ============================================

def calculate_checksum(data: Any) -> str:
    """데이터의 MD5 체크섬 계산"""
    json_str = json.dumps(data, separators=(',', ':'), sort_keys=True, default=str)
    return hashlib.md5(json_str.encode('utf-8')).hexdigest()


def calculate_payload_size(data: Dict[str, Any]) -> int:
    """페이로드 크기 계산 (KB)"""
    try:
        json_str = json.dumps(data, separators=(',', ':'))
        return len(json_str.encode('utf-8')) // 1024
    except Exception:
        return 0


def flatten_result(result: Any, context: Optional[SyncContext] = None) -> Dict[str, Any]:
    """
    📥 입력 정규화 (Normalize) - v3.2 스마트 추출
    
    액션 타입에 따라 적절한 필드를 추출합니다.
    리스트든 단일 객체든 동일한 Delta 형태로 평탄화합니다.
    
    액션별 추출 규칙:
        - sync: execution_result에서 상태 추출
        - aggregate_branches: 병렬 결과 배열에서 로그/상태 집계
        - aggregate_distributed: Map 결과에서 정렬 후 마지막 상태 선택
        - merge_callback: callback_result에서 사용자 응답 추출
        - merge_async: async_result에서 LLM 응답 추출
        - create_snapshot: 포인터 모드 결정
    
    🛡️ [v3.4] NEVER returns None - always returns dict
    """
    # 🛡️ [v3.4 Deep Guard] None 방지
    if result is None:
        _get_logger().debug("[Deep Guard] flatten_result received None, returning empty dict")
        return {}
    
    # 🛡️ context도 None일 수 있음
    if context is None:
        context = {'action': 'sync'}
    
    action = context.get('action', 'sync') if context else 'sync'
    
    # ============================================
    # Distributed Map ResultWriter 처리 (Manifest Pointer)
    # ============================================
    if isinstance(result, dict) and 'ResultWriterDetails' in result:
        rw_details = result['ResultWriterDetails']
        bucket = rw_details.get('Bucket')
        key = rw_details.get('Key')
        
        if bucket and key:
            try:
                # 1. Load Manifest Summary (Lightweight)
                s3 = _get_s3_client()
                obj = s3.get_object(Bucket=bucket, Key=key)
                manifest_data = json.loads(obj['Body'].read().decode('utf-8'))
                
                # 2. Extract Stats
                # Manifest structure varies, but typically contains stats or pointers
                # Assuming standard SFN Distributed Map output or custom aggregator format
                succeeded_count = 0
                failed_count = 0
                
                # Standard SFN Manifest typically separates Success/Failure file shards
                # We won't iterate all shards here (too heavy).
                # Instead, we rely on the manifest path itself as the "result".
                
                # If the manifest contains direct stats (some versions do):
                # otherwise we might need to assume success or check execution output?
                # Actually, for huge maps, we just store the pointer.
                
                return {
                    'segment_manifest_s3_path': f"s3://{bucket}/{key}",
                    'distributed_chunk_summary': {
                         'status': 'MANIFEST_ONLY',
                         'manifest_bucket': bucket,
                         'manifest_key': key
                    },
                    '_aggregation_complete': True
                }
            except Exception as e:
                _get_logger().error(f"Failed to process ResultWriter manifest: {e}")
                return {
                    'error': f"Failed to load manifest: {str(e)}",
                    '_aggregation_complete': False
                }

    # ============================================
    # Distributed Map 결과 (리스트 입력 - 인라인 모드)
    # ============================================
    if isinstance(result, list):
        # P1: execution_order 기준 정렬 → 논리적 마지막 세그먼트 보장
        sorted_results = sorted(
            [r for r in result if isinstance(r, dict)],
            key=lambda x: (str(x.get('execution_order', x.get('chunk_id', ''))), str(x.get('chunk_id', '')))
        )
        
        # 성공/실패 분리
        successful = [r for r in sorted_results if r.get('status') in ('COMPLETE', 'SUCCESS')]
        failed = [r for r in sorted_results if r.get('status') not in ('COMPLETE', 'SUCCESS', None)]
        
        # 마지막 성공 결과에서 상태 추출
        last_s3_path = None
        if successful:
            last_result = successful[-1]
            last_s3_path = last_result.get('output_s3_path') or last_result.get('final_state_s3_path')
        
        return {
            'state_s3_path': last_s3_path,
            'distributed_chunk_summary': {
                'total': len(result),
                'succeeded': len(successful),
                'failed': len(failed),
                'chunk_results': sorted_results[:10]  # 256KB 방지
            },
            '_failed_segments': failed,  # 내부 처리용
            '_aggregation_complete': True,
            # 🌿 [Pointer Strategy] Manifest extraction
            'segment_manifest_s3_path': successful[-1].get('segment_manifest_s3_path') if successful else None
        }
    
    # ============================================
    # 단일 객체 (딕셔너리 입력)
    # ============================================
    if isinstance(result, dict):
        delta = {}
        
        # 래퍼 패턴 제거 및 액션별 추출
        if action == 'merge_callback':
            payload = result.get('Payload', result.get('callback_result', result))
            if payload.get('user_response'):
                delta['last_hitp_response'] = payload['user_response']
            if payload.get('new_state_s3_path'):
                delta['state_s3_path'] = payload['new_state_s3_path']
            if payload.get('new_history_logs'):
                delta['new_history_logs'] = payload['new_history_logs']
            delta['_increment_segment'] = payload.get('segment_to_run') is None
                
        elif action == 'merge_async':
            payload = result.get('async_result', result)
            if payload.get('final_state_s3_path'):
                delta['state_s3_path'] = payload['final_state_s3_path']
            if payload.get('new_history_logs'):
                delta['new_history_logs'] = payload['new_history_logs']
            delta['_increment_segment'] = payload.get('segment_to_run') is None
            
        elif action == 'sync':
            payload = result.get('execution_result', result)
            if payload.get('final_state_s3_path'):
                delta['state_s3_path'] = payload['final_state_s3_path']
            if payload.get('next_segment_to_run') is not None:
                delta['segment_to_run'] = payload['next_segment_to_run']
            if payload.get('new_history_logs'):
                delta['new_history_logs'] = payload['new_history_logs']
            if payload.get('branches'):
                delta['pending_branches'] = payload['branches']
            # 🌿 [Pointer Strategy] branches_s3_path도 State Bag에 저장
            if payload.get('branches_s3_path'):
                delta['branches_s3_path'] = payload['branches_s3_path']
            # 🌿 [Pointer Strategy] Manifest extraction
            if payload.get('segment_manifest_s3_path'):
                 delta['segment_manifest_s3_path'] = payload['segment_manifest_s3_path']
            if payload.get('inner_partition_map'):
                delta['partition_map'] = payload['inner_partition_map']
                delta['segment_to_run'] = 0
            delta['_status'] = payload.get('status', 'CONTINUE')
            
            # 🔑 [Critical Fix v3.20] Merge final_state into current_state
            # run_workflow returns results directly (e.g., {'llm_raw_output': '...'})
            # These should become part of current_state for verification to find them
            final_state = payload.get('final_state')
            if isinstance(final_state, dict):
                current_state = final_state.get('current_state')
                if isinstance(current_state, dict):
                    # final_state.current_state가 있으면 그대로 사용
                    delta['current_state'] = current_state
                else:
                    # [v3.20] final_state에 current_state가 없으면 
                    # final_state 자체를 current_state로 사용 (run_workflow 결과)
                    # 단, 메타데이터 키는 제외
                    # [v3.21] 모든 Stage의 LLM output_key 포함
                    execution_result_keys = {
                        # Stage 1, 7, 8: Basic LLM
                        'llm_raw_output', 'llm_output', 'llm_result', 'parsed_summary',
                        # Stage 2: Flow Control
                        'item_result', 'quality_check_result', 'processed_items',
                        # Stage 3: Vision Basic
                        'vision_raw_output', 'parsed_vision_data',
                        # Stage 4: Vision Map
                        'image_analysis', 'vision_results',
                        # Stage 5: Hyper Stress
                        'document_analysis_raw', 'final_report_raw', 'depth_1_results', 'depth_2_results',
                        # Stage 6: Distributed Map Reduce
                        'llm_analysis_raw', 'partition_results',
                        # Stage 7: Parallel Multi LLM
                        'branch_results', 'claude_branch', 'gemini_branch',
                        # Stage 8: Slop Detection
                        'test_results', 'slop_detection_results',
                        # Common metadata
                        'usage', 'step_history', 'execution_logs', '__new_history_logs'
                    }
                    has_execution_results = any(k in final_state for k in execution_result_keys)
                    if has_execution_results or (final_state and '_' not in str(list(final_state.keys())[:1])):
                        # final_state에 실행 결과가 있으면 current_state로 승격
                        delta['current_state'] = final_state
                        _get_logger().info(f"[v3.20] Promoted final_state to current_state, keys: {list(final_state.keys())[:10]}")
            
        elif action == 'aggregate_branches':
            # 병렬 브랜치 결과 (포인터 배열)
            pointers = result.get('parallel_results', result.get('branch_pointers', []))
            if isinstance(pointers, list):
                return flatten_result(pointers, context)  # 리스트 처리로 위임
            delta = result
            
        elif action == 'create_snapshot':
            # 스냅샷: state_s3_path 존재 여부만 확인
            delta['_is_pointer_mode'] = bool(result.get('state_s3_path'))
        
        elif action == 'init':
            # 탄생 (Day-Zero Sync): 파티셔닝 결과 + 초기 상태를 그대로 전달
            # required metadata는 merge_logic에서 강제 주입됨
            
            # 🔑 [Critical] Extract bag contents and merge into delta
            # InitializeStateData passes {'bag': payload}, we need to extract payload
            # 🛡️ [Guard] bag이 None이거나 없는 경우 result 자체를 사용
            bag_contents = result.get('bag') if isinstance(result, dict) else None
            if bag_contents is None:
                # bag 키가 없거나 값이 None인 경우 result 자체 사용
                bag_contents = result if isinstance(result, dict) else {}
            
            if isinstance(bag_contents, dict):
                delta.update(bag_contents)
            else:
                _get_logger().warning(f"[Init] bag_contents is not dict: {type(bag_contents)}")
            
            delta['_is_init'] = True
            delta['_status'] = 'STARTED'
            # 🌿 [Pointer Strategy] Manifest extraction for Init
            if isinstance(result, dict) and result.get('segment_manifest_s3_path'):
                 delta['segment_manifest_s3_path'] = result['segment_manifest_s3_path']
            
        else:
            # 기본: 래퍼 제거
            if 'callback_result' in result and len(result) <= 2:
                delta = result['callback_result']
            elif 'async_result' in result and len(result) <= 2:
                delta = result['async_result']
            elif 'execution_result' in result and len(result) <= 2:
                delta = result['execution_result']
            else:
                delta = result
        
        return delta
    
    # 기타 타입 (문자열, 숫자 등)
    return {'raw_result': result}


def _shallow_copy_with_cow(base_state: Dict[str, Any], fields_to_modify: set) -> Dict[str, Any]:
    """
    ① Copy-on-Write 방식의 얕은 복사
    
    전체 deepcopy 대신 변경될 필드만 복사합니다.
    14만 줄 커널 상태의 CPU/GC 부하를 방지합니다.
    """
    # 기본은 얕은 복사 (참조 유지)
    result = base_state.copy()
    
    # 변경될 필드만 깊은 복사
    for field in fields_to_modify:
        if field in result:
            value = result[field]
            if isinstance(value, dict):
                result[field] = value.copy()
            elif isinstance(value, list):
                result[field] = value.copy()
    
    return result


def _get_log_key(log: Dict) -> str:
    """히스토리 로그의 고유 키 생성 (중복 제거용)"""
    if not isinstance(log, dict):
        return str(hash(str(log)))
    
    node_id = log.get('node_id', log.get('id', ''))
    timestamp = log.get('timestamp', log.get('created_at', ''))
    
    if node_id and timestamp:
        return f"{node_id}:{timestamp}"
    elif node_id:
        return f"node:{node_id}"
    elif timestamp:
        return f"ts:{timestamp}"
    else:
        return hashlib.md5(json.dumps(log, sort_keys=True, default=str).encode()).hexdigest()


def _merge_list_field(
    base_list: List,
    delta_list: List,
    strategy: str
) -> List:
    """
    ② 리스트 필드 병합 (원자성 보장)
    
    전략:
        - 'append': 단순 추가
        - 'replace': 교체
        - 'dedupe_append': 중복 제거 후 추가
        - 'set_union': 집합 합집합
    """
    if strategy == 'replace':
        return delta_list.copy()
    
    if strategy == 'append':
        return base_list + delta_list
    
    if strategy == 'dedupe_append':
        # 로그 중복 제거 (node_id + timestamp 기반)
        seen_keys = {_get_log_key(item) for item in base_list}
        unique_delta = [
            item for item in delta_list 
            if _get_log_key(item) not in seen_keys
        ]
        return base_list + unique_delta
    
    if strategy == 'set_union':
        # 문자열/숫자 집합
        result_set = set(base_list) | set(delta_list)
        return list(result_set)
    
    # 기본: append
    return base_list + delta_list


# 필수 메타데이터 기본값 (action='init' 전용)
INIT_REQUIRED_METADATA = {
    'segment_to_run': 0,
    'loop_counter': 0,
    'state_history': [],
    'max_loop_iterations': 100,
    'max_branch_iterations': 100,
    'distributed_mode': False,
    'distributed_strategy': 'SAFE',
    'max_concurrency': 1,
}


def merge_logic(
    base_state: Dict[str, Any],
    delta: Dict[str, Any],
    context: Optional[SyncContext] = None
) -> Dict[str, Any]:
    """
    🔀 상태 병합 (Shallow Merge + Copy-on-Write)
    
    규칙:
        1. 제어 필드는 delta 우선
        2. 히스토리는 dedupe_append (중복 제거 후 추가)
        3. 딕셔너리 필드는 shallow merge
        4. 리스트 필드는 context.merge_strategy에 따름
    
    성능:
        - deepcopy 대신 Copy-on-Write 사용
        - 변경되는 서브트리만 복사
    
    Special:
        - action='init': 빈 base_state에 필수 메타데이터 강제 주입
    
    🛡️ [v3.4] NEVER returns None - always returns dict
    """
    logger = _get_logger()
    
    # 🛡️ [v3.4 Deep Guard] None 방지 - Immutable Empty Dict
    if base_state is None:
        logger.warning("🚨 [Deep Guard] merge_logic received None base_state!")
        base_state = {}
    
    if delta is None:
        logger.debug("[Deep Guard] merge_logic received None delta, returning base_state")
        return base_state if base_state else {}
    
    if context is None:
        context = {'action': 'sync'}
    
    action = context.get('action', 'sync') if context else 'sync'
    
    # 탄생 (init): 필수 메타데이터 강제 주입
    if action == 'init':
        # 기본값 먼저 적용, 그 위에 delta 덩어쓰기
        base_with_defaults = INIT_REQUIRED_METADATA.copy()
        base_with_defaults.update(base_state)
        base_state = base_with_defaults
        logger.info(f"[Init] Injected required metadata: {list(INIT_REQUIRED_METADATA.keys())}")
    
    if not delta:
        return base_state
    
    # 변경될 필드 식별 (CoW용)
    fields_to_modify = set(delta.keys())
    if 'state_history' in delta or 'new_history_logs' in delta:
        fields_to_modify.add('state_history')
    
    # ① Copy-on-Write 방식 복사
    updated_state = _shallow_copy_with_cow(base_state, fields_to_modify)
    
    # merge_strategy 추출
    merge_strategy = (context.get('merge_strategy', {}) if context else {})
    
    for key, value in delta.items():
        # 제어 필드: 무조건 delta 우선
        if key in CONTROL_FIELDS_NEVER_OFFLOAD:
            updated_state[key] = value
            continue
        
        # new_history_logs → state_history로 병합
        if key == 'new_history_logs':
            existing = updated_state.get('state_history', [])
            strategy = LIST_FIELD_STRATEGIES.get('state_history', 'dedupe_append')
            updated_state['state_history'] = _merge_list_field(existing, value, strategy)
            continue
        
        # 기존 값 확인
        base_value = updated_state.get(key)
        
        # 리스트 필드
        if isinstance(value, list):
            if isinstance(base_value, list):
                strategy = LIST_FIELD_STRATEGIES.get(key, 'append')
                updated_state[key] = _merge_list_field(base_value, value, strategy)
            else:
                updated_state[key] = value.copy()
        
        # 딕셔너리 필드 (Shallow Merge)
        elif isinstance(value, dict):
            if isinstance(base_value, dict):
                # Shallow merge: delta 키가 base 키를 덮어씀
                merged = base_value.copy()
                merged.update(value)
                updated_state[key] = merged
            else:
                updated_state[key] = value.copy() if isinstance(value, dict) else value
        
        # 기타 타입 (문자열, 숫자 등)
        else:
            updated_state[key] = value
    
    return updated_state


def prevent_pointer_bloat(
    state: Dict[str, Any],
    idempotency_key: str
) -> Dict[str, Any]:
    """
    🔒 포인터 비대화 방지
    
    scheduling_metadata, failed_segments 등 포인터가 커질 수 있는
    필드를 간소화합니다.
    """
    logger = _get_logger()
    
    # failed_segments 오프로딩
    if 'failed_segments' in state:
        failed = state['failed_segments']
        if isinstance(failed, list) and len(failed) > 5:
            from .state_data_manager import store_to_s3, generate_s3_key
            try:
                s3_key = generate_s3_key(idempotency_key, 'failed_segments')
                s3_path = store_to_s3(failed, s3_key)
                state['failed_segments_s3_path'] = s3_path
                state['failed_segments'] = failed[:5]  # 샘플만
                logger.info(f"Offloaded {len(failed)} failed_segments to S3")
            except Exception as e:
                logger.warning(f"Failed to offload failed_segments: {e}")
    
    # current_state 내 scheduling_metadata 간소화
    if isinstance(state.get('current_state'), dict):
        current = state['current_state']
        if isinstance(current.get('scheduling_metadata'), dict):
            metadata = current['scheduling_metadata']
            batch_details = metadata.get('batch_details', [])
            if len(batch_details) > 5:
                current['scheduling_summary'] = {
                    'total_batches': len(batch_details),
                    'priority': metadata.get('priority', 1),
                    'total_items': sum(b.get('size', 0) for b in batch_details if isinstance(b, dict))
                }
                del current['scheduling_metadata']
                logger.info("Simplified scheduling_metadata to scheduling_summary")
    
    return state


def emergency_offload_large_arrays(
    state: Dict[str, Any],
    idempotency_key: str
) -> Dict[str, Any]:
    """
    🚨 응급 대용량 배열 오프로딩
    
    페이로드가 200KB의 75%를 초과할 때 호출됩니다.
    """
    logger = _get_logger()
    
    from .state_data_manager import store_to_s3, generate_s3_key
    
    # distributed_outputs 오프로딩
    if 'distributed_outputs' in state:
        outputs = state['distributed_outputs']
        if isinstance(outputs, list) and len(outputs) > 10:
            try:
                s3_key = generate_s3_key(idempotency_key, 'distributed_outputs')
                s3_path = store_to_s3(outputs, s3_key)
                state['distributed_outputs_s3_path'] = s3_path
                state['distributed_outputs'] = outputs[:10]  # 샘플 10개만
                logger.warning(f"Emergency offload: distributed_outputs ({len(outputs)} items)")
            except Exception as e:
                logger.error(f"Emergency offload failed: {e}")
    
    return state


def optimize_and_offload(
    state: Dict[str, Any],
    context: Optional[SyncContext] = None
) -> Dict[str, Any]:
    """
    🚀 통합 최적화 파이프라인 - P0~P2 자동 해결
    
    처리 순서:
        1. 히스토리 아카이빙 (>50 entries)
        2. 개별 필드 오프로딩 (>30KB)
        3. 전체 상태 오프로딩 (>100KB)
        4. 포인터 비대화 방지
        5. 최종 크기 체크 (>200KB 경고)
    
    🛡️ [v3.4] NEVER returns None - always returns dict
    """
    logger = _get_logger()
    
    # 🛡️ [v3.4 Deep Guard] None 방지
    if state is None:
        logger.warning("🚨 [Deep Guard] optimize_and_offload received None state!")
        state = {}
    
    if context is None:
        context = {'action': 'sync'}
    
    # state_data_manager의 기존 함수들 재사용
    from .state_data_manager import (
        optimize_state_history,
        optimize_current_state,
        calculate_payload_size as calc_size
    )
    
    idempotency_key = (
        context.get('idempotency_key') if context 
        else state.get('idempotency_key', 'unknown')
    )
    
    # 1. 히스토리 최적화
    if state.get('state_history'):
        optimized_history, _ = optimize_state_history(
            state['state_history'],
            idempotency_key=idempotency_key,
            max_entries=50
        )
        state['state_history'] = optimized_history
    
    # 2. current_state 최적화 (개별 필드 + 전체 상태)
    if state.get('current_state'):
        optimized_current, _ = optimize_current_state(
            state['current_state'],
            idempotency_key
        )
        state['current_state'] = optimized_current
    
    # 3. 포인터 비대화 방지
    state = prevent_pointer_bloat(state, idempotency_key)
    
    # 4. 최종 크기 체크
    final_size_kb = calc_size(state)
    warning_threshold = MAX_PAYLOAD_SIZE_KB * 0.75  # 150KB
    
    if final_size_kb > warning_threshold:
        logger.warning(f"Payload approaching limit: {final_size_kb}KB / {MAX_PAYLOAD_SIZE_KB}KB")
        state = emergency_offload_large_arrays(state, idempotency_key)
    
    # 메타데이터 업데이트
    state['payload_size_kb'] = calc_size(state)
    state['last_update_time'] = datetime.now(timezone.utc).isoformat()
    
    return state


def universal_sync_core(
    base_state: Dict[str, Any],
    new_result: Any,
    context: Optional[SyncContext] = None
) -> Dict[str, Any]:
    """
    🎯 Function-Agnostic 동기화 코어 (v3.2 Engine)
    
    모든 StateDataManager 액션이 이 함수를 통과합니다.
    9개의 액션 함수는 이제 "3줄짜리 래퍼"입니다.
    
    파이프라인:
        1. flatten_result() - 입력 정규화 (액션별 스마트 추출)
        2. merge_logic() - 상태 병합 (Shallow Merge + CoW)
        3. optimize_and_offload() - 자동 최적화 (P0~P2 해결)
        4. _compute_next_action() - next_action 결정
    
    Args:
        base_state: 기존 state_data
        new_result: 새로운 실행 결과 (단일 객체 or 리스트)
        context: 동기화 컨텍스트 (action, execution_id, merge_strategy 등)
    
    Returns:
        {
            'state_data': 최적화된 상태,
            'next_action': 'CONTINUE' | 'COMPLETE' | 'FAILED' | ...
        }
    """
    logger = _get_logger()
    
    # 🛡️ [v3.4 Deep Guard] None 방지 - Immutable Empty Dict 전략
    # 절대로 None이 파이프라인을 통과하지 못하게 함
    if base_state is None:
        logger.warning("🚨 [Deep Guard] base_state is None! Using empty dict.")
        base_state = {}
    
    if new_result is None:
        logger.warning("🚨 [Deep Guard] new_result is None! Using empty dict.")
        new_result = {}
    
    if context is None:
        context = {'action': 'sync'}
    
    action = context.get('action', 'sync') if context else 'sync'
    idempotency_key = base_state.get('idempotency_key', 'unknown') if isinstance(base_state, dict) else 'unknown'
    
    # 컨텍스트에 idempotency_key 추가
    if context:
        context['idempotency_key'] = idempotency_key
    else:
        context = {'action': action, 'idempotency_key': idempotency_key}
    
    logger.info(f"UniversalSyncCore v3.2: action={action}")
    
    # Step 1: 입력 정규화 (액션별 스마트 추출)
    normalized_delta = flatten_result(new_result, context)
    
    # Step 2: 상태 병합 (Shallow Merge + CoW)
    updated_state = merge_logic(base_state, normalized_delta, context)
    
    # 🔍 [Debug] Log loop_counter after merge for troubleshooting
    logger.info(f"[v3.14 Debug] After merge_logic: loop_counter={updated_state.get('loop_counter')}, "
               f"base_state.loop_counter={base_state.get('loop_counter') if isinstance(base_state, dict) else 'N/A'}")
    
    # Step 3: 공통 필드 업데이트 (루프 카운터, 세그먼트)
    # 🛡️ [v3.14 Fix] loop_counter 증가는 ASL IncrementLoopCounter에서만 수행
    # USC에서 중복 증가하면 무한 루프 방지 로직이 깨짐
    # should_increment_loop 로직 제거 - ASL이 loop_counter 증가 담당
    # 
    # REMOVED:
    # should_increment_loop = (action == 'sync' or normalized_delta.get('_increment_loop', False))
    # if should_increment_loop and action != 'init':
    #     updated_state['loop_counter'] = int(updated_state.get('loop_counter', 0)) + 1
    
    # 세그먼트 증가 (플래그가 있는 경우)
    if normalized_delta.get('_increment_segment', False):
        updated_state['segment_to_run'] = int(updated_state.get('segment_to_run', 0)) + 1
    
    # Step 4: 자동 최적화 (P0~P2 해결)
    optimized_state = optimize_and_offload(updated_state, context)
    
    # Step 5: next_action 결정
    next_action = _compute_next_action(optimized_state, normalized_delta, action)
    
    # pending_branches 정리 (aggregate_branches 완료 시)
    if action == 'aggregate_branches' and normalized_delta.get('_aggregation_complete'):
        optimized_state.pop('pending_branches', None)
        optimized_state['segment_to_run'] = int(optimized_state.get('segment_to_run', 0)) + 1
    
    logger.info(f"UniversalSyncCore complete: action={action}, next={next_action}, size={optimized_state.get('payload_size_kb', 0)}KB")
    
    return {
        'state_data': optimized_state,
        'next_action': next_action
    }


def _compute_next_action(
    state: Dict[str, Any],
    delta: Dict[str, Any],
    action: str
) -> str:
    """
    🎯 next_action 결정 로직 (중앙화)
    
    모든 액션의 next_action을 단일 로직으로 결정합니다.
    
    탄생 (init): 'STARTED' 반환
    
    🛡️ [v3.3] 타입 안전성 강화 - TypeError 방지
    """
    # 탄생 (init) - 시작 상태
    if action == 'init' or delta.get('_is_init'):
        return 'STARTED'
    
    # delta에서 상태 추출 (문자열 정규화)
    raw_status = delta.get('_status', 'CONTINUE')
    status = str(raw_status).upper() if raw_status is not None else 'CONTINUE'
    
    # 명시적 실패/중단 상태
    if status in ('FAILED', 'HALTED', 'SIGKILL'):
        return status
    
    # 명시적 완료
    if status == 'COMPLETE':
        return 'COMPLETE'
    
    # HITP 대기
    if status in ('PAUSED_FOR_HITP', 'PAUSE'):
        return 'PAUSED_FOR_HITP'
    
    # Distributed 전체 실패
    if delta.get('_aggregation_complete'):
        failed = delta.get('_failed_segments', [])
        chunk_summary = delta.get('distributed_chunk_summary')
        total = chunk_summary.get('total', 0) if isinstance(chunk_summary, dict) else 0
        if failed and len(failed) == total:
            return 'FAILED'
    
    # 다음 세그먼트 없으면 완료
    if delta.get('segment_to_run') is None and status == 'CONTINUE':
        # 🛡️ [Guard] 안전한 숫자 비교 - TypeError 방지
        try:
            current_segment = int(state.get('segment_to_run', 0) or 0)
            total_segments_raw = state.get('total_segments')
            
            if total_segments_raw is not None:
                total_segments = int(total_segments_raw)
                if current_segment >= total_segments:
                    return 'COMPLETE'
        except (ValueError, TypeError) as e:
            _get_logger().warning(
                f"[_compute_next_action] Invalid segment numbers: "
                f"segment_to_run={state.get('segment_to_run')}, "
                f"total_segments={state.get('total_segments')}. Error: {e}. Defaulting to CONTINUE."
            )
    
    return 'CONTINUE'


# ============================================
# Backward Compatibility - 기존 함수 래퍼
# ============================================

def load_from_s3_with_retry(
    s3_path: str,
    expected_checksum: Optional[str] = None
) -> Any:
    """
    기존 load_from_s3의 재시도 + 체크섬 검증 버전
    
    backward compatible 래퍼로, 기존 코드에서 drop-in replacement로 사용 가능
    """
    return get_default_hydrator().load_from_s3(s3_path, expected_checksum)
