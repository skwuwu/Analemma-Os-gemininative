import logging
import os
import time
import json
import random
from typing import Dict, Any, Optional, List, Tuple

# [v2.1] Centralized Retry Utility
try:
    from src.common.retry_utils import retry_call, retry_stepfunctions, retry_s3
    RETRY_UTILS_AVAILABLE = True
except ImportError:
    RETRY_UTILS_AVAILABLE = False

# 🛡️ [v2.2] Ring Protection: Prompt Security Guard
try:
    from src.services.recovery.prompt_security_guard import (
        PromptSecurityGuard,
        get_security_guard,
        RingLevel,
        SecurityViolation,
    )
    RING_PROTECTION_AVAILABLE = True
except ImportError:
    RING_PROTECTION_AVAILABLE = False
    get_security_guard = None
    RingLevel = None

# 🛡️ [v2.3] 4단계 아키텍처: Concurrency Controller
try:
    from src.services.quality_kernel.concurrency_controller import (
        ConcurrencyControllerV2,
        get_concurrency_controller,
        LoadLevel
    )
    CONCURRENCY_CONTROLLER_AVAILABLE = True
except ImportError:
    CONCURRENCY_CONTROLLER_AVAILABLE = False
    get_concurrency_controller = None
    ConcurrencyControllerV2 = None

# Services
from src.services.state.state_manager import StateManager, mask_pii_in_state
from src.services.recovery.self_healing_service import SelfHealingService
# Legacy Imports (for now, until further refactoring)
from src.services.workflow.repository import WorkflowRepository
# Using generic imports from main handler file as source of truth
from src.handlers.core.main import run_workflow, partition_workflow as _partition_workflow_dynamically, _build_segment_config
from src.common.statebag import normalize_inplace


logger = logging.getLogger(__name__)

# ============================================================================
# 🛡️ [Kernel] Dynamic Scheduling Constants
# ============================================================================
# Memory Safety Margin (Trigger split at 80% usage)
MEMORY_SAFETY_THRESHOLD = 0.8
# Minimum Node Count for Segment Splitting
MIN_NODES_PER_SUB_SEGMENT = 2
# Maximum Split Depth (Prevent infinite splitting)
MAX_SPLIT_DEPTH = 3
# Segment Status Values
SEGMENT_STATUS_PENDING = "PENDING"
SEGMENT_STATUS_RUNNING = "RUNNING"
SEGMENT_STATUS_COMPLETED = "COMPLETED"
SEGMENT_STATUS_SKIPPED = "SKIPPED"
SEGMENT_STATUS_FAILED = "FAILED"

# ============================================================================
# 🛡️ [Kernel] Aggressive Retry & Partial Success Constants
# ============================================================================
# Kernel Internal Retry Count (Attempt before Step Functions level retry)
KERNEL_MAX_RETRIES = 3
# Retry Interval (Exponential backoff base)
KERNEL_RETRY_BASE_DELAY = 1.0
# Retryable Error Patterns
RETRYABLE_ERROR_PATTERNS = [
    'ThrottlingException',
    'ServiceUnavailable',
    'TooManyRequestsException',
    'ProvisionedThroughputExceeded',
    'InternalServerError',
    'ConnectionError',
    'TimeoutError',
    'ReadTimeoutError',
    'ConnectTimeoutError',
    'BrokenPipeError',
    'ResourceNotFoundException',  # S3 eventual consistency
]
# Enable Partial Success (Continue workflow even if segment fails)
ENABLE_PARTIAL_SUCCESS = True

# ============================================================================
# 🔀 [Kernel] Parallel Scheduler Constants
# ============================================================================
# Default Concurrency Limit (Lambda account level)
DEFAULT_MAX_CONCURRENT_MEMORY_MB = 3072  # 3GB (Assuming 3 Lambda concurrent executions)
DEFAULT_MAX_CONCURRENT_TOKENS = 100000   # Tokens per minute limit
DEFAULT_MAX_CONCURRENT_BRANCHES = 10     # Maximum concurrent branches

# Scheduling Strategy
STRATEGY_SPEED_OPTIMIZED = "SPEED_OPTIMIZED"      # Maximize parallel execution
STRATEGY_RESOURCE_OPTIMIZED = "RESOURCE_OPTIMIZED" # Prioritize resource efficiency
STRATEGY_COST_OPTIMIZED = "COST_OPTIMIZED"        # Minimize cost

# Default estimated resources per branch
DEFAULT_BRANCH_MEMORY_MB = 256
DEFAULT_BRANCH_TOKENS = 5000

# Account level hard limit (checked even in SPEED_OPTIMIZED)
ACCOUNT_LAMBDA_CONCURRENCY_LIMIT = 100  # AWS default concurrency limit
ACCOUNT_MEMORY_HARD_LIMIT_MB = 10240    # 10GB hard limit

# State merge policy
MERGE_POLICY_OVERWRITE = "OVERWRITE"      # Later values overwrite (default)
MERGE_POLICY_APPEND_LIST = "APPEND_LIST"  # Lists are merged
MERGE_POLICY_KEEP_FIRST = "KEEP_FIRST"    # Keep first value
MERGE_POLICY_CONFLICT_ERROR = "ERROR"     # Error on conflict

# Key patterns requiring list merge
LIST_MERGE_KEY_PATTERNS = [
    '__new_history_logs',
    '__kernel_actions', 
    '_results',
    '_items',
    '_outputs',
    'collected_',
    'aggregated_'
]


class SegmentRunnerService:
    def __init__(self, s3_bucket: Optional[str] = None):
        self.state_manager = StateManager()
        self.healer = SelfHealingService()
        self.repo = WorkflowRepository()
        
        # [Perf Optimization] Safe threshold - 180KB for Step Functions with wrapper overhead buffer
        # 256KB SF limit - ~15KB AWS wrapper overhead = ~175KB safe, using 180KB
        SF_SAFE_THRESHOLD = 180000
        
        threshold_str = os.environ.get("STATE_SIZE_THRESHOLD", "")
        if threshold_str and threshold_str.strip():
            try:
                self.threshold = int(threshold_str.strip())
                # Warn if threshold is too high
                if self.threshold > SF_SAFE_THRESHOLD:
                    logger.warning(f"⚠️ STATE_SIZE_THRESHOLD={self.threshold} exceeds safe limit {SF_SAFE_THRESHOLD}")
            except ValueError:
                logger.warning(f"⚠️ Invalid STATE_SIZE_THRESHOLD='{threshold_str}', using default {SF_SAFE_THRESHOLD}")
                self.threshold = SF_SAFE_THRESHOLD
        else:
            self.threshold = SF_SAFE_THRESHOLD
        
        # 🛡️ [v2.5] S3 Bucket - 핸들러에서 주입받거나 환경변수 폴백
        if s3_bucket and s3_bucket.strip():
            self.s3_bucket = s3_bucket.strip()
        else:
            env_bucket = os.environ.get("S3_BUCKET") or os.environ.get("SKELETON_S3_BUCKET") or ""
            self.s3_bucket = env_bucket.strip() if env_bucket else ""
        
        if not self.s3_bucket:
            logger.warning("⚠️ [SegmentRunnerService] S3 bucket not configured - large payloads may fail")
        else:
            logger.info(f"✅ [SegmentRunnerService] S3 bucket: {self.s3_bucket}, threshold: {self.threshold}")
        
        # 🛡️ [Kernel] S3 Client (Lazy Initialization)
        self._s3_client = None
        
        # 🛡️ [v2.2] Ring Protection Security Guard
        self._security_guard = None
        
        # 🛡️ [v2.3] 4단계 아키텍처: Concurrency Controller
        self._concurrency_controller = None
    
    @property
    def security_guard(self):
        """Lazy Security Guard initialization"""
        if self._security_guard is None and RING_PROTECTION_AVAILABLE:
            self._security_guard = get_security_guard()
        return self._security_guard
    
    @property
    def concurrency_controller(self):
        """Lazy Concurrency Controller initialization"""
        if self._concurrency_controller is None and CONCURRENCY_CONTROLLER_AVAILABLE:
            # Reserved Concurrency 200 (template.yaml에서 설정)
            reserved = int(os.environ.get('RESERVED_CONCURRENCY', 200))
            max_budget = float(os.environ.get('MAX_BUDGET_USD', 10.0))
            self._concurrency_controller = get_concurrency_controller(
                workflow_id="segment_runner",
                reserved_concurrency=reserved,
                max_budget_usd=max_budget,
                enable_batching=True,
                enable_throttling=True
            )
        return self._concurrency_controller
    
    @property
    def s3_client(self):
        """Lazy S3 client initialization"""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client('s3')
        return self._s3_client

    # ========================================================================
    # � [Utility] State Merge: 무결성 보장 상태 병합
    # ========================================================================
    def _should_merge_as_list(self, key: str) -> bool:
        """
        Check if this key is a list merge target
        """
        for pattern in LIST_MERGE_KEY_PATTERNS:
            if pattern in key or key.startswith(pattern):
                return True
        return False

    def _merge_states(
        self,
        base_state: Dict[str, Any],
        new_state: Dict[str, Any],
        merge_policy: str = MERGE_POLICY_APPEND_LIST
    ) -> Dict[str, Any]:
        """
        🔧 Integrity-guaranteed state merging
        
        Policy:
        - OVERWRITE: Simple overwrite (existing behavior)
        - APPEND_LIST: Merge list keys, overwrite others
        - KEEP_FIRST: Keep existing keys
        - ERROR: Raise exception on key conflict
        
        Special handling:
        - __new_history_logs, __kernel_actions, etc. always merge as lists
        - Keys starting with _ are treated specially
        """
        if merge_policy == MERGE_POLICY_OVERWRITE:
            result = base_state.copy()
            result.update(new_state)
            return result
        
        result = base_state.copy()
        conflicts = []
        
        for key, new_value in new_state.items():
            if key not in result:
                # New key: just add
                result[key] = new_value
                continue
            
            existing_value = result[key]
            
            # Check if key is list merge target
            if self._should_merge_as_list(key):
                if isinstance(existing_value, list) and isinstance(new_value, list):
                    result[key] = existing_value + new_value
                elif isinstance(new_value, list):
                    result[key] = [existing_value] + new_value if existing_value else new_value
                elif isinstance(existing_value, list):
                    result[key] = existing_value + [new_value] if new_value else existing_value
                else:
                    result[key] = [existing_value, new_value]
                continue
            
            # Handle according to policy
            if merge_policy == MERGE_POLICY_KEEP_FIRST:
                # Keep existing value
                continue
            elif merge_policy == MERGE_POLICY_CONFLICT_ERROR:
                if existing_value != new_value:
                    conflicts.append(key)
            else:
                # APPEND_LIST default: overwrite if not list
                result[key] = new_value
        
        if conflicts:
            logger.warning(f"[Merge] State conflicts detected on keys: {conflicts}")
            if merge_policy == MERGE_POLICY_CONFLICT_ERROR:
                raise ValueError(f"State merge conflict on keys: {conflicts}")
        
        return result

    # ========================================================================
    # �🛡️ [Pattern 1] Segment-Level Self-Healing: 세그먼트 내부 동적 분할
    # ========================================================================
    def _estimate_segment_memory(self, segment_config: Dict[str, Any], state: Dict[str, Any]) -> int:
        """
        Estimate memory required for segment execution (in MB)
        
        [Optimization] Use metadata-based heuristics instead of json.dumps
        - json.dumps itself is a memory burden for large data
        - Lightweight estimation using list length, string key presence, etc.
        
        Estimation criteria:
        - Node count × base memory (10MB)
        - LLM node: additional 50MB
        - for_each node: item count × 5MB
        - State size: metadata-based estimation
        """
        base_memory = 50  # base overhead
        
        nodes = segment_config.get('nodes', [])
        if not nodes:
            return base_memory
        
        node_memory = len(nodes) * 10  # 10MB per node
        
        llm_memory = 0
        foreach_memory = 0
        
        for node in nodes:
            node_type = node.get('type', '')
            if node_type in ('llm_chat', 'aiModel'):
                llm_memory += 50  # LLM nodes get additional 50MB
            elif node_type == 'for_each':
                config = node.get('config', {})
                items_key = config.get('input_list_key', '')
                if items_key and items_key in state:
                    items = state.get(items_key, [])
                    if isinstance(items, list):
                        foreach_memory += len(items) * 5
        
        # [Optimization] State size estimation based on metadata (avoid json.dumps)
        state_size_mb = self._estimate_state_size_lightweight(state)
        
        total = base_memory + node_memory + llm_memory + foreach_memory + int(state_size_mb)
        
        logger.debug(f"[Kernel] Memory estimate: base={base_memory}, nodes={node_memory}, "
                    f"llm={llm_memory}, foreach={foreach_memory}, state={state_size_mb:.1f}MB, total={total}MB")
        
        return total

    def _estimate_state_size_lightweight(self, state: Dict[str, Any], max_sample_keys: int = 20) -> float:
        """
        [Optimization] Lightweight estimation of state size without json.dumps
        
        Strategy:
        1. Sample only top N keys to calculate average size
        2. Estimate lists as length × average item size
        3. Use len() for strings
        4. Estimate nested dicts by key count
        
        Returns:
            Estimated size (MB)
        """
        if not state or not isinstance(state, dict):
            return 0.1  # minimum 100KB
        
        total_bytes = 0
        keys = list(state.keys())[:max_sample_keys]
        
        for key in keys:
            value = state.get(key)
            total_bytes += self._estimate_value_size(value)
        
        # Estimate total size based on sampling ratio
        if len(state) > max_sample_keys:
            sample_ratio = len(state) / max_sample_keys
            total_bytes = int(total_bytes * sample_ratio)
        
        return total_bytes / (1024 * 1024)  # bytes → MB

    def _estimate_value_size(self, value: Any, depth: int = 0) -> int:
        """
        Heuristically estimate value size (bytes)
        
        Prevent infinite loops with recursion depth limit
        """
        if depth > 3:  # depth limit
            return 100  # approximate estimate
        
        if value is None:
            return 4
        elif isinstance(value, bool):
            return 4
        elif isinstance(value, (int, float)):
            return 8
        elif isinstance(value, str):
            return len(value.encode('utf-8', errors='ignore'))
        elif isinstance(value, bytes):
            return len(value)
        elif isinstance(value, list):
            if not value:
                return 2
            # Sample only first 3 items to calculate average
            sample = value[:3]
            avg_size = sum(self._estimate_value_size(v, depth + 1) for v in sample) / len(sample)
            return int(avg_size * len(value))
        elif isinstance(value, dict):
            if not value:
                return 2
            # Sample only first 5 keys
            sample_keys = list(value.keys())[:5]
            sample_size = sum(
                len(str(k)) + self._estimate_value_size(value[k], depth + 1) 
                for k in sample_keys
            )
            if len(value) > 5:
                return int(sample_size * len(value) / 5)
            return sample_size
        else:
            # Other types: approximate estimate
            return 100

    def _split_segment(self, segment_config: Dict[str, Any], split_depth: int = 0) -> List[Dict[str, Any]]:
        """
        Split segment into smaller sub-segments
        
        Splitting strategy:
        1. Split node list in half
        2. Maintain dependencies: preserve edge connections
        3. 최소 노드 수 보장
        """
        if split_depth >= MAX_SPLIT_DEPTH:
            logger.warning(f"[Kernel] Max split depth ({MAX_SPLIT_DEPTH}) reached, returning original segment")
            return [segment_config]
        
        nodes = segment_config.get('nodes', [])
        edges = segment_config.get('edges', [])
        
        if len(nodes) < MIN_NODES_PER_SUB_SEGMENT * 2:
            logger.info(f"[Kernel] Segment too small to split ({len(nodes)} nodes)")
            return [segment_config]
        
        # 노드를 반으로 분할
        mid = len(nodes) // 2
        first_nodes = nodes[:mid]
        second_nodes = nodes[mid:]
        
        first_node_ids = {n.get('id') for n in first_nodes}
        second_node_ids = {n.get('id') for n in second_nodes}
        
        # 엣지 분리: 각 서브 세그먼트 내부 엣지만 유지
        first_edges = [e for e in edges 
                      if e.get('source') in first_node_ids and e.get('target') in first_node_ids]
        second_edges = [e for e in edges 
                       if e.get('source') in second_node_ids and e.get('target') in second_node_ids]
        
        # 서브 세그먼트 생성
        original_id = segment_config.get('id', 'segment')
        
        sub_segment_1 = {
            **segment_config,
            'id': f"{original_id}_sub_1",
            'nodes': first_nodes,
            'edges': first_edges,
            '_kernel_split': True,
            '_split_depth': split_depth + 1,
            '_parent_segment_id': original_id
        }
        
        sub_segment_2 = {
            **segment_config,
            'id': f"{original_id}_sub_2",
            'nodes': second_nodes,
            'edges': second_edges,
            '_kernel_split': True,
            '_split_depth': split_depth + 1,
            '_parent_segment_id': original_id
        }
        
        logger.info(f"[Kernel] 🔧 Segment '{original_id}' split into 2 sub-segments: "
                   f"{len(first_nodes)} + {len(second_nodes)} nodes")
        
        return [sub_segment_1, sub_segment_2]

    def _execute_with_auto_split(
        self, 
        segment_config: Dict[str, Any], 
        initial_state: Dict[str, Any],
        auth_user_id: str,
        split_depth: int = 0
    ) -> Dict[str, Any]:
        """
        🛡️ [Pattern 1] 메모리 기반 자동 분할 실행
        
        메모리 부족이 예상되면 세그먼트를 분할하여 순차 실행
        """
        # 사용 가능한 Lambda 메모리
        available_memory = int(os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', 512))
        
        # 메모리 요구량 추정
        estimated_memory = self._estimate_segment_memory(segment_config, initial_state)
        
        # 안전 임계값 체크
        if estimated_memory > available_memory * MEMORY_SAFETY_THRESHOLD:
            logger.info(f"[Kernel] ⚠️ Memory pressure detected: {estimated_memory}MB estimated, "
                       f"{available_memory}MB available (threshold: {MEMORY_SAFETY_THRESHOLD*100}%)")
            
            # 분할 시도
            sub_segments = self._split_segment(segment_config, split_depth)
            
            if len(sub_segments) > 1:
                logger.info(f"[Kernel] 🔧 Executing {len(sub_segments)} sub-segments sequentially")
                
                # 서브 세그먼트 순차 실행
                current_state = initial_state.copy()
                all_logs = []
                kernel_actions = []
                
                for i, sub_seg in enumerate(sub_segments):
                    logger.info(f"[Kernel] Executing sub-segment {i+1}/{len(sub_segments)}: {sub_seg.get('id')}")
                    
                    # 재귀적으로 자동 분할 적용
                    sub_result = self._execute_with_auto_split(
                        sub_seg, current_state, auth_user_id, split_depth + 1
                    )
                    
                    # 🔧 무결성 보장 상태 병합 (리스트 키는 합침)
                    if isinstance(sub_result, dict):
                        current_state = self._merge_states(
                            current_state, 
                            sub_result,
                            merge_policy=MERGE_POLICY_APPEND_LIST
                        )
                        # all_logs는 이미 _merge_states에서 처리됨
                    
                    kernel_actions.append({
                        'action': 'SPLIT_EXECUTE',
                        'sub_segment_id': sub_seg.get('id'),
                        'index': i,
                        'timestamp': time.time()
                    })
                
                # 커널 메타데이터 추가
                current_state['__kernel_actions'] = kernel_actions
                current_state['__new_history_logs'] = all_logs
                
                return current_state
        
        # 정상 실행 (분할 불필요)
        return run_workflow(
            config_json=segment_config,
            initial_state=initial_state,
            ddb_table_name=os.environ.get("JOB_TABLE"),
            user_api_keys={},
            run_config={"user_id": auth_user_id}
        )

    # ========================================================================
    # 🛡️ [Pattern 2] Manifest Mutation: S3 Manifest 동적 수정
    # ========================================================================
    def _load_manifest_from_s3(self, manifest_s3_path: str) -> Optional[List[Dict[str, Any]]]:
        """S3에서 segment_manifest 로드"""
        if not manifest_s3_path or not manifest_s3_path.startswith('s3://'):
            return None
        
        try:
            parts = manifest_s3_path.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ''
            
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            manifest = json.loads(response['Body'].read().decode('utf-8'))
            
            logger.info(f"[Kernel] Loaded manifest from S3: {len(manifest)} segments")
            return manifest
            
        except Exception as e:
            logger.error(f"[Kernel] Failed to load manifest from S3: {e}")
            return None

    def _save_manifest_to_s3(self, manifest: List[Dict[str, Any]], manifest_s3_path: str) -> bool:
        """수정된 segment_manifest를 S3에 저장"""
        if not manifest_s3_path or not manifest_s3_path.startswith('s3://'):
            return False
        
        try:
            parts = manifest_s3_path.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ''
            
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(manifest, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'kernel_modified': 'true',
                    'modified_at': str(int(time.time()))
                }
            )
            
            logger.info(f"[Kernel] Saved modified manifest to S3: {len(manifest)} segments")
            return True
            
        except Exception as e:
            logger.error(f"[Kernel] Failed to save manifest to S3: {e}")
            return False

    def _check_segment_status(self, segment_config: Dict[str, Any]) -> str:
        """세그먼트 상태 확인 (SKIPPED 등)"""
        return segment_config.get('status', SEGMENT_STATUS_PENDING)

    def _mark_segments_for_skip(
        self, 
        manifest_s3_path: str, 
        segment_ids_to_skip: List[int], 
        reason: str
    ) -> bool:
        """
        🛡️ [Pattern 2] 특정 세그먼트를 SKIP으로 마킹
        
        사용 시나리오:
        - 조건 분기에서 특정 경로 불필요
        - 선행 세그먼트 실패로 후속 세그먼트 실행 불가
        """
        manifest = self._load_manifest_from_s3(manifest_s3_path)
        if not manifest:
            return False
        
        modified = False
        for segment in manifest:
            if segment.get('segment_id') in segment_ids_to_skip:
                segment['status'] = SEGMENT_STATUS_SKIPPED
                segment['skip_reason'] = reason
                segment['skipped_at'] = int(time.time())
                segment['skipped_by'] = 'kernel'
                modified = True
                logger.info(f"[Kernel] Marked segment {segment.get('segment_id')} for skip: {reason}")
        
        if modified:
            return self._save_manifest_to_s3(manifest, manifest_s3_path)
        
        return False

    def _inject_recovery_segments(
        self,
        manifest_s3_path: str,
        after_segment_id: int,
        recovery_segments: List[Dict[str, Any]],
        reason: str
    ) -> bool:
        """
        🛡️ [Pattern 2] 복구 세그먼트 삽입
        
        사용 시나리오:
        - API 실패 후 백업 경로 삽입
        - 에러 핸들링 세그먼트 동적 추가
        """
        manifest = self._load_manifest_from_s3(manifest_s3_path)
        if not manifest:
            return False
        
        # 삽입 위치 찾기
        insert_index = None
        for i, segment in enumerate(manifest):
            if segment.get('segment_id') == after_segment_id:
                insert_index = i + 1
                break
        
        if insert_index is None:
            logger.warning(f"[Kernel] Could not find segment {after_segment_id} for recovery injection")
            return False
        
        # 복구 세그먼트에 메타데이터 추가
        max_segment_id = max(s.get('segment_id', 0) for s in manifest)
        for i, rec_seg in enumerate(recovery_segments):
            rec_seg['segment_id'] = max_segment_id + i + 1
            rec_seg['status'] = SEGMENT_STATUS_PENDING
            rec_seg['injected_by'] = 'kernel'
            rec_seg['injection_reason'] = reason
            rec_seg['injected_at'] = int(time.time())
            rec_seg['type'] = rec_seg.get('type', 'recovery')
        
        # 매니페스트에 삽입
        new_manifest = manifest[:insert_index] + recovery_segments + manifest[insert_index:]
        
        # 후속 세그먼트 ID 재조정
        for i, segment in enumerate(new_manifest):
            segment['execution_order'] = i
        
        logger.info(f"[Kernel] 🔧 Injected {len(recovery_segments)} recovery segments after segment {after_segment_id}")
        
        return self._save_manifest_to_s3(new_manifest, manifest_s3_path)

    # ========================================================================
    # 🔀 [Pattern 3] Parallel Scheduler: 인프라 인지형 병렬 스케줄링
    # ========================================================================
    def _estimate_branch_resources(self, branch: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, int]:
        """
        브랜치의 예상 자원 요구량 추정
        
        Returns:
            {
                'memory_mb': 예상 메모리 (MB),
                'tokens': 예상 토큰 수,
                'llm_calls': LLM 호출 횟수,
                'has_shared_resource': 공유 자원 접근 여부
            }
        """
        nodes = branch.get('nodes', [])
        if not nodes:
            return {
                'memory_mb': DEFAULT_BRANCH_MEMORY_MB,
                'tokens': 0,
                'llm_calls': 0,
                'has_shared_resource': False
            }
        
        memory_mb = 50  # 기본 오버헤드
        tokens = 0
        llm_calls = 0
        has_shared_resource = False
        
        for node in nodes:
            node_type = node.get('type', '')
            config = node.get('config', {})
            
            # 메모리 추정
            memory_mb += 10  # 노드당 기본 10MB
            
            if node_type in ('llm_chat', 'aiModel'):
                memory_mb += 50  # LLM 노드 추가 메모리
                llm_calls += 1
                # 토큰 추정: 프롬프트 길이 기반
                prompt = config.get('prompt', '') or config.get('system_prompt', '')
                tokens += len(prompt) // 4 + 500  # 대략적 토큰 추정 + 응답 예상
                
            elif node_type == 'for_each':
                items_key = config.get('input_list_key', '')
                if items_key and items_key in state:
                    items = state.get(items_key, [])
                    if isinstance(items, list):
                        memory_mb += len(items) * 5
                        # for_each 내부에 LLM이 있으면 토큰 폭증
                        sub_nodes = config.get('sub_node_config', {}).get('nodes', [])
                        for sub_node in sub_nodes:
                            if sub_node.get('type') in ('llm_chat', 'aiModel'):
                                tokens += len(items) * 1000  # 아이템당 1000 토큰 예상
                                llm_calls += len(items)
            
            # 공유 자원 접근 감지
            if node_type in ('db_write', 's3_write', 'api_call'):
                has_shared_resource = True
            if config.get('write_to_db') or config.get('write_to_s3'):
                has_shared_resource = True
        
        return {
            'memory_mb': memory_mb,
            'tokens': tokens,
            'llm_calls': llm_calls,
            'has_shared_resource': has_shared_resource
        }

    def _bin_pack_branches(
        self,
        branches: List[Dict[str, Any]],
        resource_estimates: List[Dict[str, int]],
        resource_policy: Dict[str, Any]
    ) -> List[List[Dict[str, Any]]]:
        """
        🎯 Bin Packing 알고리즘: 브랜치를 실행 배치로 그룹화
        
        전략:
        1. 무거운 브랜치 먼저 배치 (First Fit Decreasing)
        2. 각 배치의 총 자원이 제한을 초과하지 않도록 구성
        3. 공유 자원 접근 브랜치는 별도 배치
        
        Returns:
            [[batch1_branches], [batch2_branches], ...]
        """
        max_memory = resource_policy.get('max_concurrent_memory_mb', DEFAULT_MAX_CONCURRENT_MEMORY_MB)
        max_tokens = resource_policy.get('max_concurrent_tokens', DEFAULT_MAX_CONCURRENT_TOKENS)
        max_branches = resource_policy.get('max_concurrent_branches', DEFAULT_MAX_CONCURRENT_BRANCHES)
        strategy = resource_policy.get('strategy', STRATEGY_RESOURCE_OPTIMIZED)
        
        # 브랜치와 자원 추정치 결합 후 크기순 정렬 (내림차순)
        indexed_branches = list(zip(branches, resource_estimates, range(len(branches))))
        
        # 전략에 따른 정렬 기준
        if strategy == STRATEGY_COST_OPTIMIZED:
            # 토큰 많은 것 먼저 (비용이 큰 작업 순차 처리)
            indexed_branches.sort(key=lambda x: x[1]['tokens'], reverse=True)
        else:
            # 메모리 많은 것 먼저 (기본)
            indexed_branches.sort(key=lambda x: x[1]['memory_mb'], reverse=True)
        
        # 공유 자원 접근 브랜치 분리
        shared_resource_branches = []
        normal_branches = []
        
        for branch, estimate, idx in indexed_branches:
            if estimate['has_shared_resource']:
                shared_resource_branches.append((branch, estimate, idx))
            else:
                normal_branches.append((branch, estimate, idx))
        
        # Bin Packing (First Fit Decreasing)
        batches: List[List[Tuple]] = []
        batch_resources: List[Dict[str, int]] = []
        
        for branch, estimate, idx in normal_branches:
            placed = False
            
            for i, batch in enumerate(batches):
                current = batch_resources[i]
                
                # 이 배치에 추가 가능한지 확인
                new_memory = current['memory_mb'] + estimate['memory_mb']
                new_tokens = current['tokens'] + estimate['tokens']
                new_count = len(batch) + 1
                
                if (new_memory <= max_memory and 
                    new_tokens <= max_tokens and 
                    new_count <= max_branches):
                    
                    batch.append((branch, estimate, idx))
                    batch_resources[i] = {
                        'memory_mb': new_memory,
                        'tokens': new_tokens
                    }
                    placed = True
                    break
            
            if not placed:
                # 새 배치 생성
                batches.append([(branch, estimate, idx)])
                batch_resources.append({
                    'memory_mb': estimate['memory_mb'],
                    'tokens': estimate['tokens']
                })
        
        # 공유 자원 브랜치는 각각 별도 배치 (Race Condition 방지)
        for branch, estimate, idx in shared_resource_branches:
            batches.append([(branch, estimate, idx)])
            batch_resources.append({
                'memory_mb': estimate['memory_mb'],
                'tokens': estimate['tokens']
            })
        
        # 결과 변환: 브랜치만 추출
        result = []
        for batch in batches:
            result.append([item[0] for item in batch])
        
        return result

    def _schedule_parallel_group(
        self,
        segment_config: Dict[str, Any],
        state: Dict[str, Any],
        segment_id: int
    ) -> Dict[str, Any]:
        """
        🔀 병렬 그룹 스케줄링: resource_policy에 따라 실행 배치 결정
        
        Returns:
            {
                'status': 'PARALLEL_GROUP' | 'SCHEDULED_PARALLEL',
                'branches': [...] (원본 또는 스케줄된 배치),
                'execution_batches': [[...], [...]] (배치 구조),
                'scheduling_metadata': {...}
            }
        """
        branches = segment_config.get('branches', [])
        resource_policy = segment_config.get('resource_policy', {})
        
        # resource_policy가 없으면 기본 병렬 실행
        if not resource_policy:
            logger.info(f"[Scheduler] No resource_policy, using default parallel execution for {len(branches)} branches")
            return {
                'status': 'PARALLEL_GROUP',
                'branches': branches,
                'execution_batches': [branches],  # 단일 배치
                'scheduling_metadata': {
                    'strategy': 'DEFAULT',
                    'total_branches': len(branches),
                    'batch_count': 1
                }
            }
        
        strategy = resource_policy.get('strategy', STRATEGY_RESOURCE_OPTIMIZED)
        
        # SPEED_OPTIMIZED: 가드레일 체크 후 최대 병렬 실행
        if strategy == STRATEGY_SPEED_OPTIMIZED:
            # 🛡️ 계정 수준 하드 리밋 체크 (시스템 패닉 방지)
            if len(branches) > ACCOUNT_LAMBDA_CONCURRENCY_LIMIT:
                logger.warning(f"[Scheduler] ⚠️ SPEED_OPTIMIZED but branch count ({len(branches)}) "
                              f"exceeds account concurrency limit ({ACCOUNT_LAMBDA_CONCURRENCY_LIMIT})")
                # 하드 리밋 적용하여 배치 분할
                forced_policy = {
                    'max_concurrent_branches': ACCOUNT_LAMBDA_CONCURRENCY_LIMIT,
                    'max_concurrent_memory_mb': ACCOUNT_MEMORY_HARD_LIMIT_MB,
                    'strategy': STRATEGY_SPEED_OPTIMIZED
                }
                # 자원 추정 및 배치 분할
                resource_estimates = [self._estimate_branch_resources(b, state) for b in branches]
                execution_batches = self._bin_pack_branches(branches, resource_estimates, forced_policy)
                
                logger.info(f"[Scheduler] 🛡️ Guardrail applied: {len(execution_batches)} batches")
                return {
                    'status': 'SCHEDULED_PARALLEL',
                    'branches': branches,
                    'execution_batches': execution_batches,
                    'scheduling_metadata': {
                        'strategy': strategy,
                        'total_branches': len(branches),
                        'batch_count': len(execution_batches),
                        'guardrail_applied': True,
                        'reason': 'Account concurrency limit exceeded'
                    }
                }
            
            logger.info(f"[Scheduler] SPEED_OPTIMIZED: All {len(branches)} branches in parallel")
            return {
                'status': 'PARALLEL_GROUP',
                'branches': branches,
                'execution_batches': [branches],
                'scheduling_metadata': {
                    'strategy': strategy,
                    'total_branches': len(branches),
                    'batch_count': 1,
                    'guardrail_applied': False
                }
            }
        
        # 자원 추정
        resource_estimates = []
        total_memory = 0
        total_tokens = 0
        
        for branch in branches:
            estimate = self._estimate_branch_resources(branch, state)
            resource_estimates.append(estimate)
            total_memory += estimate['memory_mb']
            total_tokens += estimate['tokens']
        
        logger.info(f"[Scheduler] Resource estimates: {total_memory}MB memory, {total_tokens} tokens, "
                   f"{len(branches)} branches")
        
        # 제한 확인
        max_memory = resource_policy.get('max_concurrent_memory_mb', DEFAULT_MAX_CONCURRENT_MEMORY_MB)
        max_tokens = resource_policy.get('max_concurrent_tokens', DEFAULT_MAX_CONCURRENT_TOKENS)
        
        # 제한 내라면 단일 배치
        if total_memory <= max_memory and total_tokens <= max_tokens:
            logger.info(f"[Scheduler] Resources within limits, single batch execution")
            return {
                'status': 'PARALLEL_GROUP',
                'branches': branches,
                'execution_batches': [branches],
                'scheduling_metadata': {
                    'strategy': strategy,
                    'total_branches': len(branches),
                    'batch_count': 1,
                    'total_memory_mb': total_memory,
                    'total_tokens': total_tokens
                }
            }
        
        # Bin Packing으로 배치 생성
        execution_batches = self._bin_pack_branches(branches, resource_estimates, resource_policy)
        
        logger.info(f"[Scheduler] 🔧 Created {len(execution_batches)} execution batches from {len(branches)} branches")
        for i, batch in enumerate(execution_batches):
            batch_memory = sum(self._estimate_branch_resources(b, state)['memory_mb'] for b in batch)
            logger.info(f"[Scheduler]   Batch {i+1}: {len(batch)} branches, ~{batch_memory}MB")
        
        return {
            'status': 'SCHEDULED_PARALLEL',
            'branches': branches,
            'execution_batches': execution_batches,
            'scheduling_metadata': {
                'strategy': strategy,
                'total_branches': len(branches),
                'batch_count': len(execution_batches),
                'total_memory_mb': total_memory,
                'total_tokens': total_tokens,
                'resource_policy': resource_policy
            }
        }

    # ========================================================================
    # 🔀 [Aggregator] 병렬 브랜치 결과 집계
    # ========================================================================
    def _handle_aggregator(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        병렬 브랜치 실행 결과를 집계하여 단일 상태로 병합
        
        ASL의 AggregateParallelResults에서 호출됨:
        - parallel_results: 각 브랜치의 실행 결과 배열
        - current_state: 병렬 실행 전 상태
        - map_error: (선택) Map 전체 실패 시 에러 정보
        
        Returns:
            병합된 최종 상태 + 다음 세그먼트 정보
        """
        parallel_results = event.get('parallel_results', [])
        base_state = event.get('current_state', {})
        segment_to_run = event.get('segment_to_run', 0)
        workflow_id = event.get('workflowId') or event.get('workflow_id')
        auth_user_id = event.get('ownerId') or event.get('owner_id')
        map_error = event.get('map_error')  # 🛡️ Map 전체 에러 정보
        
        logger.info(f"[Aggregator] 🔀 Aggregating {len(parallel_results)} branch results"
                   + (f" (map_error present)" if map_error else ""))
        
        # 1. 모든 브랜치 결과 병합
        aggregated_state = base_state.copy()
        all_history_logs = []
        branch_errors = []
        successful_branches = 0
        
        # 🛡️ Map 에러가 있으면 기록
        if map_error:
            branch_errors.append({
                'branch_id': '__MAP_ERROR__',
                'error': map_error
            })
            logger.warning(f"[Aggregator] ⚠️ Map execution failed: {map_error}")
        
        for i, branch_result in enumerate(parallel_results):
            if not isinstance(branch_result, dict):
                logger.warning(f"[Aggregator] Branch {i} result is not a dict: {type(branch_result)}")
                continue
            
            branch_id = branch_result.get('branch_id', f'branch_{i}')
            branch_status = branch_result.get('branch_status', 'UNKNOWN')
            branch_state = branch_result.get('final_state', {})
            branch_logs = branch_result.get('new_history_logs', [])
            error_info = branch_result.get('error_info')
            
            logger.info(f"[Aggregator] Branch {branch_id}: status={branch_status}")
            
            # 에러 수집 (부분 실패 지원)
            if error_info:
                branch_errors.append({
                    'branch_id': branch_id,
                    'error': error_info
                })
            
            if branch_status in ('COMPLETE', 'SUCCEEDED'):
                successful_branches += 1
            
            # 상태 병합 (리스트 키는 합침)
            if isinstance(branch_state, dict):
                aggregated_state = self._merge_states(
                    aggregated_state,
                    branch_state,
                    merge_policy=MERGE_POLICY_APPEND_LIST
                )
            
            # 히스토리 로그 수집
            if isinstance(branch_logs, list):
                all_history_logs.extend(branch_logs)
        
        # 2. 집계 메타데이터 추가
        aggregated_state['__aggregator_metadata'] = {
            'total_branches': len(parallel_results),
            'successful_branches': successful_branches,
            'failed_branches': len(branch_errors),
            'aggregated_at': time.time()
        }
        
        if branch_errors:
            aggregated_state['__branch_errors'] = branch_errors
        
        # 3. 상태 저장 (S3 오프로딩 포함)
        s3_bucket_raw = os.environ.get("S3_BUCKET") or os.environ.get("SKELETON_S3_BUCKET") or ""
        s3_bucket = s3_bucket_raw.strip() if s3_bucket_raw else None
        
        if not s3_bucket:
            logger.error("🚨 [CRITICAL] S3_BUCKET/SKELETON_S3_BUCKET not set for aggregation!")
        
        final_state, output_s3_path = self.state_manager.handle_state_storage(
            state=aggregated_state,
            auth_user_id=auth_user_id,
            workflow_id=workflow_id,
            segment_id=segment_to_run,
            bucket=s3_bucket,
            threshold=self.threshold
        )
        
        # 4. 다음 세그먼트 결정
        # aggregator 다음은 일반적으로 워크플로우 완료이지만,
        # partition_map에서 next_segment를 확인
        partition_map = event.get('partition_map', [])
        # [Fix] total_segments None 체크 - partition_map이 None일 때 len() 에러 방지
        raw_total_segments = event.get('total_segments')
        if raw_total_segments is not None:
            total_segments = int(raw_total_segments)
        elif partition_map and isinstance(partition_map, list):
            total_segments = len(partition_map)
        else:
            total_segments = 1  # 최소 1개 세그먼트 보장
        next_segment = segment_to_run + 1
        
        # 완료 여부 판단
        is_complete = next_segment >= total_segments
        
        logger.info(f"[Aggregator] ✅ Aggregation complete: "
                   f"{successful_branches}/{len(parallel_results)} branches succeeded, "
                   f"next_segment={next_segment if not is_complete else 'COMPLETE'}")
        
        return {
            "status": "COMPLETE" if is_complete else "SUCCEEDED",
            "final_state": final_state,
            "final_state_s3_path": output_s3_path,
            "next_segment_to_run": None if is_complete else next_segment,
            "new_history_logs": all_history_logs,
            "error_info": branch_errors if branch_errors else None,
            "branches": None,
            "segment_type": "aggregator",
            "segment_id": segment_to_run,
            "aggregator_metadata": {
                'total_branches': len(parallel_results),
                'successful_branches': successful_branches,
                'failed_branches': len(branch_errors)
            }
        }

    def _trigger_child_workflow(self, event: Dict[str, Any], branch_config: Dict[str, Any], auth_user_id: str, quota_id: str) -> Optional[Dict[str, Any]]:
        """
        Triggers a Child Step Function (Standard Orchestrator) for complex branches.
        "Fire and Forget" pattern to avoid Lambda timeouts.
        """
        try:
            import boto3
            import json
            import time
            
            sfn_client = boto3.client('stepfunctions')
            
            # 1. Resolve Orchestrator ARN
            orchestrator_arn = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
            if not orchestrator_arn:
                logger.error("WORKFLOW_ORCHESTRATOR_ARN not set. Cannot trigger child workflow.")
                return None

            # 2. Construct Payload (Full 23 fields injection)
            payload = event.copy()
            payload['workflow_config'] = branch_config
            
            parent_workflow_id = payload.get('workflowId') or payload.get('workflow_id', 'unknown')
            parent_idempotency_key = payload.get('idempotency_key', str(time.time()))
            
            # 3. Generate Child Idempotency Key
            branch_id = branch_config.get('id') or f"branch_{int(time.time()*1000)}"
            child_idempotency_key = f"{parent_idempotency_key}_{branch_id}"[:80]
            
            payload['idempotency_key'] = child_idempotency_key
            payload['parent_workflow_id'] = parent_workflow_id
            
            # 4. Start Execution with retry
            safe_exec_name = "".join(c for c in child_idempotency_key if c.isalnum() or c in "-_")
            
            logger.info(f"Triggering Child SFN: {safe_exec_name}")
            
            # [v2.1] Step Functions start_execution에 재시도 적용
            def _start_child_execution():
                return sfn_client.start_execution(
                    stateMachineArn=orchestrator_arn,
                    name=safe_exec_name,
                    input=json.dumps(payload)
                )
            
            if RETRY_UTILS_AVAILABLE:
                response = retry_call(
                    _start_child_execution,
                    max_retries=2,
                    base_delay=0.5,
                    max_delay=5.0
                )
            else:
                response = _start_child_execution()
            
            return {
                "status": "ASYNC_CHILD_WORKFLOW_STARTED",
                "executionArn": response['executionArn'],
                "startDate": response['startDate'].isoformat(),
                "executionName": safe_exec_name
            }
            
        except Exception as e:
            logger.error(f"Failed to trigger child workflow: {e}")
            return None

    # ========================================================================
    # 🛡️ [v2.2] Ring Protection: 프롬프트 보안 검증
    # ========================================================================
    def _apply_ring_protection(
        self,
        segment_config: Dict[str, Any],
        initial_state: Dict[str, Any],
        segment_id: int,
        workflow_id: str
    ) -> List[Dict[str, Any]]:
        """
        🛡️ Ring Protection: 세그먼트 내 프롬프트 보안 검증
        
        모든 LLM 노드의 프롬프트를 검증하고:
        1. Prompt Injection 패턴 탐지
        2. Ring 0 태그 위조 시도 탐지
        3. 위험 도구 직접 접근 시도 탐지
        
        Args:
            segment_config: 세그먼트 설정
            initial_state: 초기 상태
            segment_id: 세그먼트 ID
            workflow_id: 워크플로우 ID
            
        Returns:
            보안 위반 목록 (빈 리스트면 안전)
        """
        violations = []
        
        if not self.security_guard or not RING_PROTECTION_AVAILABLE:
            return violations
        
        nodes = segment_config.get('nodes', [])
        if not nodes:
            return violations
        
        context = {
            'workflow_id': workflow_id,
            'segment_id': segment_id
        }
        
        for node in nodes:
            node_id = node.get('id', 'unknown')
            node_type = node.get('type', '')
            config = node.get('config', {})
            
            # LLM 노드의 프롬프트 검증
            if node_type in ('llm_chat', 'aiModel', 'llm'):
                prompt = config.get('prompt_content') or config.get('prompt') or ''
                system_prompt = config.get('system_prompt', '')
                
                # 프롬프트 검증
                for prompt_type, prompt_content in [('prompt', prompt), ('system_prompt', system_prompt)]:
                    if prompt_content:
                        result = self.security_guard.validate_prompt(
                            content=prompt_content,
                            ring_level=RingLevel.RING_3_USER,
                            context={**context, 'node_id': node_id, 'prompt_type': prompt_type}
                        )
                        
                        if not result.is_safe:
                            for v in result.violations:
                                violations.append({
                                    'node_id': node_id,
                                    'violation_type': v.violation_type.value,
                                    'severity': v.severity,
                                    'message': v.message,
                                    'should_sigkill': result.should_sigkill
                                })
                            
                            # 프롬프트 정화 (in-place)
                            if result.sanitized_content:
                                if prompt_type == 'prompt':
                                    config['prompt_content'] = result.sanitized_content
                                    config['prompt'] = result.sanitized_content
                                else:
                                    config['system_prompt'] = result.sanitized_content
                                logger.info(f"[Ring Protection] 🛡️ Sanitized {prompt_type} in node {node_id}")
            
            # 위험 도구 접근 검증
            if node_type in ('tool', 'api_call', 'operator'):
                tool_name = config.get('tool') or config.get('method') or node_type
                allowed, violation = self.security_guard.check_tool_permission(
                    tool_name=tool_name,
                    ring_level=RingLevel.RING_3_USER,
                    context={**context, 'node_id': node_id}
                )
                
                if not allowed and violation:
                    violations.append({
                        'node_id': node_id,
                        'violation_type': violation.violation_type.value,
                        'severity': violation.severity,
                        'message': violation.message,
                        'should_sigkill': False  # 도구 접근은 경고만
                    })
        
        if violations:
            logger.warning(f"[Ring Protection] ⚠️ {len(violations)} security violations detected in segment {segment_id}")
        
        return violations

    # ========================================================================
    # 🛡️ [Kernel Defense] Aggressive Retry Helper
    # ========================================================================
    def _is_retryable_error(self, error: Exception) -> bool:
        """
        에러가 재시도 가능한지 판단
        """
        error_str = str(error)
        error_type = type(error).__name__
        
        for pattern in RETRYABLE_ERROR_PATTERNS:
            if pattern in error_str or pattern in error_type:
                return True
        
        # Boto3 ClientError 체크
        if hasattr(error, 'response'):
            error_code = error.response.get('Error', {}).get('Code', '')
            for pattern in RETRYABLE_ERROR_PATTERNS:
                if pattern in error_code:
                    return True
        
        return False

    def _execute_with_kernel_retry(
        self,
        segment_config: Dict[str, Any],
        initial_state: Dict[str, Any],
        auth_user_id: str,
        event: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        🛡️ 커널 레벨 공격적 재시도
        
        Step Functions 레벨 재시도 전에 Lambda 내부에서 먼저 해결 시도.
        - 네트워크 에러, 일시적 서비스 장애 시 재시도
        - 지수 백오프 + 지터 적용
        
        Returns:
            (result_state, error_info) - 성공 시 error_info는 None
        """
        last_error = None
        retry_history = []
        
        for attempt in range(KERNEL_MAX_RETRIES + 1):
            try:
                # 커널 동적 분할 활성화 여부 확인
                enable_kernel_split = os.environ.get('ENABLE_KERNEL_SPLIT', 'true').lower() == 'true'
                
                if enable_kernel_split and isinstance(segment_config, dict):
                    # 🛡️ [Pattern 1] 자동 분할 실행
                    result_state = self._execute_with_auto_split(
                        segment_config=segment_config,
                        initial_state=initial_state,
                        auth_user_id=auth_user_id,
                        split_depth=segment_config.get('_split_depth', 0)
                    )
                else:
                    # 기존 로직: 직접 실행
                    result_state = run_workflow(
                        config_json=segment_config,
                        initial_state=initial_state,
                        ddb_table_name=os.environ.get("JOB_TABLE"),
                        user_api_keys={},
                        run_config={"user_id": auth_user_id}
                    )
                
                # 성공
                if attempt > 0:
                    logger.info(f"[Kernel Retry] ✅ Succeeded after {attempt} retries")
                    # 재시도 이력 기록
                    if isinstance(result_state, dict):
                        result_state['__kernel_retry_history'] = retry_history
                
                return result_state, None
                
            except Exception as e:
                last_error = e
                retry_info = {
                    'attempt': attempt + 1,
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'timestamp': time.time(),
                    'retryable': self._is_retryable_error(e)
                }
                retry_history.append(retry_info)
                
                if attempt < KERNEL_MAX_RETRIES and self._is_retryable_error(e):
                    # 지수 백오프 + 지터
                    delay = KERNEL_RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"[Kernel Retry] ⚠️ Attempt {attempt + 1}/{KERNEL_MAX_RETRIES + 1} failed: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                else:
                    # 재시도 불가능 또는 최대 횟수 도달
                    logger.error(
                        f"[Kernel Retry] ❌ All {attempt + 1} attempts failed. "
                        f"Last error: {e}"
                    )
                    break
        
        # 모든 재시도 실패 - 에러 정보 반환
        error_info = {
            'error': str(last_error),
            'error_type': type(last_error).__name__,
            'retry_attempts': len(retry_history),
            'retry_history': retry_history,
            'retryable': self._is_retryable_error(last_error) if last_error else False
        }
        
        return initial_state, error_info

    def execute_segment(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution logic for a workflow segment.
        
        🛡️ [Kernel Defense] 4단계 방어 메커니즘:
        1. Reserved Concurrency: Lambda 레벨 동시성 제한 (template.yaml)
        2. Kernel Scheduling: 부하 평탄화 + 배치 처리
        3. Intelligent Retry: 적응형 품질 임계값 + 정보 증류
        4. Budget/Drift Guardrail: 비용 서킷 브레이커 + 시맨틱 드리프트 감지
        """
        execution_start_time = time.time()
        
        # ====================================================================
        # 🛡️ [2단계] Pre-Execution Check: 동시성 및 예산 체크
        # ====================================================================
        if CONCURRENCY_CONTROLLER_AVAILABLE and self.concurrency_controller:
            pre_check = self.concurrency_controller.pre_execution_check()
            if not pre_check.get('can_proceed', True):
                logger.error(f"[Kernel] ❌ Pre-execution check failed: {pre_check.get('reason')}")
                return {
                    "status": "HALTED",
                    "final_state": {},
                    "final_state_s3_path": None,
                    "next_segment_to_run": None,
                    "new_history_logs": [],
                    "error_info": {
                        "error": pre_check.get('reason', 'Unknown'),
                        "error_type": "ConcurrencyControlHalt",
                        "budget_status": pre_check.get('budget_status')
                    },
                    "branches": None,
                    "segment_type": "halted",
                    "segment_id": event.get('segment_id', 0),
                    "kernel_stats": self.concurrency_controller.get_comprehensive_stats()
                }
            
            # 로드 레벨 로깅
            snapshot = pre_check.get('snapshot')
            if snapshot and snapshot.load_level.value in ['high', 'critical']:
                logger.warning(f"[Kernel] ⚠️ High load detected: {snapshot.load_level.value} "
                             f"({snapshot.active_executions}/{snapshot.reserved_concurrency})")
        
        # [Fix] 이벤트에서 MOCK_MODE를 읽어서 환경 변수로 주입
        # 이렇게 하면 모든 하위 함수들(invoke_bedrock_model 등)이 MOCK_MODE를 인식함
        event_mock_mode = event.get('MOCK_MODE', '').lower()
        if event_mock_mode in ('true', '1', 'yes', 'on'):
            os.environ['MOCK_MODE'] = 'true'
            logger.info("🧪 MOCK_MODE enabled from event payload")
        
        # ====================================================================
        # 🔀 [Aggregator] 병렬 결과 집계 처리
        # ASL의 AggregateParallelResults에서 호출됨
        # ====================================================================
        segment_type_param = event.get('segment_type')
        if segment_type_param == 'aggregator':
            return self._handle_aggregator(event)
        
        # 0. Check for Branch Offloading
        branch_config = event.get('branch_config')
        if branch_config:
            force_child = os.environ.get('FORCE_CHILD_WORKFLOW', 'false').lower() == 'true'
            node_count = len(branch_config.get('nodes', [])) if isinstance(branch_config.get('nodes'), list) else 0
            has_hitp = branch_config.get('hitp', False) or any(n.get('hitp') for n in branch_config.get('nodes', []))
            
            should_offload = force_child or node_count > 20 or has_hitp
            
            if should_offload:
                auth_user_id = event.get('ownerId') or event.get('owner_id')
                quota_id = event.get('quota_reservation_id')
                
                child_result = self._trigger_child_workflow(event, branch_config, auth_user_id, quota_id)
                if child_result:
                    return child_result

        # 1. State Bag Normalization
        normalize_inplace(event, remove_state_data=True)
        
        # 2. Extract Context
        auth_user_id = event.get('ownerId') or event.get('owner_id') or event.get('user_id')
        workflow_id = event.get('workflowId') or event.get('workflow_id')
        # 🚀 [Hybrid Mode] Support both segment_id (hybrid) and segment_to_run (legacy)
        segment_id = event.get('segment_id') or event.get('segment_to_run', 0)
        
        # [Critical Fix] S3 bucket for large payload offloading - ensure non-empty string
        s3_bucket_raw = os.environ.get("S3_BUCKET") or os.environ.get("SKELETON_S3_BUCKET") or ""
        s3_bucket = s3_bucket_raw.strip() if s3_bucket_raw else None
        
        if not s3_bucket:
            logger.error("🚨 [CRITICAL] S3_BUCKET/SKELETON_S3_BUCKET environment variable is NOT SET or EMPTY! "
                        f"S3_BUCKET='{os.environ.get('S3_BUCKET')}', "
                        f"SKELETON_S3_BUCKET='{os.environ.get('SKELETON_S3_BUCKET')}'. "
                        "Large payloads (>256KB) will FAIL.")
        else:
            logger.debug(f"S3 bucket for state offloading: {s3_bucket}")
        
        # 3. Load State (Inline or S3)
        # [Critical Fix] Step Functions passes state as 'current_state', not 'state'
        state_s3_path = event.get('state_s3_path')
        initial_state = event.get('current_state') or event.get('state', {})
        
        if state_s3_path:
            initial_state = self.state_manager.download_state_from_s3(state_s3_path)
            
        # 4. Resolve Segment Config
        # [Critical Fix] Support both test_workflow_config (E2E tests) and workflow_config
        workflow_config = event.get('test_workflow_config') or event.get('workflow_config')
        partition_map = event.get('partition_map')
        partition_map_s3_path = event.get('partition_map_s3_path')
        
        # 🚀 [Hybrid Mode] Direct segment_config support for MAP_REDUCE/BATCHED modes
        direct_segment_config = event.get('segment_config')
        execution_mode = event.get('execution_mode')
        
        if direct_segment_config and execution_mode in ('MAP_REDUCE', 'BATCHED'):
            logger.info(f"[Hybrid Mode] Using direct segment_config for {execution_mode} mode")
            segment_config = direct_segment_config
        else:
            # [Critical Fix] Support S3 Offloaded Partition Map with retry
            if not partition_map and partition_map_s3_path:
                try:
                    import boto3
                    import json
                    s3 = boto3.client('s3')
                    bucket_name = partition_map_s3_path.replace("s3://", "").split("/")[0]
                    key_name = "/".join(partition_map_s3_path.replace("s3://", "").split("/")[1:])
                    
                    logger.info(f"Loading partition_map from S3: {partition_map_s3_path}")
                    
                    # [v2.1] S3 get_object에 재시도 적용
                    def _get_partition_map():
                        obj = s3.get_object(Bucket=bucket_name, Key=key_name)
                        return json.loads(obj['Body'].read().decode('utf-8'))
                    
                    if RETRY_UTILS_AVAILABLE:
                        partition_map = retry_call(
                            _get_partition_map,
                            max_retries=2,
                            base_delay=0.5,
                            max_delay=5.0
                        )
                    else:
                        partition_map = _get_partition_map()
                        
                except Exception as e:
                    logger.error(f"Failed to load partition_map from S3 after retries: {e}")
                    # Fallback to dynamic partitioning (handled in _resolve_segment_config)
            
            segment_config = self._resolve_segment_config(workflow_config, partition_map, segment_id)
        
        # 🛡️ [Critical Fix] segment_config이 None이거나 error 타입이면 조기 에러 반환
        if not segment_config or (isinstance(segment_config, dict) and segment_config.get('type') == 'error'):
            error_msg = segment_config.get('error', 'segment_config is None') if isinstance(segment_config, dict) else 'segment_config is None'
            logger.error(f"🚨 [Critical] segment_config resolution failed: {error_msg}")
            return {
                "status": "FAILED",
                "error": error_msg,
                "error_type": "ConfigurationError",
                "final_state": initial_state,
                "final_state_s3_path": None,
                "next_segment_to_run": None,
                "new_history_logs": [],
                "error_info": {
                    "error": error_msg,
                    "error_type": "ConfigurationError",
                    "segment_id": segment_id,
                    "workflow_config_present": workflow_config is not None,
                    "partition_map_present": partition_map is not None
                },
                "branches": None,
                "segment_type": "ERROR",
                "segment_id": segment_id
            }
        
        # [Critical Fix] parallel_group 타입 세그먼트는 바로 PARALLEL_GROUP status 반환
        # ASL의 ProcessParallelSegments가 branches를 받아서 Map으로 병렬 실행함
        # 🔀 [Pattern 3] 병렬 스케줄러 적용
        segment_type = segment_config.get('type') if isinstance(segment_config, dict) else None
        if segment_type == 'parallel_group':
            branches = segment_config.get('branches', [])
            logger.info(f"🔀 Parallel group detected with {len(branches)} branches")
            
            # 🛡️ [Critical Fix] 단일 브랜치 + 내부 partition_map 케이스 처리
            # 이 경우 실제 병렬 실행이 필요 없으므로 브랜치 내부의 첫 번째 세그먼트 직접 실행
            if len(branches) == 1:
                single_branch = branches[0]
                branch_partition_map = single_branch.get('partition_map', [])
                
                if branch_partition_map:
                    logger.info(f"[Kernel] 📌 Single branch with internal partition_map detected. "
                               f"Executing {len(branch_partition_map)} segments sequentially instead of parallel.")
                    
                    # 브랜치 내부의 첫 번째 세그먼트를 segment_config로 사용
                    first_inner_segment = branch_partition_map[0] if branch_partition_map else None
                    
                    if first_inner_segment:
                        # 🔧 내부 partition_map을 새로운 실행 컨텍스트로 변환
                        # 상태를 유지하면서 내부 세그먼트 체인 순차 실행
                        return {
                            "status": "SEQUENTIAL_BRANCH",
                            "final_state": mask_pii_in_state(initial_state),
                            "final_state_s3_path": None,
                            "next_segment_to_run": segment_id + 1,
                            "new_history_logs": [],
                            "error_info": None,
                            "branches": None,  # 병렬 실행 안함
                            "segment_type": "sequential_branch",
                            "segment_id": segment_id,
                            # 🛡️ 내부 partition_map 정보 전달 (ASL이 순차 처리하도록)
                            "inner_partition_map": branch_partition_map,
                            "inner_segment_count": len(branch_partition_map),
                            "branch_id": single_branch.get('branch_id', 'B0'),
                            "scheduling_metadata": {
                                'strategy': 'SEQUENTIAL_SINGLE_BRANCH',
                                'total_inner_segments': len(branch_partition_map),
                                'reason': 'Single branch optimization - parallel execution skipped'
                            }
                        }
            
            # 🔧 빈 브랜치 또는 노드가 없는 브랜치 필터링
            valid_branches = []
            for branch in branches:
                branch_nodes = branch.get('nodes', [])
                branch_partition = branch.get('partition_map', [])
                
                # nodes가 있거나 partition_map이 있으면 유효한 브랜치
                if branch_nodes or branch_partition:
                    valid_branches.append(branch)
                else:
                    logger.warning(f"[Kernel] ⚠️ Skipping empty branch: {branch.get('branch_id', 'unknown')}")
            
            # 🛡️ 유효한 브랜치가 없으면 SUCCEEDED로 진행
            if not valid_branches:
                logger.info(f"[Kernel] ⏭️ No valid branches to execute, skipping parallel group")
                return {
                    "status": "SUCCEEDED",
                    "final_state": mask_pii_in_state(initial_state),
                    "final_state_s3_path": None,
                    "next_segment_to_run": segment_id + 1,
                    "new_history_logs": [],
                    "error_info": None,
                    "branches": None,
                    "segment_type": "empty_parallel_group",
                    "segment_id": segment_id
                }
            
            # 병렬 스케줄러 호출
            schedule_result = self._schedule_parallel_group(
                segment_config=segment_config,
                state=initial_state,
                segment_id=segment_id
            )
            
            # SCHEDULED_PARALLEL: 배치별 순차 실행 필요
            if schedule_result['status'] == 'SCHEDULED_PARALLEL':
                execution_batches = schedule_result['execution_batches']
                metadata = schedule_result['scheduling_metadata']
                
                logger.info(f"[Scheduler] 🔧 Scheduled {metadata['total_branches']} branches into "
                           f"{metadata['batch_count']} batches (strategy: {metadata['strategy']})")
                
                return {
                    "status": "SCHEDULED_PARALLEL",
                    "final_state": mask_pii_in_state(initial_state),
                    "final_state_s3_path": None,
                    "next_segment_to_run": segment_id + 1,
                    "new_history_logs": [],
                    "error_info": None,
                    "branches": valid_branches,  # 유효한 브랜치만
                    "execution_batches": execution_batches,
                    "segment_type": "scheduled_parallel",
                    "scheduling_metadata": metadata,
                    "segment_id": segment_id
                }
            
            # PARALLEL_GROUP: 기본 병렬 실행
            return {
                "status": "PARALLEL_GROUP",
                "final_state": mask_pii_in_state(initial_state),
                "final_state_s3_path": None,
                "next_segment_to_run": segment_id + 1,
                "new_history_logs": [],
                "error_info": None,
                "branches": valid_branches,  # 유효한 브랜치만
                "execution_batches": schedule_result.get('execution_batches', [valid_branches]),
                "segment_type": "parallel_group",
                "scheduling_metadata": schedule_result.get('scheduling_metadata'),
                "segment_id": segment_id
            }
        
        # 🛡️ [Pattern 2] 커널 검증: 이 세그먼트가 SKIPPED 상태인가?
        segment_status = self._check_segment_status(segment_config)
        if segment_status == SEGMENT_STATUS_SKIPPED:
            skip_reason = segment_config.get('skip_reason', 'Kernel decision')
            logger.info(f"[Kernel] ⏭️ Segment {segment_id} SKIPPED: {skip_reason}")
            
            # 커널 액션 로그 기록
            kernel_log = {
                'action': 'SKIP',
                'segment_id': segment_id,
                'reason': skip_reason,
                'skipped_by': segment_config.get('skipped_by', 'kernel'),
                'timestamp': time.time()
            }
            
            return {
                "status": "SKIPPED",
                "final_state": mask_pii_in_state(initial_state),
                "final_state_s3_path": None,
                "next_segment_to_run": segment_id + 1,
                "new_history_logs": [],
                "error_info": None,
                "branches": None,
                "segment_type": "skipped",
                "kernel_action": kernel_log,
                "segment_id": segment_id
            }
        
        # 5. Apply Self-Healing (Prompt Injection / Refinement)
        self.healer.apply_healing(segment_config, event.get("_self_healing_metadata"))
        
        # 🛡️ [v2.2] Ring Protection: 프롬프트 보안 검증
        # 세그먼트 내 LLM 노드의 프롬프트를 검증하고 위험 패턴 탐지
        security_violations = []
        if self.security_guard and RING_PROTECTION_AVAILABLE:
            security_violations = self._apply_ring_protection(
                segment_config=segment_config,
                initial_state=initial_state,
                segment_id=segment_id,
                workflow_id=workflow_id
            )
            
            # CRITICAL 위반 시 SIGKILL (세그먼트 강제 종료)
            critical_violations = [v for v in security_violations if v.get('should_sigkill')]
            if critical_violations:
                logger.error(f"[Kernel] 🛡️ SIGKILL triggered by Ring Protection: {len(critical_violations)} critical violations")
                return {
                    "status": "SIGKILL",
                    "final_state": mask_pii_in_state(initial_state),
                    "final_state_s3_path": None,
                    "next_segment_to_run": None,
                    "new_history_logs": [],
                    "error_info": {
                        "error": "Security violation detected",
                        "error_type": "RingProtectionViolation",
                        "violations": critical_violations
                    },
                    "branches": None,
                    "segment_type": "sigkill",
                    "kernel_action": {
                        'action': 'SIGKILL',
                        'segment_id': segment_id,
                        'reason': 'Critical security violation',
                        'violations': critical_violations,
                        'timestamp': time.time()
                    },
                    "segment_id": segment_id
                }
        
        # 6. Check User Quota / Secret Resolution (Repo access)
        # Note: In a full refactor, this should move to a UserService or AuthMiddleware
        # For now, we keep it simple.
        if auth_user_id:
            try:
                self.repo.get_user(auth_user_id) # Just validating access/existence
            except Exception as e:
                logger.warning("User check failed, but proceeding if possible: %s", e)

        # 7. Execute Workflow Segment with Kernel Defense
        # 🛡️ [Kernel Defense] Aggressive Retry + Partial Success
        start_time = time.time()
        
        result_state, execution_error = self._execute_with_kernel_retry(
            segment_config=segment_config,
            initial_state=initial_state,
            auth_user_id=auth_user_id,
            event=event
        )
        
        execution_time = time.time() - start_time
        
        # 🛡️ [Partial Success] 실패해도 SUCCEEDED 반환 + 에러 메타데이터 기록
        if execution_error and ENABLE_PARTIAL_SUCCESS:
            logger.warning(
                f"[Kernel] ⚠️ Segment {segment_id} failed but returning PARTIAL_SUCCESS. "
                f"Error: {execution_error['error']}"
            )
            
            # 에러 정보를 상태에 기록
            if isinstance(result_state, dict):
                result_state['__segment_error'] = execution_error
                result_state['__segment_status'] = 'PARTIAL_FAILURE'
                result_state['__failed_segment_id'] = segment_id
            
            # Partial Success 커널 로그
            kernel_log = {
                'action': 'PARTIAL_SUCCESS',
                'segment_id': segment_id,
                'error': execution_error['error'],
                'error_type': execution_error['error_type'],
                'retry_attempts': execution_error['retry_attempts'],
                'timestamp': time.time()
            }
            
            # 🚨 핵심: FAILED 대신 SUCCEEDED 반환 (ToleratedFailureThreshold 방지)
            final_state, output_s3_path = self.state_manager.handle_state_storage(
                state=result_state,
                auth_user_id=auth_user_id,
                workflow_id=workflow_id,
                segment_id=segment_id,
                bucket=s3_bucket,
                threshold=self.threshold
            )
            
            # [Fix] total_segments None 체크 - 부분 실패 경로에서도 안전하게 처리
            raw_total = event.get('total_segments')
            if raw_total is not None:
                total_segments = int(raw_total)
            else:
                partition_map = event.get('partition_map', [])
                total_segments = len(partition_map) if partition_map and isinstance(partition_map, list) else 1
            next_segment = segment_id + 1
            
            return {
                "status": "SUCCEEDED",  # 🛡️ Partial Success: FAILED 대신 SUCCEEDED
                "final_state": final_state,
                "final_state_s3_path": output_s3_path,
                "next_segment_to_run": next_segment if next_segment < total_segments else None,
                "new_history_logs": [],
                "error_info": execution_error,  # 에러 정보는 메타데이터로 전달
                "branches": None,
                "segment_type": "partial_failure",
                "kernel_action": kernel_log,
                "segment_id": segment_id,
                "execution_time": execution_time,
                "_partial_success": True  # 클라이언트가 부분 실패 감지용
            }
        
        execution_time = time.time() - start_time
        
        # 🛡️ [Pattern 2] 조건부 스킵 결정
        # 실행 결과에서 스킵할 세그먼트가 지정되었는지 확인
        manifest_s3_path = event.get('segment_manifest_s3_path')
        if manifest_s3_path and isinstance(result_state, dict):
            skip_next_segments = result_state.get('_kernel_skip_segments', [])
            if skip_next_segments:
                skip_reason = result_state.get('_kernel_skip_reason', 'Condition not met')
                self._mark_segments_for_skip(manifest_s3_path, skip_next_segments, skip_reason)
                logger.info(f"[Kernel] Marked {len(skip_next_segments)} segments for skip: {skip_reason}")
            
            # 복구 세그먼트 삽입 요청 처리
            recovery_request = result_state.get('_kernel_inject_recovery')
            if recovery_request:
                self._inject_recovery_segments(
                    manifest_s3_path=manifest_s3_path,
                    after_segment_id=segment_id,
                    recovery_segments=recovery_request.get('segments', []),
                    reason=recovery_request.get('reason', 'Recovery injection')
                )
        
        # 8. Handle Output State Storage
        # [Critical] Pre-check result_state size before S3 offload decision
        import json
        result_state_size = len(json.dumps(result_state, ensure_ascii=False).encode('utf-8')) if result_state else 0
        logger.info(f"[Large Payload Check] result_state size: {result_state_size} bytes ({result_state_size/1024:.1f}KB), "
                   f"s3_bucket: {'SET' if s3_bucket else 'NOT SET'}, threshold: {self.threshold}")
        
        if result_state_size > 250000:  # 250KB - Step Functions limit is 256KB
            logger.warning(f"🚨 [Large Payload Warning] result_state exceeds 250KB! "
                          f"Size: {result_state_size/1024:.1f}KB. S3 offload REQUIRED.")
        
        final_state, output_s3_path = self.state_manager.handle_state_storage(
            state=result_state,
            auth_user_id=auth_user_id,
            workflow_id=workflow_id,
            segment_id=segment_id,
            bucket=s3_bucket,
            threshold=self.threshold
        )
        
        # [Critical] Log the actual return payload size
        return_payload_size = len(json.dumps(final_state, ensure_ascii=False).encode('utf-8')) if final_state else 0
        logger.info(f"[Large Payload Check] After S3 offload - final_state size: {return_payload_size} bytes ({return_payload_size/1024:.1f}KB), "
                   f"s3_path: {output_s3_path or 'None'}")
        
        # Extract history logs from result_state if available
        new_history_logs = result_state.get('__new_history_logs', []) if isinstance(result_state, dict) else []
        
        # [Critical Fix] 워크플로우 완료 여부 결정
        # 1. test_workflow_config가 주입된 경우 (E2E 테스트): 한 번에 전체 실행 후 완료
        # 2. partition_map이 없는 경우: 전체 워크플로우를 한 번에 실행했으므로 완료
        # 3. partition_map이 있는 경우: 다음 세그먼트가 있는지 확인
        is_e2e_test = event.get('test_workflow_config') is not None
        has_partition_map = partition_map is not None and len(partition_map) > 0
        
        # 🛡️ 커널 메타데이터 추출 (있는 경우)
        kernel_actions = result_state.get('__kernel_actions', []) if isinstance(result_state, dict) else []
        
        if is_e2e_test or not has_partition_map:
            # E2E 테스트 또는 파티션 없는 단일 실행: 워크플로우 완료
            return {
                "status": "COMPLETE",  # ASL이 기대하는 상태값
                "final_state": final_state,
                "final_state_s3_path": output_s3_path,
                "next_segment_to_run": None,  # 다음 세그먼트 없음
                "new_history_logs": new_history_logs,
                "error_info": None,
                "branches": None,
                "segment_type": "final",
                "state_s3_path": output_s3_path,
                "segment_id": segment_id,
                "execution_time": execution_time,
                "kernel_actions": kernel_actions if kernel_actions else None
            }
        
        # 파티션 맵이 있는 경우: 다음 세그먼트 존재 여부 확인
        # [Fix] total_segments None 체크 - partition_map이 None일 때 len() 에러 방지
        raw_total_segments = event.get('total_segments')
        if raw_total_segments is not None:
            total_segments = int(raw_total_segments)
        elif partition_map and isinstance(partition_map, list):
            total_segments = len(partition_map)
        else:
            total_segments = 1  # 최소 1개 세그먼트 보장
        
        next_segment = segment_id + 1
        
        if next_segment >= total_segments:
            # 마지막 세그먼트 완료
            return {
                "status": "COMPLETE",
                "final_state": final_state,
                "final_state_s3_path": output_s3_path,
                "next_segment_to_run": None,
                "new_history_logs": new_history_logs,
                "error_info": None,
                "branches": None,
                "segment_type": "final",
                "state_s3_path": output_s3_path,
                "segment_id": segment_id,
                "execution_time": execution_time,
                "kernel_actions": kernel_actions if kernel_actions else None
            }
        
        # 아직 실행할 세그먼트가 남아있음
        return {
            "status": "SUCCEEDED",
            "final_state": final_state,
            "final_state_s3_path": output_s3_path,
            "next_segment_to_run": next_segment,
            "new_history_logs": new_history_logs,
            "error_info": None,
            "branches": None,
            "segment_type": "normal",
            "state_s3_path": output_s3_path,
            "segment_id": segment_id,
            "execution_time": execution_time,
            "kernel_actions": kernel_actions if kernel_actions else None
        }

    def _resolve_segment_config(self, workflow_config, partition_map, segment_id):
        """
        Identical logic to original handler for partitioning.
        """
        # [Critical Fix] workflow_config이 None이면 조기 처리
        if not workflow_config:
            logger.error(f"[_resolve_segment_config] ⚠️ workflow_config is None! segment_id={segment_id}")
            # partition_map에서 직접 찾기 시도
            if partition_map:
                if isinstance(partition_map, list) and 0 <= segment_id < len(partition_map):
                    return partition_map[segment_id]
                elif isinstance(partition_map, dict) and str(segment_id) in partition_map:
                    return partition_map[str(segment_id)]
            # 에러 정보를 포함한 기본 segment_config 반환
            return {
                "type": "error",
                "error": "workflow_config is None",
                "segment_id": segment_id,
                "nodes": [],
                "edges": []
            }
        
        # Basic full workflow or pre-chunked
        # If we are strictly running a segment, we might need to simulate partitioning if map is missing
        # For simplicity, we assume workflow_config IS the segment config if partition_map is missing
        # OR we call the dynamic partitioner.
        if not partition_map:
            # Fallback to dynamic partitioning logic
            parts = _partition_workflow_dynamically(workflow_config) # arbitrary chunks removed
            if 0 <= segment_id < len(parts):
                return parts[segment_id]
            return workflow_config # Fallback

        # 🚨 [Critical Fix] partition_map이 list 또는 dict일 수 있음
        if partition_map:
            if isinstance(partition_map, list):
                # list인 경우: 인덱스로 접근
                if 0 <= segment_id < len(partition_map):
                    return partition_map[segment_id]
            elif isinstance(partition_map, dict):
                # dict인 경우: 문자열 키로 접근
                if str(segment_id) in partition_map:
                    return partition_map[str(segment_id)]
            
        # Simplified fallback - workflow_config 또는 에러 상태
        if workflow_config:
            return workflow_config
        
        # [Critical Fix] 모든 fallback 실패 시 에러 상태 반환 (None 반환 방지)
        logger.error(f"[_resolve_segment_config] 🚨 All fallbacks failed! segment_id={segment_id}")
        return {
            "type": "error",
            "error": "Failed to resolve segment config - both workflow_config and partition_map are invalid",
            "segment_id": segment_id,
            "nodes": [],
            "edges": []
        }
