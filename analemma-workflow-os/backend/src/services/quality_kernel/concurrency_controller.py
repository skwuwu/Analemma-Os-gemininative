"""
Concurrency Controller - 4단계 아키텍처 구현

1단계: Reserved Concurrency (RC) - Lambda 레벨 (template.yaml)
2단계: 커널 스케줄링 및 부하 평탄화 (Scheduling Layer)
3단계: 지능형 품질 및 재시도 제어 (Execution Layer)  
4단계: 비용 및 드리프트 모니터링 (Guardrail Layer)

이 모듈은 2~4단계를 구현합니다.

v2.0 추가 기능:
  - 분산 환경 상태 동기화 (DynamoDB Atomic Counter)
  - Fast Track 경로 (priority: realtime)
"""

import os
import time
import json
import logging
import hashlib
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable, Tuple
from collections import deque
import threading
from decimal import Decimal

logger = logging.getLogger(__name__)


# ============================================================
# 🌐 분산 상태 관리자 (DynamoDB Atomic Counter)
# ============================================================

@dataclass
class DistributedStateConfig:
    """분산 상태 관리 설정"""
    # 환경 변수에서 테이블 이름 가져오기 (template.yaml에서 설정)
    table_name: str = field(
        default_factory=lambda: os.getenv("KERNEL_STATE_TABLE", "analemma-kernel-state")
    )
    state_key: str = "global-concurrency-state"
    # [Critical] DynamoDB 테이블이 composite key (pk + sk)를 사용하는 경우 sk 추가
    sort_key: str = "CONCURRENCY_STATE"  # Sort key for composite key tables
    ttl_seconds: int = 3600  # 1시간
    enable_distributed: bool = True  # False면 로컬 모드
    sync_interval_ms: int = 500  # 동기화 주기
    

class DistributedStateManager:
    """
    분산 환경 상태 관리자
    
    Lambda Scale-out 시에도 전역 가드레일이 작동하도록
    DynamoDB Atomic Counter를 활용한 상태 동기화를 제공합니다.
    
    저장 항목:
        - global_active_executions: 전역 동시 실행 수
        - global_accumulated_cost: 전역 누적 비용
        - last_updated: 마지막 업데이트 시간
    """
    
    def __init__(self, config: Optional[DistributedStateConfig] = None):
        self.config = config or DistributedStateConfig()
        self._local_cache = {
            'active_executions': 0,
            'accumulated_cost': 0.0,
            'last_sync': 0
        }
        self._dynamodb = None
        self._table = None
        self._initialized = False
        self._lock = threading.Lock()
        
    def _ensure_initialized(self) -> bool:
        """DynamoDB 연결 초기화 (Lazy Loading)"""
        if self._initialized:
            return True
            
        if not self.config.enable_distributed:
            logger.debug("[DistributedState] Running in local mode")
            return False
            
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            self._dynamodb = boto3.resource('dynamodb')
            self._table = self._dynamodb.Table(self.config.table_name)
            
            # 테이블 존재 확인
            self._table.load()
            self._initialized = True
            logger.info(f"[DistributedState] Connected to {self.config.table_name}")
            return True
            
        except Exception as e:
            logger.warning(f"[DistributedState] Failed to connect: {e}. Falling back to local mode.")
            self.config.enable_distributed = False
            return False
    
    def _should_sync(self) -> bool:
        """동기화 필요 여부 확인"""
        elapsed_ms = (time.time() - self._local_cache['last_sync']) * 1000
        return elapsed_ms >= self.config.sync_interval_ms
    
    def increment_executions(self, delta: int = 1) -> int:
        """
        전역 실행 수 증가 (Atomic)
        
        Returns:
            업데이트 후 전역 실행 수
        """
        with self._lock:
            self._local_cache['active_executions'] += delta
            
            if not self._ensure_initialized():
                return self._local_cache['active_executions']
            
            try:
                # [Critical] Composite key support
                key = {'pk': self.config.state_key}
                if hasattr(self.config, 'sort_key') and self.config.sort_key:
                    key['sk'] = self.config.sort_key
                
                response = self._table.update_item(
                    Key=key,
                    UpdateExpression='SET active_executions = if_not_exists(active_executions, :zero) + :delta, '
                                     'last_updated = :now',
                    ExpressionAttributeValues={
                        ':delta': delta,
                        ':zero': 0,
                        ':now': Decimal(str(time.time()))
                    },
                    ReturnValues='UPDATED_NEW'
                )
                global_count = int(response['Attributes'].get('active_executions', 0))
                self._local_cache['last_sync'] = time.time()
                return global_count
                
            except Exception as e:
                logger.warning(f"[DistributedState] Atomic increment failed: {e}")
                return self._local_cache['active_executions']
    
    def decrement_executions(self, delta: int = 1) -> int:
        """
        전역 실행 수 감소 (Atomic)
        """
        return self.increment_executions(-delta)
    
    def add_cost(self, cost_usd: float, workflow_id: str = "unknown") -> float:
        """
        전역 비용 누적 (Atomic)
        
        Returns:
            업데이트 후 전역 누적 비용
        """
        with self._lock:
            self._local_cache['accumulated_cost'] += cost_usd
            
            if not self._ensure_initialized():
                return self._local_cache['accumulated_cost']
            
            try:
                # [Critical] Composite key support
                key = {'pk': self.config.state_key}
                if hasattr(self.config, 'sort_key') and self.config.sort_key:
                    key['sk'] = self.config.sort_key
                
                response = self._table.update_item(
                    Key=key,
                    UpdateExpression='SET accumulated_cost = if_not_exists(accumulated_cost, :zero) + :cost, '
                                     'last_updated = :now, '
                                     'last_workflow = :wf',
                    ExpressionAttributeValues={
                        ':cost': Decimal(str(round(cost_usd, 8))),
                        ':zero': Decimal('0'),
                        ':now': Decimal(str(time.time())),
                        ':wf': workflow_id
                    },
                    ReturnValues='UPDATED_NEW'
                )
                global_cost = float(response['Attributes'].get('accumulated_cost', 0))
                self._local_cache['last_sync'] = time.time()
                return global_cost
                
            except Exception as e:
                logger.warning(f"[DistributedState] Atomic cost add failed: {e}")
                return self._local_cache['accumulated_cost']
    
    def get_global_state(self) -> Dict[str, Any]:
        """
        전역 상태 조회
        
        Returns:
            {
                'active_executions': int,
                'accumulated_cost': float,
                'is_distributed': bool,
                'last_sync_age_ms': float
            }
        """
        if not self._ensure_initialized() or not self._should_sync():
            return {
                'active_executions': self._local_cache['active_executions'],
                'accumulated_cost': self._local_cache['accumulated_cost'],
                'is_distributed': False,
                'last_sync_age_ms': (time.time() - self._local_cache['last_sync']) * 1000
            }
        
        try:
            # [Critical] Composite key support (pk + sk)
            # Try with sort key first, fallback to pk-only
            key = {'pk': self.config.state_key}
            if hasattr(self.config, 'sort_key') and self.config.sort_key:
                key['sk'] = self.config.sort_key
            
            response = self._table.get_item(Key=key)
            item = response.get('Item', {})
            
            global_state = {
                'active_executions': int(item.get('active_executions', 0)),
                'accumulated_cost': float(item.get('accumulated_cost', 0)),
                'is_distributed': True,
                'last_sync_age_ms': 0
            }
            
            # 로컬 캐시 동기화
            self._local_cache['active_executions'] = global_state['active_executions']
            self._local_cache['accumulated_cost'] = global_state['accumulated_cost']
            self._local_cache['last_sync'] = time.time()
            
            return global_state
            
        except Exception as e:
            # [Debug] Log actual error for schema mismatch diagnosis
            error_type = type(e).__name__
            if 'ValidationException' in str(e):
                logger.error(f"[DistributedState] DynamoDB key schema mismatch: {e}. "
                           f"Table may use composite key (pk+sk). Key used: {key}")
            else:
                logger.warning(f"[DistributedState] Get state failed ({error_type}): {e}")
            
            return {
                'active_executions': self._local_cache['active_executions'],
                'accumulated_cost': self._local_cache['accumulated_cost'],
                'is_distributed': False,
                'last_sync_age_ms': (time.time() - self._local_cache['last_sync']) * 1000
            }
    
    def reset_global_state(self) -> bool:
        """
        전역 상태 초기화 (관리 목적)
        """
        with self._lock:
            self._local_cache = {
                'active_executions': 0,
                'accumulated_cost': 0.0,
                'last_sync': time.time()
            }
            
            if not self._ensure_initialized():
                return True
            
            try:
                # [Critical] Composite key support
                item = {
                    'pk': self.config.state_key,
                    'active_executions': 0,
                    'accumulated_cost': Decimal('0'),
                    'last_updated': Decimal(str(time.time())),
                    'reset_at': Decimal(str(time.time()))
                }
                if hasattr(self.config, 'sort_key') and self.config.sort_key:
                    item['sk'] = self.config.sort_key
                
                self._table.put_item(Item=item)
                return True
            except Exception as e:
                logger.error(f"[DistributedState] Reset failed: {e}")
                return False


# 싱글톤 분산 상태 관리자
_distributed_state_manager: Optional[DistributedStateManager] = None

def get_distributed_state_manager() -> DistributedStateManager:
    """분산 상태 관리자 싱글톤 획득"""
    global _distributed_state_manager
    if _distributed_state_manager is None:
        _distributed_state_manager = DistributedStateManager()
    return _distributed_state_manager


# ============================================================
# 🛡️ 2단계: 커널 스케줄링 및 부하 평탄화 (Scheduling Layer)
# ============================================================

class LoadLevel(Enum):
    """시스템 부하 수준"""
    LOW = "low"           # 동시성 < 30%
    NORMAL = "normal"     # 30% <= 동시성 < 60%
    HIGH = "high"         # 60% <= 동시성 < 85%
    CRITICAL = "critical" # 동시성 >= 85%


@dataclass
class ConcurrencySnapshot:
    """동시성 스냅샷"""
    timestamp: float
    active_executions: int
    reserved_concurrency: int
    utilization_ratio: float  # 0.0 ~ 1.0
    load_level: LoadLevel
    
    @property
    def is_throttle_risk(self) -> bool:
        """쓰로틀링 위험 여부"""
        return self.utilization_ratio >= 0.85


class TaskPriority(Enum):
    """작업 우선순위"""
    REALTIME = "realtime"    # 즉시 실행 (Fast Track)
    HIGH = "high"            # 우선 처리
    NORMAL = "normal"        # 일반 처리
    LOW = "low"              # 배치 대기 가능
    BACKGROUND = "background"  # 백그라운드 처리


@dataclass 
class TaskBatch:
    """배치 처리를 위한 작업 그룹"""
    batch_id: str
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    max_size: int = 10
    max_wait_ms: int = 100  # 최대 대기 시간
    
    @property
    def is_full(self) -> bool:
        return len(self.tasks) >= self.max_size
    
    @property
    def is_expired(self) -> bool:
        elapsed_ms = (time.time() - self.created_at) * 1000
        return elapsed_ms >= self.max_wait_ms
    
    @property
    def should_flush(self) -> bool:
        return self.is_full or self.is_expired


class KernelTaskScheduler:
    """
    커널 레벨 작업 스케줄러
    
    폭발적인 요청을 OS가 감당할 수 있는 속도로 정제합니다.
    
    Features:
        1. Task Buffering: 즉시 실행 대신 버퍼링
        2. Batching: 짧은 연산 노드들을 묶어서 처리
        3. Throttling: 부하 기반 속도 조절
        4. Priority Queue: 중요도 기반 우선순위
        5. Fast Track: priority=realtime 시 배치 bypass (v2.0)
        6. Distributed State: 분산 환경 동시성 추적 (v2.0)
    """
    
    # 부하 레벨별 처리 지연 (ms)
    THROTTLE_DELAYS = {
        LoadLevel.LOW: 0,
        LoadLevel.NORMAL: 10,
        LoadLevel.HIGH: 50,
        LoadLevel.CRITICAL: 200
    }
    
    # Fast Track 우선순위는 쓰로틀링 면제
    FAST_TRACK_PRIORITIES = {TaskPriority.REALTIME, TaskPriority.HIGH}
    
    # 부하 레벨별 배치 크기
    BATCH_SIZES = {
        LoadLevel.LOW: 5,
        LoadLevel.NORMAL: 10,
        LoadLevel.HIGH: 20,
        LoadLevel.CRITICAL: 50  # 더 많이 묶어서 호출 줄임
    }
    
    def __init__(
        self,
        reserved_concurrency: int = 200,
        enable_batching: bool = True,
        enable_throttling: bool = True,
        enable_distributed_state: bool = True
    ):
        self.reserved_concurrency = reserved_concurrency
        self.enable_batching = enable_batching
        self.enable_throttling = enable_throttling
        self.enable_distributed_state = enable_distributed_state
        
        # 상태 추적 (로컬)
        self._active_executions = 0
        self._lock = threading.Lock()
        
        # 분산 상태 관리자 (Scale-out 대응)
        self._distributed_state = get_distributed_state_manager() if enable_distributed_state else None
        
        # 배치 버퍼
        self._pending_batches: Dict[str, TaskBatch] = {}
        
        # 통계
        self._stats = {
            'total_scheduled': 0,
            'batched_executions': 0,
            'throttle_delays_ms': 0,
            'peak_concurrency': 0,
            'fast_track_executions': 0,
            'distributed_sync_count': 0
        }
    
    def get_concurrency_snapshot(self, use_distributed: bool = True) -> ConcurrencySnapshot:
        """
        현재 동시성 상태 스냅샷
        
        Args:
            use_distributed: True면 분산 상태 기반, False면 로컬 상태만
        """
        with self._lock:
            # 분산 상태 사용 시 전역 동시성 조회
            if use_distributed and self._distributed_state:
                global_state = self._distributed_state.get_global_state()
                active = global_state['active_executions']
                self._stats['distributed_sync_count'] += 1
            else:
                active = self._active_executions
            
            ratio = active / max(self.reserved_concurrency, 1)
            
            if ratio < 0.3:
                level = LoadLevel.LOW
            elif ratio < 0.6:
                level = LoadLevel.NORMAL
            elif ratio < 0.85:
                level = LoadLevel.HIGH
            else:
                level = LoadLevel.CRITICAL
            
            return ConcurrencySnapshot(
                timestamp=time.time(),
                active_executions=active,
                reserved_concurrency=self.reserved_concurrency,
                utilization_ratio=ratio,
                load_level=level
            )
    
    def acquire_execution_slot(self) -> bool:
        """
        실행 슬롯 획득 시도
        
        분산 환경에서는 전역 동시성을 추적합니다.
        """
        with self._lock:
            # 분산 상태 사용 시
            if self._distributed_state:
                global_count = self._distributed_state.increment_executions(1)
                if global_count > self.reserved_concurrency:
                    # 롤백
                    self._distributed_state.decrement_executions(1)
                    logger.warning(f"[Scheduler] Global concurrency limit reached: {global_count}/{self.reserved_concurrency}")
                    return False
                self._active_executions = global_count
            else:
                if self._active_executions >= self.reserved_concurrency:
                    logger.warning(f"[Scheduler] Concurrency limit reached: {self._active_executions}/{self.reserved_concurrency}")
                    return False
                self._active_executions += 1
            
            self._stats['peak_concurrency'] = max(
                self._stats['peak_concurrency'],
                self._active_executions
            )
            return True
    
    def release_execution_slot(self):
        """
        실행 슬롯 해제
        
        분산 환경에서는 전역 동시성을 감소시킵니다.
        """
        with self._lock:
            if self._distributed_state:
                self._distributed_state.decrement_executions(1)
            self._active_executions = max(0, self._active_executions - 1)
    
    def apply_throttling(
        self, 
        snapshot: Optional[ConcurrencySnapshot] = None,
        priority: TaskPriority = TaskPriority.NORMAL
    ) -> int:
        """
        부하 기반 쓰로틀링 적용
        
        Args:
            snapshot: 동시성 스냅샷
            priority: 작업 우선순위 (REALTIME/HIGH는 쓰로틀링 면제)
        
        Returns:
            적용된 지연 시간 (ms)
        """
        # Fast Track: 실시간 우선순위는 쓰로틀링 bypass
        if priority in self.FAST_TRACK_PRIORITIES:
            logger.debug(f"[Scheduler] Fast Track: {priority.value} priority bypasses throttling")
            return 0
        
        if not self.enable_throttling:
            return 0
        
        snapshot = snapshot or self.get_concurrency_snapshot()
        delay_ms = self.THROTTLE_DELAYS.get(snapshot.load_level, 0)
        
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
            self._stats['throttle_delays_ms'] += delay_ms
            logger.debug(f"[Scheduler] Throttle applied: {delay_ms}ms (load: {snapshot.load_level.value})")
        
        return delay_ms
    
    def _get_task_priority(self, task_config: Dict[str, Any], state: Dict[str, Any]) -> TaskPriority:
        """
        작업의 우선순위 추출
        
        우선순위 결정 순서:
            1. task_config.priority
            2. state.workflow_priority
            3. state.metadata.priority
            4. 기본값: NORMAL
        """
        # task_config에서 직접 지정
        priority_str = task_config.get('priority', '').lower()
        
        # state에서 워크플로우 레벨 우선순위
        if not priority_str:
            priority_str = state.get('workflow_priority', '').lower()
        
        # metadata에서 추출
        if not priority_str:
            # [Fix] None defense: state['metadata']가 None일 수 있음
            metadata = state.get('metadata') or {}
            priority_str = metadata.get('priority', '').lower()
        
        # 문자열 -> Enum 변환
        priority_map = {
            'realtime': TaskPriority.REALTIME,
            'high': TaskPriority.HIGH,
            'normal': TaskPriority.NORMAL,
            'low': TaskPriority.LOW,
            'background': TaskPriority.BACKGROUND
        }
        
        return priority_map.get(priority_str, TaskPriority.NORMAL)
    
    def should_batch(
        self, 
        task_config: Dict[str, Any],
        priority: TaskPriority = TaskPriority.NORMAL
    ) -> bool:
        """
        해당 작업을 배치 처리해야 하는지 판단
        
        배치 대상:
            - operator 타입 노드 (짧은 연산)
            - 예상 실행 시간 < 100ms
            - LLM 호출 없음
            - priority가 REALTIME/HIGH가 아님 (Fast Track bypass)
        """
        if not self.enable_batching:
            return False
        
        # Fast Track: 실시간/높은 우선순위는 배치 bypass
        if priority in self.FAST_TRACK_PRIORITIES:
            logger.debug(f"[Scheduler] Fast Track: {priority.value} priority bypasses batching")
            return False
        
        node_type = task_config.get('type', 'unknown')
        if node_type != 'operator':
            return False
        
        # LLM 노드는 배치 불가
        config = task_config.get('config', {})
        if config.get('model') or config.get('llm'):
            return False
        
        # 코드가 복잡하면 배치 불가 (휴리스틱)
        code = config.get('code', '')
        if len(code) > 500:  # 긴 코드는 복잡할 가능성
            return False
        
        return True
    
    def add_to_batch(
        self,
        workflow_id: str,
        task_config: Dict[str, Any],
        state: Dict[str, Any]
    ) -> Optional[TaskBatch]:
        """
        작업을 배치에 추가
        
        Returns:
            flush가 필요한 배치 (준비 완료 시)
        """
        snapshot = self.get_concurrency_snapshot()
        batch_size = self.BATCH_SIZES.get(snapshot.load_level, 10)
        
        batch_key = f"{workflow_id}_batch"
        
        with self._lock:
            if batch_key not in self._pending_batches:
                import uuid
                self._pending_batches[batch_key] = TaskBatch(
                    batch_id=f"batch_{uuid.uuid4().hex[:8]}",
                    max_size=batch_size
                )
            
            batch = self._pending_batches[batch_key]
            batch.tasks.append({
                'config': task_config,
                'state': state,
                'added_at': time.time()
            })
            
            if batch.should_flush:
                del self._pending_batches[batch_key]
                self._stats['batched_executions'] += len(batch.tasks)
                return batch
        
        return None
    
    def execute_batch(
        self,
        batch: TaskBatch,
        executor: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        배치 내 모든 작업 순차 실행
        
        단일 Lambda 인스턴스에서 여러 작업 처리
        """
        results = []
        merged_state = {}
        
        logger.info(f"[Scheduler] Executing batch {batch.batch_id} with {len(batch.tasks)} tasks")
        
        for task in batch.tasks:
            try:
                # 상태 병합 (이전 결과를 다음 입력으로)
                task_state = {**task['state'], **merged_state}
                result = executor(task['config'], task_state)
                
                if isinstance(result, dict):
                    merged_state.update(result)
                
                results.append({
                    'success': True,
                    'result': result
                })
            except Exception as e:
                logger.error(f"[Scheduler] Batch task failed: {e}")
                results.append({
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    def schedule_task(
        self,
        workflow_id: str,
        task_config: Dict[str, Any],
        state: Dict[str, Any],
        executor: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        작업 스케줄링 및 실행
        
        전체 흐름:
            1. 우선순위 추출 (Fast Track 판단)
            2. 동시성 확인 및 쓰로틀링 (Fast Track은 bypass)
            3. 배치 가능 여부 판단 (Fast Track은 bypass)
            4. 배치 또는 즉시 실행
        """
        self._stats['total_scheduled'] += 1
        
        # 0. 우선순위 추출
        priority = self._get_task_priority(task_config, state)
        is_fast_track = priority in self.FAST_TRACK_PRIORITIES
        
        if is_fast_track:
            self._stats['fast_track_executions'] += 1
            logger.info(f"[Scheduler] 🚀 Fast Track activated: priority={priority.value}")
        
        # 1. 동시성 스냅샷
        snapshot = self.get_concurrency_snapshot()
        
        # 2. 쓰로틀링 적용 (Fast Track은 bypass)
        self.apply_throttling(snapshot, priority)
        
        # 3. 슬롯 획득
        if not self.acquire_execution_slot():
            raise RuntimeError(
                f"Concurrency limit exceeded ({self._active_executions}/{self.reserved_concurrency}). "
                "System is under heavy load."
            )
        
        try:
            # 4. 배치 처리 검토 (Fast Track은 bypass)
            if self.should_batch(task_config, priority):
                batch = self.add_to_batch(workflow_id, task_config, state)
                if batch:
                    results = self.execute_batch(batch, executor)
                    # 마지막 결과 반환 (상태 연쇄)
                    return results[-1].get('result', {}) if results else {}
                else:
                    # 배치에 추가됨, 아직 실행 안 함
                    return {'__batched': True, '__batch_pending': True}
            
            # 5. 즉시 실행
            return executor(task_config, state)
            
        finally:
            self.release_execution_slot()
    
    def get_stats(self) -> Dict[str, Any]:
        """스케줄러 통계"""
        snapshot = self.get_concurrency_snapshot()
        
        # 분산 상태 정보 추가
        distributed_info = {}
        if self._distributed_state:
            global_state = self._distributed_state.get_global_state()
            distributed_info = {
                'distributed_active': global_state['active_executions'],
                'distributed_cost': global_state['accumulated_cost'],
                'is_distributed': global_state['is_distributed']
            }
        
        return {
            **self._stats,
            'current_concurrency': snapshot.active_executions,
            'concurrency_utilization': round(snapshot.utilization_ratio * 100, 1),
            'load_level': snapshot.load_level.value,
            'pending_batches': len(self._pending_batches),
            **distributed_info
        }


# ============================================================
# 🛡️ 3단계: 지능형 품질 및 재시도 제어 (Execution Layer)
# ============================================================

@dataclass
class AdaptiveThresholdConfig:
    """적응형 임계값 설정"""
    # 기본 임계값
    base_quality_threshold: float = 0.6
    
    # 부하 레벨별 완화 정도
    threshold_reduction_per_level: float = 0.1
    
    # 재시도 횟수별 완화
    threshold_reduction_per_retry: float = 0.1
    
    # 최소 품질 임계값 (아무리 완화해도 이 이하로 내려가지 않음)
    min_quality_threshold: float = 0.3
    
    def get_effective_threshold(
        self,
        load_level: LoadLevel,
        retry_count: int
    ) -> float:
        """
        현재 상황에 맞는 실효 임계값 계산
        
        높은 부하 + 많은 재시도 → 임계값 완화 → 빠른 통과
        """
        load_reduction = {
            LoadLevel.LOW: 0,
            LoadLevel.NORMAL: 1,
            LoadLevel.HIGH: 2,
            LoadLevel.CRITICAL: 3
        }.get(load_level, 0) * self.threshold_reduction_per_level
        
        retry_reduction = retry_count * self.threshold_reduction_per_retry
        
        effective = self.base_quality_threshold - load_reduction - retry_reduction
        return max(self.min_quality_threshold, effective)


class IntelligentRetryController:
    """
    지능형 재시도 제어기
    
    품질 저하가 비용 폭증으로 이어지는 '루프'를 끊습니다.
    
    Features:
        1. Quality Gate Interceptor: 저품질 시 정보 증류로 부분 수술
        2. Adaptive Threshold: 부하/재시도에 따른 단계적 완화
        3. Best-Effort Mode: 시스템 보호를 위한 최선 결과 반환
    """
    
    def __init__(
        self,
        scheduler: Optional[KernelTaskScheduler] = None,
        threshold_config: Optional[AdaptiveThresholdConfig] = None,
        max_retries: int = 3,
        enable_distillation: bool = True
    ):
        self.scheduler = scheduler
        self.threshold_config = threshold_config or AdaptiveThresholdConfig()
        self.max_retries = max_retries
        self.enable_distillation = enable_distillation
        
        # 재시도 기록
        self._retry_history: Dict[str, List[Dict[str, Any]]] = {}
    
    def get_current_threshold(self, node_id: str) -> float:
        """현재 노드의 실효 품질 임계값"""
        retry_count = len(self._retry_history.get(node_id, []))
        
        if self.scheduler:
            snapshot = self.scheduler.get_concurrency_snapshot()
            load_level = snapshot.load_level
        else:
            load_level = LoadLevel.NORMAL
        
        return self.threshold_config.get_effective_threshold(load_level, retry_count)
    
    def should_distill_instead_of_retry(
        self,
        node_id: str,
        quality_score: float,
        slop_issues: List[str]
    ) -> Tuple[bool, str]:
        """
        재시도 대신 증류(Distillation)를 수행해야 하는지 판단
        
        Returns:
            (should_distill, reason)
        """
        retry_count = len(self._retry_history.get(node_id, []))
        
        # 재시도 한도 도달 시 증류로 전환
        if retry_count >= self.max_retries:
            return True, f"Max retries ({self.max_retries}) reached"
        
        # 부분적 문제만 있으면 증류가 효율적
        minor_issues = ['redundant_phrases', 'filler_words', 'slight_repetition']
        if all(issue in minor_issues for issue in slop_issues):
            return True, "Minor issues only - distillation more efficient"
        
        # 고부하 시 증류 우선
        if self.scheduler:
            snapshot = self.scheduler.get_concurrency_snapshot()
            if snapshot.load_level in [LoadLevel.HIGH, LoadLevel.CRITICAL]:
                return True, f"High load ({snapshot.load_level.value}) - conserving resources"
        
        # 품질이 임계값 근처면 증류 시도
        threshold = self.get_current_threshold(node_id)
        if quality_score >= threshold * 0.8:
            return True, f"Score {quality_score:.2f} close to threshold {threshold:.2f}"
        
        return False, "Retry recommended"
    
    def record_retry(
        self,
        node_id: str,
        quality_score: float,
        action_taken: str,
        success: bool
    ):
        """재시도 기록"""
        if node_id not in self._retry_history:
            self._retry_history[node_id] = []
        
        self._retry_history[node_id].append({
            'timestamp': time.time(),
            'quality_score': quality_score,
            'action': action_taken,
            'success': success,
            'threshold_used': self.get_current_threshold(node_id)
        })
    
    def evaluate_and_act(
        self,
        node_id: str,
        text: str,
        quality_score: float,
        slop_issues: List[str],
        regenerate_func: Callable[[], str],
        distill_func: Callable[[str, List[str]], str]
    ) -> Tuple[str, str, bool]:
        """
        품질 평가 후 적절한 조치 수행
        
        Returns:
            (final_text, action_taken, passed_quality)
        """
        threshold = self.get_current_threshold(node_id)
        
        # 품질 통과
        if quality_score >= threshold:
            self.record_retry(node_id, quality_score, "PASS", True)
            return text, "PASS", True
        
        # 증류 vs 재시도 결정
        should_distill, reason = self.should_distill_instead_of_retry(
            node_id, quality_score, slop_issues
        )
        
        if should_distill and self.enable_distillation:
            # 정보 증류 (부분 수술)
            try:
                distilled_text = distill_func(text, slop_issues)
                self.record_retry(node_id, quality_score, f"DISTILL: {reason}", True)
                return distilled_text, f"DISTILL: {reason}", True
            except Exception as e:
                logger.warning(f"[RetryController] Distillation failed: {e}")
        
        # 재생성 시도
        try:
            new_text = regenerate_func()
            self.record_retry(node_id, quality_score, "REGENERATE", True)
            return new_text, "REGENERATE", False  # 재평가 필요
        except Exception as e:
            logger.error(f"[RetryController] Regeneration failed: {e}")
            # Best-Effort: 원본 반환
            self.record_retry(node_id, quality_score, "BEST_EFFORT", False)
            return text, "BEST_EFFORT (regeneration failed)", True
    
    def get_retry_stats(self, node_id: Optional[str] = None) -> Dict[str, Any]:
        """재시도 통계"""
        if node_id:
            history = self._retry_history.get(node_id, [])
            return {
                'node_id': node_id,
                'retry_count': len(history),
                'history': history
            }
        
        return {
            'total_nodes': len(self._retry_history),
            'total_retries': sum(len(h) for h in self._retry_history.values()),
            'nodes': {k: len(v) for k, v in self._retry_history.items()}
        }


# ============================================================
# 🛡️ 4단계: 비용 및 드리프트 모니터링 (Guardrail Layer)
# ============================================================

@dataclass
class BudgetWatchdogConfig:
    """비용 감시 설정"""
    max_budget_usd: float = 10.0
    warning_threshold: float = 0.7   # 70%에서 경고
    critical_threshold: float = 0.9  # 90%에서 강제 전환
    halt_threshold: float = 1.0      # 100%에서 중단
    
    # 모델 다운그레이드 경로
    model_downgrade_path: List[str] = field(default_factory=lambda: [
        'gemini-1.5-pro',      # 최고급
        'gemini-1.5-flash',    # 중급
        'gemini-2.0-flash',    # 중급 (최신)
        'gemini-1.5-flash-8b'  # 최저가
    ])


class BudgetWatchdog:
    """
    비용 서킷 브레이커
    
    경제적 파멸을 막는 최후의 보루입니다.
    
    Actions:
        - WARNING (70%): 로그 및 알림
        - CRITICAL (90%): 저비용 모델로 강제 전환
        - HALT (100%): 실행 중단
    
    v2.0: 분산 환경에서 전역 비용 추적 지원
    """
    
    def __init__(
        self, 
        config: Optional[BudgetWatchdogConfig] = None,
        enable_distributed: bool = True,
        workflow_id: str = "unknown"
    ):
        self.config = config or BudgetWatchdogConfig()
        self._accumulated_cost: float = 0.0
        self._cost_history: List[Dict[str, Any]] = []
        self._model_override: Optional[str] = None
        self._halted: bool = False
        self._workflow_id = workflow_id
        
        # 분산 상태 연동 (전역 비용 추적)
        self._distributed_state = get_distributed_state_manager() if enable_distributed else None
    
    def record_cost(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        node_id: str = "unknown"
    ) -> Dict[str, Any]:
        """
        비용 기록 및 가드레일 확인
        
        분산 환경에서는 전역 비용을 기준으로 가드레일을 적용합니다.
        
        Returns:
            {
                'cost_usd': float,
                'total_cost_usd': float,
                'global_cost_usd': float (v2.0),
                'budget_ratio': float,
                'action': str,  # 'CONTINUE', 'WARNING', 'DOWNGRADE', 'HALT'
                'new_model': Optional[str],
                'is_distributed': bool (v2.0)
            }
        """
        # 모델별 가격 (per 1K tokens)
        PRICING = {
            'gemini-1.5-pro': {'input': 0.00125, 'output': 0.005},
            'gemini-1.5-flash': {'input': 0.000075, 'output': 0.0003},
            'gemini-2.0-flash': {'input': 0.0001, 'output': 0.0004},
            'gemini-1.5-flash-8b': {'input': 0.0000375, 'output': 0.00015},
            'gpt-4o': {'input': 0.005, 'output': 0.015},
            'gpt-4o-mini': {'input': 0.00015, 'output': 0.0006},
            'claude-3.5-sonnet': {'input': 0.003, 'output': 0.015},
        }
        
        pricing = PRICING.get(model, {'input': 0.001, 'output': 0.002})
        cost = (input_tokens / 1000 * pricing['input']) + \
               (output_tokens / 1000 * pricing['output'])
        
        # 로컬 비용 누적
        self._accumulated_cost += cost
        self._cost_history.append({
            'timestamp': time.time(),
            'model': model,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'cost_usd': cost,
            'node_id': node_id
        })
        
        # 분산 환경: 전역 비용 누적 및 조회
        global_cost = self._accumulated_cost
        is_distributed = False
        
        if self._distributed_state:
            global_cost = self._distributed_state.add_cost(cost, self._workflow_id)
            is_distributed = True
        
        # 전역 비용 기준으로 가드레일 적용
        budget_ratio = global_cost / self.config.max_budget_usd
        
        result = {
            'cost_usd': round(cost, 6),
            'total_cost_usd': round(self._accumulated_cost, 4),
            'global_cost_usd': round(global_cost, 4),
            'budget_ratio': round(budget_ratio, 3),
            'action': 'CONTINUE',
            'new_model': None,
            'is_distributed': is_distributed
        }
        
        # 가드레일 체크
        if budget_ratio >= self.config.halt_threshold:
            self._halted = True
            result['action'] = 'HALT'
            logger.critical(f"[BudgetWatchdog] 🚨 HALT: Budget exhausted (${self._accumulated_cost:.2f}/${self.config.max_budget_usd})")
        
        elif budget_ratio >= self.config.critical_threshold:
            # 최저가 모델로 강제 전환
            self._model_override = self.config.model_downgrade_path[-1]
            result['action'] = 'DOWNGRADE'
            result['new_model'] = self._model_override
            logger.warning(f"[BudgetWatchdog] ⚠️ CRITICAL: Forcing model downgrade to {self._model_override}")
        
        elif budget_ratio >= self.config.warning_threshold:
            result['action'] = 'WARNING'
            logger.warning(f"[BudgetWatchdog] ⚠️ WARNING: Budget at {budget_ratio*100:.1f}%")
        
        return result
    
    def get_effective_model(self, requested_model: str) -> str:
        """강제 다운그레이드 적용된 실제 모델 반환"""
        if self._model_override:
            return self._model_override
        return requested_model
    
    def is_halted(self) -> bool:
        """실행 중단 상태 여부"""
        return self._halted
    
    def get_budget_status(self) -> Dict[str, Any]:
        """
        예산 상태 요약
        
        v2.0: 분산 환경에서 전역 비용 정보 포함
        """
        # 분산 상태에서 전역 비용 조회
        global_cost = self._accumulated_cost
        is_distributed = False
        
        if self._distributed_state:
            global_state = self._distributed_state.get_global_state()
            global_cost = global_state['accumulated_cost']
            is_distributed = global_state['is_distributed']
        
        return {
            'local_cost_usd': round(self._accumulated_cost, 4),
            'global_cost_usd': round(global_cost, 4),
            'accumulated_cost_usd': round(global_cost, 4),  # 호환성 유지
            'max_budget_usd': self.config.max_budget_usd,
            'budget_ratio': round(global_cost / self.config.max_budget_usd, 3),
            'is_halted': self._halted,
            'model_override': self._model_override,
            'total_requests': len(self._cost_history),
            'is_distributed': is_distributed
        }


@dataclass
class SemanticDriftResult:
    """시맨틱 드리프트 감지 결과"""
    is_drifting: bool
    similarity_score: float  # 0.0 ~ 1.0 (1.0 = 완전 동일)
    consecutive_similar_count: int
    recommendation: str  # 'CONTINUE', 'WARN', 'HALT_FOR_HITL'


class SemanticDriftDetector:
    """
    시맨틱 드리프트 감지기
    
    AI가 똑같은 대답을 반복하며 루프를 돌고 있다면,
    인프라 자원을 더 낭비하기 전에 실행을 중단하고
    인간(HITL)에게 제어권을 넘깁니다.
    """
    
    def __init__(
        self,
        similarity_threshold: float = 0.95,
        max_consecutive_similar: int = 3,
        history_size: int = 10
    ):
        self.similarity_threshold = similarity_threshold
        self.max_consecutive_similar = max_consecutive_similar
        self.history_size = history_size
        
        self._output_history: deque = deque(maxlen=history_size)
        self._consecutive_similar = 0
    
    def _compute_similarity(self, text1: str, text2: str) -> float:
        """
        두 텍스트의 유사도 계산 (Jaccard + Length)
        
        실제 프로덕션에서는 임베딩 기반 유사도 사용 권장
        """
        if not text1 or not text2:
            return 0.0
        
        # 정규화
        t1 = text1.lower().strip()
        t2 = text2.lower().strip()
        
        # 완전 동일
        if t1 == t2:
            return 1.0
        
        # 길이 유사도
        len_sim = min(len(t1), len(t2)) / max(len(t1), len(t2))
        
        # 단어 기반 Jaccard 유사도
        words1 = set(t1.split())
        words2 = set(t2.split())
        
        if not words1 or not words2:
            return len_sim
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        jaccard = intersection / union if union > 0 else 0
        
        # 가중 평균
        return 0.3 * len_sim + 0.7 * jaccard
    
    def _compute_hash(self, text: str) -> str:
        """텍스트 해시 (빠른 중복 체크용)"""
        return hashlib.md5(text.encode()).hexdigest()[:16]
    
    def check_drift(self, output_text: str) -> SemanticDriftResult:
        """
        새 출력의 드리프트 여부 확인
        """
        if not self._output_history:
            self._output_history.append(output_text)
            return SemanticDriftResult(
                is_drifting=False,
                similarity_score=0.0,
                consecutive_similar_count=0,
                recommendation='CONTINUE'
            )
        
        # 마지막 출력과 비교
        last_output = self._output_history[-1]
        similarity = self._compute_similarity(output_text, last_output)
        
        if similarity >= self.similarity_threshold:
            self._consecutive_similar += 1
        else:
            self._consecutive_similar = 0
        
        self._output_history.append(output_text)
        
        # 드리프트 판정
        is_drifting = self._consecutive_similar >= self.max_consecutive_similar
        
        if is_drifting:
            recommendation = 'HALT_FOR_HITL'
            logger.warning(
                f"[DriftDetector] 🔄 Semantic drift detected! "
                f"{self._consecutive_similar} consecutive similar outputs (threshold: {self.similarity_threshold})"
            )
        elif self._consecutive_similar >= 2:
            recommendation = 'WARN'
        else:
            recommendation = 'CONTINUE'
        
        return SemanticDriftResult(
            is_drifting=is_drifting,
            similarity_score=similarity,
            consecutive_similar_count=self._consecutive_similar,
            recommendation=recommendation
        )
    
    def reset(self):
        """히스토리 초기화"""
        self._output_history.clear()
        self._consecutive_similar = 0


# ============================================================
# 🛡️ 통합 컨트롤러: 4단계 아키텍처 통합
# ============================================================

class ConcurrencyControllerV2:
    """
    4단계 아키텍처 통합 컨트롤러
    
    1단계: Reserved Concurrency (Lambda 레벨 - template.yaml)
    2단계: 커널 스케줄링 (KernelTaskScheduler)
    3단계: 지능형 재시도 (IntelligentRetryController)
    4단계: 비용/드리프트 가드레일 (BudgetWatchdog, SemanticDriftDetector)
    """
    
    def __init__(
        self,
        workflow_id: str = "default",
        reserved_concurrency: int = 200,
        max_budget_usd: float = 10.0,
        enable_batching: bool = True,
        enable_throttling: bool = True,
        enable_distillation: bool = True
    ):
        self.workflow_id = workflow_id
        
        # 2단계: 스케줄러
        self.scheduler = KernelTaskScheduler(
            reserved_concurrency=reserved_concurrency,
            enable_batching=enable_batching,
            enable_throttling=enable_throttling
        )
        
        # 3단계: 재시도 컨트롤러
        self.retry_controller = IntelligentRetryController(
            scheduler=self.scheduler,
            enable_distillation=enable_distillation
        )
        
        # 4단계: 가드레일
        self.budget_watchdog = BudgetWatchdog(
            BudgetWatchdogConfig(max_budget_usd=max_budget_usd)
        )
        self.drift_detector = SemanticDriftDetector()
    
    def pre_execution_check(self) -> Dict[str, Any]:
        """
        실행 전 체크
        
        Returns:
            {
                'can_proceed': bool,
                'reason': str,
                'snapshot': ConcurrencySnapshot,
                'budget_status': dict
            }
        """
        # 예산 체크
        if self.budget_watchdog.is_halted():
            return {
                'can_proceed': False,
                'reason': 'Budget exhausted - execution halted',
                'budget_status': self.budget_watchdog.get_budget_status()
            }
        
        # 동시성 체크
        snapshot = self.scheduler.get_concurrency_snapshot()
        if snapshot.is_throttle_risk:
            # 쓰로틀링은 하되 진행은 가능
            self.scheduler.apply_throttling(snapshot)
        
        return {
            'can_proceed': True,
            'reason': 'OK',
            'snapshot': snapshot,
            'budget_status': self.budget_watchdog.get_budget_status()
        }
    
    def post_execution_check(
        self,
        output_text: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        node_id: str
    ) -> Dict[str, Any]:
        """
        실행 후 체크
        
        Returns:
            {
                'should_halt': bool,
                'halt_reason': Optional[str],
                'cost_result': dict,
                'drift_result': SemanticDriftResult,
                'effective_model': str
            }
        """
        # 비용 기록
        cost_result = self.budget_watchdog.record_cost(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            node_id=node_id
        )
        
        # 드리프트 체크
        drift_result = self.drift_detector.check_drift(output_text)
        
        should_halt = False
        halt_reason = None
        
        if cost_result['action'] == 'HALT':
            should_halt = True
            halt_reason = 'Budget exhausted'
        elif drift_result.recommendation == 'HALT_FOR_HITL':
            should_halt = True
            halt_reason = f"Semantic drift detected ({drift_result.consecutive_similar_count} similar outputs)"
        
        return {
            'should_halt': should_halt,
            'halt_reason': halt_reason,
            'cost_result': cost_result,
            'drift_result': drift_result,
            'effective_model': self.budget_watchdog.get_effective_model(model)
        }
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """전체 통계"""
        return {
            'workflow_id': self.workflow_id,
            'scheduler': self.scheduler.get_stats(),
            'retry': self.retry_controller.get_retry_stats(),
            'budget': self.budget_watchdog.get_budget_status()
        }


# Singleton 인스턴스 (Lambda warm start 활용)
_controller_instance: Optional[ConcurrencyControllerV2] = None


def get_concurrency_controller(
    workflow_id: str = "default",
    **kwargs
) -> ConcurrencyControllerV2:
    """싱글톤 컨트롤러 인스턴스 획득"""
    global _controller_instance
    
    if _controller_instance is None or _controller_instance.workflow_id != workflow_id:
        _controller_instance = ConcurrencyControllerV2(workflow_id=workflow_id, **kwargs)
    
    return _controller_instance
