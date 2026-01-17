"""
Orchestrator selection and performance metrics management

Monitor the performance and effectiveness of dynamic orchestrator selection through CloudWatch metrics.

🚀 Key metrics:
- Orchestrator selection distribution
- Cache hit rate
- Response time improvement
- Workflow complexity distribution
- Selection accuracy
"""

import time
import logging
from typing import Dict, Any, Optional
from src.dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

try:
    import boto3
    cloudwatch = boto3.client('cloudwatch')
    CLOUDWATCH_AVAILABLE = True
except ImportError:
    CLOUDWATCH_AVAILABLE = False
    logger.warning("CloudWatch not available, metrics will be logged only")


@dataclass
class OrchestratorMetrics:
    """Orchestrator metrics data"""
    orchestrator_type: str
    selection_time_ms: float
    complexity_score: float
    segment_count: int
    estimated_events: int
    cache_hit: bool
    selection_reason: str
    workflow_id: str
    owner_id: str


class MetricsCollector:
    """Metrics collection and transmission manager"""
    
    def __init__(self, namespace: str = "WorkflowOrchestrator"):
        self.namespace = namespace
        self.metrics_buffer = []
        self.buffer_size = 20  # CloudWatch PutMetricData 제한
    
    def record_orchestrator_selection(
        self,
        orchestrator_type: str,
        selection_time_ms: float,
        complexity_score: float,
        segment_count: int,
        estimated_events: int,
        cache_hit: bool,
        selection_reason: str,
        workflow_id: str,
        owner_id: str
    ) -> None:
        """오케스트레이터 선택 메트릭 기록"""
        
        metrics = OrchestratorMetrics(
            orchestrator_type=orchestrator_type,
            selection_time_ms=selection_time_ms,
            complexity_score=complexity_score,
            segment_count=segment_count,
            estimated_events=estimated_events,
            cache_hit=cache_hit,
            selection_reason=selection_reason,
            workflow_id=workflow_id,
            owner_id=owner_id
        )
        
        # 로그 기록
        logger.info(
            f"Orchestrator metrics: type={orchestrator_type}, "
            f"time={selection_time_ms:.2f}ms, score={complexity_score:.1f}, "
            f"segments={segment_count}, cache_hit={cache_hit}"
        )
        
        # CloudWatch 메트릭 전송
        if CLOUDWATCH_AVAILABLE:
            self._send_cloudwatch_metrics(metrics)
        
        # 버퍼에 추가 (배치 전송용)
        self.metrics_buffer.append(metrics)
        
        # 버퍼가 가득 차면 배치 전송
        if len(self.metrics_buffer) >= self.buffer_size:
            self.flush_metrics()
    
    def _send_cloudwatch_metrics(self, metrics: OrchestratorMetrics) -> None:
        """개별 CloudWatch 메트릭 전송"""
        try:
            timestamp = datetime.now(timezone.utc)
            
            metric_data = [
                # 오케스트레이터 선택 분포
                {
                    'MetricName': 'OrchestratorSelection',
                    'Dimensions': [
                        {'Name': 'OrchestratorType', 'Value': metrics.orchestrator_type}
                    ],
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': timestamp
                },
                
                # 선택 시간
                {
                    'MetricName': 'SelectionLatency',
                    'Dimensions': [
                        {'Name': 'OrchestratorType', 'Value': metrics.orchestrator_type}
                    ],
                    'Value': metrics.selection_time_ms,
                    'Unit': 'Milliseconds',
                    'Timestamp': timestamp
                },
                
                # 복잡도 점수
                {
                    'MetricName': 'ComplexityScore',
                    'Dimensions': [
                        {'Name': 'OrchestratorType', 'Value': metrics.orchestrator_type}
                    ],
                    'Value': metrics.complexity_score,
                    'Unit': 'None',
                    'Timestamp': timestamp
                },
                
                # 세그먼트 수
                {
                    'MetricName': 'SegmentCount',
                    'Dimensions': [
                        {'Name': 'OrchestratorType', 'Value': metrics.orchestrator_type}
                    ],
                    'Value': metrics.segment_count,
                    'Unit': 'Count',
                    'Timestamp': timestamp
                },
                
                # 예상 이벤트 수
                {
                    'MetricName': 'EstimatedEvents',
                    'Dimensions': [
                        {'Name': 'OrchestratorType', 'Value': metrics.orchestrator_type}
                    ],
                    'Value': metrics.estimated_events,
                    'Unit': 'Count',
                    'Timestamp': timestamp
                },
                
                # 캐시 히트율
                {
                    'MetricName': 'CacheHit',
                    'Dimensions': [
                        {'Name': 'CacheStatus', 'Value': 'Hit' if metrics.cache_hit else 'Miss'}
                    ],
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': timestamp
                }
            ]
            
            # CloudWatch에 메트릭 전송
            cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=metric_data
            )
            
        except Exception as e:
            logger.error(f"Failed to send CloudWatch metrics: {e}")
    
    def record_cache_performance(
        self,
        hit_rate_percent: float,
        cache_size: int,
        max_size: int,
        avg_lookup_time_ms: float
    ) -> None:
        """캐시 성능 메트릭 기록"""
        
        logger.info(
            f"Cache performance: hit_rate={hit_rate_percent:.1f}%, "
            f"size={cache_size}/{max_size}, lookup_time={avg_lookup_time_ms:.2f}ms"
        )
        
        if not CLOUDWATCH_AVAILABLE:
            return
        
        try:
            timestamp = datetime.now(timezone.utc)
            
            metric_data = [
                {
                    'MetricName': 'CacheHitRate',
                    'Value': hit_rate_percent,
                    'Unit': 'Percent',
                    'Timestamp': timestamp
                },
                {
                    'MetricName': 'CacheUtilization',
                    'Value': (cache_size / max_size) * 100,
                    'Unit': 'Percent',
                    'Timestamp': timestamp
                },
                {
                    'MetricName': 'CacheLookupTime',
                    'Value': avg_lookup_time_ms,
                    'Unit': 'Milliseconds',
                    'Timestamp': timestamp
                }
            ]
            
            cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=metric_data
            )
            
        except Exception as e:
            logger.error(f"Failed to send cache performance metrics: {e}")
    
    def record_selection_accuracy(
        self,
        expected_type: str,
        actual_type: str,
        confidence_score: float
    ) -> None:
        """선택 정확도 메트릭 기록"""
        
        is_accurate = expected_type == actual_type
        
        logger.info(
            f"Selection accuracy: expected={expected_type}, actual={actual_type}, "
            f"accurate={is_accurate}, confidence={confidence_score:.2f}"
        )
        
        if not CLOUDWATCH_AVAILABLE:
            return
        
        try:
            timestamp = datetime.now(timezone.utc)
            
            metric_data = [
                {
                    'MetricName': 'SelectionAccuracy',
                    'Dimensions': [
                        {'Name': 'ExpectedType', 'Value': expected_type},
                        {'Name': 'ActualType', 'Value': actual_type}
                    ],
                    'Value': 1 if is_accurate else 0,
                    'Unit': 'None',
                    'Timestamp': timestamp
                },
                {
                    'MetricName': 'SelectionConfidence',
                    'Dimensions': [
                        {'Name': 'OrchestratorType', 'Value': actual_type}
                    ],
                    'Value': confidence_score,
                    'Unit': 'None',
                    'Timestamp': timestamp
                }
            ]
            
            cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=metric_data
            )
            
        except Exception as e:
            logger.error(f"Failed to send selection accuracy metrics: {e}")
    
    def flush_metrics(self) -> None:
        """버퍼된 메트릭 배치 전송"""
        if not self.metrics_buffer:
            return
        
        logger.info(f"Flushing {len(self.metrics_buffer)} buffered metrics")
        
        # 배치 처리 로직 (필요시 구현)
        # 현재는 개별 전송이므로 버퍼만 클리어
        self.metrics_buffer.clear()
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """메트릭 요약 정보 반환"""
        if not self.metrics_buffer:
            return {'buffer_size': 0, 'summary': 'No metrics in buffer'}
        
        # 버퍼된 메트릭 분석
        standard_count = sum(1 for m in self.metrics_buffer if m.orchestrator_type == 'standard')
        distributed_count = sum(1 for m in self.metrics_buffer if m.orchestrator_type == 'distributed')
        cache_hits = sum(1 for m in self.metrics_buffer if m.cache_hit)
        
        avg_selection_time = sum(m.selection_time_ms for m in self.metrics_buffer) / len(self.metrics_buffer)
        avg_complexity = sum(m.complexity_score for m in self.metrics_buffer) / len(self.metrics_buffer)
        
        return {
            'buffer_size': len(self.metrics_buffer),
            'standard_selections': standard_count,
            'distributed_selections': distributed_count,
            'cache_hit_rate': (cache_hits / len(self.metrics_buffer)) * 100,
            'avg_selection_time_ms': avg_selection_time,
            'avg_complexity_score': avg_complexity
        }


# 전역 메트릭 수집기
_global_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """전역 메트릭 수집기 반환"""
    global _global_collector
    
    if _global_collector is None:
        _global_collector = MetricsCollector()
    
    return _global_collector


def record_orchestrator_selection_metrics(
    orchestrator_type: str,
    selection_metadata: Dict[str, Any],
    selection_start_time: float,
    workflow_id: str,
    owner_id: str,
    cache_hit: bool = False
) -> None:
    """
    오케스트레이터 선택 메트릭을 기록하는 편의 함수
    
    Args:
        orchestrator_type: 선택된 오케스트레이터 타입
        selection_metadata: 선택 메타데이터
        selection_start_time: 선택 시작 시간 (time.time())
        workflow_id: 워크플로우 ID
        owner_id: 소유자 ID
        cache_hit: 캐시 히트 여부
    """
    try:
        collector = get_metrics_collector()
        
        selection_time_ms = (time.time() - selection_start_time) * 1000
        
        complexity = selection_metadata.get('complexity', {})
        complexity_score = complexity.get('complexity_score', 0)
        segment_count = complexity.get('total_segments', 0)
        estimated_events = complexity.get('estimated_events', 0)
        selection_reason = selection_metadata.get('selection_reason', 'Unknown')
        
        collector.record_orchestrator_selection(
            orchestrator_type=orchestrator_type,
            selection_time_ms=selection_time_ms,
            complexity_score=complexity_score,
            segment_count=segment_count,
            estimated_events=estimated_events,
            cache_hit=cache_hit,
            selection_reason=selection_reason,
            workflow_id=workflow_id,
            owner_id=owner_id
        )
        
    except Exception as e:
        logger.error(f"Failed to record orchestrator selection metrics: {e}")


def record_cache_metrics(cache_stats: Dict[str, Any]) -> None:
    """
    캐시 성능 메트릭을 기록하는 편의 함수
    
    Args:
        cache_stats: 캐시 통계 딕셔너리
    """
    try:
        collector = get_metrics_collector()
        
        collector.record_cache_performance(
            hit_rate_percent=cache_stats.get('hit_rate_percent', 0),
            cache_size=cache_stats.get('cache_size', 0),
            max_size=cache_stats.get('max_size', 1),
            avg_lookup_time_ms=cache_stats.get('avg_lookup_time_ms', 0)
        )
        
    except Exception as e:
        logger.error(f"Failed to record cache metrics: {e}")


def create_cloudwatch_dashboard() -> Dict[str, Any]:
    """
    CloudWatch 대시보드 설정을 생성합니다.
    
    Returns:
        대시보드 설정 딕셔너리
    """
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0, "y": 0,
                "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["WorkflowOrchestrator", "OrchestratorSelection", "OrchestratorType", "standard"],
                        [".", ".", ".", "distributed"]
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "region": "us-east-1",
                    "title": "Orchestrator Selection Distribution"
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 0,
                "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["WorkflowOrchestrator", "CacheHitRate"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-east-1",
                    "title": "Cache Hit Rate"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 6,
                "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["WorkflowOrchestrator", "SelectionLatency", "OrchestratorType", "standard"],
                        [".", ".", ".", "distributed"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-east-1",
                    "title": "Selection Latency"
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 6,
                "width": 12, "height": 6,
                "properties": {
                    "metrics": [
                        ["WorkflowOrchestrator", "ComplexityScore", "OrchestratorType", "standard"],
                        [".", ".", ".", "distributed"]
                    ],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-east-1",
                    "title": "Workflow Complexity Distribution"
                }
            }
        ]
    }
    
    return {
        "DashboardName": "WorkflowOrchestratorMetrics",
        "DashboardBody": json.dumps(dashboard_body)
    }


# 🧪 테스트 및 디버깅용 함수들

def test_metrics_collection():
    """메트릭 수집 테스트"""
    collector = get_metrics_collector()
    
    # 테스트 메트릭 기록
    test_metrics = [
        ('standard', 15.5, 25.0, 50, 1000, True, 'Small workflow', 'test_wf_1', 'user_1'),
        ('distributed', 45.2, 85.5, 500, 25000, False, 'Large workflow', 'test_wf_2', 'user_2'),
        ('standard', 8.1, 15.0, 20, 500, True, 'Simple workflow', 'test_wf_3', 'user_3')
    ]
    
    for metrics in test_metrics:
        collector.record_orchestrator_selection(*metrics)
    
    # 캐시 성능 테스트
    collector.record_cache_performance(
        hit_rate_percent=85.5,
        cache_size=750,
        max_size=1000,
        avg_lookup_time_ms=2.3
    )
    
    # 선택 정확도 테스트
    collector.record_selection_accuracy(
        expected_type='distributed',
        actual_type='distributed',
        confidence_score=0.95
    )
    
    return collector.get_metrics_summary()


def simulate_production_metrics(duration_minutes: int = 5):
    """운영 환경 메트릭 시뮬레이션"""
    import random
    import time
    
    collector = get_metrics_collector()
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    metrics_count = 0
    
    while time.time() < end_time:
        # 랜덤 워크플로우 메트릭 생성
        orchestrator_type = random.choice(['standard', 'standard', 'standard', 'distributed'])  # 75% standard
        selection_time = random.uniform(5, 50)
        complexity_score = random.uniform(10, 90)
        segment_count = random.randint(10, 1000)
        estimated_events = segment_count * random.randint(3, 50)
        cache_hit = random.choice([True, True, True, False])  # 75% cache hit
        
        collector.record_orchestrator_selection(
            orchestrator_type=orchestrator_type,
            selection_time_ms=selection_time,
            complexity_score=complexity_score,
            segment_count=segment_count,
            estimated_events=estimated_events,
            cache_hit=cache_hit,
            selection_reason=f'Simulated selection {metrics_count}',
            workflow_id=f'sim_wf_{metrics_count}',
            owner_id=f'sim_user_{metrics_count % 10}'
        )
        
        metrics_count += 1
        time.sleep(random.uniform(1, 10))  # 1-10초 간격
    
    return {
        'duration_minutes': duration_minutes,
        'metrics_generated': metrics_count,
        'summary': collector.get_metrics_summary()
    }