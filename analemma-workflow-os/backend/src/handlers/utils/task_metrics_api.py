# -*- coding: utf-8 -*-
"""
Task Metrics API Handler

Bento Grid UI 전용 메트릭스 API 엔드포인트입니다.
프론트엔드가 별도의 계산 로직 없이 grid_items를 1:1로 매핑할 수 있는 형태로 제공합니다.

엔드포인트: GET /tasks/{task_id}/metrics

응답 스키마:
{
  "display": {
    "title": "execution_alias",
    "status_color": "green | yellow | red",
    "eta_text": "약 3분 남음"
  },
  "grid_items": {
    "progress": { "value": 45, "label": "진행률", "sub_text": "지침 분석 중" },
    "confidence": { "value": 88.5, "level": "High", "breakdown": { "reflection": 90, "schema": 100 } },
    "autonomy": { "value": 95, "display": "자율도 95% (우수)" },
    "intervention": { "count": 2, "summary": "승인 2회", "history": [...] }
  }
}
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

# ETA 계산을 위한 기존 node_stats 유틸리티 (WMA 기반)
try:
    from handlers.utils.node_stats_collector import get_node_stats_for_eta
except ImportError:
    get_node_stats_for_eta = None  # Lambda 배포 환경에서 사용


# =============================================================================
# DynamoDB Decimal → JSON 호환 유틸리티
# =============================================================================

class DecimalEncoder(json.JSONEncoder):
    """
    DynamoDB Decimal을 JSON 직렬화 가능한 타입으로 변환.
    
    - Decimal → int (if whole number) or float
    - 프론트엔드 차트 라이브러리가 기대하는 number 타입 보존
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            # 정수인 경우 int로, 아니면 float으로 변환
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def convert_decimals(obj: Any) -> Any:
    """
    재귀적으로 모든 Decimal을 Python native 타입으로 변환.
    
    json.loads(json.dumps(obj, cls=DecimalEncoder)) 대신 사용하여
    중간 문자열 변환 오버헤드 제거.
    """
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
TASK_TABLE = os.environ.get("TASK_TABLE", "TaskTable")
EXECUTIONS_TABLE = os.environ.get("EXECUTIONS_TABLE", "ExecutionsTable")
NODE_STATS_TABLE = os.environ.get("NODE_STATS_TABLE", "NodeStatsTable")

# DynamoDB 클라이언트
dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))
task_table = dynamodb.Table(TASK_TABLE)
executions_table = dynamodb.Table(EXECUTIONS_TABLE)
node_stats_table = dynamodb.Table(NODE_STATS_TABLE)


# =============================================================================
# Pydantic 스키마 정의
# =============================================================================

class DisplayInfo(BaseModel):
    """상단 표시 정보"""
    title: str = Field(description="작업 별칭 또는 제목")
    status_color: str = Field(description="상태 색상 (green, yellow, red)")
    eta_text: str = Field(description="예상 완료 시간 텍스트")
    status: str = Field(description="현재 상태")
    status_label: str = Field(description="상태 라벨 (한국어)")


class ProgressItem(BaseModel):
    """진행률 그리드 아이템"""
    value: int = Field(description="진행률 (0-100)")
    label: str = Field(default="진행률", description="라벨")
    sub_text: str = Field(description="현재 단계 설명")


class ConfidenceBreakdown(BaseModel):
    """신뢰도 세부 구성"""
    reflection: float = Field(description="자기 평가 점수")
    schema_match: float = Field(description="스키마 일치율", alias="schema")
    alignment: float = Field(description="지침 정합도")
    
    class Config:
        populate_by_name = True


class ConfidenceItem(BaseModel):
    """신뢰도 그리드 아이템"""
    value: float = Field(description="종합 신뢰도 (0-100)")
    level: str = Field(description="수준 (High, Medium, Low)")
    breakdown: ConfidenceBreakdown = Field(description="세부 구성")


class AutonomyItem(BaseModel):
    """
    자율도 그리드 아이템 (인간-AI 협업 밀도 측정)
    
    사용자 개입(Intervention) 시 자율도가 실시간으로 차감되어
    Analemma OS가 단순 자동화 툴이 아닌 '협업 밀도' 측정 플랫폼임을 증명.
    """
    value: float = Field(description="자율도 (0-100)")
    display: str = Field(description="표시 문자열")
    penalty_per_intervention: float = Field(
        default=5.0,
        description="개입 1회당 자율도 차감률 (%)"
    )
    intervention_impact: str = Field(
        default="",
        description="개입 영향 설명 (예: '2회 개입으로 -10% 감소')"
    )


class InterventionHistoryEntry(BaseModel):
    """개입 이력 항목"""
    timestamp: Optional[str] = Field(description="발생 시간")
    type: str = Field(description="유형 (positive, negative, neutral)")
    reason: str = Field(description="사유")
    node_id: Optional[str] = Field(description="노드 ID")


class InterventionItem(BaseModel):
    """개입 이력 그리드 아이템"""
    count: int = Field(description="총 개입 횟수")
    summary: str = Field(description="요약 텍스트")
    positive_count: int = Field(description="긍정 개입 수")
    negative_count: int = Field(description="부정 개입 수")
    history: List[InterventionHistoryEntry] = Field(description="최근 이력")


class ResourcesItem(BaseModel):
    """리소스 사용량 그리드 아이템 (Resource Efficiency 시연용)"""
    tokens: int = Field(description="사용된 토큰 수")
    cost_usd: float = Field(description="비용 (USD)")
    compute_time: str = Field(description="컴퓨팅 시간")
    cost_display: str = Field(
        default="$0.00",
        description="실시간 비용 표시 (예: '$0.05')"
    )
    efficiency_message: str = Field(
        default="",
        description="효율성 메시지 (예: '단 $0.05로 이만큼의 업무를 수행했습니다')"
    )


class GridItems(BaseModel):
    """벤토 그리드 아이템 컬렉션"""
    progress: ProgressItem
    confidence: ConfidenceItem
    autonomy: AutonomyItem
    intervention: InterventionItem
    resources: ResourcesItem


class TaskMetricsResponse(BaseModel):
    """전체 응답 스키마"""
    display: DisplayInfo
    grid_items: GridItems
    last_updated: str = Field(description="마지막 업데이트 시간")


# =============================================================================
# 메인 핸들러
# =============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    API Gateway에서 GET /tasks/{task_id}/metrics 요청 처리
    """
    try:
        # Path Parameter에서 task_id 추출
        path_params = event.get("pathParameters", {}) or {}
        task_id = path_params.get("task_id") or path_params.get("taskId")
        
        if not task_id:
            return _error_response(400, "task_id is required")
        
        # 소유자 ID 추출 (Cognito JWT claims)
        # HTTP API v2 format: requestContext.authorizer.jwt.claims.sub
        # REST API v1 format: requestContext.authorizer.claims.sub
        authorizer = event.get("requestContext", {}).get("authorizer", {})
        claims = authorizer.get("jwt", {}).get("claims", {}) or authorizer.get("claims", {})
        request_owner_id = claims.get("sub")
        
        if not request_owner_id:
            logger.error(f"No owner_id found. Event structure: {json.dumps(event.get('requestContext', {}), default=str)}")
            return _error_response(401, "Unauthorized: missing owner_id")
        
        # Query Parameter에서 tenant_id 추출 (multi-tenant 지원)
        query_params = event.get("queryStringParameters", {}) or {}
        tenant_id = query_params.get("tenant_id")
        
        # 메트릭스 조회 및 구성
        metrics = _get_task_metrics(task_id, request_owner_id, tenant_id)
        
        if not metrics:
            return _error_response(404, f"Task not found: {task_id}")
        
        # DecimalEncoder를 사용하여 number 타입 보존
        # (default=str는 모든 숫자를 문자열로 변환해 차트 라이브러리 호환성 문제 발생)
        return {
            "statusCode": 200,
            "body": json.dumps(metrics, ensure_ascii=False, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error getting task metrics: {e}", exc_info=True)
        return _error_response(500, str(e))


def _get_task_metrics(task_id: str, request_owner_id: str, tenant_id: Optional[str] = None) -> Optional[Dict]:
    """
    DynamoDB에서 Task 정보를 조회하고 Bento Grid 형식으로 변환
    소유권 검증 포함
    
    [v2.2] 개선사항:
    - 병렬 쿼리로 지연 시간 최소화
    - Decimal 타입 변환으로 JSON 호환성 보장
    
    TODO (Architecture Improvement):
    DynamoDB Streams를 활용하여 모든 메트릭 정보를 TaskMetricsTable로
    집약(Aggregated View)하면 단일 쿼리로 처리 가능.
    참고: backend/infrastructure/fargate-async-config/README.md
    """
    try:
        # [v2.2] 병렬 쿼리로 지연 시간 최소화
        task = None
        
        def query_task_table():
            return task_table.get_item(Key={"execution_id": task_id}).get("Item")
        
        def query_executions_table():
            return executions_table.get_item(Key={"execution_id": task_id}).get("Item")
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(query_task_table): "task_table",
                executor.submit(query_executions_table): "executions_table"
            }
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    task = result
                    break  # 첫 번째 결과 발견 시 종료
        
        if not task:
            return None
        
        # Decimal 타입 변환 (프론트엔드 호환성)
        task = convert_decimals(task)
        
        # 소유권 검증
        task_owner_id = task.get("ownerId") or task.get("user_id") or task.get("created_by")
        if task_owner_id != request_owner_id:
            logger.warning(f"Unauthorized access attempt: user {request_owner_id} tried to access task {task_id}")
            return None
        
        # Bento Grid 형식으로 변환
        return _format_bento_grid_response(task)
        
    except ClientError as e:
        logger.error(f"DynamoDB error: {e}")
        return None


def _format_bento_grid_response(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task 데이터를 Bento Grid 응답 형식으로 변환
    모든 Decimal을 float로 변환하여 JSON 직렬화 이슈 해결
    """
    # 상태 정보
    status = task.get("status", "UNKNOWN")
    progress = float(task.get("progress_percentage", 0) or 0)
    
    # 1. Display 정보 구성
    display = {
        "title": task.get("execution_alias") or task.get("task_summary") or f"작업 #{task.get('execution_id', '')[:8]}",
        "status_color": _get_status_color(status, progress),
        "eta_text": task.get("estimated_completion_time") or _calculate_eta_text(progress, status, task),
        "status": status,
        "status_label": _get_status_label(status)
    }
    
    # 2. Progress 아이템
    progress_item = {
        "value": progress,
        "label": "진행률",
        "sub_text": task.get("current_step_name") or task.get("current_thought") or _get_progress_text(progress)
    }
    
    # 3. Confidence 아이템
    confidence_score = float(task.get("confidence_score", 70) or 70)
    confidence_components = task.get("confidence_components", {}) or {}
    
    confidence_item = {
        "value": confidence_score,
        "level": _get_confidence_level(confidence_score),
        "breakdown": {
            "reflection": float(confidence_components.get("self_reflection", 70)),
            "schema": float(confidence_components.get("schema_match", 90)),
            "alignment": float(confidence_components.get("instruction_alignment", 70))
        }
    }
    
    # 4. Autonomy 아이템 (인간-AI 협업 밀도 측정)
    # 개입 횟수에 따라 자율도 실시간 차감
    base_autonomy = float(task.get("base_autonomy_rate", 100) or 100)
    intervention_count = int(task.get("intervention_count", 0) or 0)
    penalty_per_intervention = 5.0  # 개입 1회당 5% 차감
    
    # 자율도 = 기본값 - (개입 횟수 × 차감률), 최소 0%
    autonomy_penalty = intervention_count * penalty_per_intervention
    autonomy_rate = max(0, base_autonomy - autonomy_penalty)
    
    intervention_impact = ""
    if intervention_count > 0:
        intervention_impact = f"{intervention_count}회 개입으로 -{autonomy_penalty:.0f}% 감소"
    
    autonomy_item = {
        "value": autonomy_rate,
        "display": task.get("autonomy_display") or _get_autonomy_display(autonomy_rate),
        "penalty_per_intervention": penalty_per_intervention,
        "intervention_impact": intervention_impact
    }
    
    # 5. Intervention 아이템
    intervention = task.get("intervention_history", {}) or {}
    if isinstance(intervention, str):
        try:
            intervention = json.loads(intervention)
        except json.JSONDecodeError:
            intervention = {}
    
    intervention_item = {
        "count": int(intervention.get("total_count", 0) or 0),
        "summary": intervention.get("summary") or "개입 없음",
        "positive_count": int(intervention.get("positive_count", 0) or 0),
        "negative_count": int(intervention.get("negative_count", 0) or 0),
        "history": intervention.get("history", [])[:5]  # 최근 5개만
    }
    
    # 6. Resource Usage 아이템 (Resource Efficiency 시연)
    # "단 $X로 이만큼의 업무를 수행했습니다" 메시지로 비즈니스 파괴력 극대화
    resource_usage = task.get("resource_usage", {}) or {}
    tokens = int(resource_usage.get("tokens", 0) or 0)
    cost_usd = float(resource_usage.get("cost_usd", 0) or 0)
    compute_time = resource_usage.get("compute_time", "0s") or "0s"
    
    # 실시간 비용 표시 및 효율성 메시지 생성
    cost_display = f"${cost_usd:.2f}"
    efficiency_message = _generate_efficiency_message(cost_usd, progress, tokens)
    
    resources_item = {
        "tokens": tokens,
        "cost_usd": cost_usd,
        "compute_time": compute_time,
        "cost_display": cost_display,
        "efficiency_message": efficiency_message
    }
    
    return {
        "display": display,
        "grid_items": {
            "progress": progress_item,
            "confidence": confidence_item,
            "autonomy": autonomy_item,
            "intervention": intervention_item,
            "resources": resources_item  # 추가
        },
        "last_updated": task.get("updated_at") or datetime.now(timezone.utc).isoformat()
    }


def _get_status_color(status: str, progress: int) -> str:
    """상태와 진행률에 따른 색상 결정"""
    status_upper = status.upper()
    
    if status_upper in ("COMPLETED", "SUCCEEDED", "COMPLETE"):
        return "green"
    elif status_upper in ("FAILED", "TIMED_OUT", "ABORTED"):
        return "red"
    elif status_upper in ("PAUSED", "PAUSED_FOR_HITP", "PENDING_APPROVAL"):
        return "yellow"
    elif status_upper == "RUNNING":
        if progress >= 80:
            return "green"
        elif progress >= 40:
            return "yellow"
        else:
            return "blue"
    else:
        return "gray"


def _get_status_label(status: str) -> str:
    """상태 라벨 (한국어)"""
    labels = {
        "RUNNING": "진행 중",
        "COMPLETED": "완료",
        "SUCCEEDED": "완료",
        "COMPLETE": "완료",
        "FAILED": "실패",
        "TIMED_OUT": "시간 초과",
        "ABORTED": "중단됨",
        "PAUSED": "일시 정지",
        "PAUSED_FOR_HITP": "승인 대기",
        "PENDING_APPROVAL": "승인 대기",
        "QUEUED": "대기 중",
        "CANCELLED": "취소됨"
    }
    return labels.get(status.upper(), status)


def _calculate_eta_text(progress: float, status: str, task: Dict[str, Any]) -> str:
    """
    진행률과 상태 기반 ETA 텍스트 생성.
    
    [v2.2] 개선사항:
    - 단순 remaining_progress * avg_duration 대신
    - "남은 세그먼트들의 평균 소요 시간 합"으로 계산
    - 오차 범위 효과적으로 감소
    """
    if status.upper() in ("COMPLETED", "SUCCEEDED", "COMPLETE"):
        return "완료됨"
    elif status.upper() in ("FAILED", "TIMED_OUT", "ABORTED"):
        return "-"
    elif progress >= 95:
        return "곧 완료"
    
    try:
        # [v2.2] 세그먼트 기반 ETA 계산
        estimated_seconds = _calculate_segment_based_eta(task, progress)
        
        if estimated_seconds is not None:
            return _format_duration_text(estimated_seconds)
            
    except Exception as e:
        logger.warning(f"Failed to calculate segment-based ETA: {e}")
    
    # 폴백: 기존 하드코딩 방식
    if progress >= 80:
        return "약 1분"
    elif progress >= 50:
        return "약 2-3분"
    elif progress >= 20:
        return "약 5분"
    else:
        return "계산 중"


def _calculate_segment_based_eta(task: Dict[str, Any], progress: float) -> Optional[float]:
    """
    [v2.3] 남은 세그먼트들의 평균 소요 시간 합으로 ETA 계산.
    
    계산 우선순위:
    1. exec_status_helper 세그먼트 기반 계산 (average_segment_duration × remaining_segments)
    2. remaining_nodes[] 기반 계산 (각 노드의 WMA 통계 합산)
    3. 워크플로우 전체 평균 시간 기반 추정
    
    Returns:
        예상 남은 시간 (초), 또는 계산 불가 시 None
    """
    
    # [Priority 1] exec_status_helper에서 주입한 세그먼트 기반 ETA 사용
    # 이 방식이 가장 정확: (elapsed_time / completed_segments) × remaining_segments
    estimated_remaining = task.get("estimated_remaining_seconds")
    if estimated_remaining is not None:
        try:
            return float(estimated_remaining)
        except (ValueError, TypeError):
            pass
    
    # average_segment_duration이 있으면 직접 계산
    avg_segment_duration = task.get("average_segment_duration")
    total_segments = task.get("total_segments")
    current_segment = task.get("current_segment", 0)
    
    if avg_segment_duration and total_segments:
        try:
            remaining_segments = max(int(total_segments) - int(current_segment) - 1, 0)
            return float(avg_segment_duration) * remaining_segments
        except (ValueError, TypeError):
            pass
    
    # [Priority 2] remaining_nodes[] 기반 계산
    workflow_name = task.get("workflow_name") or "default"
    remaining_nodes = task.get("remaining_nodes", [])
    
    if remaining_nodes:
        # get_node_stats_for_eta 함수 활용 (가능한 경우)
        if get_node_stats_for_eta:
            node_types = [f"node:{workflow_name}:{node_id}" for node_id in remaining_nodes]
            stats = get_node_stats_for_eta(node_types)
            return sum(stats.values())
        else:
            # 직접 DynamoDB 쿼리 (폴백)
            total_remaining_seconds = 0.0
            for node_id in remaining_nodes:
                node_type = f"node:{workflow_name}:{node_id}"
                try:
                    response = node_stats_table.get_item(Key={"node_type": node_type})
                    node_stats = response.get("Item", {})
                    avg_duration = float(node_stats.get("avg_duration_seconds", 30))
                    total_remaining_seconds += avg_duration
                except Exception:
                    total_remaining_seconds += 30
            return total_remaining_seconds
    
    # [Priority 3] 워크플로우 전체 평균 시간 기반 추정
    workflow_type = f"workflow:{workflow_name}"
    try:
        response = node_stats_table.get_item(Key={"node_type": workflow_type})
        workflow_stats = response.get("Item", {})
        avg_total_duration = float(workflow_stats.get("avg_duration_seconds", 300))
        
        remaining_ratio = (100 - progress) / 100
        return remaining_ratio * avg_total_duration
        
    except Exception:
        return None


def _format_duration_text(seconds: float) -> str:
    """소요 시간을 사용자 친화적 텍스트로 변환."""
    if seconds < 60:
        return f"약 {int(seconds)}초"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"약 {minutes}분"
    else:
        hours = seconds / 3600
        if hours < 2:
            return f"약 {int(hours * 60)}분"  # 90분 등으로 표시
        return f"약 {hours:.1f}시간"


def _get_progress_text(progress: int) -> str:
    """진행률 기반 상태 텍스트"""
    if progress >= 90:
        return "마무리 중..."
    elif progress >= 70:
        return "결과 검증 중..."
    elif progress >= 50:
        return "데이터 처리 중..."
    elif progress >= 30:
        return "분석 진행 중..."
    elif progress >= 10:
        return "초기화 완료..."
    else:
        return "준비 중..."


def _get_confidence_level(score: float) -> str:
    """신뢰도 점수에 따른 수준"""
    if score >= 85:
        return "High"
    elif score >= 60:
        return "Medium"
    else:
        return "Low"


def _get_autonomy_display(rate: float) -> str:
    """자율도 표시 문자열"""
    if rate >= 90:
        return f"자율도 {rate:.0f}% (우수)"
    elif rate >= 70:
        return f"자율도 {rate:.0f}% (양호)"
    else:
        return f"자율도 {rate:.0f}% (개선 필요)"


def _generate_efficiency_message(cost_usd: float, progress: float, tokens: int) -> str:
    """
    Resource Efficiency 시연용 효율성 메시지 생성.
    
    "이 에이전트는 단 $X로 이만큼의 업무를 수행했습니다"
    메시지로 비즈니스 파괴력 극대화.
    """
    if cost_usd <= 0:
        return "분석 준비 중..."
    
    # 토큰당 비용 효율 계산
    cost_per_1k_tokens = (cost_usd / max(tokens, 1)) * 1000 if tokens > 0 else 0
    
    # 진행률 기반 메시지 생성
    if progress >= 100:
        return f"단 ${cost_usd:.2f}로 작업을 완료했습니다"
    elif progress >= 50:
        return f"단 ${cost_usd:.2f}로 {progress:.0f}%의 업무를 수행 중"
    else:
        return f"${cost_usd:.2f} 사용 중 ({tokens:,} 토큰)"


def _error_response(status_code: int, message: str) -> Dict[str, Any]:
    """에러 응답 생성"""
    return {
        "statusCode": status_code,
        "body": json.dumps({"error": message}, ensure_ascii=False)
    }
