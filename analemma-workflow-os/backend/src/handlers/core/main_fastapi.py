from fastapi import FastAPI, Body, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
import os
import json
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from src.common.auth_utils import extract_owner_id_from_fastapi_request
from src.services.task_service import TaskService
from src.services.checkpoint_service import CheckpointService
from src.services.time_machine_service import TimeMachineService
from src.services.plan_briefing_service import PlanBriefingService
from src.services.draft_generator import DraftResultGenerator
from src.services.crud_service import (
    ExecutionCRUDService,
    WorkflowCRUDService,
    NotificationCRUDService
)

def get_user_tier(owner_id: str) -> str:
    """
    사용자 티어 정보를 조회합니다.
    
    현재는 환경변수로 제어되며, 향후 DynamoDB나 Cognito에서 조회하도록 개선 가능합니다.
    """
    # 환경변수에서 developer/enterprise 사용자 목록을 가져옴 (빈 문자열 필터링)
    developer_users = [u.strip() for u in os.environ.get('DEVELOPER_USERS', '').split(',') if u.strip()]
    enterprise_users = [u.strip() for u in os.environ.get('ENTERPRISE_USERS', '').split(',') if u.strip()]
    
    if owner_id in developer_users:
        return 'developer'
    elif owner_id in enterprise_users:
        return 'enterprise'
    else:
        return 'free'

app = FastAPI(title="Workflow API", description="Step Functions & Lambda OpenAPI 동기화 예시", version="1.0.0")

# By default the example FastAPI app should not expose internal-only endpoints.
# To enable these endpoints for local testing, set environment variable
# ALLOW_PUBLIC_FASTAPI=true. In production the routes will return 404.
ALLOW_PUBLIC_FASTAPI = os.environ.get("ALLOW_PUBLIC_FASTAPI", "false").lower() == "true"

class StoreTaskTokenRequest(BaseModel):
    TaskToken: str
    conversation_id: Optional[str]
    execution_id: Optional[str]
    workflow_config: Optional[Dict[str, Any]]
    current_state: Optional[Dict[str, Any]]
    segment_to_run: Optional[int]

class StoreTaskTokenResponse(BaseModel):
    message: str
    conversation_id: Optional[str]
    execution_id: Optional[str]

class ResumeRequest(BaseModel):
    execution_id: Optional[str]
    conversation_id: Optional[str]
    response: str

class ResumeResponse(BaseModel):
    message: str

@app.post("/store-task-token", response_model=StoreTaskTokenResponse)
def store_task_token(req: StoreTaskTokenRequest):
    # 실제 로직은 backend/store_task_token.py 참고
    if not ALLOW_PUBLIC_FASTAPI:
        # Hide internal endpoint by default
        raise HTTPException(status_code=404, detail="Not found")
    return StoreTaskTokenResponse(message="TaskToken stored", conversation_id=req.conversation_id, execution_id=req.execution_id)

@app.post("/resume", response_model=ResumeResponse)
def resume(req: ResumeRequest):
    # 실제 로직은 backend/resume_handler.py 참고
    if not ALLOW_PUBLIC_FASTAPI:
        # Hide internal endpoint by default
        raise HTTPException(status_code=404, detail="Not found")
    return ResumeResponse(message="Step Functions resumed successfully")


# ──────────────────────────────────────────────────────────────
# Co-design Assistant API Endpoints
# ──────────────────────────────────────────────────────────────

class CodesignRequest(BaseModel):
    """Co-design 요청 모델"""
    request: str = Field(..., description="사용자 요청 메시지")
    current_workflow: Dict[str, Any] = Field(
        default_factory=lambda: {"nodes": [], "edges": []},
        description="현재 워크플로우 JSON"
    )
    recent_changes: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="최근 사용자 변경 목록"
    )
    session_id: Optional[str] = Field(None, description="세션 ID")
    mode: Optional[str] = Field("codesign", description="모드 (codesign, explain, suggest)")


class AuditRequest(BaseModel):
    """워크플로우 검증 요청 모델"""
    workflow: Dict[str, Any] = Field(
        default_factory=lambda: {"nodes": [], "edges": []},
        description="검증할 워크플로우 JSON"
    )


class SimulateRequest(BaseModel):
    """워크플로우 시뮬레이션 요청 모델"""
    workflow: Dict[str, Any] = Field(
        default_factory=lambda: {"nodes": [], "edges": []},
        description="시뮬레이션할 워크플로우 JSON"
    )
    mock_inputs: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="시뮬레이션 입력값"
    )


class ExplainRequest(BaseModel):
    """워크플로우 설명 요청 모델"""
    workflow: Dict[str, Any] = Field(
        default_factory=lambda: {"nodes": [], "edges": []},
        description="설명할 워크플로우 JSON"
    )


@app.post("/codesign")
async def codesign_endpoint(req: CodesignRequest):
    """
    Co-design Assistant 스트리밍 엔드포인트
    
    사용자 요청을 분석하고 워크플로우 수정 제안을 스트리밍으로 반환합니다.
    
    Response: JSONL 스트림
    - {"type": "node", "data": {...}}
    - {"type": "edge", "data": {...}}
    - {"type": "suggestion", "data": {...}}
    - {"type": "audit", "data": {...}}
    - {"type": "text", "data": "..."}
    - {"type": "status", "data": "done"}
    """
    from src.services.design.codesign_assistant import stream_codesign_response
    
    async def generate():
        for chunk in stream_codesign_response(
            user_request=req.request,
            current_workflow=req.current_workflow,
            recent_changes=req.recent_changes,
            session_id=req.session_id
        ):
            yield chunk
    
    return StreamingResponse(
        generate(), 
        media_type="text/plain",
        headers={
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-cache"
        }
    )


@app.post("/audit")
async def audit_endpoint(req: AuditRequest):
    """
    워크플로우 검증 엔드포인트
    
    워크플로우의 논리적 오류, 경고, 정보를 반환합니다.
    
    Response:
    {
        "issues": [
            {
                "level": "error|warning|info",
                "type": "issue_type",
                "message": "설명",
                "affected_nodes": ["node_id"],
                "suggestion": "수정 제안"
            }
        ],
        "is_valid": true/false
    }
    """
    from src.handlers.core.logical_auditor import audit_workflow
    
    issues = audit_workflow(req.workflow)
    error_count = sum(1 for i in issues if i.get("level") == "error")
    
    return {
        "issues": issues,
        "is_valid": error_count == 0,
        "summary": {
            "errors": error_count,
            "warnings": sum(1 for i in issues if i.get("level") == "warning"),
            "info": sum(1 for i in issues if i.get("level") == "info"),
            "total": len(issues)
        }
    }


@app.post("/simulate")
async def simulate_endpoint(req: SimulateRequest):
    """
    워크플로우 시뮬레이션 엔드포인트
    
    워크플로우를 실제 실행 없이 시뮬레이션하고 결과를 반환합니다.
    
    Response:
    {
        "success": true/false,
        "trace": [...],
        "errors": [...],
        "visited_nodes": [...],
        "coverage": 0.0~1.0
    }
    """
    from src.handlers.core.logical_auditor import simulate_workflow
    
    result = simulate_workflow(req.workflow, req.mock_inputs)
    return result


@app.post("/explain")
async def explain_endpoint(req: ExplainRequest):
    """
    워크플로우 설명 엔드포인트
    
    워크플로우를 자연어로 설명합니다.
    
    Response:
    {
        "summary": "워크플로우 요약",
        "steps": [...],
        "data_flow": "데이터 흐름 설명",
        "issues": [...],
        "suggestions": [...]
    }
    """
    from src.services.design.codesign_assistant import explain_workflow
    
    explanation = explain_workflow(req.workflow)
    return explanation


@app.post("/validate-schema")
async def validate_schema_endpoint(req: AuditRequest):
    """
    워크플로우 스키마 검증 엔드포인트
    
    워크플로우 JSON이 스키마에 맞는지 검증합니다.
    
    Response:
    {
        "valid": true/false,
        "errors": [...]
    }
    """
    from src.common.graph_dsl import validate_workflow
    
    errors = validate_workflow(req.workflow)
    return {
        "valid": len(errors) == 0,
        "errors": errors
    }


@app.get("/codesign/health")
async def codesign_health():
    """Co-design Assistant 헬스체크"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "features": [
            "streaming_codesign",
            "workflow_audit",
            "workflow_simulation",
            "workflow_explanation",
            "plan_briefing",
            "time_machine_debugging"
        ]
    }


# ──────────────────────────────────────────────────────────────
# Plan Briefing & Time Machine Debugging API Endpoints
# ──────────────────────────────────────────────────────────────

class PreviewRequest(BaseModel):
    """워크플로우 미리보기 요청 모델"""
    workflow_config: Dict[str, Any] = Field(
        ..., description="워크플로우 설정 (nodes, edges 포함)"
    )
    initial_statebag: Dict[str, Any] = Field(
        default_factory=dict, description="초기 상태 데이터"
    )
    user_context: Optional[Dict[str, Any]] = Field(
        None, description="사용자 컨텍스트"
    )
    use_llm: bool = Field(
        default=True, description="LLM 사용 여부"
    )


class DetailedDraftRequest(BaseModel):
    """상세 초안 요청 모델"""
    node_config: Dict[str, Any] = Field(..., description="노드 설정")
    input_data: Dict[str, Any] = Field(..., description="입력 데이터")
    output_type: str = Field(..., description="출력 타입 (email, document, etc.)")


class RollbackRequestModel(BaseModel):
    """롤백 요청 모델"""
    thread_id: str = Field(..., description="스레드 ID")
    target_checkpoint_id: str = Field(..., description="대상 체크포인트 ID")
    state_modifications: Dict[str, Any] = Field(
        default_factory=dict, description="수정할 상태 값"
    )
    reason: Optional[str] = Field(None, description="롤백 사유")
    preview_only: bool = Field(default=False, description="미리보기만 수행")


class CompareCheckpointsRequest(BaseModel):
    """체크포인트 비교 요청 모델"""
    thread_id: str = Field(..., description="스레드 ID")
    checkpoint_id_a: str = Field(..., description="첫 번째 체크포인트 ID")
    checkpoint_id_b: str = Field(..., description="두 번째 체크포인트 ID")


@app.post("/workflows/preview")
async def preview_workflow(req: PreviewRequest):
    """
    워크플로우 실행 전 미리보기 생성
    
    실행 계획 요약, 예상 결과물 초안, 위험 분석을 포함한 브리핑을 생성합니다.
    
    Response:
    {
        "briefing_id": "uuid",
        "workflow_id": "...",
        "workflow_name": "...",
        "summary": "1-2문장 요약",
        "total_steps": 5,
        "estimated_total_duration_seconds": 30,
        "steps": [...],
        "draft_results": [...],
        "overall_risk_level": "low|medium|high",
        "warnings": [...],
        "requires_confirmation": false,
        "confirmation_token": "uuid"
    }
    """
    async with PlanBriefingService() as service:
        briefing = await service.generate_briefing(
            workflow_config=req.workflow_config,
            initial_statebag=req.initial_statebag,
            user_context=req.user_context,
            use_llm=req.use_llm
        )
    
    return briefing.dict()


@app.post("/workflows/preview/detailed-draft")
async def get_detailed_draft(req: DetailedDraftRequest):
    """
    특정 노드의 상세 결과 초안 조회
    
    이메일, 문서 등 결과물의 상세 내용을 미리 생성합니다.
    
    Response:
    {
        "type": "email",
        "draft": {
            "to": ["..."],
            "subject": "...",
            "body": "..."
        },
        "warnings": [...],
        "can_edit": true
    }
    """
    generator = DraftResultGenerator()
    draft = await generator.generate_detailed_draft(
        node_config=req.node_config,
        input_data=req.input_data,
        output_type=req.output_type
    )
    
    return draft


@app.get("/executions/{thread_id}/timeline")
async def get_execution_timeline(thread_id: str, include_state: bool = True):
    """
    실행 타임라인 조회
    
    시각적 타임라인 표시용 체크포인트 목록을 반환합니다.
    
    Response:
    {
        "thread_id": "...",
        "timeline": [
            {
                "checkpoint_id": "...",
                "step": 1,
                "node_id": "...",
                "node_name": "...",
                "timestamp": "...",
                "can_rollback": true,
                "state_preview": {...}
            }
        ]
    }
    """
    service = TimeMachineService()
    timeline = await service.get_execution_timeline(
        thread_id=thread_id,
        include_state_preview=include_state
    )
    
    return {
        "thread_id": thread_id,
        "timeline": timeline
    }


@app.get("/executions/{thread_id}/checkpoints")
async def list_checkpoints(thread_id: str, limit: int = Query(50, ge=1, le=100, description="최대 조회 개수")):
    """
    스레드의 체크포인트 목록 조회
    
    Response:
    {
        "thread_id": "...",
        "checkpoints": [
            {
                "checkpoint_id": "...",
                "node_id": "...",
                "step_number": 1,
                "created_at": "...",
                "status": "active"
            }
        ]
    }
    """
    checkpoint_service = CheckpointService()
    checkpoints = checkpoint_service.list_checkpoints(thread_id, limit=limit)
    
    return {
        "thread_id": thread_id,
        "checkpoints": checkpoints
    }


@app.get("/executions/{thread_id}/checkpoints/{checkpoint_id}")
async def get_checkpoint(thread_id: str, checkpoint_id: str):
    """
    특정 체크포인트 상세 조회
    
    Response:
    {
        "checkpoint_id": "...",
        "thread_id": "...",
        "state": {...},
        "summary": {...}
    }
    """
    checkpoint_service = CheckpointService()
    state = checkpoint_service.get_at_checkpoint(thread_id, checkpoint_id)
    
    if not state:
        raise HTTPException(status_code=404, detail="Checkpoint not found")
    
    summary = checkpoint_service.get_checkpoint_summary(thread_id, checkpoint_id)
    
    return {
        "checkpoint_id": checkpoint_id,
        "thread_id": thread_id,
        "state": state,
        "summary": summary,
        "state_preview": checkpoint_service.create_state_preview(state)
    }


@app.post("/executions/rollback/preview")
async def preview_rollback(req: RollbackRequestModel):
    """
    롤백 미리보기
    
    실제 롤백 없이 결과를 예측합니다.
    
    Response:
    {
        "original_state_preview": {...},
        "modified_state_preview": {...},
        "diff": {...},
        "resume_from_node": "...",
        "estimated_impact": "..."
    }
    """
    tm_service = TimeMachineService()
    preview = await tm_service.preview_rollback(
        thread_id=req.thread_id,
        target_checkpoint_id=req.target_checkpoint_id,
        state_modifications=req.state_modifications
    )
    
    return preview


@app.post("/executions/rollback")
async def rollback_and_branch(req: RollbackRequestModel, request: Request):
    """
    체크포인트로 롤백하고 새 분기 생성
    
    지정된 체크포인트의 상태로 돌아가고 수정 사항을 적용하여
    새로운 분기 실행을 생성합니다.
    
    Response:
    {
        "success": true,
        "original_thread_id": "...",
        "branched_thread_id": "...",
        "branch_point_checkpoint_id": "...",
        "state_modifications": {...},
        "resume_from_node": "...",
        "ready_to_resume": true
    }
    """
    from src.models.checkpoint import RollbackRequest
    
    tm_service = TimeMachineService()
    
    # API Gateway Authorizer가 인증 처리, owner_id는 헤더에서 추출
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    rollback_req = RollbackRequest(
        thread_id=req.thread_id,
        target_checkpoint_id=req.target_checkpoint_id,
        state_modifications=req.state_modifications,
        reason=req.reason,
        preview_only=req.preview_only
    )
    
    # 소유권 검증: execution의 owner_id 확인
    from src.services.crud_service import ExecutionCRUDService
    from src.common.exceptions import ExecutionNotFound, ExecutionForbidden
    
    execution_service = ExecutionCRUDService()
    
    # thread_id는 execution_id와 동일함 (segment_runner_handler.py:1065 참조)
    execution_status = execution_service.get_status(owner_id, req.thread_id)
    
    if not execution_status:
        logger.warning(f"Execution {req.thread_id} not found or access denied for user {owner_id}")
        raise HTTPException(
            status_code=404, 
            detail="Execution not found or access denied"
        )
    
    logger.info(f"Execution ownership verified for {req.thread_id} by user {owner_id}")
    
    new_thread_id, modified_state, branch_info = await tm_service.rollback_and_branch(
        request=rollback_req,
        user_id=owner_id
    )
    
    return {
        "success": True,
        "original_thread_id": branch_info.original_thread_id,
        "branched_thread_id": branch_info.branched_thread_id,
        "branch_point_checkpoint_id": branch_info.branch_point_checkpoint_id,
        "state_modifications": branch_info.state_modifications,
        "branch_depth": branch_info.branch_depth,
        "resume_from_node": branch_info.resume_from_node,
        "ready_to_resume": branch_info.ready_to_resume
    }


@app.post("/executions/checkpoints/compare")
async def compare_checkpoints(req: CompareCheckpointsRequest):
    """
    두 체크포인트 간 상태 비교
    
    Response:
    {
        "checkpoint_a": "...",
        "checkpoint_b": "...",
        "added": {...},
        "removed": {...},
        "modified": {...},
        "unchanged_count": 10,
        "total_changes": 3
    }
    """
    tm_service = TimeMachineService()
    result = await tm_service.compare_checkpoints(
        thread_id=req.thread_id,
        checkpoint_id_a=req.checkpoint_id_a,
        checkpoint_id_b=req.checkpoint_id_b
    )
    
    return result


@app.get("/executions/{thread_id}/branches")
async def get_branch_history(thread_id: str):
    """
    스레드의 분기 히스토리 조회
    
    Response:
    {
        "thread_id": "...",
        "branches": [...]
    }
    """
    tm_service = TimeMachineService()
    branches = await tm_service.get_branch_history(thread_id)
    
    return {
        "thread_id": thread_id,
        "branches": branches
    }


@app.get("/executions/{thread_id}/rollback-suggestions")
async def get_rollback_suggestions(thread_id: str):
    """
    롤백 추천 지점 조회
    
    에러 발생 시 가장 적합한 롤백 지점을 추천합니다.
    
    Response:
    {
        "thread_id": "...",
        "suggestions": [
            {
                "checkpoint_id": "...",
                "node_id": "...",
                "reason": "마지막 성공 지점",
                "priority": 3
            }
        ]
    }
    """
    tm_service = TimeMachineService()
    suggestions = await tm_service.suggest_rollback_points(thread_id)
    
    return {
        "thread_id": thread_id,
        "suggestions": suggestions
    }


# ──────────────────────────────────────────────────────────────
# Task Manager API Endpoints
# ──────────────────────────────────────────────────────────────

class TaskListRequest(BaseModel):
    """Task 목록 조회 요청 모델"""
    status_filter: Optional[str] = Field(
        None, 
        description="상태 필터 (pending_approval, in_progress, completed, failed)"
    )
    limit: int = Field(default=50, ge=1, le=100, description="최대 조회 개수")
    include_completed: bool = Field(default=True, description="완료된 Task 포함 여부")


@app.get("/tasks")
async def list_tasks(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=100, description="최대 조회 개수"),
    include_completed: bool = True
):
    """
    Task 목록 조회 (비즈니스 관점)
    
    기존 /executions와 달리 비즈니스 친화적인 형식으로 반환합니다.
    
    Query Parameters:
    - status: 상태 필터 (pending_approval, in_progress, completed, failed)
    - limit: 최대 조회 개수 (기본 50)
    - include_completed: 완료된 Task 포함 여부 (기본 true)
    
    Response:
    {
        "tasks": [
            {
                "task_id": "...",
                "task_summary": "11월 미수금 정산 보고서 작성",
                "agent_name": "AI Assistant",
                "status": "in_progress",
                "progress_percentage": 45,
                "current_thought": "데이터를 분석하고 있습니다...",
                "is_interruption": false,
                "started_at": "...",
                "updated_at": "..."
            }
        ],
        "total": 10,
        "filters_applied": {
            "status": "in_progress",
            "include_completed": true
        }
    }
    """
    service = TaskService()
    
    # API Gateway Authorizer가 인증 처리, owner_id는 헤더에서 추출
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    tasks = await service.get_tasks(
        owner_id=owner_id,
        status_filter=status,
        limit=limit,
        include_completed=include_completed
    )
    
    return {
        "tasks": tasks,
        "total": len(tasks),
        "filters_applied": {
            "status": status,
            "include_completed": include_completed
        }
    }


@app.get("/tasks/{task_id}")
async def get_task_detail(
    task_id: str,
    request: Request,
    include_technical_logs: bool = False
):
    """
    Task 상세 정보 조회
    
    Query Parameters:
    - include_technical_logs: 기술 로그 포함 여부 (권한 필요)
    
    Response:
    {
        "task_id": "...",
        "task_summary": "...",
        "agent_name": "AI Assistant",
        "status": "pending_approval",
        "progress_percentage": 75,
        "current_step_name": "승인 대기",
        "current_thought": "사용자의 승인을 기다리고 있습니다.",
        "pending_decision": {
            "question": "계속 진행하시겠습니까?",
            "context": "...",
            "options": [...]
        },
        "artifacts": [...],
        "thought_history": [...],
        "error_message": null,
        "technical_logs": [...] // include_technical_logs=true인 경우만
    }
    """
    service = TaskService()
    
    # API Gateway Authorizer가 인증 처리, owner_id는 헤더에서 추출
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    # 권한 확인 (Developer/Enterprise 티어만 technical_logs 접근 가능)
    user_tier = get_user_tier(owner_id)
    if include_technical_logs and user_tier not in ['developer', 'enterprise']:
        include_technical_logs = False
    
    task = await service.get_task_detail(
        task_id=task_id,
        owner_id=owner_id,
        include_technical_logs=include_technical_logs
    )
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return task


# ──────────────────────────────────────────────────────────────
# CRUD API Endpoints (Lambda 핸들러 통합)
# ──────────────────────────────────────────────────────────────

# --- Execution CRUD ---

@app.get("/executions")
async def list_executions(
    request: Request,
    limit: int = Query(20, ge=1, le=100, description="최대 조회 개수"),
    nextToken: Optional[str] = None
):
    """
    내 실행 목록 조회
    
    기존 Lambda: list_my_executions
    
    Query Parameters:
    - limit: 조회 개수 (기본 20, 최대 100)
    - nextToken: 페이지네이션 토큰
    
    Response:
    {
        "items": [...],
        "nextToken": "..."
    }
    """
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = ExecutionCRUDService()
    
    try:
        items, next_token = service.list_executions(
            owner_id=owner_id,
            limit=limit,
            next_token=nextToken
        )
        return {"items": items, "nextToken": next_token}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_execution_status(
    request: Request,
    executionArn: Optional[str] = None,
    execution_arn: Optional[str] = None
):
    """
    실행 상태 조회
    
    기존 Lambda: get_status
    프론트엔드 호출: GET /status?executionArn=...
    
    Response:
    {
        "executionArn": "...",
        "status": "RUNNING",
        "startDate": "...",
        "workflowId": "..."
    }
    """
    # executionArn 또는 execution_arn 둘 다 지원
    execution_id = executionArn or execution_arn
    if not execution_id:
        raise HTTPException(status_code=400, detail="Missing executionArn in query string")
    
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = ExecutionCRUDService()
    
    try:
        result = service.get_status(owner_id=owner_id, execution_arn=execution_id)
        if not result:
            raise HTTPException(status_code=404, detail="Execution not found")
        return result
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/executions/history")
async def get_execution_history(
    request: Request,
    executionArn: Optional[str] = None,
    execution_arn: Optional[str] = None
):
    """
    실행 히스토리 조회 (S3 Claim Check 지원)
    
    기존 Lambda: get_execution_history
    프론트엔드 호출: GET /executions/history?executionArn=...
    
    Response:
    {
        "executionArn": "...",
        "state_history": [...],
        ...
    }
    """
    # executionArn 또는 execution_arn 둘 다 지원
    execution_id = executionArn or execution_arn
    if not execution_id:
        raise HTTPException(status_code=400, detail="Missing executionArn in query string")
    
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = ExecutionCRUDService()
    
    try:
        result = service.get_execution_history(owner_id=owner_id, execution_arn=execution_id)
        if not result:
            raise HTTPException(status_code=404, detail="Execution not found")
        return result
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/executions")
async def delete_execution(
    request: Request,
    executionArn: Optional[str] = None,
    execution_arn: Optional[str] = None
):
    """
    실행 삭제
    
    기존 Lambda: delete_execution
    프론트엔드 호출: DELETE /executions?executionArn=...
    
    Response:
    {
        "message": "Execution deleted successfully"
    }
    """
    # executionArn 또는 execution_arn 둘 다 지원
    execution_id = executionArn or execution_arn
    if not execution_id:
        raise HTTPException(status_code=400, detail="Missing executionArn in query string")
    
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = ExecutionCRUDService()
    
    try:
        success = service.delete_execution(owner_id=owner_id, execution_arn=execution_id)
        if not success:
            raise HTTPException(status_code=404, detail="Execution not found or not authorized")
        return {"message": "Execution deleted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Workflow CRUD ---

class SaveWorkflowRequest(BaseModel):
    """워크플로우 저장 요청"""
    workflowId: Optional[str] = Field(None, description="워크플로우 ID (없으면 새로 생성)")
    name: str = Field(..., description="워크플로우 이름")
    config: Dict[str, Any] = Field(..., description="워크플로우 설정 (nodes, edges 등)")
    description: Optional[str] = Field(None, description="설명")


@app.get("/workflows/{workflow_id}")
async def get_workflow(
    workflow_id: str,
    request: Request,
    version: Optional[str] = None
):
    """
    워크플로우 조회
    
    기존 Lambda: get_workflow
    
    Query Parameters:
    - version: 버전 (없으면 최신 버전)
    
    Response:
    {
        "pk": "workflow-id",
        "sk": "v0",
        "name": "...",
        "config": {...}
    }
    """
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = WorkflowCRUDService()
    
    try:
        result = service.get_workflow(
            owner_id=owner_id,
            workflow_id=workflow_id,
            version=version
        )
        if not result:
            raise HTTPException(status_code=404, detail="Workflow not found")
        return result
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflows/by-name/{name}")
async def get_workflow_by_name(
    name: str,
    request: Request
):
    """
    이름으로 워크플로우 조회
    
    기존 Lambda: get_workflow_by_name
    
    Response:
    {
        "pk": "workflow-id",
        "name": "...",
        "config": {...}
    }
    """
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = WorkflowCRUDService()
    
    try:
        result = service.get_workflow_by_name(owner_id=owner_id, name=name)
        if not result:
            raise HTTPException(status_code=404, detail="Workflow not found")
        return result
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/workflows/{workflow_id}")
async def delete_workflow(
    workflow_id: str,
    request: Request,
    delete_all_versions: bool = False
):
    """
    워크플로우 삭제
    
    기존 Lambda: delete_workflow
    
    Query Parameters:
    - delete_all_versions: 모든 버전 삭제 여부 (기본 false)
    
    Response:
    {
        "message": "Workflow deleted successfully"
    }
    """
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = WorkflowCRUDService()
    
    try:
        success = service.delete_workflow(
            owner_id=owner_id,
            workflow_id=workflow_id,
            delete_all_versions=delete_all_versions
        )
        if not success:
            raise HTTPException(status_code=404, detail="Workflow not found or not authorized")
        return {"message": "Workflow deleted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveWorkflowBodyRequest(BaseModel):
    """워크플로우 저장 요청 (Body)"""
    name: Optional[str] = Field(None, description="워크플로우 이름")
    config: Dict[str, Any] = Field(..., description="워크플로우 설정 (nodes, edges 등)")
    description: Optional[str] = Field(None, description="설명")
    is_scheduled: Optional[bool] = Field(None, description="스케줄 여부")
    next_run_time: Optional[Any] = Field(None, description="다음 실행 시간")


@app.put("/workflows/{workflow_id}")
async def update_workflow(
    workflow_id: str,
    req: SaveWorkflowBodyRequest,
    request: Request
):
    """
    워크플로우 수정 (PUT /workflows/{id})
    
    프론트엔드 호출: PUT /workflows/{workflowId}
    
    Response:
    {
        "workflowId": "...",
        "message": "Workflow saved successfully"
    }
    """
    from src.handlers.utils.save_workflow import lambda_handler as save_workflow_handler
    
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    # Lambda 핸들러 형식의 이벤트 생성
    lambda_event = {
        'requestContext': {
            'authorizer': {
                'jwt': {
                    'claims': {
                        'sub': owner_id
                    }
                }
            }
        },
        'pathParameters': {'id': workflow_id},
        'body': json.dumps({
            'workflowId': workflow_id,
            'name': req.name,
            'config': req.config,
            'description': req.description,
            'is_scheduled': req.is_scheduled,
            'next_run_time': req.next_run_time
        })
    }
    
    result = save_workflow_handler(lambda_event, None)
    
    if result.get('statusCode', 200) >= 400:
        body = json.loads(result.get('body', '{}'))
        raise HTTPException(
            status_code=result.get('statusCode', 500),
            detail=body.get('error', 'Failed to save workflow')
        )
    
    return json.loads(result.get('body', '{}'))


@app.put("/workflows")
@app.post("/workflows")
async def save_workflow(
    req: SaveWorkflowRequest,
    request: Request
):
    """
    워크플로우 저장 (생성/수정)
    
    기존 Lambda: save_workflow
    
    복잡한 로직 (버전관리, S3 저장, 파티셔닝)을 처리합니다.
    기존 save_workflow Lambda 핸들러의 로직을 재사용합니다.
    
    Response:
    {
        "workflowId": "...",
        "message": "Workflow saved successfully"
    }
    """
    from src.handlers.utils.save_workflow import lambda_handler as save_workflow_handler
    
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    # Lambda 핸들러 형식의 이벤트 생성
    lambda_event = {
        'requestContext': {
            'authorizer': {
                'jwt': {
                    'claims': {
                        'sub': owner_id
                    }
                }
            }
        },
        'body': json.dumps({
            'workflowId': req.workflowId,
            'name': req.name,
            'config': req.config,
            'description': req.description
        })
    }
    
    # 기존 Lambda 핸들러 호출
    result = save_workflow_handler(lambda_event, None)
    
    if result.get('statusCode', 200) >= 400:
        body = json.loads(result.get('body', '{}'))
        raise HTTPException(
            status_code=result.get('statusCode', 500),
            detail=body.get('error', 'Failed to save workflow')
        )
    
    return json.loads(result.get('body', '{}'))


# --- Notification CRUD ---

@app.get("/notifications")
async def list_notifications(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=100, description="최대 조회 개수")
):
    """
    알림 목록 조회
    
    기존 Lambda: list_notifications
    
    Query Parameters:
    - status: 상태 필터 (unread, dismissed)
    - limit: 조회 개수 (기본 50)
    
    Response:
    {
        "notifications": [...],
        "total": 10
    }
    """
    owner_id = extract_owner_id_from_event({"headers": dict(request.headers)}) or "default"
    
    service = NotificationCRUDService()
    
    try:
        notifications, _ = service.list_notifications(
            owner_id=owner_id,
            status=status,
            limit=limit
        )
        return {
            "notifications": notifications,
            "total": len(notifications)
        }
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


class DismissNotificationRequest(BaseModel):
    """알림 무시 요청 모델"""
    executionId: str = Field(..., description="실행 ARN (알림 ID로 사용)")


@app.post("/notifications/dismiss")
async def dismiss_notification(
    req: DismissNotificationRequest,
    request: Request
):
    """
    알림 무시 처리
    
    기존 Lambda: dismiss_notification
    프론트엔드 호출: POST /notifications/dismiss (body: { executionId: ... })
    
    Response:
    {
        "message": "Notification dismissed successfully"
    }
    """
    owner_id = extract_owner_id_from_fastapi_request(request) or "default"
    
    service = NotificationCRUDService()
    
    try:
        success = service.dismiss_notification(
            owner_id=owner_id,
            notification_id=req.executionId
        )
        if not success:
            raise HTTPException(status_code=404, detail="Notification not found or not authorized")
        return {"message": "Notification dismissed successfully"}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
