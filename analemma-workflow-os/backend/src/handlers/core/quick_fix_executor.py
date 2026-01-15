"""
Quick Fix Executor Lambda
==========================

동적 에러 복구 액션을 실행하는 Lambda 핸들러.

Quick Fix 유형:
- RETRY: 동일 노드 재시도 (일시적 오류)
- REDIRECT: 대체 경로로 우회 (리소스 가용성 문제)
- SELF_HEALING: LLM 기반 자동 수정 (구문/논리 오류)
- INPUT: 사용자 입력 요청 (필수 정보 누락)
- ESCALATE: 관리자 에스컬레이션 (권한/보안 문제)

Author: Analemma Team
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from enum import Enum

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
dynamodb = boto3.resource("dynamodb")
sfn_client = boto3.client("stepfunctions")
sns_client = boto3.client("sns")

# Environment Variables - 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
EXECUTIONS_TABLE = os.environ.get("EXECUTIONS_TABLE", "ExecutionsTableV3")
# 🚨 [Critical Fix] 환경변수 통일: TASK_TOKENS_TABLE_NAME 우선 사용
TASK_TOKENS_TABLE = os.environ.get("TASK_TOKENS_TABLE_NAME", os.environ.get("TASK_TOKENS_TABLE", "TaskTokensTableV3"))
WORKFLOW_ORCHESTRATOR_ARN = os.environ.get("WORKFLOW_ORCHESTRATOR_ARN", "")
WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN = os.environ.get("WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN", "")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

# Circuit Breaker 설정
MAX_RETRY_ATTEMPTS = int(os.environ.get("MAX_RETRY_ATTEMPTS", "3"))
MAX_SELF_HEALING_ATTEMPTS = int(os.environ.get("MAX_SELF_HEALING_ATTEMPTS", "3"))
CIRCUIT_BREAKER_WINDOW_MINUTES = int(os.environ.get("CIRCUIT_BREAKER_WINDOW_MINUTES", "60"))


class QuickFixType(str, Enum):
    """Quick Fix 유형"""
    RETRY = "retry"
    REDIRECT = "redirect"
    SELF_HEALING = "self_healing"
    INPUT = "input"
    ESCALATE = "escalate"


def lambda_handler(event: dict, context: Any) -> dict:
    """
    POST /tasks/quick-fix
    
    Request Body:
    {
        "task_id": "exec-123",
        "fix_type": "retry",
        "payload": { ... },  # fix_type별 추가 데이터
        "owner_id": "user-abc"
    }
    
    Response:
    {
        "success": true,
        "action_taken": "retry_initiated",
        "new_execution_arn": "arn:aws:states:...",
        "message": "재시도가 시작되었습니다."
    }
    """
    logger.info(f"Quick Fix request: {json.dumps(event)}")
    
    try:
        # Parse request body
        body = _parse_request_body(event)
        
        task_id = body.get("task_id")
        fix_type = body.get("fix_type")
        payload = body.get("payload", {})
        owner_id = _extract_owner_id(event, body)
        
        if not task_id or not fix_type:
            return _error_response(400, "task_id와 fix_type은 필수입니다.")
        
        # Validate fix type
        try:
            fix_type_enum = QuickFixType(fix_type)
        except ValueError:
            return _error_response(400, f"유효하지 않은 fix_type: {fix_type}")
        
        # Execute quick fix based on type
        result = _execute_quick_fix(
            task_id=task_id,
            fix_type=fix_type_enum,
            payload=payload,
            owner_id=owner_id
        )
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(result, ensure_ascii=False)
        }
        
    except PermissionError as e:
        logger.warning(f"Quick Fix permission denied: {str(e)}")
        return _error_response(403, str(e))
        
    except Exception as e:
        logger.exception("Quick Fix execution failed")
        return _error_response(500, f"Quick Fix 실행 실패: {str(e)}")


def _execute_quick_fix(
    task_id: str,
    fix_type: QuickFixType,
    payload: dict,
    owner_id: str
) -> dict:
    """Quick Fix 유형별 실행 로직"""
    
    # Get execution details
    execution = _get_execution(owner_id, task_id)
    if not execution:
        raise ValueError(f"Execution not found: {task_id}")
    
    # IDOR Prevention: 소유권 검증
    db_owner_id = execution.get("ownerId")
    if db_owner_id and db_owner_id != owner_id:
        logger.warning(f"IDOR attempt blocked: requested by {owner_id}, owned by {db_owner_id}")
        raise PermissionError("이 작업에 대한 접근 권한이 없습니다.")
    
    # Circuit Breaker 검증 (RETRY, SELF_HEALING에만 적용)
    if fix_type in (QuickFixType.RETRY, QuickFixType.SELF_HEALING):
        _check_circuit_breaker(execution, fix_type)
    
    handlers = {
        QuickFixType.RETRY: _handle_retry,
        QuickFixType.REDIRECT: _handle_redirect,
        QuickFixType.SELF_HEALING: _handle_self_healing,
        QuickFixType.INPUT: _handle_input,
        QuickFixType.ESCALATE: _handle_escalate,
    }
    
    handler = handlers.get(fix_type)
    if not handler:
        raise ValueError(f"No handler for fix_type: {fix_type}")
    
    return handler(execution, payload, owner_id)


def _handle_retry(execution: dict, payload: dict, owner_id: str) -> dict:
    """
    RETRY: 동일한 워크플로우를 처음부터 재시작
    
    사용 사례:
    - LLM API 일시적 오류
    - 네트워크 타임아웃
    - 리소스 부족으로 인한 일시적 실패
    """
    workflow_input = execution.get("input", {})
    if isinstance(workflow_input, str):
        try:
            workflow_input = json.loads(workflow_input)
        except json.JSONDecodeError:
            workflow_input = {}
    
    # Retry count 추적 (Atomic Update로 레이스 컨디션 방지)
    current_retry_count = _increment_retry_count(owner_id, original_arn, "retry_count")
    
    # Determine orchestrator - DB에 저장된 ARN 우선 사용
    original_arn = execution.get("executionArn", "")
    state_machine_arn = _resolve_state_machine_arn(execution, original_arn)
    
    # Add retry metadata
    workflow_input["_retry_metadata"] = {
        "original_execution_arn": original_arn,
        "retry_timestamp": datetime.now(timezone.utc).isoformat(),
        "retry_reason": payload.get("reason", "user_initiated_retry")
    }
    
    # Start new execution
    new_execution = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(workflow_input)
    )
    
    # Update original execution with retry info (retry_execution_arn만 업데이트)
    _update_execution_status(
        owner_id=owner_id,
        execution_arn=original_arn,
        updates={
            "retry_execution_arn": new_execution["executionArn"],
            "retried_at": datetime.now(timezone.utc).isoformat()
        }
    )
    
    return {
        "success": True,
        "action_taken": "retry_initiated",
        "new_execution_arn": new_execution["executionArn"],
        "retry_count": current_retry_count,
        "max_retries": MAX_RETRY_ATTEMPTS,
        "message": f"워크플로우가 재시작되었습니다. ({current_retry_count}/{MAX_RETRY_ATTEMPTS})"
    }


def _handle_redirect(execution: dict, payload: dict, owner_id: str) -> dict:
    """
    REDIRECT: 대체 경로나 폴백 노드로 우회
    
    사용 사례:
    - 특정 LLM 모델 서비스 불가
    - 외부 API 장애
    - 리전별 가용성 문제
    """
    redirect_target = payload.get("redirect_target")
    if not redirect_target:
        raise ValueError("redirect_target이 필요합니다.")
    
    original_arn = execution.get("executionArn", "")
    workflow_input = execution.get("input", {})
    if isinstance(workflow_input, str):
        try:
            workflow_input = json.loads(workflow_input)
        except json.JSONDecodeError:
            workflow_input = {}
    
    # Add redirect metadata
    workflow_input["_redirect_metadata"] = {
        "original_execution_arn": original_arn,
        "redirect_target": redirect_target,
        "redirect_timestamp": datetime.now(timezone.utc).isoformat(),
        "redirect_reason": payload.get("reason", "resource_unavailable")
    }
    
    # Apply redirect config
    if redirect_target == "fallback_model":
        workflow_input["model_override"] = payload.get("fallback_model", "claude-3-haiku")
    elif redirect_target == "alternative_provider":
        workflow_input["provider_override"] = payload.get("alternative_provider", "bedrock")
    
    # Determine orchestrator - DB에 저장된 ARN 우선 사용
    state_machine_arn = _resolve_state_machine_arn(execution, original_arn)
    
    # Start redirected execution
    new_execution = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(workflow_input)
    )
    
    return {
        "success": True,
        "action_taken": "redirect_initiated",
        "new_execution_arn": new_execution["executionArn"],
        "redirect_target": redirect_target,
        "message": f"대체 경로로 우회되었습니다: {redirect_target}"
    }


def _handle_self_healing(execution: dict, payload: dict, owner_id: str) -> dict:
    """
    SELF_HEALING: LLM 기반 자동 수정 후 재시작
    
    사용 사례:
    - 프롬프트 구문 오류
    - 스키마 불일치
    - 논리적 모순
    """
    # Self-healing은 현재 원본 입력에 수정 힌트를 추가하여 재시작
    original_arn = execution.get("executionArn", "")
    workflow_input = execution.get("input", {})
    if isinstance(workflow_input, str):
        try:
            workflow_input = json.loads(workflow_input)
        except json.JSONDecodeError:
            workflow_input = {}
    
    # Self-healing count 추적 (Atomic Update로 레이스 컨디션 방지)
    current_healing_count = _increment_retry_count(owner_id, original_arn, "self_healing_count")
    
    # 정확한 에러 맥락 확보: Step Functions 실행 히스토리에서 마지막 실패 이벤트 추출
    error_context = _get_last_failure_context(original_arn)
    suggested_fix = payload.get("suggested_fix", "")
    
    # Add self-healing metadata for the workflow to use
    workflow_input["_self_healing_metadata"] = {
        "original_execution_arn": original_arn,
        "error_context": error_context,
        "suggested_fix": suggested_fix,
        "healing_timestamp": datetime.now(timezone.utc).isoformat(),
        "enable_auto_correction": True,
        "healing_attempt": current_healing_count
    }
    
    # Determine orchestrator - DB에 저장된 ARN 우선 사용
    state_machine_arn = _resolve_state_machine_arn(execution, original_arn)
    
    # Start self-healing execution
    new_execution = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(workflow_input)
    )
    
    # Update healing count (healing_execution_arn만 업데이트)
    _update_execution_status(
        owner_id=execution.get("ownerId", "unknown"),
        execution_arn=original_arn,
        updates={
            "healing_execution_arn": new_execution["executionArn"]
        }
    )
    
    return {
        "success": True,
        "action_taken": "self_healing_initiated",
        "new_execution_arn": new_execution["executionArn"],
        "healing_count": current_healing_count,
        "max_healing_attempts": MAX_SELF_HEALING_ATTEMPTS,
        "message": f"자동 수정 후 재시작되었습니다. ({current_healing_count}/{MAX_SELF_HEALING_ATTEMPTS})"
    }


def _handle_input(execution: dict, payload: dict, owner_id: str) -> dict:
    """
    INPUT: HITL Task Token으로 사용자 입력 대기 상태 생성
    
    사용 사례:
    - 필수 정보 누락
    - 모호한 지시사항
    - 사용자 확인 필요
    """
    # Get the task token if available (for HITL resume)
    task_token = _get_task_token(owner_id, execution.get("executionArn", ""))
    
    required_fields = payload.get("required_fields", [])
    prompt_message = payload.get("prompt_message", "추가 정보를 입력해주세요.")
    
    if task_token:
        # There's an active HITL session - update pending notification
        return {
            "success": True,
            "action_taken": "input_requested",
            "task_token_exists": True,
            "required_fields": required_fields,
            "prompt_message": prompt_message,
            "message": "사용자 입력 대기 중입니다. HITL 패널에서 응답해주세요."
        }
    else:
        # No active HITL - this is informational
        return {
            "success": True,
            "action_taken": "input_info_provided",
            "task_token_exists": False,
            "required_fields": required_fields,
            "prompt_message": prompt_message,
            "message": "워크플로우를 수정하여 필수 입력을 추가하세요."
        }


def _handle_escalate(execution: dict, payload: dict, owner_id: str) -> dict:
    """
    ESCALATE: 관리자 에스컬레이션
    
    사용 사례:
    - 권한 부족
    - 보안 정책 위반
    - 시스템 레벨 오류
    """
    escalation_reason = payload.get("reason", "manual_escalation")
    priority = payload.get("priority", "normal")
    
    # Record escalation in execution metadata
    original_arn = execution.get("executionArn", "")
    _update_execution_status(
        owner_id=owner_id,
        execution_arn=original_arn,
        updates={
            "escalation": {
                "status": "pending",
                "reason": escalation_reason,
                "priority": priority,
                "escalated_at": datetime.now(timezone.utc).isoformat(),
                "escalated_by": owner_id
            }
        }
    )
    
    # 즉각적인 관리자 알림 (SNS 연동)
    if ALERT_TOPIC_ARN:
        try:
            sns_client.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Subject=f"🚨 Analemma Escalation: {execution.get('task_id', 'Unknown')}",
                Message=f"User: {owner_id}\nReason: {escalation_reason}\nPriority: {priority}\nExecution ARN: {original_arn}\nError Context: {payload.get('error_context', 'N/A')}"
            )
            logger.info(f"Escalation alert sent to SNS topic: {ALERT_TOPIC_ARN}")
        except ClientError as e:
            logger.error(f"Failed to send escalation alert: {e}")
    
    # TODO: In production, integrate with ticketing system (Jira, PagerDuty, etc.)
    
    return {
        "success": True,
        "action_taken": "escalation_created",
        "priority": priority,
        "message": f"관리자에게 에스컬레이션되었습니다. (우선순위: {priority})"
    }


# --- Helper Functions ---

def _parse_request_body(event: dict) -> dict:
    """Parse request body from API Gateway event"""
    body = event.get("body", "{}")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def _extract_owner_id(event: dict, body: dict) -> str:
    """Extract owner_id from JWT claims or request body"""
    from src.common.auth_utils import extract_owner_id_from_event
    
    # Try JWT claims first
    owner_id = extract_owner_id_from_event(event)
    if owner_id:
        return owner_id
    
    # Fallback to request body
    return body.get("owner_id", "anonymous")


def _get_execution(owner_id: str, execution_arn: str) -> Optional[dict]:
    """Get execution details from DynamoDB"""
    table = dynamodb.Table(EXECUTIONS_TABLE)
    
    try:
        response = table.get_item(
            Key={
                "ownerId": owner_id,
                "executionArn": execution_arn
            }
        )
        return response.get("Item")
    except ClientError as e:
        logger.error(f"Failed to get execution: {e}")
        return None


def _get_task_token(owner_id: str, execution_arn: str) -> Optional[str]:
    """Get active task token for HITL resume"""
    table = dynamodb.Table(TASK_TOKENS_TABLE)
    
    # 🚨 [Critical Fix] GSI 이름을 환경변수에서 가져옴 (template.yaml과 일치)
    execution_id_index = os.environ.get('EXECUTION_ID_INDEX', 'ExecutionIdIndex')
    
    try:
        response = table.query(
            IndexName=execution_id_index,
            KeyConditionExpression="ownerId = :oid AND execution_id = :eid",
            ExpressionAttributeValues={
                ":oid": owner_id,
                ":eid": execution_arn
            },
            Limit=1
        )
        items = response.get("Items", [])
        if items:
            return items[0].get("task_token")
        return None
    except ClientError as e:
        logger.error(f"Failed to get task token: {e}")
        return None


def _update_execution_status(owner_id: str, execution_arn: str, updates: dict) -> None:
    """Update execution record in DynamoDB"""
    table = dynamodb.Table(EXECUTIONS_TABLE)
    
    update_expr_parts = []
    expr_attr_values = {}
    expr_attr_names = {}
    
    for idx, (key, value) in enumerate(updates.items()):
        placeholder = f":val{idx}"
        name_placeholder = f"#attr{idx}"
        update_expr_parts.append(f"{name_placeholder} = {placeholder}")
        expr_attr_values[placeholder] = value
        expr_attr_names[name_placeholder] = key
    
    if not update_expr_parts:
        return
    
    try:
        table.update_item(
            Key={
                "ownerId": owner_id,
                "executionArn": execution_arn
            },
            UpdateExpression="SET " + ", ".join(update_expr_parts),
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names
        )
    except ClientError as e:
        logger.error(f"Failed to update execution: {e}")


def _error_response(status_code: int, message: str) -> dict:
    """Generate error response"""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "success": False,
            "error": message
        }, ensure_ascii=False)
    }


def _check_circuit_breaker(execution: dict, fix_type: QuickFixType) -> None:
    """
    Circuit Breaker: 태스크별 누적 재시도/자가 치유 횟수를 제한하여 API 비용 폭증 방지.
    
    MAX_RETRY_ATTEMPTS 또는 MAX_SELF_HEALING_ATTEMPTS를 초과하면
    PermissionError를 발생시켜 ESCALATE로 강제 전환을 유도합니다.
    
    정책: 태스크별 누적 한도 (시간 윈도우 제거로 코드 간소화)
    """
    if fix_type == QuickFixType.RETRY:
        retry_count = execution.get("retry_count", 0)
        
        if retry_count >= MAX_RETRY_ATTEMPTS:
            logger.warning(
                f"Circuit breaker triggered: retry_count={retry_count}, "
                f"max={MAX_RETRY_ATTEMPTS}"
            )
            raise PermissionError(
                f"재시도 횟수가 한도({MAX_RETRY_ATTEMPTS}회)를 초과했습니다. "
                f"관리자에게 에스컬레이션하세요."
            )
    
    elif fix_type == QuickFixType.SELF_HEALING:
        healing_count = execution.get("self_healing_count", 0)
        
        if healing_count >= MAX_SELF_HEALING_ATTEMPTS:
            logger.warning(
                f"Circuit breaker triggered: healing_count={healing_count}, "
                f"max={MAX_SELF_HEALING_ATTEMPTS}"
            )
            raise PermissionError(
                f"자가 치유 횟수가 한도({MAX_SELF_HEALING_ATTEMPTS}회)를 초과했습니다. "
                f"관리자에게 에스컬레이션하세요."
            )


def _increment_retry_count(owner_id: str, execution_arn: str, field_name: str) -> int:
    """
    DynamoDB 내부에서 카운트를 안전하게 1 증가시키고, 새로운 카운트 값을 반환합니다.
    레이스 컨디션을 방지하기 위해 Atomic Update를 사용합니다.
    """
    table = dynamodb.Table(EXECUTIONS_TABLE)
    
    try:
        response = table.update_item(
            Key={
                "ownerId": owner_id,
                "executionArn": execution_arn
            },
            UpdateExpression=f"SET {field_name} = if_not_exists({field_name}, :zero) + :one, last_{field_name}_at = :now",
            ExpressionAttributeValues={
                ":zero": 0,
                ":one": 1,
                ":now": datetime.now(timezone.utc).isoformat()
            },
            ReturnValues="UPDATED_NEW"
        )
        return int(response["Attributes"][field_name])
    except ClientError as e:
        logger.error(f"Failed to increment {field_name}: {e}")
        raise


def _get_last_failure_context(execution_arn: str) -> dict:
    """
    Step Functions 실행 히스토리에서 마지막 실패 이벤트를 찾아 에러 맥락을 반환합니다.
    Self-Healing의 정확도를 높이기 위해 사용됩니다.
    """
    try:
        response = sfn_client.get_execution_history(
            executionArn=execution_arn,
            maxResults=20,  # 최근 20개 이벤트만 확인
            reverseOrder=True  # 최신 이벤트부터
        )
        
        events = response.get("events", [])
        
        # 실패한 이벤트 찾기 (ExecutionFailed, TaskFailed 등)
        for event in events:
            state_entered_event_id = event.get("stateEnteredEventId")
            if state_entered_event_id:
                # TaskFailed 이벤트 찾기
                task_failed_events = [e for e in events if e.get("taskFailedEventId") == state_entered_event_id]
                if task_failed_events:
                    failed_event = task_failed_events[0]
                    return {
                        "error_type": "TaskFailed",
                        "error_name": failed_event.get("error"),
                        "error_message": failed_event.get("errorCause", ""),
                        "state_name": event.get("stateEnteredEventDetails", {}).get("name", ""),
                        "timestamp": event.get("timestamp", "").isoformat() if event.get("timestamp") else ""
                    }
            
            # ExecutionFailed 이벤트 직접 확인
            if event.get("type") == "ExecutionFailed":
                return {
                    "error_type": "ExecutionFailed",
                    "error_name": event.get("executionFailedEventDetails", {}).get("error"),
                    "error_message": event.get("executionFailedEventDetails", {}).get("cause", ""),
                    "timestamp": event.get("timestamp", "").isoformat() if event.get("timestamp") else ""
                }

            # 🎯 [Fix] TaskFailed 이벤트 직접 확인
            if event.get("type") == "TaskFailed":
                details = event.get("taskFailedEventDetails", {})
                return {
                    "error_type": "TaskFailed",
                    "error_name": details.get("error"),
                    "error_message": details.get("cause", ""),
                    "timestamp": event.get("timestamp", "").isoformat() if event.get("timestamp") else ""
                }
        
        # 실패 이벤트가 없으면 기본 맥락 반환
        return {
            "error_type": "Unknown",
            "error_name": "NoFailureFound",
            "error_message": "실행 히스토리에서 실패 이벤트를 찾을 수 없습니다.",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except ClientError as e:
        logger.error(f"Failed to get execution history: {e}")
        return {
            "error_type": "HistoryRetrievalFailed",
            "error_name": str(e),
            "error_message": "실행 히스토리를 가져올 수 없습니다.",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


def _resolve_state_machine_arn(execution: dict, original_arn: str) -> str:
    """
    State Machine ARN 해결 로직.
    
    DB에 명시적으로 저장된 state_machine_arn이 있으면 그것을 사용하고,
    없으면 기존 문자열 기반 판별 로직을 폴백으로 사용합니다.
    """
    # 1. DB에 저장된 ARN 우선 사용
    stored_arn = execution.get("state_machine_arn")
    if stored_arn:
        logger.info(f"Using stored state_machine_arn: {stored_arn}")
        return stored_arn
    
    # 2. 워크플로우 정의에서 지정된 오케스트레이터 확인
    workflow_config = execution.get("workflow_config", {})
    if isinstance(workflow_config, str):
        try:
            workflow_config = json.loads(workflow_config)
        except json.JSONDecodeError:
            workflow_config = {}
    
    orchestrator_type = workflow_config.get("orchestrator_type")
    if orchestrator_type == "distributed":
        return WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN
    elif orchestrator_type == "standard":
        return WORKFLOW_ORCHESTRATOR_ARN
    
    # 3. 폴백: 기존 문자열 기반 판별 (레거시 호환성)
    if "Distributed" in original_arn:
        logger.info("Fallback: detected Distributed orchestrator from ARN string")
        return WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN
    
    return WORKFLOW_ORCHESTRATOR_ARN
