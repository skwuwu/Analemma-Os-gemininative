"""
Auto Healing Trigger Lambda (v3.9)
===================================

EventBridge에서 Step Functions 실패 이벤트를 수신하여
에러 분류 후 자동 복구(Deterministic) 또는 수동 대기(Semantic) 경로로 분기합니다.

이벤트 흐름:
1. Step Functions 실패 → EventBridge 이벤트 발행
2. 이 Lambda가 이벤트 수신
3. ErrorClassifier로 에러 분류
4. Deterministic → QuickFixExecutor 호출 (자동 복구)
5. Semantic → 상태를 AWAITING_MANUAL_HEALING으로 변경

Circuit Breaker:
- healing_count > 3 일 경우 자동 복구 중단
- 증거 보존: auto_fixed, gemini_logic_fix 필드 추가
"""

import json
import os
import boto3
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients (lazy init)
_dynamodb = None
_lambda_client = None

# Environment
EXECUTIONS_TABLE = os.environ.get("EXECUTIONS_TABLE", "analemma-executions")
QUICK_FIX_FUNCTION_NAME = os.environ.get("QUICK_FIX_FUNCTION_NAME", "quick-fix-executor-v2")
NOTIFICATION_BUS_NAME = os.environ.get("NOTIFICATION_BUS_NAME", "analemma-notifications")


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _get_lambda_client():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


def _get_original_owner_id(execution_arn: str) -> Optional[str]:
    """
    DynamoDB에서 executionArn으로 원래 실행 기록을 조회하여 ownerId를 반환합니다.
    ExecutionsTableV3의 구조: PK(ownerId), SK(executionArn)
    """
    try:
        dynamodb = _get_dynamodb()
        table = dynamodb.Table(EXECUTIONS_TABLE)
        
        # executionArn으로 스캔 (비효율적이지만 정확함)
        # FilterExpression을 사용하여 executionArn이 정확히 일치하는 항목만 필터링
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr("executionArn").eq(execution_arn)
        )
        
        items = response.get("Items", [])
        if items:
            # 첫 번째 일치 항목의 ownerId 반환
            owner_id = items[0].get("ownerId")
            logger.info(f"Found owner_id: {owner_id} for execution: {execution_arn}")
            return owner_id
        
        logger.error(f"No execution record found for {execution_arn}")
        return None
        
    except Exception as e:
        logger.error(f"Failed to retrieve owner_id for {execution_arn}: {e}")
        return None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    EventBridge 이벤트를 수신하여 Self-Healing을 트리거합니다.
    
    EventBridge Detail 구조:
    {
        "executionArn": "arn:aws:states:...",
        "ownerId": "user-123",
        "error_type": "JSONDecodeError",
        "error_message": "...",
        "segment_index": 3,
        "healing_count": 0
    }
    """
    logger.info(f"AutoHealingTrigger received event: {json.dumps(event, default=str)}")
    
    try:
        # EventBridge 이벤트 파싱
        detail = event.get("detail", {})
        
        execution_arn = detail.get("executionArn") or detail.get("execution_arn")
        error_type = detail.get("error_type", detail.get("Error", "UnknownError"))
        error_message = detail.get("error_message", detail.get("Cause", ""))
        healing_count = detail.get("healing_count", detail.get("_self_healing_count", 0))
        
        if not execution_arn:
            logger.error("Missing executionArn in event")
            return {"status": "error", "message": "Missing executionArn"}
        
        # DynamoDB에서 원래 실행 기록을 조회하여 ownerId 가져오기
        owner_id = _get_original_owner_id(execution_arn)
        if not owner_id:
            logger.error(f"Could not find original execution record for {execution_arn}")
            return {"status": "error", "message": "Could not find original execution record"}
        
        logger.info(f"Retrieved original owner_id: {owner_id} for execution: {execution_arn}")
        
        # ErrorClassifier import (동적 로드)
        from src.services.recovery.error_classifier import get_error_classifier, ErrorCategory
        
        classifier = get_error_classifier()
        
        # 에러 분류 수행
        category, reason = classifier.classify(
            error_type=error_type,
            error_message=error_message,
            healing_count=healing_count,
            context=detail
        )
        
        logger.info(f"Error classification: {category.value} - {reason}")
        
        if category == ErrorCategory.DETERMINISTIC:
            # 자동 복구 경로
            result = _trigger_auto_healing(
                execution_arn=execution_arn,
                owner_id=owner_id,
                error_type=error_type,
                error_message=error_message,
                healing_count=healing_count,
                classifier=classifier
            )
            
            # 프론트엔드 알림: "자동 복구 중"
            _notify_frontend(
                execution_arn=execution_arn,
                owner_id=owner_id,
                message="오류 감지됨, Gemini가 코드를 수정하여 자동 복구 중입니다...",
                healing_status="AUTO_HEALING_IN_PROGRESS",
                details={
                    "error_type": error_type,
                    "healing_count": healing_count + 1,
                    "classification": "DETERMINISTIC",
                    "reason": reason
                }
            )
            
            return {
                "status": "auto_healing_triggered",
                "category": "DETERMINISTIC",
                "reason": reason,
                "result": result
            }
        
        else:
            # 수동 개입 경로
            result = _set_awaiting_manual_healing(
                execution_arn=execution_arn,
                owner_id=owner_id,
                error_type=error_type,
                error_message=error_message,
                healing_count=healing_count,
                reason=reason,
                classifier=classifier
            )
            
            # 프론트엔드 알림: "수동 승인 대기"
            _notify_frontend(
                execution_arn=execution_arn,
                owner_id=owner_id,
                message=f"자동 복구 불가: {reason}. 수동 승인이 필요합니다.",
                healing_status="AWAITING_MANUAL_HEALING",
                details={
                    "error_type": error_type,
                    "healing_count": healing_count,
                    "classification": "SEMANTIC",
                    "reason": reason,
                    "suggested_fix": result.get("suggested_fix")
                }
            )
            
            return {
                "status": "awaiting_manual_healing",
                "category": "SEMANTIC",
                "reason": reason,
                "result": result
            }
    
    except Exception as e:
        logger.error(f"AutoHealingTrigger error: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e)
        }


def _trigger_auto_healing(
    execution_arn: str,
    owner_id: str,
    error_type: str,
    error_message: str,
    healing_count: int,
    classifier
) -> Dict[str, Any]:
    """
    QuickFixExecutor Lambda를 호출하여 자동 복구를 수행합니다.
    """
    logger.info(f"Triggering auto-healing for {execution_arn}")
    
    # 휴리스틱 수정 제안 생성
    suggested_fix = classifier.get_healing_advice(error_type, error_message)
    if not suggested_fix:
        suggested_fix = f"Auto-fix for {error_type}: Review and correct the error."
    
    # QuickFixExecutor 호출 페이로드
    payload = {
        "body": json.dumps({
            "task_id": execution_arn,
            "fix_type": "self_healing",
            "payload": {
                "suggested_fix": suggested_fix,
                "auto_triggered": True,
                "classification": "DETERMINISTIC"
            },
            "owner_id": owner_id
        }),
        "requestContext": {
            "authorizer": {
                "claims": {
                    "sub": owner_id
                }
            }
        }
    }
    
    try:
        lambda_client = _get_lambda_client()
        response = lambda_client.invoke(
            FunctionName=QUICK_FIX_FUNCTION_NAME,
            InvocationType="RequestResponse",  # 동기 호출
            Payload=json.dumps(payload)
        )
        
        response_payload = json.loads(response["Payload"].read().decode())
        logger.info(f"QuickFixExecutor response: {response_payload}")
        
        # 증거 보존: auto_fixed 플래그 추가
        _update_execution_metadata(
            execution_arn=execution_arn,
            owner_id=owner_id,
            updates={
                "auto_fixed": True,
                "auto_healing_timestamp": datetime.now(timezone.utc).isoformat(),
                "gemini_logic_fix": suggested_fix,
                "_self_healing_count": healing_count + 1
            }
        )
        
        return response_payload
    
    except Exception as e:
        logger.error(f"Failed to invoke QuickFixExecutor: {e}")
        raise


def _set_awaiting_manual_healing(
    execution_arn: str,
    owner_id: str,
    error_type: str,
    error_message: str,
    healing_count: int,
    reason: str,
    classifier
) -> Dict[str, Any]:
    """
    수동 개입이 필요한 에러의 경우, 상태를 AWAITING_MANUAL_HEALING으로 변경합니다.
    """
    logger.info(f"Setting awaiting_manual_healing for {execution_arn}")
    
    # 제안 수정안 생성
    suggested_fix = classifier.get_healing_advice(error_type, error_message)
    if not suggested_fix:
        suggested_fix = f"Manual review required for {error_type}."
    
    # DynamoDB 상태 업데이트
    _update_execution_metadata(
        execution_arn=execution_arn,
        owner_id=owner_id,
        updates={
            "status": "AWAITING_MANUAL_HEALING",
            "healing_blocked_reason": reason,
            "healing_blocked_at": datetime.now(timezone.utc).isoformat(),
            "pending_suggested_fix": suggested_fix,
            "_self_healing_count": healing_count,
            "error_type": error_type,
            "error_message": error_message[:500]  # 길이 제한
        }
    )
    
    return {
        "status": "AWAITING_MANUAL_HEALING",
        "reason": reason,
        "suggested_fix": suggested_fix
    }


def _update_execution_metadata(
    execution_arn: str,
    owner_id: str,
    updates: Dict[str, Any]
) -> None:
    """
    DynamoDB에 실행 메타데이터를 업데이트합니다.
    """
    try:
        dynamodb = _get_dynamodb()
        table = dynamodb.Table(EXECUTIONS_TABLE)
        
        # Update expression 동적 생성
        update_parts = []
        expression_values = {}
        expression_names = {}
        
        for key, value in updates.items():
            safe_key = key.replace("-", "_")
            update_parts.append(f"#{safe_key} = :{safe_key}")
            expression_names[f"#{safe_key}"] = key
            expression_values[f":{safe_key}"] = value
        
        table.update_item(
            Key={
                "ownerId": owner_id,
                "executionArn": execution_arn
            },
            UpdateExpression=f"SET {', '.join(update_parts)}",
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values
        )
        
        logger.info(f"Updated execution metadata: {list(updates.keys())}")
    
    except Exception as e:
        logger.error(f"Failed to update execution metadata: {e}")
        # 실패해도 계속 진행 (critical이 아님)


def _notify_frontend(
    execution_arn: str,
    owner_id: str,
    message: str,
    healing_status: str,
    details: Dict[str, Any]
) -> None:
    """
    EventBridge를 통해 프론트엔드에 알림을 보냅니다.
    WebSocket을 통해 실시간 토스트 메시지로 표시됩니다.
    """
    try:
        events = _get_events_client()
        
        events.put_events(
            Entries=[
                {
                    "Source": "analemma.self-healing",
                    "DetailType": "SelfHealingStatusUpdate",
                    "Detail": json.dumps({
                        "executionArn": execution_arn,
                        "ownerId": owner_id,
                        "message": message,
                        "healing_status": healing_status,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        **details
                    }),
                    "EventBusName": NOTIFICATION_BUS_NAME
                }
            ]
        )
        
        logger.info(f"Frontend notification sent: {healing_status}")
    
    except Exception as e:
        logger.warning(f"Failed to notify frontend: {e}")
        # 실패해도 계속 진행
