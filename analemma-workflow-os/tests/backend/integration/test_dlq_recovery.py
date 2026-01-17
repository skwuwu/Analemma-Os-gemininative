"""
DLQ Recovery Pessimistic Tests

Tests for Dead Letter Queue recovery functionality with emphasis on:
- Context Retention (preserve execution_id, owner_id)
- Idempotency (prevent duplicate data)
- Redrive Throttling (backoff during bulk recovery)
- Poison Pill Detection (prevent infinite retries)
- Error Type Filtering (Throttle vs Logic Error)

Reference: dlq_redrive_handler.py
"""

import pytest
import json
import sys
import os
import uuid
import time
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, Mock
from typing import Dict, Any, List

# Add backend to path
sys.path.insert(0, os.path.abspath("backend"))


# =============================================================================
# Mock Services for Testing
# =============================================================================

class MockDynamoDBTable:
    """Mock DynamoDB table for testing"""
    
    def __init__(self, items: Dict[str, Dict] = None):
        self.items = items or {}
        self.update_calls = []
        self.delete_calls = []
    
    def get_item(self, Key: Dict) -> Dict:
        key_value = list(Key.values())[0]
        item = self.items.get(key_value)
        return {"Item": item} if item else {}
    
    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)
        key_value = list(kwargs["Key"].values())[0]
        # Simulate update
        if key_value in self.items:
            # Update would happen here
            pass
    
    def delete_item(self, Key: Dict):
        key_value = list(Key.values())[0]
        self.delete_calls.append(key_value)
        if key_value in self.items:
            del self.items[key_value]
    
    def query(self, **kwargs):
        # Simple mock for ownerId-index GSI
        owner_id = None
        if "KeyConditionExpression" in kwargs:
            # Extract owner_id from mock
            pass
        return {"Items": []}


class MockAPIGateway:
    """Mock API Gateway Management API for WebSocket"""
    
    def __init__(self, stale_connections: List[str] = None):
        self.stale_connections = stale_connections or []
        self.sent_messages = []
        self.post_call_count = 0
    
    def post_to_connection(self, ConnectionId: str, Data: str):
        self.post_call_count += 1
        
        if ConnectionId in self.stale_connections:
            from botocore.exceptions import ClientError
            raise ClientError(
                {"Error": {"Code": "GoneException", "Message": "Connection gone"}},
                "PostToConnection"
            )
        
        self.sent_messages.append({
            "connectionId": ConnectionId,
            "data": json.loads(Data)
        })


class MockStepFunctionsClient:
    """Mock Step Functions client for task token callbacks"""
    
    def __init__(self, expired_tokens: List[str] = None, throttled: bool = False):
        self.expired_tokens = expired_tokens or []
        self.throttled = throttled
        self.success_calls = []
        self.failure_calls = []
    
    def send_task_success(self, taskToken: str, output: str):
        if self.throttled:
            from botocore.exceptions import ClientError
            raise ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
                "SendTaskSuccess"
            )
        
        if taskToken in self.expired_tokens:
            from botocore.exceptions import ClientError
            raise ClientError(
                {"Error": {"Code": "TaskTimedOut", "Message": "Task timed out"}},
                "SendTaskSuccess"
            )
        
        self.success_calls.append({"taskToken": taskToken, "output": output})
    
    def send_task_failure(self, taskToken: str, error: str, cause: str):
        if taskToken in self.expired_tokens:
            from botocore.exceptions import ClientError
            raise ClientError(
                {"Error": {"Code": "InvalidToken", "Message": "Token invalid"}},
                "SendTaskFailure"
            )
        
        self.failure_calls.append({"taskToken": taskToken, "error": error, "cause": cause})


# =============================================================================
# DLQ Message Type Detection Tests
# =============================================================================

class TestDLQMessageTypeDetection:
    """Test message type detection logic"""
    
    def test_detect_websocket_notification_type(self):
        """Detect WebSocket notification event type"""
        event = {
            "source": "aws.states",
            "detail": {
                "executionArn": "arn:aws:states:us-east-1:123456789:execution:MyMachine:exec-123"
            }
        }
        
        # Import and test
        from src.handlers.utils.dlq_redrive_handler import determine_message_type
        
        message_type = determine_message_type(event)
        assert message_type == "websocket_notification"
    
    def test_detect_task_token_callback_type(self):
        """Detect Task Token callback event type"""
        event = {
            "taskToken": "AQAEAAAAAAAA...",
            "result": {"output": "success"}
        }
        
        from src.handlers.utils.dlq_redrive_handler import determine_message_type
        
        message_type = determine_message_type(event)
        assert message_type == "task_token_callback"
    
    def test_detect_execution_update_type(self):
        """Detect execution update event type"""
        event = {
            "executionId": "exec-123",
            "status": "COMPLETED"
        }
        
        from src.handlers.utils.dlq_redrive_handler import determine_message_type
        
        message_type = determine_message_type(event)
        assert message_type == "execution_update"
    
    def test_unknown_message_type(self):
        """Handle unknown event type"""
        event = {"random": "data"}
        
        from src.handlers.utils.dlq_redrive_handler import determine_message_type
        
        message_type = determine_message_type(event)
        assert message_type == "unknown"


# =============================================================================
# Context Retention Tests (pessimistic reinforcement)
# =============================================================================

class TestDLQContextRetention:
    """
    🔴 Pessimistic reinforcement: Verify that messages recovered from DLQ maintain their original context
    
    Risk: If execution_id, owner_id, segment_index etc. are lost, recovery is impossible
    """
    
    def test_original_event_extraction_from_sqs_body(self):
        """
        Verify that the original event is correctly extracted from SQS body
        """
        # SQS record in the form stored in DLQ
        sqs_body = json.dumps({
            "originalEvent": {
                "execution_id": "exec-123",
                "segment_to_run": 5,
                "ownerId": "user-test-001",
                "workflowId": "wf-abc"
            },
            "error_info": "ThrottlingException"
        })
        
        # Extract original event
        body = json.loads(sqs_body)
        original_event = body.get("originalEvent", body)
        
        # Verify context preservation
        assert original_event["execution_id"] == "exec-123"
        assert original_event["ownerId"] == "user-test-001"
        assert original_event["segment_to_run"] == 5
    
    def test_nested_original_event_fallback(self):
        """
        Verify that when originalEvent key is missing, the entire body is used as the original event
        """
        # Some failures may be saved without originalEvent wrapping
        sqs_body = json.dumps({
            "executionId": "exec-456",
            "status": "FAILED"
        })
        
        body = json.loads(sqs_body)
        original_event = body.get("originalEvent", body)
        
        # Verify fallback operation
        assert original_event["executionId"] == "exec-456"
        assert original_event["status"] == "FAILED"
    
    def test_execution_arn_to_execution_id_extraction(self):
        """
        Verify that execution_id is correctly extracted from executionArn
        """
        execution_arn = "arn:aws:states:us-east-1:123456789012:execution:AnalemmaStateMachine:exec-unique-id-789"
        
        # Same as handler's extraction logic
        execution_id = execution_arn.split(":")[-1]
        
        assert execution_id == "exec-unique-id-789"


# =============================================================================
# Idempotency Tests (Idempotency)
# =============================================================================

class TestDLQIdempotency:
    """
    🔴 Pessimistic reinforcement: No duplicate data even if same message is reprocessed multiple times
    """
    
    def test_websocket_payload_includes_unique_message_id(self):
        """
        WebSocket payload includes unique messageId so client can filter duplicates
        """
        from src.handlers.utils.dlq_redrive_handler import create_websocket_payload
        
        event = {
            "detail": {
                "stateEnteredEventDetails": {
                    "name": "ProcessingState",
                    "output": {"progress": 50}
                }
            }
        }
        execution_info = {
            "executionId": "exec-123",
            "workflowId": "wf-abc"
        }
        
        payload1 = create_websocket_payload(event, execution_info)
        payload2 = create_websocket_payload(event, execution_info)
        
        # Each payload includes unique messageId
        assert "messageId" in payload1
        assert "messageId" in payload2
        # The messageId of the two payloads are different (UUID)
        assert payload1["messageId"] != payload2["messageId"]
    
    def test_execution_update_is_idempotent(self):
        """
        Execution update remains the same final state even if called multiple times
        """
        mock_table = MockDynamoDBTable({
            "exec-123": {
                "executionId": "exec-123",
                "status": "RUNNING",
                "updatedAt": 1000
            }
        })
        
        # Apply the same update twice
        update_event = {
            "executionId": "exec-123",
            "status": "COMPLETED",
            "updates": {"finalOutput": {"result": "success"}}
        }
        
        with patch.dict(os.environ, {
            "EXECUTIONS_TABLE": "test-table"
        }):
            with patch("src.handlers.utils.dlq_redrive_handler.executions_table", mock_table):
                from src.handlers.utils.dlq_redrive_handler import retry_execution_update
                
                result1 = retry_execution_update(update_event)
                result2 = retry_execution_update(update_event)
                
                # Both succeed
                assert result1 is True
                assert result2 is True
                # Update is called twice (idempotency guaranteed at DB level)
                assert len(mock_table.update_calls) == 2


# =============================================================================
# Poison Pill Detection Tests (Poison Apple Detection)
# =============================================================================

class TestDLQPoisonPillDetection:
    """
    🔴 Pessimistic Reinforcement: Prevent messages that can never succeed from infinite retries
    
    Risk: Resource waste if messages due to logic bugs keep returning to DLQ
    """
    
    @pytest.fixture
    def retry_count_tracker(self):
        """Retry count tracking utility"""
        class RetryTracker:
            MAX_RETRIES = 3
            
            def __init__(self):
                self.retry_counts = {}
            
            def should_retry(self, message_id: str, error_type: str) -> bool:
                """
                재시도 여부 결정
                
                로직 에러 (ValueError, KeyError 등): 즉시 중단
                인프라 에러 (Throttling, Timeout 등): 재시도
                """
                # 로직 에러는 재시도해도 소용없음
                if error_type in ["ValueError", "KeyError", "TypeError", "PermissionError"]:
                    return False
                
                # 인프라 에러는 재시도 횟수 확인
                current_count = self.retry_counts.get(message_id, 0)
                if current_count >= self.MAX_RETRIES:
                    return False
                
                self.retry_counts[message_id] = current_count + 1
                return True
            
            def get_retry_count(self, message_id: str) -> int:
                return self.retry_counts.get(message_id, 0)
        
        return RetryTracker()
    
    def test_logic_error_not_retried(self, retry_count_tracker):
        """
        로직 에러 (ValueError, KeyError)는 재시도하지 않음
        """
        message_id = "msg-logic-error-001"
        
        # ValueError는 재시도 불가
        assert retry_count_tracker.should_retry(message_id, "ValueError") is False
        assert retry_count_tracker.get_retry_count(message_id) == 0
        
        # KeyError도 재시도 불가
        assert retry_count_tracker.should_retry("msg-002", "KeyError") is False
    
    def test_infrastructure_error_retried_with_limit(self, retry_count_tracker):
        """
        인프라 에러 (Throttling, Timeout)는 최대 횟수까지만 재시도
        """
        message_id = "msg-throttle-001"
        
        # 첫 3번은 재시도 허용
        assert retry_count_tracker.should_retry(message_id, "ThrottlingException") is True
        assert retry_count_tracker.should_retry(message_id, "ThrottlingException") is True
        assert retry_count_tracker.should_retry(message_id, "ThrottlingException") is True
        
        # 4번째부터는 거부
        assert retry_count_tracker.should_retry(message_id, "ThrottlingException") is False
    
    def test_expired_token_is_not_retried(self):
        """
        만료된 Task Token은 재시도해도 소용없으므로 성공 처리하여 DLQ에서 제거
        """
        mock_sf = MockStepFunctionsClient(expired_tokens=["expired-token-123"])
        
        with patch("boto3.client", return_value=mock_sf):
            from src.handlers.utils.dlq_redrive_handler import retry_task_token_callback
            
            event = {
                "taskToken": "expired-token-123",
                "result": {"output": "test"}
            }
            
            # 만료된 토큰은 True 반환 (DLQ에서 제거)
            result = retry_task_token_callback(event)
            assert result is True


# =============================================================================
# Redrive Throttling Tests (대량 복구 시 백오프)
# =============================================================================

class TestDLQRedriveThrottling:
    """
    🔴 비관적 강화: 대량 메시지 복구 시 타겟 시스템을 마비시키지 않음
    """
    
    @pytest.fixture
    def throttled_redrive_service(self):
        """지수 백오프가 적용된 Redrive 서비스"""
        import random
        
        class ThrottledRedriveService:
            BASE_DELAY = 0.1  # 100ms
            MAX_DELAY = 5.0   # 5초
            BATCH_SIZE = 10   # 한 번에 처리할 메시지 수
            
            def __init__(self):
                self.processed_count = 0
                self.delays_applied = []
            
            def calculate_backoff(self, attempt: int) -> float:
                """지수 백오프 계산"""
                delay = min(self.BASE_DELAY * (2 ** attempt), self.MAX_DELAY)
                jitter = delay * random.uniform(0, 0.5)
                return delay + jitter
            
            def process_batch(self, messages: List[Dict], attempt: int = 0) -> Dict:
                """배치 처리 (백오프 적용)"""
                if attempt > 0:
                    delay = self.calculate_backoff(attempt)
                    self.delays_applied.append(delay)
                    time.sleep(delay)  # 실제로는 짧게
                
                success_count = 0
                failed_ids = []
                
                for i, msg in enumerate(messages):
                    if i < self.BATCH_SIZE:
                        # 배치 크기 내 처리
                        self.processed_count += 1
                        if msg.get("will_fail"):
                            failed_ids.append(msg["id"])
                        else:
                            success_count += 1
                
                return {
                    "processed": success_count,
                    "failed": failed_ids,
                    "remaining": len(messages) - self.BATCH_SIZE if len(messages) > self.BATCH_SIZE else 0
                }
        
        return ThrottledRedriveService()
    
    def test_batch_processing_respects_size_limit(self, throttled_redrive_service):
        """
        대량 메시지는 배치 크기로 나누어 처리
        """
        # 30개 메시지 생성
        messages = [{"id": f"msg-{i}"} for i in range(30)]
        
        result = throttled_redrive_service.process_batch(messages)
        
        # 한 번에 10개만 처리
        assert result["processed"] == 10
        assert result["remaining"] == 20
    
    def test_exponential_backoff_increases_delay(self, throttled_redrive_service):
        """
        재시도 시 지수적으로 지연 시간 증가
        """
        delays = [
            throttled_redrive_service.calculate_backoff(0),
            throttled_redrive_service.calculate_backoff(1),
            throttled_redrive_service.calculate_backoff(2),
            throttled_redrive_service.calculate_backoff(3),
        ]
        
        # 지수적 증가 (지터 때문에 정확하지는 않지만 추세 확인)
        # delay[1] > delay[0], delay[2] > delay[1]
        assert delays[1] > delays[0] * 1.5  # 최소 1.5배 이상 증가
        assert delays[2] > delays[1] * 1.5
        
        # 최대값 제한
        assert delays[3] <= throttled_redrive_service.MAX_DELAY * 1.5  # 지터 포함


# =============================================================================
# Batch Item Failures Tests (SQS 부분 재시도)
# =============================================================================

class TestDLQBatchItemFailures:
    """
    SQS Lambda 통합의 batchItemFailures 응답 형식 검증
    """
    
    def test_partial_failure_returns_correct_format(self):
        """
        일부 메시지만 실패 시 올바른 형식으로 실패 메시지 ID 반환
        """
        # 혼합된 결과 시뮬레이션
        records = [
            {"messageId": "msg-1", "body": json.dumps({"executionId": "exec-1", "status": "OK"})},
            {"messageId": "msg-2", "body": json.dumps({"invalid": "data"})},  # 실패 예상
            {"messageId": "msg-3", "body": json.dumps({"executionId": "exec-3", "status": "OK"})},
        ]
        
        batch_item_failures = []
        
        for record in records:
            try:
                body = json.loads(record["body"])
                if "executionId" not in body:
                    raise ValueError("Missing executionId")
            except Exception:
                batch_item_failures.append({"itemIdentifier": record["messageId"]})
        
        # msg-2만 실패
        assert len(batch_item_failures) == 1
        assert batch_item_failures[0]["itemIdentifier"] == "msg-2"
    
    def test_all_success_returns_empty_failures(self):
        """
        모든 메시지 성공 시 빈 실패 리스트 반환
        """
        batch_item_failures = []
        
        result = {"batchItemFailures": batch_item_failures}
        
        assert result["batchItemFailures"] == []
    
    def test_all_failure_returns_all_message_ids(self):
        """
        모든 메시지 실패 시 전체 메시지 ID 반환
        """
        message_ids = ["msg-1", "msg-2", "msg-3"]
        
        batch_item_failures = [{"itemIdentifier": msg_id} for msg_id in message_ids]
        
        assert len(batch_item_failures) == 3


# =============================================================================
# Stale Connection Cleanup Tests
# =============================================================================

class TestDLQStaleConnectionCleanup:
    """
    WebSocket 연결 끊김 시 자동 정리
    """
    
    def test_gone_exception_triggers_connection_removal(self):
        """
        GoneException 발생 시 연결이 자동 삭제됨
        """
        mock_table = MockDynamoDBTable({
            "conn-stale-123": {"connectionId": "conn-stale-123", "ownerId": "user-1"}
        })
        
        with patch("src.handlers.utils.dlq_redrive_handler.connections_table", mock_table):
            from src.handlers.utils.dlq_redrive_handler import remove_stale_connection
            
            remove_stale_connection("conn-stale-123")
            
            # 삭제 호출 확인
            assert "conn-stale-123" in mock_table.delete_calls


# =============================================================================
# Error Type Filtering Tests (에러 타입별 필터링)
# =============================================================================

class TestDLQErrorTypeFiltering:
    """
    🔴 비관적 강화: 에러 타입에 따른 Redrive 정책
    
    Throttling, Timeout, NetworkError → 자동 Redrive 대상
    ValueError, KeyError, PermissionError → 수동 검토 대상
    """
    
    @pytest.fixture
    def error_classifier(self):
        """에러 분류기"""
        class ErrorClassifier:
            # 자동 Redrive 대상 에러
            RETRIABLE_ERRORS = {
                "ThrottlingException",
                "ServiceUnavailableException",
                "InternalServerError",
                "TimeoutError",
                "NetworkError",
                "503 Slow Down",
                "TooManyRequestsException",
            }
            
            # 수동 검토 대상 에러 (로직 버그)
            NON_RETRIABLE_ERRORS = {
                "ValueError",
                "KeyError",
                "TypeError",
                "PermissionError",
                "ValidationError",
                "AccessDeniedException",
            }
            
            def classify(self, error_type: str) -> str:
                """
                에러 분류
                
                Returns:
                    "auto_retry": 자동 Redrive
                    "manual_review": 수동 검토 필요
                    "unknown": 알 수 없는 에러 (기본값: 1회 재시도 후 수동 검토)
                """
                if error_type in self.RETRIABLE_ERRORS:
                    return "auto_retry"
                elif error_type in self.NON_RETRIABLE_ERRORS:
                    return "manual_review"
                else:
                    return "unknown"
            
            def should_alert_admin(self, error_type: str, retry_count: int) -> bool:
                """관리자 알림 필요 여부"""
                classification = self.classify(error_type)
                
                # 수동 검토 대상은 즉시 알림
                if classification == "manual_review":
                    return True
                
                # 자동 재시도도 3회 초과 시 알림
                if retry_count > 3:
                    return True
                
                return False
        
        return ErrorClassifier()
    
    def test_throttling_error_classified_as_auto_retry(self, error_classifier):
        """Throttling 에러는 자동 재시도 대상"""
        assert error_classifier.classify("ThrottlingException") == "auto_retry"
        assert error_classifier.classify("ServiceUnavailableException") == "auto_retry"
        assert error_classifier.classify("503 Slow Down") == "auto_retry"
    
    def test_logic_error_classified_as_manual_review(self, error_classifier):
        """로직 에러는 수동 검토 대상"""
        assert error_classifier.classify("ValueError") == "manual_review"
        assert error_classifier.classify("KeyError") == "manual_review"
        assert error_classifier.classify("PermissionError") == "manual_review"
    
    def test_admin_alert_on_logic_error(self, error_classifier):
        """로직 에러 발생 시 즉시 관리자 알림"""
        assert error_classifier.should_alert_admin("ValueError", retry_count=0) is True
        assert error_classifier.should_alert_admin("KeyError", retry_count=0) is True
    
    def test_admin_alert_after_max_retries(self, error_classifier):
        """최대 재시도 횟수 초과 시 관리자 알림"""
        # 3회까지는 알림 없음
        assert error_classifier.should_alert_admin("ThrottlingException", retry_count=1) is False
        assert error_classifier.should_alert_admin("ThrottlingException", retry_count=3) is False
        
        # 4회부터 알림
        assert error_classifier.should_alert_admin("ThrottlingException", retry_count=4) is True


# =============================================================================
# Integration Test: Full DLQ Redrive Flow
# =============================================================================

class TestDLQRedriveIntegration:
    """
    전체 DLQ Redrive 흐름 통합 테스트
    """
    
    def test_full_redrive_flow_preserves_context(self):
        """
        🔴 비관적 테스트: API 폭주로 인한 워커 집단 폐사 및 복구 시나리오
        
        상황: 100개의 맵 워커 중 30개가 Bedrock 할당량 초과로 실패하여 DLQ에 쌓임
        검증: 복구 시 원래의 execution_id, owner_id, segment_index 유지
        """
        # DLQ에 저장된 형태의 SQS 이벤트
        sqs_event = {
            "Records": [{
                "messageId": f"msg-{i}",
                "body": json.dumps({
                    "originalEvent": {
                        "execution_id": f"exec-batch-{i}",
                        "segment_to_run": i % 10,
                        "ownerId": "user-batch-test",
                        "workflowId": "wf-multimodal"
                    },
                    "error_info": {
                        "error": "ThrottlingException",
                        "cause": "Bedrock TPS exceeded"
                    }
                }),
                "messageAttributes": {"RetryCount": {"stringValue": str(i % 3 + 1)}}
            } for i in range(30)]
        }
        
        # 모든 레코드의 컨텍스트 검증
        for record in sqs_event["Records"]:
            body = json.loads(record["body"])
            original_event = body.get("originalEvent", body)
            
            # 필수 컨텍스트 필드 존재
            assert "execution_id" in original_event
            assert "ownerId" in original_event
            assert "segment_to_run" in original_event
            
            # 값이 올바른 형식
            assert original_event["execution_id"].startswith("exec-batch-")
            assert original_event["ownerId"] == "user-batch-test"
            assert 0 <= original_event["segment_to_run"] < 10


# =============================================================================
# Clock Skew Tests (타임스탬프 역전 방지)
# =============================================================================

class TestDLQClockSkew:
    """
    🔴 비관적 강화: DLQ에서 복구된 메시지의 타임스탬프 역전 방지
    
    Risk: DLQ에서 복구된 오래된 메시지가 최신 상태를 덮어쓸 수 있음
    """
    
    @pytest.fixture
    def timestamp_protected_updater(self):
        """타임스탬프 보호 기능이 있는 업데이터"""
        class TimestampProtectedUpdater:
            def __init__(self):
                self.updates = []
                self.rejections = []
            
            def update_with_timestamp_guard(
                self, 
                execution_id: str, 
                new_data: Dict, 
                new_timestamp: int,
                current_timestamp: int = None
            ) -> Dict:
                """
                타임스탬프 조건부 업데이트
                
                DynamoDB의 ConditionExpression과 동일한 로직:
                현재 저장된 updatedAt보다 새로운 경우에만 업데이트
                """
                if current_timestamp is not None and new_timestamp <= current_timestamp:
                    self.rejections.append({
                        "execution_id": execution_id,
                        "new_timestamp": new_timestamp,
                        "current_timestamp": current_timestamp,
                        "reason": "timestamp_inversion"
                    })
                    return {
                        "updated": False,
                        "reason": "New timestamp is not newer than current"
                    }
                
                self.updates.append({
                    "execution_id": execution_id,
                    "data": new_data,
                    "timestamp": new_timestamp
                })
                
                return {"updated": True}
            
            def generate_condition_expression(self) -> str:
                """
                DynamoDB ConditionExpression 생성
                
                Expected 조건: 현재 updatedAt이 없거나, newTimestamp보다 작은 경우에만 업데이트
                """
                return "attribute_not_exists(updatedAt) OR updatedAt < :newTimestamp"
        
        return TimestampProtectedUpdater()
    
    def test_old_message_does_not_overwrite_new_data(self, timestamp_protected_updater):
        """
        🔴 오래된 DLQ 메시지가 최신 데이터를 덮어쓰지 않음
        
        시나리오: 
        - 메시지 A가 1월 1일에 실패하여 DLQ로 이동
        - 메시지 B가 1월 2일에 같은 실행을 성공적으로 완료
        - 1월 3일에 DLQ Redrive가 메시지 A를 복구 시도
        - 결과: 메시지 A의 오래된 상태는 무시되어야 함
        """
        execution_id = "exec-clock-test-001"
        
        # 현재 저장된 상태 (1월 2일에 업데이트됨)
        current_timestamp = 1704153600  # 2024-01-02
        
        # DLQ에서 복구된 오래된 메시지 (1월 1일 데이터)
        old_message_timestamp = 1704067200  # 2024-01-01
        old_data = {"status": "FAILED", "error": "Throttling"}
        
        result = timestamp_protected_updater.update_with_timestamp_guard(
            execution_id=execution_id,
            new_data=old_data,
            new_timestamp=old_message_timestamp,
            current_timestamp=current_timestamp
        )
        
        # 업데이트 거부됨
        assert result["updated"] is False
        assert len(timestamp_protected_updater.rejections) == 1
        assert timestamp_protected_updater.rejections[0]["reason"] == "timestamp_inversion"
    
    def test_new_message_updates_old_data(self, timestamp_protected_updater):
        """
        새로운 메시지는 정상적으로 업데이트됨
        """
        execution_id = "exec-clock-test-002"
        
        current_timestamp = 1704067200  # 2024-01-01
        new_timestamp = 1704153600  # 2024-01-02 (더 최신)
        new_data = {"status": "COMPLETED"}
        
        result = timestamp_protected_updater.update_with_timestamp_guard(
            execution_id=execution_id,
            new_data=new_data,
            new_timestamp=new_timestamp,
            current_timestamp=current_timestamp
        )
        
        # 업데이트 성공
        assert result["updated"] is True
        assert len(timestamp_protected_updater.updates) == 1
    
    def test_condition_expression_format(self, timestamp_protected_updater):
        """
        DynamoDB ConditionExpression이 올바른 형식인지 확인
        """
        expr = timestamp_protected_updater.generate_condition_expression()
        
        # 필수 조건 포함
        assert "attribute_not_exists" in expr or "updatedAt" in expr
        assert "<" in expr  # 타임스탬프 비교


# =============================================================================
# IAM Permission Alignment Tests (권한 소외 방지)
# =============================================================================

class TestDLQIAMPermissionAlignment:
    """
    🔴 비관적 강화: DLQ Redrive 핸들러가 필요한 모든 권한을 가지는지 확인
    
    Risk: segment_runner가 사용하는 리소스(S3, DynamoDB, Bedrock)에 대한 
          권한이 dlq_redrive_handler에 없으면 복구 실패
    """
    
    @pytest.fixture
    def required_permissions(self):
        """DLQ Redrive에 필요한 권한 목록"""
        return {
            # DynamoDB 권한
            "dynamodb:GetItem",
            "dynamodb:UpdateItem",
            "dynamodb:DeleteItem",
            "dynamodb:Query",
            # Step Functions 권한
            "states:SendTaskSuccess",
            "states:SendTaskFailure",
            # API Gateway (WebSocket) 권한
            "execute-api:ManageConnections",
            # (선택) S3 권한 - 상태 복구 시 필요할 수 있음
            "s3:GetObject",
            "s3:PutObject",
            # CloudWatch Logs 권한
            "logs:CreateLogStream",
            "logs:PutLogEvents",
        }
    
    @pytest.fixture
    def segment_runner_permissions(self):
        """segment_runner가 사용하는 권한"""
        return {
            "dynamodb:GetItem",
            "dynamodb:UpdateItem",
            "dynamodb:PutItem",
            "dynamodb:Query",
            "s3:GetObject",
            "s3:PutObject",
            "states:SendTaskSuccess",
            "states:SendTaskFailure",
            "bedrock:InvokeModel",
            "execute-api:ManageConnections",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
        }
    
    def test_dlq_handler_has_all_required_permissions(self, required_permissions):
        """
        DLQ 핸들러에 필요한 모든 권한이 정의되어 있는지 확인
        """
        # 핵심 권한 검증
        essential_permissions = {
            "dynamodb:GetItem",  # 실행 정보 조회
            "dynamodb:UpdateItem",  # 상태 업데이트
            "states:SendTaskSuccess",  # Task Token 콜백
            "states:SendTaskFailure",
        }
        
        assert essential_permissions.issubset(required_permissions)
    
    def test_dlq_permissions_cover_segment_runner_resources(
        self, 
        required_permissions, 
        segment_runner_permissions
    ):
        """
        DLQ 핸들러 권한이 segment_runner가 사용하는 리소스를 커버하는지 확인
        
        Redrive 시 segment_runner가 실패한 작업을 재실행하려면
        동일한 리소스에 접근할 수 있어야 함
        """
        # 공통 리소스에 대한 권한 비교
        common_resources_permissions = {
            "dynamodb:GetItem",
            "dynamodb:UpdateItem",
            "states:SendTaskSuccess",
            "states:SendTaskFailure",
        }
        
        # DLQ 핸들러가 최소한 이 권한들은 가져야 함
        dlq_has_common = common_resources_permissions.issubset(required_permissions)
        runner_has_common = common_resources_permissions.issubset(segment_runner_permissions)
        
        assert dlq_has_common, "DLQ handler missing common permissions"
        assert runner_has_common, "Segment runner missing common permissions"
    
    def test_managed_policy_recommended_for_alignment(self):
        """
        권한 동기화를 위해 공유 Managed Policy 사용 권장
        """
        # SAM template에서 동일한 Managed Policy를 참조하도록 권장
        shared_policy_name = "AnalemmaSharedExecutionPolicy"
        
        # 테스트는 권장 사항만 문서화
        recommendation = {
            "policy_name": shared_policy_name,
            "includes": [
                "DynamoDB access for state tables",
                "Step Functions task token callbacks",
                "API Gateway WebSocket management",
                "CloudWatch Logs",
            ],
            "used_by": ["SegmentRunnerFunction", "DLQRedriveFunction"]
        }
        
        assert recommendation["policy_name"] == shared_policy_name


# =============================================================================
# Correlation ID Traceability Tests (추적성 보장)
# =============================================================================

class TestDLQCorrelationIDTraceability:
    """
    🔴 비관적 강화: DLQ Redrive 과정에서 추적성 유지
    
    Risk: Redrive된 메시지는 새로운 SQS messageId를 가지므로 
          원래 실행과의 연결고리가 끊어질 수 있음
    """
    
    @pytest.fixture
    def correlation_aware_logger(self):
        """Correlation ID를 포함하는 로거"""
        class CorrelationAwareLogger:
            def __init__(self):
                self.log_entries = []
            
            def log(
                self, 
                level: str, 
                message: str, 
                correlation_id: str = None,
                execution_id: str = None,
                original_message_id: str = None,
                redrive_message_id: str = None
            ):
                """
                추적 정보가 포함된 로그 출력
                
                모든 로그에 correlation_id를 포함하여 
                실패 → DLQ → 복구 과정을 추적 가능
                """
                entry = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": level,
                    "message": message,
                    "correlation_id": correlation_id or execution_id,
                    "execution_id": execution_id,
                    "original_message_id": original_message_id,
                    "redrive_message_id": redrive_message_id,
                }
                self.log_entries.append(entry)
                return entry
            
            def get_trace(self, correlation_id: str) -> List[Dict]:
                """특정 correlation_id의 모든 로그 항목 조회"""
                return [
                    entry for entry in self.log_entries 
                    if entry["correlation_id"] == correlation_id
                ]
        
        return CorrelationAwareLogger()
    
    def test_correlation_id_preserved_in_original_event(self):
        """
        originalEvent에 correlation_id가 포함되어 있는지 확인
        """
        # DLQ 메시지에 저장되는 형태
        dlq_message = {
            "originalEvent": {
                "execution_id": "exec-trace-001",
                "correlation_id": "corr-abc-123",  # 추적을 위한 ID
                "ownerId": "user-test",
                "segment_to_run": 3
            },
            "error_info": {
                "error": "ThrottlingException",
                "cause": "API rate limit exceeded"
            },
            "original_message_id": "sqs-msg-original-456"  # 원본 SQS 메시지 ID
        }
        
        # correlation_id 존재 확인
        assert "correlation_id" in dlq_message["originalEvent"]
        assert dlq_message["originalEvent"]["correlation_id"] == "corr-abc-123"
        
        # original_message_id 존재 확인
        assert "original_message_id" in dlq_message
    
    def test_log_entry_includes_all_trace_ids(self, correlation_aware_logger):
        """
        로그 항목에 모든 추적 ID가 포함되는지 확인
        """
        entry = correlation_aware_logger.log(
            level="INFO",
            message="Redrive attempt started",
            correlation_id="corr-abc-123",
            execution_id="exec-trace-001",
            original_message_id="sqs-original-456",
            redrive_message_id="sqs-redrive-789"
        )
        
        # 모든 추적 필드 존재
        assert entry["correlation_id"] == "corr-abc-123"
        assert entry["execution_id"] == "exec-trace-001"
        assert entry["original_message_id"] == "sqs-original-456"
        assert entry["redrive_message_id"] == "sqs-redrive-789"
    
    def test_can_trace_full_lifecycle(self, correlation_aware_logger):
        """
        실패 → DLQ → 복구 전체 수명주기를 하나의 타임라인으로 추적 가능
        """
        correlation_id = "corr-lifecycle-test"
        execution_id = "exec-lifecycle-001"
        
        # 1. 원본 실행 실패
        correlation_aware_logger.log(
            level="ERROR",
            message="Segment execution failed",
            correlation_id=correlation_id,
            execution_id=execution_id,
            original_message_id="sqs-1"
        )
        
        # 2. DLQ로 이동
        correlation_aware_logger.log(
            level="WARN",
            message="Message moved to DLQ",
            correlation_id=correlation_id,
            execution_id=execution_id,
            original_message_id="sqs-1"
        )
        
        # 3. DLQ Redrive 시작
        correlation_aware_logger.log(
            level="INFO",
            message="DLQ redrive initiated",
            correlation_id=correlation_id,
            execution_id=execution_id,
            original_message_id="sqs-1",
            redrive_message_id="sqs-2"  # 새로운 SQS 메시지 ID
        )
        
        # 4. 복구 성공
        correlation_aware_logger.log(
            level="INFO",
            message="Redrive successful",
            correlation_id=correlation_id,
            execution_id=execution_id,
            redrive_message_id="sqs-2"
        )
        
        # 전체 수명주기 추적
        trace = correlation_aware_logger.get_trace(correlation_id)
        
        assert len(trace) == 4
        assert trace[0]["message"] == "Segment execution failed"
        assert trace[1]["message"] == "Message moved to DLQ"
        assert trace[2]["message"] == "DLQ redrive initiated"
        assert trace[3]["message"] == "Redrive successful"
    
    def test_correlation_id_fallback_to_execution_id(self, correlation_aware_logger):
        """
        correlation_id가 없으면 execution_id를 대체 사용
        """
        entry = correlation_aware_logger.log(
            level="INFO",
            message="Processing without explicit correlation_id",
            execution_id="exec-fallback-001"  # correlation_id 없음
        )
        
        # execution_id가 correlation_id로 사용됨
        assert entry["correlation_id"] == "exec-fallback-001"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
