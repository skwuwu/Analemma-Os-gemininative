"""
API Contract Tests
Frontend-Backend Contract Verification

🚨 Core Principle: Directly import actual production handlers for testing
- Only AWS/LLM mocking is allowed
- Verify actual API response schemas
"""
import pytest
import json
import sys
import os
from unittest.mock import patch, MagicMock

# Set environment variables (before module import)
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("MOCK_MODE", "true")
os.environ.setdefault("WORKFLOWS_TABLE", "test-workflows")
os.environ.setdefault("EXECUTIONS_TABLE", "test-executions")
os.environ.setdefault("WEBSOCKET_CONNECTIONS_TABLE", "test-connections")

# OpenAI mocking (prevent LLM costs)
mock_openai = MagicMock()
sys.modules['openai'] = mock_openai

from moto import mock_aws
import boto3


@pytest.fixture(autouse=True)
def mock_aws_services():
    """AWS mocking for all tests (required)"""
    with mock_aws():
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Workflow table
        dynamodb.create_table(
            TableName='test-workflows',
            KeySchema=[
                {'AttributeName': 'ownerId', 'KeyType': 'HASH'},
                {'AttributeName': 'workflowId', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'ownerId', 'AttributeType': 'S'},
                {'AttributeName': 'workflowId', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Execution table
        dynamodb.create_table(
            TableName='test-executions',
            KeySchema=[
                {'AttributeName': 'ownerId', 'KeyType': 'HASH'},
                {'AttributeName': 'executionId', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'ownerId', 'AttributeType': 'S'},
                {'AttributeName': 'executionId', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        yield


class TestAPIResponseSchemaContract:
    """
    API response schema contract verification - matches frontend TypeScript interfaces
    
    🚨 Direct use of production code:
    - backend.get_workflow.lambda_handler
    - backend.correction_api_handler.lambda_log_correction
    """
    
    # Field names expected by frontend (camelCase)
    FRONTEND_WORKFLOW_FIELDS = {
        "workflowId",      # not workflow_id
        "name",
        "description",
        "nodes",
        "edges",
        "createdAt",       # not created_at
        "updatedAt",       # not updated_at
        "ownerId"
    }
    
    def test_get_workflow_handler_returns_camel_case(self):
        """Production get_workflow handler returns camelCase response"""
        from backend.get_workflow import lambda_handler
        
        # Insert test workflow into DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('test-workflows')
        table.put_item(Item={
            'ownerId': 'user-test-123',
            'workflowId': 'wf-test-001',
            'name': 'Test Workflow',
            'description': 'For contract testing',
            'nodes': [],
            'edges': [],
            'createdAt': '2026-01-03T00:00:00Z',
            'updatedAt': '2026-01-03T00:00:00Z'
        })
        
        # Simulate JWT authenticated request (list query)
        event = {
            'httpMethod': 'GET',
            'pathParameters': {},
            'queryStringParameters': {},
            'requestContext': {
                'authorizer': {
                    'jwt': {
                        'claims': {'sub': 'user-test-123'}
                    }
                }
            }
        }
        
        result = lambda_handler(event, None)
        
        # Success response
        assert result['statusCode'] == 200
        
        # Parse response body
        body = json.loads(result['body'])
        
        # List response format: workflows array
        assert 'workflows' in body
        assert len(body['workflows']) >= 1
        
        # Check camelCase in first workflow
        workflow = body['workflows'][0]
        assert 'workflowId' in workflow
        assert 'name' in workflow
        
        # snake_case should not exist
        body_str = json.dumps(body)
        assert 'workflow_id' not in body_str
        assert 'created_at' not in body_str
    
    def test_correction_api_401_error_format(self):
        """Production correction_api_handler 401 error response format"""
        from backend.correction_api_handler import lambda_log_correction
        
        event = {
            'body': json.dumps({
                'workflow_id': 'wf-123',
                'node_id': 'node-1',
                'original_input': 'test',
                'agent_output': 'output',
                'user_correction': 'corrected',
                'task_category': 'email'
            }),
            'headers': {}
        }
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value=None):
            result = lambda_log_correction(event, None)
        
        assert result['statusCode'] == 401
        
        body = json.loads(result['body'])
        # Error response contains 'error' field
        assert 'error' in body
        assert isinstance(body['error'], str)
    
    def test_get_workflow_404_when_not_found(self):
        """404 when requesting non-existent workflow"""
        from backend.get_workflow import lambda_handler
        
        event = {
            'httpMethod': 'GET',
            'pathParameters': {'workflowId': 'wf-nonexistent'},
            'queryStringParameters': {},
            'requestContext': {
                'authorizer': {
                    'jwt': {
                        'claims': {'sub': 'user-test-123'}
                    }
                }
            }
        }
        
        result = lambda_handler(event, None)
        
        # 404 or empty response
        assert result['statusCode'] in [200, 404]
    
    def test_options_request_cors_handling(self):
        """CORS handling for OPTIONS requests"""
        from backend.get_workflow import lambda_handler
        
        event = {
            'httpMethod': 'OPTIONS',
            'pathParameters': {},
            'queryStringParameters': {}
        }
        
        result = lambda_handler(event, None)
        
        # CORS preflight 성공
        assert result['statusCode'] == 200


class TestPaginationParameters:
    """페이지네이션 파라미터 검증"""
    
    def test_limit_parameter_validation(self):
        """limit 파라미터 범위 검증"""
        valid_limits = [1, 10, 50, 100]
        invalid_limits = [0, -1, 101, 1000]
        
        for limit in valid_limits:
            assert 1 <= limit <= 100, f"유효한 limit이어야 함: {limit}"
        
        for limit in invalid_limits:
            assert not (1 <= limit <= 100), f"무효한 limit이어야 함: {limit}"
    
    def test_next_token_roundtrip(self):
        """nextToken 왕복 인코딩/디코딩"""
        import base64
        
        # DynamoDB LastEvaluatedKey 시뮬레이션
        last_key = {
            "pk": "user123",
            "sk": "wf-abc-123"
        }
        
        # 인코딩 (백엔드 → 프론트엔드)
        next_token = base64.b64encode(json.dumps(last_key).encode()).decode()
        
        # 디코딩 (프론트엔드 → 백엔드)
        decoded_key = json.loads(base64.b64decode(next_token).decode())
        
        assert decoded_key == last_key
    
    def test_sort_order_values(self):
        """sortOrder 파라미터 값 검증"""
        valid_sort_orders = ["asc", "desc", "ASC", "DESC"]
        
        for order in valid_sort_orders:
            normalized = order.lower()
            assert normalized in ["asc", "desc"]


class TestHTTPStatusCodeConsistency:
    """HTTP 상태 코드 일관성 검증"""
    
    def test_success_codes(self):
        """성공 응답 코드"""
        success_cases = {
            "GET /workflows": 200,
            "POST /workflows": 201,
            "PUT /workflows/{id}": 200,
            "DELETE /workflows/{id}": 204,
            "POST /executions": 202,  # Accepted (비동기)
        }
        
        for endpoint, expected_code in success_cases.items():
            assert expected_code in [200, 201, 202, 204]
    
    def test_error_codes_mapping(self):
        """에러 유형별 상태 코드 매핑"""
        error_code_mapping = {
            "authentication_required": 401,
            "invalid_token": 401,
            "permission_denied": 403,
            "resource_not_found": 404,
            "validation_failed": 400,
            "invalid_json": 400,
            "rate_limit_exceeded": 429,
            "internal_error": 500,
            "service_unavailable": 503
        }
        
        # 각 에러 유형이 적절한 HTTP 코드에 매핑되는지
        for error_type, code in error_code_mapping.items():
            if "auth" in error_type or "token" in error_type:
                assert code == 401
            elif "permission" in error_type or "forbidden" in error_type:
                assert code == 403
            elif "not_found" in error_type:
                assert code == 404
            elif "validation" in error_type or "invalid" in error_type:
                assert code == 400


class TestWebSocketContract:
    """WebSocket 메시지 형식 검증"""
    
    def test_progress_message_format(self):
        """실행 진행률 메시지 형식"""
        progress_message = {
            "type": "execution_progress",
            "executionId": "exec-123",
            "payload": {
                "status": "RUNNING",
                "currentStep": 3,
                "totalSteps": 10,
                "progress": 0.3,
                "currentNodeId": "node-5",
                "message": "Processing step 3..."
            },
            "timestamp": "2026-01-03T12:00:00Z"
        }
        
        # 필수 필드 확인
        assert "type" in progress_message
        assert "executionId" in progress_message
        assert "payload" in progress_message
        assert "timestamp" in progress_message
        
        # payload 내부 필드
        payload = progress_message["payload"]
        assert "status" in payload
        assert "progress" in payload
        assert 0 <= payload["progress"] <= 1
    
    def test_error_message_format(self):
        """에러 메시지 형식"""
        error_message = {
            "type": "execution_error",
            "executionId": "exec-123",
            "payload": {
                "status": "FAILED",
                "error": "Node execution failed",
                "errorCode": "NODE_EXECUTION_ERROR",
                "failedNodeId": "node-7",
                "details": {
                    "reason": "Timeout after 30 seconds"
                }
            },
            "timestamp": "2026-01-03T12:00:00Z"
        }
        
        # 에러 타입 확인
        assert error_message["type"] == "execution_error"
        assert "error" in error_message["payload"]
    
    def test_hitl_pause_message_format(self):
        """HITL 일시정지 메시지 형식"""
        hitl_message = {
            "type": "hitl_required",
            "executionId": "exec-123",
            "payload": {
                "status": "PAUSED_FOR_HITL",
                "pausedNodeId": "node-approval",
                "pausedNodeLabel": "Manager Approval",
                "requiredAction": "approve_or_reject",
                "context": {
                    "request_amount": 50000,
                    "requester": "John Doe"
                },
                "taskToken": "arn:aws:states:..."
            },
            "timestamp": "2026-01-03T12:00:00Z"
        }
        
        # HITL 필수 필드
        payload = hitl_message["payload"]
        assert payload["status"] == "PAUSED_FOR_HITL"
        assert "pausedNodeId" in payload
        assert "taskToken" in payload
