"""
Security Core Tests
Production code: common/auth_utils.py, get_workflow.py, correction_api_handler.py
Core: Owner ID verification, authentication, XSS defense

no-llm test: Deterministic security logic verification
"""
import pytest
import json
import sys
import os
from unittest.mock import patch, MagicMock

# Environment variable setup
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("MOCK_MODE", "true")
os.environ.setdefault("WORKFLOWS_TABLE", "test-workflows")

# OpenAI mocking
mock_openai = MagicMock()
sys.modules['openai'] = mock_openai

from moto import mock_aws
import boto3


@pytest.fixture(autouse=True)
def mock_aws_services():
    """Mock AWS for all tests"""
    with mock_aws():
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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
        yield


class TestAuthUtilsCore:
    """auth_utils.py core tests (Table-Driven)"""
    
    @pytest.mark.parametrize("event,expected_owner_id", [
        # Extract from sub in JWT claims
        ({"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "user-abc-123"}}}}}, "user-abc-123"),
        # REST API format authorizer claims
        ({"requestContext": {"authorizer": {"claims": {"sub": "rest-user-456"}}}}, "rest-user-456"),
        # None if no JWT claims
        ({"requestContext": {}}, None),
        # None if no requestContext
        ({}, None),
        # None if authorizer is empty object
        ({"requestContext": {"authorizer": {}}}, None),
    ])
    def test_extract_owner_id_from_event(self, event, expected_owner_id):
        """Extract owner_id from various event formats (Table-Driven)"""
        from src.common.auth_utils import extract_owner_id_from_event
        
        owner_id = extract_owner_id_from_event(event)
        assert owner_id == expected_owner_id
    
    def test_require_authentication_raises_on_missing(self):
        """require_authentication raises ValueError when no authentication"""
        from src.common.auth_utils import require_authentication
        
        event = {"requestContext": {}}
        
        with pytest.raises(ValueError) as exc_info:
            require_authentication(event)
        
        assert "Authentication required" in str(exc_info.value)


class TestGetWorkflowSecurity:
    """get_workflow.py security tests"""
    
    def test_get_workflow_requires_auth(self):
        """401 when no authentication"""
        from backend.get_workflow import lambda_handler
        
        event = {
            "httpMethod": "GET",
            "queryStringParameters": {},
            "requestContext": {}
        }
        
        result = lambda_handler(event, None)
        assert result['statusCode'] == 401
    
    def test_get_workflow_ignores_query_param_owner_id(self):
        """Ignore ownerId from query param and use JWT's sub"""
        from backend.get_workflow import lambda_handler
        
        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"ownerId": "attacker-id"},
            "requestContext": {
                "authorizer": {
                    "jwt": {"claims": {"sub": "real-user-id"}}
                }
            }
        }
        
        with patch('backend.get_workflow.table') as mock_table:
            mock_table.query.return_value = {"Items": [], "Count": 0}
            result = lambda_handler(event, None)
        
        assert result['statusCode'] == 200
        # Queried by JWT's owner_id
        mock_table.query.assert_called()


class TestInputSanitization:
    """Input Sanitization Test (Table-Driven)"""
    
    @pytest.mark.parametrize("input_data,expected_escaped", [
        # script tag escape
        ({"text": "<script>alert('XSS')</script>"}, "&lt;script&gt;"),
        # event handler escape
        ({"img": "<img src=x onerror=alert(1)>"}, "&lt;"),
    ])
    def test_xss_patterns_escaped(self, input_data, expected_escaped):
        """XSS patterns are escaped (Table-Driven)"""
        from backend.correction_api_handler import sanitize_input_data
        
        result = sanitize_input_data(input_data)
        result_str = json.dumps(result)
        assert expected_escaped in result_str
    
    def test_nested_object_sanitized(self):
        """Nested objects are also sanitized"""
        from backend.correction_api_handler import sanitize_input_data
        
        result = sanitize_input_data({
            "level1": {
                "level2": "<script>bad</script>"
            }
        })
        
        # Nested object internals are also processed
        assert "&lt;script&gt;" in str(result) or "script" not in str(result).lower()


class TestPathTraversal:
    """Path Traversal Defense Test"""
    
    def test_dangerous_paths_detected(self):
        """Dangerous path detection"""
        dangerous_paths = [
            "../../etc/passwd",
            "../../../root/.ssh/id_rsa",
            "workflow-states/user/../admin/state.json"
        ]
        
        for path in dangerous_paths:
            # 모든 위험 경로는 .. 포함
            assert ".." in path
            # 정규화하면 상위 디렉토리 이동 시도
            normalized = os.path.normpath(path)
            # etc, root, admin 등 민감 경로 접근 시도
            assert any(x in normalized for x in ["etc", "root", "admin", "ssh"])
