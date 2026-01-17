"""
Correction System Core Tests
Production code: backend/apps/backend/backend/correction_api_handler.py
Schema: CorrectionLogRequest (Pydantic)
Handler: lambda_log_correction

no-llm test: Verify deterministic behavior with OpenAI and AWS mocking
"""
import pytest
import json
import sys
import os
from unittest.mock import patch, AsyncMock, MagicMock

# 환경 변수 설정 (모듈 import 전)
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("MOCK_MODE", "true")
os.environ.setdefault("CORRECTION_LOGS_TABLE", "test-correction-logs")

# OpenAI 모킹 - 모듈 import 전에 설정
mock_openai = MagicMock()
sys.modules['openai'] = mock_openai

# boto3 DynamoDB 모킹 (moto 최신 버전)
from moto import mock_aws
import boto3


@pytest.fixture(autouse=True)
def mock_aws_services():
    """모든 테스트에 AWS 모킹 적용"""
    with mock_aws():
        # DynamoDB 테이블 생성
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
            TableName='test-correction-logs',
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        yield


class TestCorrectionSystemCore:
    """핵심 Correction API 기능 테스트 - 프로덕션 스키마 정확 반영"""
    
    def test_auth_missing_returns_401(self):
        """인증 없으면 401 (extract_and_verify_user_id=None)"""
        from backend.correction_api_handler import lambda_log_correction
        
        event = {
            "body": json.dumps({
                "workflow_id": "wf-123", "node_id": "node-1",
                "original_input": "test", "agent_output": "output",
                "user_correction": "corrected", "task_category": "email"
            }),
            "headers": {}
        }
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value=None):
            result = lambda_log_correction(event, None)
        
        assert result['statusCode'] == 401
        assert 'Unauthorized' in json.loads(result['body']).get('error', '')
    
    def test_validation_missing_node_id(self):
        """필수 필드 node_id 누락 시 400"""
        from backend.correction_api_handler import lambda_log_correction
        
        event = {
            "body": json.dumps({
                "workflow_id": "wf-123",
                "original_input": "test", "agent_output": "output",
                "user_correction": "corrected", "task_category": "email"
            }),
            "headers": {"Authorization": "Bearer test"}
        }
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value='user123'):
            result = lambda_log_correction(event, None)
        
        assert result['statusCode'] == 400
        assert 'node_id' in str(result['body']).lower() or 'validation' in str(result['body']).lower()
    
    def test_successful_correction_submission(self):
        """정상 요청은 200 + correction_id"""
        from backend.correction_api_handler import lambda_log_correction
        
        event = {
            "body": json.dumps({
                "workflow_id": "wf-123", "node_id": "node-1",
                "original_input": "test input", "agent_output": "test output",
                "user_correction": "corrected output", "task_category": "email"
            }),
            "headers": {"Authorization": "Bearer test"}
        }
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value='user123'), \
             patch('backend.correction_api_handler.correction_service') as mock_svc:
            mock_svc.log_correction = AsyncMock(return_value="corr-id-123")
            result = lambda_log_correction(event, None)
        
        # 디버그: 실패 시 응답 본문 출력
        if result['statusCode'] != 200:
            print(f"DEBUG Response: {result['body']}")
        
        assert result['statusCode'] == 200, f"Expected 200, got {result['statusCode']}: {result['body']}"
        body = json.loads(result['body'])
        assert 'correction_id' in body
    
    def test_large_payload_exceeds_max_length(self):
        """5000자 초과 시 400 (Field max_length=5000)"""
        from backend.correction_api_handler import lambda_log_correction
        
        event = {
            "body": json.dumps({
                "workflow_id": "wf-123", "node_id": "node-1",
                "original_input": "x" * 5001,
                "agent_output": "output", "user_correction": "corrected",
                "task_category": "email"
            }),
            "headers": {"Authorization": "Bearer test"}
        }
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value='user123'):
            result = lambda_log_correction(event, None)
        
        assert result['statusCode'] == 400
        assert '5000' in str(result['body']) or 'validation' in str(result['body']).lower()
    
    def test_invalid_json_returns_400(self):
        """잘못된 JSON 시 400"""
        from backend.correction_api_handler import lambda_log_correction
        
        event = {"body": '{invalid', "headers": {"Authorization": "Bearer test"}}
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value='user123'):
            result = lambda_log_correction(event, None)
        
        assert result['statusCode'] == 400
        assert 'json' in str(result['body']).lower() or 'format' in str(result['body']).lower()
    
    def test_special_characters_handled(self):
        """특수 문자 처리 (html.escape, Pydantic str_strip_whitespace)"""
        from backend.correction_api_handler import lambda_log_correction
        
        special = 'Test "quoted" \n newlines 한글 🎉'
        event = {
            "body": json.dumps({
                "workflow_id": "wf-123", "node_id": "node-1",
                "original_input": special, "agent_output": special,
                "user_correction": special, "task_category": "email"
            }),
            "headers": {"Authorization": "Bearer test"}
        }
        
        with patch('backend.correction_api_handler.extract_and_verify_user_id', return_value='user123'), \
             patch('backend.correction_api_handler.correction_service') as mock_svc:
            mock_svc.log_correction = AsyncMock(return_value="corr-id")
            result = lambda_log_correction(event, None)
        
        # Sanitization 처리 후 성공
        assert result['statusCode'] == 200
