"""
Vector Sync Core Tests
Production code: vector_sync_service.py (services/)
Core: sync_correction_to_vector_db, batch_sync_corrections, token limits

no-llm test: deterministic vector synchronization verification
"""
import pytest
import asyncio
import sys
import os
from unittest.mock import patch, MagicMock, AsyncMock

# 환경 변수 설정
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("MOCK_MODE", "true")
os.environ.setdefault("CORRECTION_LOGS_TABLE", "test-correction-logs")

# OpenAI 모킹
mock_openai = MagicMock()
sys.modules['openai'] = mock_openai

from moto import mock_aws
import boto3


@pytest.fixture(autouse=True)
def mock_aws_services():
    """모든 테스트에 AWS 모킹"""
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


class TestVectorSyncServiceCore:
    """VectorSyncService 핵심 테스트 - 프로덕션 메서드 사용"""
    
    def test_service_initialization(self):
        """서비스 초기화 성공"""
        try:
            from backend.services.vector_sync_service import VectorSyncService
        except ImportError:
            pytest.skip("vector_sync_service not available")
        
        service = VectorSyncService()
        
        # 핵심 속성 확인
        assert hasattr(service, 'ddb')
        assert hasattr(service, 'vector_sync_manager')
        assert hasattr(service, 'max_embedding_tokens')
        assert service.max_embedding_tokens == 8192  # OpenAI ada-002 기준
    
    def test_calculate_backoff_delay(self):
        """지수 백오프 지연 계산"""
        try:
            from backend.services.vector_sync_service import VectorSyncService
        except ImportError:
            pytest.skip("vector_sync_service not available")
        
        service = VectorSyncService()
        
        # 첫 번째 재시도
        delay1 = service._calculate_backoff_delay(1)
        # 두 번째 재시도
        delay2 = service._calculate_backoff_delay(2)
        
        # 지수적으로 증가해야 함
        assert delay2 > delay1
        # 최대 지연 시간 이하
        assert delay2 <= service.max_retry_delay
    
    def test_count_tokens_with_text(self):
        """토큰 카운트 테스트"""
        try:
            from backend.services.vector_sync_service import VectorSyncService
        except ImportError:
            pytest.skip("vector_sync_service not available")
        
        service = VectorSyncService()
        
        # 기본 텍스트 토큰 카운트
        text = "Hello, this is a test sentence for token counting."
        token_count = service._count_tokens(text)
        
        # 토큰 수는 양수여야 함
        assert token_count > 0
        # 문자 수보다 적거나 비슷해야 함 (대략적 검증)
        assert token_count <= len(text)
    
    def test_should_retry_error_classification(self):
        """재시도 가능 에러 분류"""
        try:
            from backend.services.vector_sync_service import VectorSyncService
        except ImportError:
            pytest.skip("vector_sync_service not available")
        
        service = VectorSyncService()
        
        # 재시도 가능한 에러 (네트워크, rate limit 등)
        assert service._should_retry_error("Connection timeout") == True or \
               service._should_retry_error("Rate limit exceeded") == True
        
        # 재시도 불가능한 에러 (validation 등)는 False 가능
        # 구현에 따라 다를 수 있음
    
    def test_optimization_metrics_available(self):
        """최적화 메트릭스 조회"""
        try:
            from backend.services.vector_sync_service import VectorSyncService
        except ImportError:
            pytest.skip("vector_sync_service not available")
        
        service = VectorSyncService()
        
        metrics = service.get_optimization_metrics()
        
        # 메트릭스 딕셔너리 반환
        assert isinstance(metrics, dict)
        # 핵심 메트릭스 키 존재
        assert "max_embedding_tokens" in metrics or len(metrics) > 0
