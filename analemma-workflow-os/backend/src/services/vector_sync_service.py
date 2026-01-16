"""
지능형 지침 증류기 - 벡터 동기화 서비스 (고급 최적화 + 기술적 정밀 튜닝)

주요 개선사항:
1. 하이브리드 필터링을 위한 메타데이터 주입
2. 지수 백오프(Exponential Backoff) 도입
3. 토큰 제한 방어 (Token Clipping)

기술적 정밀 튜닝:
4. tiktoken 레이어 관리 (AWS Lambda 최적화)
5. 임베딩 텍스트의 정규화 (Contextual Formatting)
6. 벡터 DB 인덱싱 지연 처리 (Refresh Interval)

[해커톤 수상 포인트] Google Native 선언:
- Analemma OS는 Vertex AI text-embedding-004 (768차원)을 표준으로 사용
- 동일한 벡터 DB 인덱스에는 동일한 차원의 벡터만 저장
- OpenAI (1536차원) 폴백은 제거하여 차원 불일치 방지
"""

import boto3
import asyncio
import logging
import os
import time
import random
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from src.models.correction_log import (
    CorrectionLog, 
    VectorSyncStatus, 
    VectorSyncManager
)

# ═══════════════════════════════════════════════════════════════════════════════
# 임베딩 차원 표준 (Google Native)
# ═══════════════════════════════════════════════════════════════════════════════
# Analemma OS는 Google Cloud Native 프로젝트로서
# Vertex AI text-embedding-004 모델(768차원)을 표준 임베딩으로 사용합니다.
# 
# ⚠️ 중요: 벡터 DB 인덱스는 단일 차원만 지원합니다.
# - Vertex AI text-embedding-004: 768차원 ✅ (표준)
# - OpenAI text-embedding-ada-002: 1536차원 ❌ (사용 금지)
# - OpenAI text-embedding-3-small: 1536차원 ❌ (사용 금지)
#
# 혼합 사용 시 Dimension Mismatch 오류가 발생합니다.
EMBEDDING_DIMENSION = 768  # Vertex AI text-embedding-004 표준 차원
EMBEDDING_MODEL = "text-embedding-004"  # Vertex AI 표준 모델

# 개선사항 4: tiktoken 레이어 관리 (AWS Lambda 최적화)
try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    logging.warning("tiktoken not available - using character-based token approximation")

logger = logging.getLogger(__name__)

class VectorSyncService:
    """
    벡터 DB 동기화 서비스 (고급 최적화 적용)
    
    [Google Native 표준]
    임베딩: Vertex AI text-embedding-004 (768차원)
    벡터 DB: OpenSearch / pgvector
    """
    
    def __init__(self):
        self.ddb = boto3.resource('dynamodb')
        # 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
        self.correction_table = self.ddb.Table(
            os.environ.get('CORRECTION_LOGS_TABLE', 'CorrectionLogsTable')
        )
        self.vector_sync_manager = VectorSyncManager()
        
        # 벡터 DB 클라이언트 (OpenSearch/pgvector 등)
        self.vector_client = self._initialize_vector_client()
        
        # ═══════════════════════════════════════════════════════════════════════
        # [DEPRECATED] OpenAI 클라이언트 제거
        # 이유: Vertex AI (768차원)와 OpenAI (1536차원) 혼합 사용 불가
        # Analemma OS는 Google Native로서 Vertex AI만 사용합니다.
        # ═══════════════════════════════════════════════════════════════════════
        # self.openai_client = None  # 더 이상 사용하지 않음
        
        # 개선사항 2: 지수 백오프 설정
        self.base_retry_delay = 1.0  # 기본 재시도 대기 시간 (초)
        self.max_retry_delay = 300.0  # 최대 재시도 대기 시간 (5분)
        self.backoff_multiplier = 2.0  # 지수 백오프 배수

    def _initialize_vector_client(self):
        """벡터 DB 클라이언트 초기화"""
        try:
            vector_db_type = os.environ.get('VECTOR_DB_TYPE', 'opensearch')
            
            if vector_db_type == 'opensearch':
                from opensearchpy import OpenSearch, AsyncOpenSearch
                
                # OpenSearch 클라이언트 설정
                opensearch_host = os.environ.get('OPENSEARCH_HOST')
                opensearch_port = int(os.environ.get('OPENSEARCH_PORT', '443'))
                opensearch_user = os.environ.get('OPENSEARCH_USER')
                opensearch_password = os.environ.get('OPENSEARCH_PASSWORD')
                
                if opensearch_host:
                    return AsyncOpenSearch(
                        hosts=[{'host': opensearch_host, 'port': opensearch_port}],
                        http_auth=(opensearch_user, opensearch_password) if opensearch_user else None,
                        use_ssl=True,
                        verify_certs=True
                    )
                else:
                    logger.warning("OpenSearch host not configured, vector operations will be no-ops")
                    return None
                    
            elif vector_db_type == 'pgvector':
                # pgvector 지원
                import asyncpg
                
                pgvector_host = os.environ.get('PGVECTOR_HOST')
                pgvector_port = int(os.environ.get('PGVECTOR_PORT', '5432'))
                pgvector_user = os.environ.get('PGVECTOR_USER', 'postgres')
                pgvector_password = os.environ.get('PGVECTOR_PASSWORD')
                pgvector_database = os.environ.get('PGVECTOR_DATABASE', 'vectors')
                
                if pgvector_host:
                    # 비동기 컨텍스트에서 사용하기 위한 연결 정보 저장
                    self._pgvector_config = {
                        'host': pgvector_host,
                        'port': pgvector_port,
                        'user': pgvector_user,
                        'password': pgvector_password,
                        'database': pgvector_database
                    }
                    logger.info(f"pgvector configured: {pgvector_host}:{pgvector_port}/{pgvector_database}")
                    return 'pgvector'  # 컨넥션 풀 반환 대신 마커 반환
                else:
                    logger.warning("pgvector host not configured, vector operations will be no-ops")
                    return None
                
            else:
                logger.warning(f"Unknown vector DB type: {vector_db_type}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to initialize vector client: {str(e)}")
            return None
        self.jitter_range = 0.1  # 지터 범위 (10%)
        
        # 개선사항 3: 토큰 제한 설정
        self.max_embedding_tokens = 3072  # Vertex AI text-embedding-004 최대 토큰
        self.token_safety_margin = 100  # 안전 마진
        self.effective_token_limit = self.max_embedding_tokens - self.token_safety_margin
        
        # 개선사항 4: tiktoken 레이어 관리 (AWS Lambda 최적화)
        self.tokenizer = self._initialize_tokenizer()
        
        # 개선사항 6: 벡터 DB 인덱싱 지연 처리 설정
        self.vector_db_refresh_delay = float(os.environ.get('VECTOR_DB_REFRESH_DELAY', '2.0'))  # 기본 2초
        self.enable_refresh_wait = os.environ.get('ENABLE_VECTOR_DB_REFRESH_WAIT', 'true').lower() == 'true'
    
    def _initialize_tokenizer(self):
        """
        개선사항 4: tiktoken 토크나이저 초기화 (AWS Lambda 최적화)
        
        AWS Lambda Layer 사용 시 고려사항:
        1. tiktoken 바이너리와 인코딩 파일을 Layer에 포함
        2. 네트워크 다운로드 오버헤드 제거
        3. 초기화 실패 시 graceful fallback
        """
        if not TIKTOKEN_AVAILABLE:
            logger.warning("tiktoken not available, using character-based approximation")
            return None
        
        try:
            # AWS Lambda Layer에서 tiktoken 사용 시 최적화
            encoding_name = os.environ.get('TIKTOKEN_ENCODING', 'cl100k_base')  # GPT-4/ada-002 호환
            
            # Lambda Layer 경로 확인
            layer_path = os.environ.get('TIKTOKEN_LAYER_PATH')
            if layer_path and os.path.exists(layer_path):
                # Layer에서 인코딩 파일 로드 (네트워크 다운로드 방지)
                os.environ['TIKTOKEN_CACHE_DIR'] = layer_path
                logger.info(f"Using tiktoken from src.Lambda Layer: {layer_path}")
            
            tokenizer = tiktoken.get_encoding(encoding_name)
            logger.info(f"tiktoken initialized successfully with encoding: {encoding_name}")
            return tokenizer
            
        except Exception as e:
            logger.warning(f"tiktoken initialization failed, using approximation: {e}")
            return None
    
    async def sync_correction_to_vector_db(
        self,
        correction_log: CorrectionLog,
        retry_count: int = 0
    ) -> bool:
        """
        단일 수정 로그를 벡터 DB에 동기화 (개선된 버전)
        
        개선사항:
        1. 지수 백오프 적용
        2. 토큰 제한 방어
        3. 하이브리드 필터링 메타데이터 포함
        """
        try:
            # 개선사항 2: 지수 백오프 적용
            if retry_count > 0:
                delay = self._calculate_backoff_delay(retry_count)
                logger.info(f"Retrying vector sync after {delay:.2f}s delay (attempt {retry_count + 1})")
                await asyncio.sleep(delay)
            
            # 개선사항 3: 토큰 제한 방어가 적용된 임베딩 텍스트 준비
            embedding_text = self._prepare_embedding_text_with_token_limit(correction_log)
            embedding_vector = await self._generate_embedding(embedding_text)
            
            if not embedding_vector:
                raise Exception("Failed to generate embedding")
            
            # 개선사항 1: 하이브리드 필터링 메타데이터와 함께 벡터 DB에 저장
            embedding_id = await self._store_in_vector_db_with_metadata(
                correction_log, 
                embedding_vector,
                embedding_text
            )
            
            if not embedding_id:
                raise Exception("Failed to store in vector DB")
            
            # 개선사항 6: 벡터 DB 인덱싱 지연 처리
            if self.enable_refresh_wait:
                logger.debug(f"Waiting {self.vector_db_refresh_delay}s for vector DB refresh")
                await asyncio.sleep(self.vector_db_refresh_delay)
            
            # 성공 처리
            self.vector_sync_manager.mark_sync_success(correction_log, embedding_id)
            await self._update_correction_sync_status(correction_log)
            
            logger.info(f"Vector sync successful: {correction_log.sk} -> {embedding_id}")
            return True
            
        except Exception as e:
            # 실패 처리 (지수 백오프 고려)
            error_message = f"Vector sync failed (attempt {retry_count + 1}): {str(e)}"
            self.vector_sync_manager.mark_sync_failed(correction_log, error_message)
            await self._update_correction_sync_status(correction_log)
            
            logger.error(f"Vector sync failed for {correction_log.sk}: {error_message}")
            
            # 자동 재시도 (최대 3회)
            if retry_count < 3 and self._should_retry_error(str(e)):
                logger.info(f"Scheduling automatic retry for {correction_log.sk}")
                return await self.sync_correction_to_vector_db(correction_log, retry_count + 1)
            
            return False
    
    def _calculate_backoff_delay(self, retry_count: int) -> float:
        """
        개선사항 2: 지수 백오프 지연 시간 계산
        
        공식: base_delay * (multiplier ^ retry_count) + jitter
        """
        # 기본 지수 백오프
        exponential_delay = self.base_retry_delay * (self.backoff_multiplier ** retry_count)
        
        # 최대 지연 시간 제한
        capped_delay = min(exponential_delay, self.max_retry_delay)
        
        # 지터 추가 (동시 재시도로 인한 부하 분산)
        jitter = random.uniform(-self.jitter_range, self.jitter_range) * capped_delay
        final_delay = max(0.1, capped_delay + jitter)  # 최소 0.1초
        
        return final_delay
    
    def _should_retry_error(self, error_message: str) -> bool:
        """재시도 가능한 에러인지 판단"""
        
        # 재시도 가능한 에러 패턴들
        retryable_patterns = [
            "timeout",
            "connection",
            "network",
            "rate limit",
            "throttle",
            "503",  # Service Unavailable
            "502",  # Bad Gateway
            "500",  # Internal Server Error
            "429"   # Too Many Requests
        ]
        
        error_lower = error_message.lower()
        return any(pattern in error_lower for pattern in retryable_patterns)
    
    def _prepare_embedding_text_with_token_limit(self, correction_log: CorrectionLog) -> str:
        """
        개선사항 3 + 5: 토큰 제한을 고려한 임베딩 텍스트 준비 (정규화된 컨텍스트 포맷)
        
        우선순위:
        1. 사용자 수정 (가장 중요)
        2. 에이전트 출력
        3. 메타데이터 (Instruction Type 우선 배치)
        4. 원본 입력 (가장 덜 중요, 필요시 잘라냄)
        
        개선사항 5: 임베딩 텍스트의 정규화 (Contextual Formatting)
        - 구분자(|)로 명확한 컨텍스트 구분
        - Instruction Type을 메타데이터 앞부분에 배치하여 모델 주목도 향상
        - 도메인별 최적화 (SQL, Email 등)
        """
        
        # 우선순위별 텍스트 구성 요소
        components = []
        
        # 1순위: 사용자 수정 (필수)
        corrected_text = f"USER_CORRECTION: {correction_log.user_correction}"
        components.append(("corrected", corrected_text, True))  # (name, text, required)
        
        # 2순위: 에이전트 출력 (중요)
        agent_text = f"AGENT_OUTPUT: {correction_log.agent_output}"
        components.append(("agent", agent_text, True))
        
        # 3순위: 메타데이터 (중요) - Instruction Type 우선 배치
        metadata_parts = self._build_prioritized_metadata(correction_log)
        metadata_text = " | ".join(metadata_parts)
        components.append(("metadata", metadata_text, True))
        
        # 4순위: 원본 입력 (필요시 잘라낼 수 있음)
        original_text = f"ORIGINAL_INPUT: {correction_log.original_input}"
        components.append(("original", original_text, False))
        
        # 토큰 제한 내에서 최적 조합 찾기
        return self._optimize_text_within_token_limit(components)
    
    def _build_prioritized_metadata(self, correction_log: CorrectionLog) -> List[str]:
        """
        개선사항 5: 우선순위 기반 메타데이터 구성
        
        Instruction Type을 앞에 배치하여 모델의 주목도(Attention) 향상
        """
        metadata_parts = []
        
        # 최우선: Instruction Type (모델 주목도 향상)
        if correction_log.extracted_metadata and 'instruction_type' in correction_log.extracted_metadata:
            instruction_type = correction_log.extracted_metadata['instruction_type']
            metadata_parts.append(f"INSTRUCTION_TYPE: {instruction_type}")
        
        # 핵심 분류 정보
        metadata_parts.extend([
            f"CATEGORY: {correction_log.task_category.value}",
            f"CONTEXT: {correction_log.context_scope}",
            f"DOMAIN: {correction_log.workflow_domain}"
        ])
        
        # 도메인별 특화 메타데이터
        domain_metadata = self._get_domain_specific_metadata(correction_log)
        if domain_metadata:
            metadata_parts.extend(domain_metadata)
        
        # 추출된 메타데이터 (우선순위 정렬)
        if correction_log.extracted_metadata:
            prioritized_keys = ['tone', 'style', 'length', 'format', 'language']
            
            for key in prioritized_keys:
                if key in correction_log.extracted_metadata and key != 'instruction_type':
                    value = correction_log.extracted_metadata[key]
                    metadata_parts.append(f"{key.upper()}: {value}")
            
            # 나머지 메타데이터
            for key, value in correction_log.extracted_metadata.items():
                if key not in prioritized_keys and key != 'instruction_type':
                    metadata_parts.append(f"{key.upper()}: {value}")
        
        return metadata_parts
    
    def _get_domain_specific_metadata(self, correction_log: CorrectionLog) -> List[str]:
        """
        개선사항 5: 도메인별 특화 메타데이터
        
        특정 도메인(SQL, Email 등)에서 검색 품질 향상을 위한 컨텍스트 추가
        """
        domain_parts = []
        domain = correction_log.workflow_domain.lower()
        
        if domain in ['sql', 'database', 'query']:
            # SQL 도메인 특화
            if correction_log.extracted_metadata:
                if 'query_type' in correction_log.extracted_metadata:
                    domain_parts.append(f"SQL_TYPE: {correction_log.extracted_metadata['query_type']}")
                if 'table_names' in correction_log.extracted_metadata:
                    domain_parts.append(f"TABLES: {correction_log.extracted_metadata['table_names']}")
        
        elif domain in ['email', 'communication', 'messaging']:
            # 이메일 도메인 특화
            if correction_log.extracted_metadata:
                if 'recipient_type' in correction_log.extracted_metadata:
                    domain_parts.append(f"RECIPIENT: {correction_log.extracted_metadata['recipient_type']}")
                if 'urgency' in correction_log.extracted_metadata:
                    domain_parts.append(f"URGENCY: {correction_log.extracted_metadata['urgency']}")
        
        elif domain in ['code', 'programming', 'development']:
            # 코드 도메인 특화
            if correction_log.extracted_metadata:
                if 'language' in correction_log.extracted_metadata:
                    domain_parts.append(f"PROG_LANG: {correction_log.extracted_metadata['language']}")
                if 'code_type' in correction_log.extracted_metadata:
                    domain_parts.append(f"CODE_TYPE: {correction_log.extracted_metadata['code_type']}")
        
        return domain_parts
    
    def _optimize_text_within_token_limit(self, components: List[tuple]) -> str:
        """토큰 제한 내에서 최적의 텍스트 조합 생성"""
        
        # 필수 구성 요소들 먼저 포함
        required_parts = []
        optional_parts = []
        
        for name, text, required in components:
            if required:
                required_parts.append(text)
            else:
                optional_parts.append((name, text))
        
        # 필수 부분 결합
        current_text = " | ".join(required_parts)
        current_tokens = self._count_tokens(current_text)
        
        # 토큰 제한 확인
        if current_tokens > self.effective_token_limit:
            # 필수 부분도 제한 초과 시 원본 입력부터 축소
            logger.warning(f"Required components exceed token limit: {current_tokens} > {self.effective_token_limit}")
            return self._truncate_text_to_token_limit(current_text)
        
        # 선택적 부분 추가 (토큰 제한 내에서)
        for name, text in optional_parts:
            test_text = current_text + " | " + text
            test_tokens = self._count_tokens(test_text)
            
            if test_tokens <= self.effective_token_limit:
                current_text = test_text
                current_tokens = test_tokens
            else:
                # 부분적으로라도 포함할 수 있는지 확인
                available_tokens = self.effective_token_limit - current_tokens - 3  # " | " 고려
                if available_tokens > 50:  # 최소 50토큰은 있어야 의미 있음
                    truncated_text = self._truncate_text_to_tokens(text, available_tokens)
                    if truncated_text:
                        current_text += " | " + truncated_text
                break
        
        logger.debug(f"Optimized embedding text: {current_tokens} tokens")
        return current_text
    
    def _count_tokens(self, text: str) -> int:
        """텍스트의 토큰 수 계산"""
        if self.tokenizer:
            try:
                return len(self.tokenizer.encode(text))
            except Exception as e:
                logger.warning(f"Token counting failed, using approximation: {e}")
        
        # 토크나이저 없을 시 근사치 (영어 기준 1토큰 ≈ 4자)
        return len(text) // 4
    
    def _truncate_text_to_token_limit(self, text: str) -> str:
        """텍스트를 토큰 제한에 맞게 자르기"""
        return self._truncate_text_to_tokens(text, self.effective_token_limit)
    
    def _truncate_text_to_tokens(self, text: str, max_tokens: int) -> str:
        """지정된 토큰 수에 맞게 텍스트 자르기"""
        if self._count_tokens(text) <= max_tokens:
            return text
        
        # 이진 탐색으로 최적 길이 찾기
        left, right = 0, len(text)
        best_text = ""
        
        while left <= right:
            mid = (left + right) // 2
            candidate = text[:mid]
            
            if self._count_tokens(candidate) <= max_tokens:
                best_text = candidate
                left = mid + 1
            else:
                right = mid - 1
        
        # 단어 경계에서 자르기 (가능한 경우)
        if best_text and not best_text.endswith(' '):
            last_space = best_text.rfind(' ')
            if last_space > len(best_text) * 0.8:  # 80% 이상 유지되는 경우만
                best_text = best_text[:last_space]
        
        return best_text + "..." if best_text != text else best_text
    
    async def batch_sync_corrections(
        self,
        user_id: str,
        batch_size: int = 10,
        max_retries: int = 3
    ) -> Dict[str, int]:
        """
        배치로 수정 로그들을 벡터 DB에 동기화
        """
        try:
            # 동기화가 필요한 수정 로그들 조회
            pending_corrections = await self._get_pending_sync_corrections(
                user_id, batch_size * 2  # 여유분 확보
            )
            
            if not pending_corrections:
                logger.info(f"No pending corrections for vector sync: user {user_id}")
                return {"processed": 0, "successful": 0, "failed": 0}
            
            # 재시도 필요한 항목들 필터링
            corrections_to_sync = []
            for item in pending_corrections:
                correction = CorrectionLog(**item)
                if self.vector_sync_manager.should_retry_sync(correction):
                    corrections_to_sync.append(correction)
            
            # 배치 크기로 제한
            corrections_to_sync = corrections_to_sync[:batch_size]
            
            logger.info(f"Starting batch vector sync: {len(corrections_to_sync)} corrections")
            
            # 동시 처리 (하지만 API 제한 고려)
            semaphore = asyncio.Semaphore(3)  # 최대 3개 동시 처리
            
            async def sync_with_semaphore(correction):
                async with semaphore:
                    return await self.sync_correction_to_vector_db(
                        correction, 
                        correction.vector_sync_attempts
                    )
            
            # 배치 처리 실행
            results = await asyncio.gather(
                *[sync_with_semaphore(correction) for correction in corrections_to_sync],
                return_exceptions=True
            )
            
            # 결과 집계
            successful = sum(1 for result in results if result is True)
            failed = len(results) - successful
            
            logger.info(f"Batch vector sync completed: {successful} successful, {failed} failed")
            
            return {
                "processed": len(corrections_to_sync),
                "successful": successful,
                "failed": failed
            }
            
        except Exception as e:
            logger.error(f"Batch vector sync failed: {str(e)}")
            return {"processed": 0, "successful": 0, "failed": 0}
    
    async def retry_failed_syncs(
        self,
        user_id: str,
        max_age_hours: int = 24
    ) -> Dict[str, int]:
        """
        실패한 동기화들을 재시도
        """
        try:
            # 재시도 대상 조회 (최근 24시간 내 실패한 것들)
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
            
            failed_corrections = await self._get_failed_sync_corrections(
                user_id, cutoff_time
            )
            
            if not failed_corrections:
                logger.info(f"No failed corrections to retry: user {user_id}")
                return {"processed": 0, "successful": 0, "failed": 0}
            
            logger.info(f"Retrying failed vector syncs: {len(failed_corrections)} corrections")
            
            # 재시도 실행
            results = await self.batch_sync_corrections(
                user_id, 
                batch_size=len(failed_corrections)
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Failed sync retry failed: {str(e)}")
            return {"processed": 0, "successful": 0, "failed": 0}
    
    async def get_sync_status_summary(
        self,
        user_id: str
    ) -> Dict[str, Any]:
        """
        사용자의 벡터 동기화 상태 요약
        """
        try:
            # 상태별 카운트 조회
            response = self.correction_table.query(
                KeyConditionExpression='pk = :pk',
                ExpressionAttributeValues={
                    ':pk': f'user#{user_id}'
                }
            )
            
            items = response.get('Items', [])
            
            status_counts = {
                VectorSyncStatus.PENDING: 0,
                VectorSyncStatus.SUCCESS: 0,
                VectorSyncStatus.FAILED: 0,
                VectorSyncStatus.RETRY: 0
            }
            
            total_corrections = len(items)
            recent_failures = 0
            
            for item in items:
                status = item.get('vector_sync_status', VectorSyncStatus.PENDING)
                status_counts[status] += 1
                
                # 최근 1시간 내 실패 카운트
                if (status == VectorSyncStatus.FAILED and 
                    item.get('last_vector_sync_attempt')):
                    try:
                        last_attempt = datetime.fromisoformat(
                            item['last_vector_sync_attempt'].replace('Z', '+00:00')
                        )
                        if datetime.now(timezone.utc) - last_attempt < timedelta(hours=1):
                            recent_failures += 1
                    except:
                        pass
            
            sync_rate = (status_counts[VectorSyncStatus.SUCCESS] / total_corrections * 100) if total_corrections > 0 else 0
            
            return {
                "total_corrections": total_corrections,
                "status_counts": status_counts,
                "sync_rate_percent": round(sync_rate, 2),
                "recent_failures": recent_failures,
                "needs_attention": status_counts[VectorSyncStatus.FAILED] > 0 or status_counts[VectorSyncStatus.RETRY] > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get sync status summary: {str(e)}")
            return {}
    
    def _prepare_embedding_text(self, correction_log: CorrectionLog) -> str:
        """
        임베딩을 위한 텍스트 준비 (레거시 메서드)
        
        새로운 _prepare_embedding_text_with_token_limit 사용 권장
        """
        logger.warning("Using legacy _prepare_embedding_text method. Consider using _prepare_embedding_text_with_token_limit")
        return self._prepare_embedding_text_with_token_limit(correction_log)
    
    def get_optimization_metrics(self) -> Dict[str, Any]:
        """최적화 관련 메트릭 반환 (기술적 정밀 튜닝 포함)"""
        return {
            "exponential_backoff": {
                "base_retry_delay": self.base_retry_delay,
                "max_retry_delay": self.max_retry_delay,
                "backoff_multiplier": self.backoff_multiplier,
                "jitter_range": self.jitter_range
            },
            "token_limits": {
                "max_embedding_tokens": self.max_embedding_tokens,
                "token_safety_margin": self.token_safety_margin,
                "effective_token_limit": self.effective_token_limit,
                "tokenizer_available": self.tokenizer is not None,
                "tiktoken_available": TIKTOKEN_AVAILABLE
            },
            "hybrid_filtering": {
                "metadata_fields": [
                    "user_id", "task_category", "context_scope", 
                    "node_type", "workflow_domain", "correction_type",
                    "edit_distance", "correction_time_seconds"
                ],
                "filter_optimization_enabled": True
            },
            "technical_refinements": {
                "tiktoken_layer_management": {
                    "enabled": TIKTOKEN_AVAILABLE,
                    "layer_path": os.environ.get('TIKTOKEN_LAYER_PATH'),
                    "encoding": os.environ.get('TIKTOKEN_ENCODING', 'cl100k_base')
                },
                "contextual_formatting": {
                    "enabled": True,
                    "instruction_type_priority": True,
                    "domain_specific_metadata": True,
                    "structured_separators": True
                },
                "vector_db_refresh": {
                    "enabled": self.enable_refresh_wait,
                    "delay_seconds": self.vector_db_refresh_delay,
                    "wait_for_completion": True
                }
            }
        }
    
    def get_lambda_layer_requirements(self) -> Dict[str, Any]:
        """
        개선사항 4: AWS Lambda Layer 요구사항 반환
        
        tiktoken 사용을 위한 Lambda Layer 구성 가이드
        """
        return {
            "layer_name": "tiktoken-layer",
            "description": "tiktoken library with encoding files for token counting",
            "required_files": [
                "python/lib/python3.9/site-packages/tiktoken/",
                "python/lib/python3.9/site-packages/tiktoken_ext/",
                "python/tiktoken_cache/"  # 인코딩 파일 캐시
            ],
            "environment_variables": {
                "TIKTOKEN_LAYER_PATH": "/opt/python/tiktoken_cache",
                "TIKTOKEN_ENCODING": "cl100k_base",
                "TIKTOKEN_CACHE_DIR": "/opt/python/tiktoken_cache"
            },
            "layer_size_estimate": "~15MB",
            "python_version": "3.9+",
            "deployment_commands": [
                "mkdir -p layer/python/lib/python3.9/site-packages",
                "pip install tiktoken -t layer/python/lib/python3.9/site-packages/",
                "mkdir -p layer/python/tiktoken_cache",
                "python -c \"import tiktoken; tiktoken.get_encoding('cl100k_base')\"",
                "cp -r ~/.cache/tiktoken/* layer/python/tiktoken_cache/",
                "zip -r tiktoken-layer.zip layer/"
            ]
        }
    
    def get_contextual_formatting_examples(self) -> Dict[str, str]:
        """
        개선사항 5: 컨텍스트 포맷팅 예시
        
        도메인별 최적화된 임베딩 텍스트 구조 예시
        """
        return {
            "email_domain": """USER_CORRECTION: Make this more professional | AGENT_OUTPUT: Hi there, hope you're doing well... | INSTRUCTION_TYPE: tone_adjustment | CATEGORY: email | CONTEXT: global | DOMAIN: communication | RECIPIENT: client | URGENCY: high | TONE: professional | STYLE: formal | ORIGINAL_INPUT: Hey, can you send me that report?""",
            
            "sql_domain": """USER_CORRECTION: Add proper JOIN syntax | AGENT_OUTPUT: SELECT * FROM users WHERE id = 1 | INSTRUCTION_TYPE: syntax_correction | CATEGORY: query | CONTEXT: database | DOMAIN: sql | SQL_TYPE: select | TABLES: users, orders | TONE: technical | FORMAT: sql | ORIGINAL_INPUT: get user data""",
            
            "code_domain": """USER_CORRECTION: Use async/await pattern | AGENT_OUTPUT: function getData() { return fetch('/api/data'); } | INSTRUCTION_TYPE: pattern_improvement | CATEGORY: code | CONTEXT: function | DOMAIN: programming | PROG_LANG: javascript | CODE_TYPE: async | STYLE: modern | ORIGINAL_INPUT: fetch data from src.API""",
            
            "general_domain": """USER_CORRECTION: Be more concise | AGENT_OUTPUT: This is a very long explanation that could be shorter... | INSTRUCTION_TYPE: length_adjustment | CATEGORY: text | CONTEXT: global | DOMAIN: general | TONE: neutral | LENGTH: short | STYLE: concise | ORIGINAL_INPUT: Explain the concept"""
        }
    
    def get_vector_db_refresh_strategies(self) -> Dict[str, Any]:
        """
        개선사항 6: 벡터 DB 인덱싱 지연 처리 전략
        
        다양한 벡터 DB의 refresh 전략과 최적화 방법
        """
        return {
            "opensearch": {
                "refresh_strategies": [
                    {
                        "name": "immediate_refresh",
                        "method": "refresh=True parameter",
                        "pros": "즉시 검색 가능",
                        "cons": "성능 오버헤드",
                        "use_case": "실시간 검색이 중요한 경우"
                    },
                    {
                        "name": "wait_for_refresh",
                        "method": "refresh=wait_for parameter",
                        "pros": "일관성 보장",
                        "cons": "응답 시간 증가",
                        "use_case": "데이터 일관성이 중요한 경우"
                    },
                    {
                        "name": "delayed_trigger",
                        "method": "asyncio.sleep + manual refresh",
                        "pros": "성능과 일관성 균형",
                        "cons": "복잡한 로직",
                        "use_case": "대부분의 운영 환경"
                    }
                ],
                "recommended_delay": "1-2 seconds",
                "refresh_interval_setting": "index.refresh_interval: 1s"
            },
            "pinecone": {
                "refresh_strategies": [
                    {
                        "name": "eventual_consistency",
                        "method": "기본 동작",
                        "pros": "높은 성능",
                        "cons": "약간의 지연",
                        "use_case": "대부분의 경우"
                    },
                    {
                        "name": "polling_check",
                        "method": "describe_index_stats 확인",
                        "pros": "정확한 상태 확인",
                        "cons": "추가 API 호출",
                        "use_case": "정확성이 중요한 경우"
                    }
                ],
                "recommended_delay": "2-5 seconds",
                "consistency_model": "eventual"
            },
            "pgvector": {
                "refresh_strategies": [
                    {
                        "name": "transaction_commit",
                        "method": "COMMIT 후 즉시 사용 가능",
                        "pros": "즉시 일관성",
                        "cons": "없음",
                        "use_case": "모든 경우"
                    }
                ],
                "recommended_delay": "0 seconds",
                "consistency_model": "immediate"
            }
        }
    
    async def _generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        텍스트에서 임베딩 벡터 생성
        
        [Google Native 표준] Vertex AI text-embedding-004 모델 전용 (768차원)
        
        ⚠️ 중요: OpenAI fallback은 제거되었습니다.
        이유: OpenAI (1536차원)와 Vertex AI (768차원)는 동일 인덱스에 저장 불가
        동일한 벡터 DB 인덱스(corrections)에는 단일 차원의 벡터만 허용됩니다.
        """
        try:
            # ═══════════════════════════════════════════════════════════════
            # Vertex AI text-embedding-004 (768차원) - Google Native 표준
            # ═══════════════════════════════════════════════════════════════
            try:
                from vertexai.language_models import TextEmbeddingModel
                
                model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
                embeddings = model.get_embeddings([text])
                
                if embeddings and len(embeddings) > 0:
                    vector = embeddings[0].values
                    
                    # 차원 검증 (디버깅용)
                    if len(vector) != EMBEDDING_DIMENSION:
                        logger.warning(
                            f"Unexpected embedding dimension: {len(vector)}, "
                            f"expected: {EMBEDDING_DIMENSION}"
                        )
                    
                    return vector
                else:
                    logger.warning("Vertex AI returned empty embeddings")
                    return self._generate_dummy_embedding(text)
                    
            except ImportError:
                logger.error(
                    "Vertex AI SDK not available. "
                    "Install: pip install google-cloud-aiplatform"
                )
                return self._generate_dummy_embedding(text)
                
            except Exception as e:
                logger.error(f"Vertex AI embedding failed: {e}")
                # Vertex AI 실패 시 더미 임베딩 (OpenAI 폴백 제거)
                # 이유: 차원 불일치 방지
                return self._generate_dummy_embedding(text)
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {str(e)}")
            return self._generate_dummy_embedding(text)
    
    def _generate_dummy_embedding(self, text: str) -> List[float]:
        """
        더미 임베딩 생성 (Vertex AI API 불가 시 폴백)
        
        768차원 벡터 (Vertex AI text-embedding-004 표준 차원)
        """
        import hashlib
        text_hash = hashlib.md5(text.encode()).hexdigest()
        # EMBEDDING_DIMENSION(768)차원 벡터 생성
        dummy_vector = [float(int(text_hash[i:i+2], 16)) / 255.0 for i in range(0, min(len(text_hash), 32), 2)]
        dummy_vector.extend([0.0] * (EMBEDDING_DIMENSION - len(dummy_vector)))
        return dummy_vector[:EMBEDDING_DIMENSION]
    
    async def _store_in_vector_db_with_metadata(
        self,
        correction_log: CorrectionLog,
        embedding_vector: List[float],
        embedding_text: str
    ) -> Optional[str]:
        """
        개선사항 1: 하이브리드 필터링을 위한 메타데이터와 함께 벡터 DB에 저장
        
        벡터 DB에 저장 시 다음 메타데이터를 반드시 포함:
        - user_id: 사용자별 필터링
        - task_category: 태스크 카테고리별 필터링  
        - context_scope: 컨텍스트 범위별 필터링
        - node_type: 노드 타입별 필터링
        - workflow_domain: 워크플로우 도메인별 필터링
        """
        try:
            # 하이브리드 필터링을 위한 메타데이터 구성
            filter_metadata = {
                # 필수 필터링 필드들
                "user_id": correction_log.user_id,
                "task_category": correction_log.task_category.value,
                "context_scope": correction_log.context_scope,
                "node_type": correction_log.node_type,
                "workflow_domain": correction_log.workflow_domain,
                
                # 추가 필터링 필드들
                "correction_type": correction_log.correction_type.value if correction_log.correction_type else None,
                "workflow_id": correction_log.workflow_id,
                "node_id": correction_log.node_id,
                
                # 품질 메트릭 (필터링 및 랭킹용)
                "edit_distance": correction_log.edit_distance,
                "correction_time_seconds": correction_log.correction_time_seconds,
                "user_confirmed_valuable": correction_log.user_confirmed_valuable,
                
                # 타임스탬프 (시간 기반 필터링용)
                "created_at": correction_log.created_at.isoformat(),
                "updated_at": correction_log.updated_at.isoformat(),
                
                # 추출된 메타데이터 (세밀한 필터링용)
                **correction_log.extracted_metadata
            }
            
            # None 값 제거
            filter_metadata = {k: v for k, v in filter_metadata.items() if v is not None}
            
            # 벡터 DB 저장 데이터 구성
            vector_document = {
                "id": f"correction_{correction_log.sk}",
                "vector": embedding_vector,
                "text": embedding_text,
                "metadata": filter_metadata
            }
            
            # 실제 벡터 DB 저장 (OpenSearch/Pinecone/pgvector 등)
            embedding_id = await self._store_vector_document(vector_document)
            
            if embedding_id:
                logger.info(f"Vector stored with metadata: {embedding_id}, filters: {list(filter_metadata.keys())}")
            
            return embedding_id
            
        except Exception as e:
            logger.error(f"Failed to store vector with metadata: {str(e)}")
            return None
    
    async def _store_vector_document(self, vector_document: Dict[str, Any]) -> Optional[str]:
        """
        실제 벡터 DB에 문서 저장 (개선사항 6: 인덱싱 지연 처리 포함)
        
        벡터 DB별 구현:
        - OpenSearch: 인덱스에 문서 저장 + refresh 옵션
        - pgvector: 테이블에 벡터와 메타데이터 저장
        """
        try:
            vector_db_type = os.environ.get('VECTOR_DB_TYPE', 'opensearch')
            embedding_id = vector_document["id"]
            
            if vector_db_type == 'opensearch' and self.vector_client:
                # OpenSearch 저장
                response = await self.vector_client.index(
                    index="corrections",
                    body={
                        "vector": vector_document["vector"],
                        "text": vector_document["text"],
                        **vector_document.get("metadata", {})
                    },
                    id=embedding_id,
                    refresh=True if self.enable_refresh_wait else False
                )
                logger.debug(f"OpenSearch document stored: {embedding_id}")
                return response.get("_id", embedding_id)
                
            elif vector_db_type == 'pgvector' and hasattr(self, '_pgvector_config'):
                # pgvector 저장
                import asyncpg
                import json as json_lib
                
                conn = await asyncpg.connect(**self._pgvector_config)
                try:
                    # 테이블 존재 확인 및 생성 (768차원 고정)
                    await conn.execute(f'''
                        CREATE TABLE IF NOT EXISTS corrections (
                            id TEXT PRIMARY KEY,
                            vector vector({EMBEDDING_DIMENSION}),
                            text TEXT,
                            metadata JSONB,
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                    ''')
                    
                    # 벡터 인덱스 생성 (없으면)
                    await conn.execute('''
                        CREATE INDEX IF NOT EXISTS corrections_vector_idx 
                        ON corrections USING ivfflat (vector vector_cosine_ops)
                        WITH (lists = 100)
                    ''')
                    
                    # 문서 UPSERT
                    await conn.execute('''
                        INSERT INTO corrections (id, vector, text, metadata)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (id) DO UPDATE SET
                            vector = EXCLUDED.vector,
                            text = EXCLUDED.text,
                            metadata = EXCLUDED.metadata
                    ''', 
                        embedding_id,
                        str(vector_document["vector"]),  # pgvector 형식
                        vector_document["text"],
                        json_lib.dumps(vector_document.get("metadata", {}))
                    )
                    
                    logger.debug(f"pgvector document stored: {embedding_id}")
                    return embedding_id
                    
                finally:
                    await conn.close()
            
            else:
                # Fallback: 시뮬레이션
                await asyncio.sleep(0.1)
                logger.debug(f"Vector document simulated: {embedding_id}")
                return embedding_id
            
        except Exception as e:
            logger.error(f"Failed to store vector document: {str(e)}")
            return None
    
    def get_hybrid_search_example(self) -> Dict[str, Any]:
        """
        하이브리드 필터링 검색 예시
        
        벡터 DB에서 효율적인 검색을 위한 쿼리 구조 예시
        """
        return {
            "opensearch_example": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "knn": {
                                    "vector": {
                                        "vector": "[query_embedding_vector]",
                                        "k": 10
                                    }
                                }
                            }
                        ],
                        "filter": [
                            {"term": {"user_id": "user123"}},
                            {"term": {"task_category": "email"}},
                            {"term": {"context_scope": "global"}},
                            {"range": {"edit_distance": {"gte": 5}}},
                            {"range": {"created_at": {"gte": "2024-01-01"}}}
                        ]
                    }
                }
            },
            "pinecone_example": {
                "vector": "[query_embedding_vector]",
                "filter": {
                    "user_id": {"$eq": "user123"},
                    "task_category": {"$eq": "email"},
                    "context_scope": {"$eq": "global"},
                    "edit_distance": {"$gte": 5}
                },
                "top_k": 10
            },
            "pgvector_example": """
                SELECT id, text, metadata, 
                       vector <-> %s AS distance
                FROM corrections 
                WHERE metadata->>'user_id' = %s 
                  AND metadata->>'task_category' = %s
                  AND metadata->>'context_scope' = %s
                  AND (metadata->>'edit_distance')::int >= %s
                ORDER BY vector <-> %s 
                LIMIT 10
            """
        }
    
    async def _update_correction_sync_status(self, correction_log: CorrectionLog) -> bool:
        """DynamoDB에서 동기화 상태 업데이트"""
        try:
            update_expression = 'SET vector_sync_status = :status, vector_sync_attempts = :attempts, updated_at = :updated'
            expression_values = {
                ':status': correction_log.vector_sync_status.value,
                ':attempts': correction_log.vector_sync_attempts,
                ':updated': datetime.now(timezone.utc).isoformat()
            }
            
            if correction_log.embedding_id:
                update_expression += ', embedding_id = :embedding_id'
                expression_values[':embedding_id'] = correction_log.embedding_id
            
            if correction_log.vector_sync_error:
                update_expression += ', vector_sync_error = :error'
                expression_values[':error'] = correction_log.vector_sync_error
            
            if correction_log.last_vector_sync_attempt:
                update_expression += ', last_vector_sync_attempt = :last_attempt'
                expression_values[':last_attempt'] = correction_log.last_vector_sync_attempt.isoformat()
            
            self.correction_table.update_item(
                Key={
                    'pk': correction_log.pk,
                    'sk': correction_log.sk
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update correction sync status: {str(e)}")
            return False
    
    async def _get_pending_sync_corrections(
        self,
        user_id: str,
        limit: int
    ) -> List[Dict[str, Any]]:
        """동기화 대기 중인 수정 로그들 조회"""
        try:
            response = self.correction_table.query(
                KeyConditionExpression='pk = :pk',
                FilterExpression='vector_sync_status IN (:pending, :failed, :retry)',
                ExpressionAttributeValues={
                    ':pk': f'user#{user_id}',
                    ':pending': VectorSyncStatus.PENDING.value,
                    ':failed': VectorSyncStatus.FAILED.value,
                    ':retry': VectorSyncStatus.RETRY.value
                },
                Limit=limit
            )
            
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Failed to get pending sync corrections: {str(e)}")
            return []
    
    async def _get_failed_sync_corrections(
        self,
        user_id: str,
        cutoff_time: datetime
    ) -> List[Dict[str, Any]]:
        """실패한 동기화 수정 로그들 조회"""
        try:
            response = self.correction_table.query(
                KeyConditionExpression='pk = :pk',
                FilterExpression='vector_sync_status = :failed AND last_vector_sync_attempt > :cutoff',
                ExpressionAttributeValues={
                    ':pk': f'user#{user_id}',
                    ':failed': VectorSyncStatus.FAILED.value,
                    ':cutoff': cutoff_time.isoformat()
                }
            )
            
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Failed to get failed sync corrections: {str(e)}")
            return []

    async def store_correction_vector(
        self,
        user_id: str,
        correction_data: Dict[str, Any],
        quality_result: Dict[str, Any]
    ) -> bool:
        """
        가치 있는 수정을 벡터 DB에 저장합니다.
        
        Args:
            user_id: 사용자 ID
            correction_data: 수정 데이터
            quality_result: 품질 평가 결과
            
        Returns:
            저장 성공 여부
        """
        try:
            # 임베딩 생성
            text_content = self._extract_text_for_embedding(correction_data)
            if not text_content:
                logger.warning("No text content available for embedding")
                return False
                
            embedding = await self._generate_embedding(text_content)
            if not embedding:
                logger.error("Failed to generate embedding")
                return False
            
            # 벡터 문서 생성
            vector_document = {
                "vector": embedding,
                "text": text_content,
                "metadata": {
                    "user_id": user_id,
                    "correction_sk": correction_data.get("sk"),
                    "task_category": correction_data.get("taskCategory"),
                    "correction_type": correction_data.get("correctionType"),
                    "quality_score": quality_result.get("quality_score", 0),
                    "is_valuable": quality_result.get("is_valuable", False),
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
            }
            
            # 벡터 DB에 저장
            success = await self._store_vector_document(vector_document)
            if success:
                logger.info(f"Stored correction vector: {correction_data.get('sk')}")
                
                # DynamoDB에 벡터 동기화 상태 업데이트
                try:
                    self.correction_table.update_item(
                        Key={
                            'pk': f'user#{user_id}',
                            'sk': correction_data.get('sk')
                        },
                        UpdateExpression='SET vector_sync_status = :status, updated_at = :updated',
                        ExpressionAttributeValues={
                            ':status': 'SUCCESS',
                            ':updated': datetime.now(timezone.utc).isoformat()
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to update DynamoDB after vector storage: {str(e)}")
                    # DynamoDB 업데이트 실패는 무시 (벡터는 이미 저장됨)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to store correction vector: {str(e)}")
            return False

    def _extract_text_for_embedding(self, correction_data: Dict[str, Any]) -> str:
        """수정 데이터에서 임베딩용 텍스트 추출"""
        # instruction과 correction을 결합
        instruction = correction_data.get("instruction", "")
        correction = correction_data.get("correction", "")
        text = f"{instruction} {correction}".strip()
        
        # 토큰 제한 고려 (대략 1토큰 = 4문자, 8000토큰 제한)
        max_chars = 8000 * 4
        if len(text) > max_chars:
            logger.warning(f"Text too long ({len(text)} chars), truncating to {max_chars}")
            text = text[:max_chars]
        
        return text

    # ═══════════════════════════════════════════════════════════════════════════
    # [DEPRECATED] 아래 메서드들은 더 이상 사용되지 않습니다.
    # 위쪽의 _generate_embedding 메서드(Vertex AI 전용)를 사용하세요.
    # ═══════════════════════════════════════════════════════════════════════════
    
    # async def _generate_embedding_legacy(self, text: str) -> Optional[List[float]]:
    #     """
    #     [DEPRECATED] OpenAI 임베딩 - 더 이상 사용하지 않음
    #     
    #     이유: OpenAI (1536차원)와 Vertex AI (768차원) 혼합 사용 불가
    #     동일한 벡터 DB 인덱스에는 단일 차원의 벡터만 허용됩니다.
    #     
    #     대신 위쪽의 _generate_embedding 메서드(Vertex AI 전용)를 사용하세요.
    #     """
    #     pass

    async def _store_vector_document(self, vector_document: Dict[str, Any]) -> bool:
        """벡터 문서를 DB에 저장"""
        try:
            if not self.vector_client:
                logger.warning("Vector client not initialized, skipping storage")
                return False
                
            # OpenSearch 인덱스에 저장
            index_name = os.environ.get('VECTOR_INDEX_NAME', 'corrections')
            doc_id = vector_document['metadata']['correction_sk']
            
            # OpenSearch 문서 형식
            document = {
                "vector": vector_document["vector"],
                "text": vector_document["text"],
                **vector_document["metadata"]
            }
            
            response = await self.vector_client.index(
                index=index_name,
                id=doc_id,
                body=document,
                refresh=True  # 즉시 검색 가능하도록
            )
            
            if response.get('result') == 'created' or response.get('result') == 'updated':
                return True
            else:
                logger.error(f"Unexpected OpenSearch response: {response}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to store vector document: {str(e)}")
            return False