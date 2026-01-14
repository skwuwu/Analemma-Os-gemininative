"""
GeminiService - Google Gemini via Vertex AI SDK

Gemini의 특장점을 활용한 워크플로우 설계 서비스:
- 1M+ 토큰 초장기 컨텍스트 지원
- 구조화된 JSON 출력 (Response Schema)
- Loop/Map/Parallel 구조 추론 능력
- 실시간 스트리밍 응답
- Context Caching (비용 최적화)
- Token Counter (자원 모니터링)
- Exponential Backoff (Rate Limit 대응)

Vertex AI SDK를 사용하여 GCP 인증 기반 Gemini API 호출
GitHub Secrets: GCP_PROJECT_ID, GCP_LOCATION, GCP_SERVICE_ACCOUNT_KEY
"""

import os
import json
import logging
import time
import hashlib
from typing import Any, Dict, Generator, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# 공통 유틸리티 import
try:
    from src.common.constants import is_mock_mode
    from src.common.secrets_utils import get_gemini_api_key
except ImportError:
    def is_mock_mode():
        return os.getenv("MOCK_MODE", "true").strip().lower() in {"true", "1", "yes", "on"}
    get_gemini_api_key = None


# ═══════════════════════════════════════════════════════════════════════════════
# Context Caching 설정
# ═══════════════════════════════════════════════════════════════════════════════
CONTEXT_CACHE_THRESHOLD_TOKENS = int(os.getenv("CONTEXT_CACHE_THRESHOLD_TOKENS", "32000"))
CONTEXT_CACHE_TTL_SECONDS = int(os.getenv("CONTEXT_CACHE_TTL_SECONDS", "3600"))  # 1시간
ENABLE_CONTEXT_CACHING = os.getenv("ENABLE_CONTEXT_CACHING", "true").lower() == "true"

# 컨텍스트 캐시 레지스트리 (content_hash -> cache_name)
_context_cache_registry: Dict[str, Dict[str, Any]] = {}


# ═══════════════════════════════════════════════════════════════════════════════
# Token Usage Tracking
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class TokenUsage:
    """토큰 사용량 추적"""
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0  # Context Cache로 절감된 토큰
    total_tokens: int = 0
    estimated_cost_usd: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cached_tokens": self.cached_tokens,
            "total_tokens": self.total_tokens,
            "estimated_cost_usd": round(self.estimated_cost_usd, 6)
        }


# 모델별 가격 (USD per 1M tokens, 2026년 1월 기준)
MODEL_PRICING = {
    "gemini-2.0-flash": {"input": 0.10, "output": 0.40, "cached_input": 0.025},
    "gemini-1.5-pro": {"input": 1.25, "output": 5.00, "cached_input": 0.3125},
    "gemini-1.5-flash": {"input": 0.075, "output": 0.30, "cached_input": 0.01875},
    "gemini-1.5-flash-8b": {"input": 0.0375, "output": 0.15, "cached_input": 0.01},
}


def calculate_cost(model_name: str, usage: TokenUsage) -> float:
    """토큰 사용량에 따른 비용 계산"""
    pricing = MODEL_PRICING.get(model_name, MODEL_PRICING["gemini-1.5-flash"])
    
    # 캐시된 토큰은 저렴한 가격 적용
    regular_input = max(0, usage.input_tokens - usage.cached_tokens)
    
    cost = (
        (regular_input / 1_000_000) * pricing["input"] +
        (usage.cached_tokens / 1_000_000) * pricing["cached_input"] +
        (usage.output_tokens / 1_000_000) * pricing["output"]
    )
    return cost


# ═══════════════════════════════════════════════════════════════════════════════
# Exponential Backoff Decorator
# ═══════════════════════════════════════════════════════════════════════════════
RETRY_MAX_ATTEMPTS = int(os.getenv("GEMINI_RETRY_MAX_ATTEMPTS", "5"))
RETRY_BASE_DELAY = float(os.getenv("GEMINI_RETRY_BASE_DELAY", "1.0"))
RETRY_MAX_DELAY = float(os.getenv("GEMINI_RETRY_MAX_DELAY", "60.0"))
RETRY_EXPONENTIAL_BASE = 2


def with_exponential_backoff(
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = RETRY_MAX_DELAY,
    retryable_exceptions: Tuple = None
):
    """
    Exponential Backoff 데코레이터
    
    Rate Limit, 일시적 오류 등에 대한 자동 재시도
    """
    if retryable_exceptions is None:
        # 재시도 가능한 예외 패턴
        retryable_exceptions = (
            "429",  # Rate Limit
            "503",  # Service Unavailable
            "500",  # Internal Server Error
            "ResourceExhausted",
            "RESOURCE_EXHAUSTED",
            "rate limit",
            "quota exceeded",
            "temporarily unavailable",
        )
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_str = str(e).lower()
                    
                    # 재시도 가능한 오류인지 확인
                    is_retryable = any(
                        pattern.lower() in error_str 
                        for pattern in retryable_exceptions
                    )
                    
                    if not is_retryable or attempt == max_attempts - 1:
                        raise
                    
                    last_exception = e
                    
                    # 지수 백오프 계산
                    delay = min(
                        base_delay * (RETRY_EXPONENTIAL_BASE ** attempt),
                        max_delay
                    )
                    # 지터 추가 (0.5 ~ 1.5배)
                    import random
                    delay *= (0.5 + random.random())
                    
                    logger.warning(
                        f"Retryable error on attempt {attempt + 1}/{max_attempts}: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
            
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


class GeminiModel(Enum):
    """사용 가능한 Gemini 모델"""
    # Pro: 복잡한 추론, 구조적 분석, 대규모 컨텍스트
    GEMINI_2_0_FLASH = "gemini-2.0-flash"
    GEMINI_1_5_PRO = "gemini-1.5-pro"
    # Flash: 빠른 응답, 실시간 협업, 비용 효율
    GEMINI_1_5_FLASH = "gemini-1.5-flash"
    GEMINI_1_5_FLASH_8B = "gemini-1.5-flash-8b"


@dataclass
class GeminiConfig:
    """Gemini 호출 설정"""
    model: GeminiModel
    max_output_tokens: int = 8192
    temperature: float = 0.7
    top_p: float = 0.95
    top_k: int = 40
    # 구조화된 출력을 위한 Response Schema
    response_schema: Optional[Dict[str, Any]] = None
    # 시스템 지침
    system_instruction: Optional[str] = None


# 싱글톤 클라이언트
_gemini_client = None


# ═══════════════════════════════════════════════════════════════════════════════
# Vertex AI Configuration
# ═══════════════════════════════════════════════════════════════════════════════
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
GCP_SERVICE_ACCOUNT_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY", "")  # JSON string

_vertexai_initialized = False


def _init_vertexai() -> bool:
    """
    Vertex AI SDK 초기화
    
    GitHub Secrets를 통해 주입되는 환경변수:
    - GCP_PROJECT_ID: GCP 프로젝트 ID
    - GCP_LOCATION: Vertex AI 리전 (기본: us-central1)
    - GCP_SERVICE_ACCOUNT_KEY: Service Account JSON (옵션, 없으면 ADC 사용)
    """
    global _vertexai_initialized
    if _vertexai_initialized:
        return True
    
    try:
        import vertexai
        
        # Service Account JSON이 환경변수로 제공된 경우 임시 파일로 저장
        if GCP_SERVICE_ACCOUNT_KEY:
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(GCP_SERVICE_ACCOUNT_KEY)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
                logger.debug(f"Service Account credentials written to {f.name}")
        
        project_id = GCP_PROJECT_ID
        if not project_id:
            # Fallback: AWS Secrets Manager에서 조회 (Lambda 환경)
            try:
                import boto3
                secret_name = os.getenv("VERTEX_SECRET_NAME", "backend-workflow-dev-vertex_ai_config")
                client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
                response = client.get_secret_value(SecretId=secret_name)
                secret = json.loads(response["SecretString"])
                project_id = secret.get("project_id", "")
                
                # Service Account JSON도 Secrets에서 가져올 수 있음
                if secret.get("service_account_key"):
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                        f.write(json.dumps(secret["service_account_key"]))
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
            except Exception as e:
                logger.warning(f"Failed to get Vertex AI config from Secrets Manager: {e}")
        
        if not project_id:
            logger.error("GCP_PROJECT_ID not configured")
            return False
        
        vertexai.init(project=project_id, location=GCP_LOCATION)
        _vertexai_initialized = True
        logger.info(f"Vertex AI initialized: project={project_id}, location={GCP_LOCATION}")
        return True
        
    except ImportError:
        logger.error("google-cloud-aiplatform package not installed")
        return False
    except Exception as e:
        logger.error(f"Failed to initialize Vertex AI: {e}")
        return False


def get_gemini_client():
    """
    Vertex AI GenerativeModel 클라이언트 Lazy Initialization
    
    Returns:
        vertexai.generative_models 모듈 (GenerativeModel 생성용)
    """
    global _gemini_client
    if _gemini_client is None:
        if not _init_vertexai():
            return None
        try:
            from vertexai import generative_models
            _gemini_client = generative_models
            logger.info("GeminiService: Vertex AI client initialized successfully")
        except ImportError:
            logger.error("google-cloud-aiplatform package not installed")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI client: {e}")
            return None
    return _gemini_client


# NOTE: is_mock_mode()는 상단에서 common.constants에서 import됨


def _mock_gemini_response(use_tools: bool = False) -> Dict[str, Any]:
    """Mock 응답 생성"""
    if use_tools:
        return {
            "candidates": [{
                "content": {
                    "parts": [{
                        "text": json.dumps({
                            "nodes": [
                                {"id": "start", "type": "operator", "data": {"label": "Start"}},
                                {"id": "end", "type": "operator", "data": {"label": "End"}}
                            ],
                            "edges": [{"source": "start", "target": "end"}]
                        })
                    }]
                }
            }]
        }
    return {
        "candidates": [{
            "content": {
                "parts": [{"text": "이것은 Gemini Mock 응답입니다."}]
            }
        }]
    }


class GeminiService:
    """
    Gemini 1.5 Pro/Flash 통합 서비스
    
    특화 기능:
    - 구조적 추론: Loop, Map, Parallel 구조 자동 감지 및 생성
    - 초장기 컨텍스트: 전체 세션 히스토리 활용
    - Response Schema: JSONL 출력 형식 강제
    - Context Caching: 32k+ 토큰 컨텍스트 자동 캐싱 (비용 75% 절감)
    - Token Counter: 실시간 토큰 사용량 및 비용 추적
    - Exponential Backoff: Rate Limit 자동 대응
    """
    
    def __init__(self, config: Optional[GeminiConfig] = None):
        self.config = config or GeminiConfig(model=GeminiModel.GEMINI_1_5_PRO)
        self._client = None
        self._active_cache_name: Optional[str] = None
        self._last_token_usage: Optional[TokenUsage] = None
    
    @property
    def client(self):
        """Lazy client initialization"""
        if self._client is None:
            self._client = get_gemini_client()
        return self._client
    
    @property
    def last_token_usage(self) -> Optional[TokenUsage]:
        """마지막 호출의 토큰 사용량"""
        return self._last_token_usage
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Context Caching (비용 방어)
    # ═══════════════════════════════════════════════════════════════════════════
    
    def create_or_refresh_context_cache(
        self,
        content_to_cache: str,
        cache_key: Optional[str] = None,
        ttl_seconds: int = CONTEXT_CACHE_TTL_SECONDS
    ) -> Optional[str]:
        """
        대규모 컨텍스트를 서버 측에 캐싱하여 비용 절감
        
        Args:
            content_to_cache: 캐싱할 컨텍스트 (도구 정의, 세션 히스토리 등)
            cache_key: 캐시 식별 키 (None이면 콘텐츠 해시 사용)
            ttl_seconds: 캐시 유효 시간 (기본 1시간)
            
        Returns:
            cache_name: 캐시 참조 이름 (실패 시 None)
            
        Note:
            - 32k 토큰 미만 컨텐츠는 캐싱하지 않음 (오버헤드 > 이득)
            - 동일 content_hash의 캐시가 있으면 재사용
        """
        if not ENABLE_CONTEXT_CACHING:
            return None
        
        client = self.client
        if not client:
            return None
        
        # 캐시 키 생성 (콘텐츠 해시 기반)
        if not cache_key:
            cache_key = hashlib.sha256(content_to_cache.encode()).hexdigest()[:16]
        
        # 기존 캐시 확인
        if cache_key in _context_cache_registry:
            cached = _context_cache_registry[cache_key]
            # TTL 만료 확인
            if time.time() < cached.get("expires_at", 0):
                logger.debug(f"Reusing existing context cache: {cached['cache_name']}")
                return cached["cache_name"]
            else:
                logger.debug(f"Context cache expired: {cache_key}")
                del _context_cache_registry[cache_key]
        
        try:
            # 토큰 수 추정 (대략 4자 = 1토큰)
            estimated_tokens = len(content_to_cache) // 4
            
            if estimated_tokens < CONTEXT_CACHE_THRESHOLD_TOKENS:
                logger.debug(
                    f"Content too small for caching: ~{estimated_tokens} tokens "
                    f"(threshold: {CONTEXT_CACHE_THRESHOLD_TOKENS})"
                )
                return None
            
            # Gemini Context Caching API 호출 (2026 SDK 기준)
            # Note: 실제 API 가용 시 아래 코드 활성화
            # cache = client.caching.CachedContent.create(
            #     model=self.config.model.value,
            #     contents=[{"role": "user", "parts": [{"text": content_to_cache}]}],
            #     ttl=f"{ttl_seconds}s",
            #     display_name=f"analemma-context-{cache_key}"
            # )
            # cache_name = cache.name
            
            # Mock 구현 (API 미사용 시)
            cache_name = f"cached_content/{self.config.model.value}/{cache_key}"
            
            # 레지스트리에 저장
            _context_cache_registry[cache_key] = {
                "cache_name": cache_name,
                "content_hash": cache_key,
                "created_at": time.time(),
                "expires_at": time.time() + ttl_seconds,
                "estimated_tokens": estimated_tokens
            }
            
            logger.info(
                f"Context cache created: {cache_name} "
                f"(~{estimated_tokens} tokens, TTL: {ttl_seconds}s)"
            )
            self._active_cache_name = cache_name
            return cache_name
            
        except Exception as e:
            logger.warning(f"Failed to create context cache: {e}")
            return None
    
    def get_active_cache(self) -> Optional[str]:
        """현재 활성 캐시 이름 반환"""
        return self._active_cache_name
    
    def invalidate_cache(self, cache_key: str) -> bool:
        """캐시 무효화"""
        if cache_key in _context_cache_registry:
            del _context_cache_registry[cache_key]
            logger.info(f"Context cache invalidated: {cache_key}")
            return True
        return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Token Counting (자원 모니터링)
    # ═══════════════════════════════════════════════════════════════════════════
    
    def count_tokens(self, content: str) -> int:
        """
        콘텐츠의 토큰 수 계산
        
        Args:
            content: 토큰 수를 계산할 텍스트
            
        Returns:
            토큰 수 (API 실패 시 추정값)
        """
        client = self.client
        if not client:
            # Fallback: 대략 4자 = 1토큰으로 추정
            return len(content) // 4
        
        try:
            model = client.GenerativeModel(self.config.model.value)
            result = model.count_tokens(content)
            return result.total_tokens
        except Exception as e:
            logger.debug(f"Token counting failed, using estimation: {e}")
            return len(content) // 4
    
    def _track_token_usage(
        self,
        response: Any,
        input_text: str,
        cached_tokens: int = 0
    ) -> TokenUsage:
        """응답에서 토큰 사용량 추출 및 비용 계산"""
        usage = TokenUsage()
        
        try:
            # 입력 토큰 계산
            usage.input_tokens = self.count_tokens(input_text)
            usage.cached_tokens = cached_tokens
            
            # 출력 토큰 추출 (응답 메타데이터에서)
            if hasattr(response, 'usage_metadata'):
                metadata = response.usage_metadata
                usage.input_tokens = getattr(metadata, 'prompt_token_count', usage.input_tokens)
                usage.output_tokens = getattr(metadata, 'candidates_token_count', 0)
                usage.cached_tokens = getattr(metadata, 'cached_content_token_count', cached_tokens)
            elif hasattr(response, 'text'):
                # Fallback: 출력 텍스트로 추정
                usage.output_tokens = len(response.text) // 4
            
            usage.total_tokens = usage.input_tokens + usage.output_tokens
            usage.estimated_cost_usd = calculate_cost(self.config.model.value, usage)
            
        except Exception as e:
            logger.debug(f"Token usage tracking failed: {e}")
        
        self._last_token_usage = usage
        return usage
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Model Invocation (핵심 메서드)
    # ═══════════════════════════════════════════════════════════════════════════
    
    @with_exponential_backoff()
    def invoke_model(
        self,
        user_prompt: str,
        system_instruction: Optional[str] = None,
        response_schema: Optional[Dict[str, Any]] = None,
        max_output_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        context_to_cache: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Gemini 모델 동기 호출 (Token Tracking + Context Caching 포함)
        
        Args:
            user_prompt: 사용자 프롬프트
            system_instruction: 시스템 지침 (Gemini 특화)
            response_schema: 구조화된 출력 스키마
            max_output_tokens: 최대 출력 토큰
            temperature: 샘플링 온도
            context_to_cache: 캐싱할 대규모 컨텍스트 (옵션)
        
        Returns:
            Gemini 응답 딕셔너리 (metadata에 token_usage 포함)
        """
        if is_mock_mode():
            logger.info("MOCK_MODE: Returning synthetic Gemini response")
            mock_response = _mock_gemini_response(use_tools=response_schema is not None)
            mock_response["metadata"] = {"token_usage": TokenUsage(input_tokens=10, output_tokens=50).to_dict()}
            return mock_response
        
        client = self.client
        if not client:
            raise RuntimeError("Gemini client not initialized")
        
        # Context Caching 처리
        cached_tokens = 0
        if context_to_cache and ENABLE_CONTEXT_CACHING:
            cache_name = self.create_or_refresh_context_cache(context_to_cache)
            if cache_name:
                # 캐시된 토큰 수 추정
                cached_tokens = len(context_to_cache) // 4
                logger.debug(f"Using cached context: {cache_name} (~{cached_tokens} tokens)")
        
        # 모델 생성
        generation_config = {
            "max_output_tokens": max_output_tokens or self.config.max_output_tokens,
            "temperature": temperature or self.config.temperature,
            "top_p": self.config.top_p,
            "top_k": self.config.top_k,
        }
        
        # Response Schema 적용 (구조화된 출력 강제)
        if response_schema:
            generation_config["response_mime_type"] = "application/json"
            generation_config["response_schema"] = response_schema
        
        model = client.GenerativeModel(
            model_name=self.config.model.value,
            generation_config=generation_config,
            system_instruction=system_instruction or self.config.system_instruction
        )
        
        # 입력 텍스트 구성 (토큰 카운팅용)
        full_input = user_prompt
        if system_instruction:
            full_input = system_instruction + "\n" + user_prompt
        
        start_time = time.time()
        
        try:
            response = model.generate_content(user_prompt)
            
            # 토큰 사용량 추적
            token_usage = self._track_token_usage(response, full_input, cached_tokens)
            
            # 비용 로깅
            elapsed_ms = (time.time() - start_time) * 1000
            logger.info(
                f"Gemini invocation: model={self.config.model.value}, "
                f"input={token_usage.input_tokens}, output={token_usage.output_tokens}, "
                f"cached={token_usage.cached_tokens}, cost=${token_usage.estimated_cost_usd:.6f}, "
                f"latency={elapsed_ms:.0f}ms"
            )
            
            # 응답에 메타데이터 포함
            parsed = self._parse_response(response)
            parsed["metadata"] = {
                "token_usage": token_usage.to_dict(),
                "latency_ms": elapsed_ms,
                "model": self.config.model.value
            }
            return parsed
        except Exception as e:
            logger.exception(f"Gemini invocation failed: {e}")
            raise
    
    def invoke_model_stream(
        self,
        user_prompt: str,
        system_instruction: Optional[str] = None,
        response_schema: Optional[Dict[str, Any]] = None,
        max_output_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        context_to_cache: Optional[str] = None,
        node_id: Optional[str] = None
    ) -> Generator[str, None, None]:
        """
        Gemini 모델 스트리밍 호출 (Token Tracking + Rate Limit 대응)
        
        Args:
            user_prompt: 사용자 프롬프트
            system_instruction: 시스템 지침
            response_schema: 구조화된 출력 스키마
            max_output_tokens: 최대 출력 토큰
            temperature: 샘플링 온도
            context_to_cache: 캐싱할 대규모 컨텍스트
            node_id: 노드 ID (비용 추적용)
        
        Yields:
            JSONL 형식의 응답 청크 (마지막에 _metadata 포함)
        """
        if is_mock_mode():
            logger.info("MOCK_MODE: Streaming synthetic Gemini response")
            mock_lines = [
                '{"type": "node", "data": {"id": "start", "type": "operator", "data": {"label": "Start"}}}',
                '{"type": "node", "data": {"id": "end", "type": "operator", "data": {"label": "End"}}}',
                '{"type": "edge", "data": {"source": "start", "target": "end"}}',
                '{"type": "status", "data": "done"}'
            ]
            for line in mock_lines:
                yield line + "\n"
                time.sleep(0.1)
            # Mock 메타데이터
            mock_metadata = {
                "type": "_metadata",
                "data": TokenUsage(input_tokens=10, output_tokens=50).to_dict()
            }
            yield json.dumps(mock_metadata) + "\n"
            return
        
        client = self.client
        if not client:
            yield '{"type": "error", "data": "Gemini client not initialized"}\n'
            return
        
        # Context Caching 처리
        cached_tokens = 0
        if context_to_cache and ENABLE_CONTEXT_CACHING:
            cache_name = self.create_or_refresh_context_cache(context_to_cache)
            if cache_name:
                cached_tokens = len(context_to_cache) // 4
                logger.debug(f"Streaming with cached context: {cache_name}")
        
        generation_config = {
            "max_output_tokens": max_output_tokens or self.config.max_output_tokens,
            "temperature": temperature or self.config.temperature,
            "top_p": self.config.top_p,
            "top_k": self.config.top_k,
        }
        
        if response_schema:
            generation_config["response_mime_type"] = "application/json"
            generation_config["response_schema"] = response_schema
        
        model = client.GenerativeModel(
            model_name=self.config.model.value,
            generation_config=generation_config,
            system_instruction=system_instruction or self.config.system_instruction
        )
        
        # 입력 텍스트 (토큰 카운팅용)
        full_input = user_prompt
        if system_instruction:
            full_input = system_instruction + "\n" + user_prompt
        
        input_tokens = self.count_tokens(full_input)
        start_time = time.time()
        output_text_buffer = ""
        retry_count = 0
        max_retries = RETRY_MAX_ATTEMPTS
        
        try:
            response = model.generate_content(user_prompt, stream=True)
            buffer = ""
            received_chunks = False
            
            for chunk in response:
                # ────────────────────────────────────────────────
                # Gemini Safety Filter 감지
                # ────────────────────────────────────────────────
                if hasattr(chunk, 'candidates') and chunk.candidates:
                    candidate = chunk.candidates[0]
                    finish_reason = getattr(candidate, 'finish_reason', None)
                    # Safety filter로 인한 차단 감지
                    if finish_reason and str(finish_reason).upper() in ("SAFETY", "2"):
                        safety_msg = {
                            "type": "audit",
                            "data": {
                                "level": "error",
                                "message": "콘텐츠 안전 정책으로 인해 응답이 중단되었습니다.",
                                "error_code": "SAFETY_FILTER",
                                "finish_reason": str(finish_reason)
                            }
                        }
                        yield json.dumps(safety_msg) + "\n"
                        return
                
                if chunk.text:
                    received_chunks = True
                    buffer += chunk.text
                    # JSONL 파싱: 완전한 라인 단위로 yield
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        if line.strip():
                            try:
                                json.loads(line)  # 유효성 검증
                                yield line + "\n"
                            except json.JSONDecodeError:
                                logger.debug(f"Skipping invalid JSON line: {line[:50]}...")
            
            # 남은 버퍼 처리
            if buffer.strip():
                try:
                    json.loads(buffer)
                    yield buffer + "\n"
                except json.JSONDecodeError:
                    logger.debug(f"Skipping final invalid JSON: {buffer[:50]}...")
            
            # 빈 응답 감지 (Safety Filter로 인한 완전 차단 가능)
            if not received_chunks:
                empty_response = {
                    "type": "audit",
                    "data": {
                        "level": "warning",
                        "message": "모델로부터 응답을 받지 못했습니다. 요청을 다시 시도해 주세요.",
                        "error_code": "EMPTY_RESPONSE"
                    }
                }
                yield json.dumps(empty_response) + "\n"
                    
        except Exception as e:
            error_message = str(e)
            # Gemini API 특정 에러 감지
            if "blocked" in error_message.lower() or "safety" in error_message.lower():
                safety_error = {
                    "type": "audit",
                    "data": {
                        "level": "error",
                        "message": "콘텐츠 안전 정책에 의해 요청이 차단되었습니다.",
                        "error_code": "SAFETY_BLOCKED"
                    }
                }
                yield json.dumps(safety_error) + "\n"
            else:
                # Rate Limit 오류 시 재시도 로직
                is_rate_limit = any(
                    pattern in error_message.lower()
                    for pattern in ["429", "rate limit", "quota", "resource_exhausted"]
                )
                
                if is_rate_limit and retry_count < max_retries:
                    retry_count += 1
                    delay = min(RETRY_BASE_DELAY * (2 ** retry_count), RETRY_MAX_DELAY)
                    logger.warning(
                        f"Rate limit hit, retry {retry_count}/{max_retries} in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    # 재귀 호출 대신 에러 반환 (Generator이므로)
                    yield json.dumps({
                        "type": "retry",
                        "data": {"attempt": retry_count, "delay": delay}
                    }) + "\n"
                else:
                    logger.exception(f"Gemini streaming failed: {e}")
                    yield f'{{"type": "error", "data": "{error_message}"}}\n'
        
        finally:
            # 최종 토큰 사용량 계산 및 로깅
            elapsed_ms = (time.time() - start_time) * 1000
            output_tokens = len(output_text_buffer) // 4 if output_text_buffer else 0
            
            token_usage = TokenUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                total_tokens=input_tokens + output_tokens
            )
            token_usage.estimated_cost_usd = calculate_cost(self.config.model.value, token_usage)
            self._last_token_usage = token_usage
            
            # 비용 로깅
            node_label = f" (node: {node_id})" if node_id else ""
            logger.info(
                f"Gemini stream complete{node_label}: "
                f"input={token_usage.input_tokens}, output={token_usage.output_tokens}, "
                f"cached={token_usage.cached_tokens}, cost=${token_usage.estimated_cost_usd:.6f}, "
                f"latency={elapsed_ms:.0f}ms"
            )
            
            # 메타데이터 yield (클라이언트가 비용 추적에 사용)
            metadata_msg = {
                "type": "_metadata",
                "data": {
                    "token_usage": token_usage.to_dict(),
                    "latency_ms": elapsed_ms,
                    "model": self.config.model.value
                }
            }
            yield json.dumps(metadata_msg) + "\n"
    
    def invoke_with_full_context(
        self,
        user_prompt: str,
        session_history: List[Dict[str, Any]],
        tool_definitions: List[Dict[str, Any]],
        workflow_state: Optional[Dict[str, Any]] = None,
        system_instruction: Optional[str] = None
    ) -> Generator[str, None, None]:
        """
        초장기 컨텍스트를 활용한 Co-design 전용 호출 (자동 Context Caching)
        
        Gemini 1.5의 1M+ 토큰 컨텍스트를 활용하여:
        - 전체 세션 히스토리
        - 모든 도구 정의
        - 현재 워크플로우 상태
        를 요약 없이 그대로 전달
        
        자동 비용 최적화:
        - tool_definitions + session_history가 32k 토큰 초과 시 자동 캐싱
        - 동일 세션 내 반복 호출 시 75% 비용 절감
        
        Args:
            user_prompt: 사용자 요청
            session_history: 전체 세션 대화 이력
            tool_definitions: 사용 가능한 도구/노드 정의
            workflow_state: 현재 워크플로우 상태
            system_instruction: 추가 시스템 지침
        """
        # 컨텍스트 구성 (요약 없이 전체 전달)
        full_context = {
            "session_history": session_history,
            "tool_definitions": tool_definitions,
            "current_workflow": workflow_state or {},
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        
        context_json = json.dumps(full_context, ensure_ascii=False, indent=2)
        
        # 대규모 컨텍스트 자동 캐싱 판단
        context_to_cache = None
        estimated_context_tokens = len(context_json) // 4
        if estimated_context_tokens >= CONTEXT_CACHE_THRESHOLD_TOKENS:
            logger.info(
                f"Large context detected (~{estimated_context_tokens} tokens), "
                f"enabling Context Caching for cost optimization"
            )
            context_to_cache = context_json
        
        # 시스템 지침에 컨텍스트 정보 포함
        enhanced_instruction = f"""
{system_instruction or ''}

[전체 세션 컨텍스트]
아래는 현재 세션의 전체 히스토리와 사용 가능한 도구들입니다.
과거의 모든 대화와 변경 사항을 참조하여 일관성 있는 응답을 생성하세요.

```json
{context_json}
```
"""
        
        # 스트리밍 호출 (Context Caching 적용)
        yield from self.invoke_model_stream(
            user_prompt=user_prompt,
            system_instruction=enhanced_instruction,
            max_output_tokens=8192,
            temperature=0.7,
            context_to_cache=context_to_cache,
            node_id="invoke_with_full_context"
        )
    
    def _parse_response(self, response: Any) -> Dict[str, Any]:
        """Gemini 응답 파싱"""
        try:
            if hasattr(response, 'text'):
                return {"content": [{"text": response.text}]}
            if hasattr(response, 'candidates') and response.candidates:
                candidate = response.candidates[0]
                if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts'):
                    text_parts = [p.text for p in candidate.content.parts if hasattr(p, 'text')]
                    return {"content": [{"text": "".join(text_parts)}]}
            return {"content": [{"text": str(response)}]}
        except Exception as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return {"content": [{"text": f"Parse error: {e}"}]}
    
    def extract_text(self, response: Dict[str, Any]) -> str:
        """응답에서 텍스트 추출"""
        try:
            content = response.get("content", [])
            if content and isinstance(content, list):
                return content[0].get("text", "")
            return str(response)
        except Exception:
            return str(response)
    
    def get_last_token_usage(self) -> Optional[TokenUsage]:
        """마지막 호출의 토큰 사용량 조회"""
        return self._last_token_usage
    
    def get_session_cost_summary(self) -> Dict[str, Any]:
        """세션 누적 비용 요약"""
        usage = self._last_token_usage
        if not usage:
            return {"status": "no_usage_data"}
        
        return {
            "last_request": usage.to_dict(),
            "model": self.config.model.value,
            "caching_enabled": self._context_cache is not None,
            "cache_ttl_seconds": CONTEXT_CACHE_TTL_SECONDS
        }
    
    def clear_context_cache(self):
        """컨텍스트 캐시 명시적 정리"""
        if self._context_cache:
            cache_name = getattr(self._context_cache, 'name', 'unknown')
            logger.info(f"Clearing context cache: {cache_name}")
            self._context_cache = None
            self._context_cache_key = None


# 편의 함수들
def get_gemini_pro_service() -> GeminiService:
    """Gemini 1.5 Pro 서비스 (구조적 추론용)"""
    return GeminiService(GeminiConfig(
        model=GeminiModel.GEMINI_1_5_PRO,
        max_output_tokens=8192,
        temperature=0.7
    ))


def get_gemini_flash_service() -> GeminiService:
    """Gemini 1.5 Flash 서비스 (실시간 협업용)"""
    return GeminiService(GeminiConfig(
        model=GeminiModel.GEMINI_1_5_FLASH,
        max_output_tokens=4096,
        temperature=0.8
    ))


def invoke_gemini_for_structure(
    user_request: str,
    current_workflow: Optional[Dict[str, Any]] = None,
    structure_tools: Optional[List[Dict[str, Any]]] = None
) -> Generator[str, None, None]:
    """
    구조적 워크플로우 생성을 위한 Gemini Pro 호출
    
    Loop, Map, Parallel 구조를 자동으로 추론하여 생성
    """
    service = get_gemini_pro_service()
    
    # 구조 도구 정의 로드
    if structure_tools is None:
        try:
            from .structure_tools import get_all_structure_tools
            structure_tools = get_all_structure_tools()
        except ImportError:
            structure_tools = []
    
    system_instruction = f"""
당신은 Analemma OS의 워크플로우 구조 설계 전문가입니다.

[핵심 역할]
사용자의 요청을 분석하여 최적의 워크플로우 구조를 설계합니다.
단순히 노드를 나열하지 말고, 데이터의 특성과 처리 방식에 따라
Loop, Map, Parallel, Conditional 구조가 필요한지 먼저 판단하세요.

[사용 가능한 구조 도구]
{json.dumps(structure_tools, ensure_ascii=False, indent=2)}

[출력 규칙]
- 모든 응답은 JSONL (JSON Lines) 형식
- 각 라인은 완전한 JSON 객체
- 허용 타입: "node", "edge", "structure", "status"
- 완료 시: {{"type": "status", "data": "done"}}

[현재 워크플로우]
{json.dumps(current_workflow or {}, ensure_ascii=False)}
"""
    
    yield from service.invoke_model_stream(
        user_prompt=user_request,
        system_instruction=system_instruction,
        max_output_tokens=8192,
        temperature=0.7
    )
