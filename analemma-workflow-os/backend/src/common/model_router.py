"""
Model Router: Canvas 모드에 따른 동적 모델 선택

비용 최적화를 위해 작업 복잡도에 따라 적절한 모델을 선택합니다.
- Agentic Designer (전체 생성): 고성능 모델 (Gemini 1.5 Pro, Claude 3.5 Sonnet, GPT-4o)
- Co-design (부분 수정): 경량 모델 (Gemini 1.5 Flash, Claude Haiku)

Gemini Native 통합:
- 구조적 복잡성(Loop/Map) 해결: Gemini 1.5 Pro 우선 할당
- 초장기 문맥(1M+ 토큰): Gemini 1.5 Pro/Flash 활용
- 실시간 협업: Gemini 1.5 Flash (1초 미만 응답)
"""

import os
import logging
from typing import Dict, Any, Literal, Optional, List
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class ModelTier(Enum):
    """모델 성능 티어"""
    PREMIUM = "premium"      # 최고 성능, 높은 비용 (전체 워크플로우 생성)
    STANDARD = "standard"    # 중간 성능, 중간 비용 (복잡한 Co-design)
    ECONOMY = "economy"      # 기본 성능, 낮은 비용 (단순 Co-design)


class ModelProvider(Enum):
    """모델 제공자"""
    GEMINI = "gemini"        # Google Gemini (1M+ context, 구조적 추론)
    ANTHROPIC = "anthropic"  # Anthropic Claude (Bedrock)
    OPENAI = "openai"        # OpenAI GPT (직접 API)
    META = "meta"            # Meta Llama (Bedrock)


@dataclass
class ModelConfig:
    """
    모델 설정
    
    [2026년 고도화]
    - supports_context_caching: Gemini Context Caching API 지원 (입력 비용 최대 90% 절감)
    - expected_ttft_ms: 첫 번째 토큰 생성 시간 (Time To First Token)
    - cached_cost_per_1k_tokens: 캐싱 적용 시 비용
    """
    model_id: str
    max_tokens: int
    cost_per_1k_tokens: float
    tier: ModelTier
    provider: ModelProvider = ModelProvider.ANTHROPIC
    # Gemini 특화 기능 플래그
    supports_long_context: bool = False      # 1M+ 토큰 컨텍스트 지원
    supports_structured_output: bool = False  # Response Schema 지원
    supports_streaming: bool = True           # 스트리밍 지원
    # [② Context Caching 지원]
    supports_context_caching: bool = False   # Gemini Context Caching API 지원
    cached_cost_per_1k_tokens: float = 0.0   # 캐싱 적용 시 비용 (90% 절감)
    # [③ Latency 지표]
    expected_ttft_ms: int = 500              # 첫 번째 토큰 생성 시간 (ms)
    expected_tps: int = 50                   # 초당 토큰 생성 속도 (tokens/sec)


# ============================================================
# 사용 가능한 모델들 (Gemini Native 우선)
# ============================================================
AVAILABLE_MODELS = {
    # ──────────────────────────────────────────────────────────
    # Gemini Tier - 구조적 추론 및 장기 컨텍스트 특화
    # ──────────────────────────────────────────────────────────
    "gemini-2.0-flash": ModelConfig(
        model_id="gemini-2.0-flash",
        max_tokens=8192,
        cost_per_1k_tokens=0.075,  # $0.075 per 1M input tokens
        tier=ModelTier.PREMIUM,
        provider=ModelProvider.GEMINI,
        supports_long_context=True,
        supports_structured_output=True,
        supports_context_caching=True,
        cached_cost_per_1k_tokens=0.01875,  # 캐싱 시 75% 할인
        expected_ttft_ms=150,               # 매우 빠른 응답
        expected_tps=150,                   # 높은 처리량
    ),
    "gemini-1.5-pro": ModelConfig(
        model_id="gemini-1.5-pro",
        max_tokens=8192,
        cost_per_1k_tokens=1.25,  # $1.25 per 1M input tokens
        tier=ModelTier.PREMIUM,
        provider=ModelProvider.GEMINI,
        supports_long_context=True,
        supports_structured_output=True,
        supports_context_caching=True,
        cached_cost_per_1k_tokens=0.3125,   # 캐싱 시 75% 할인
        expected_ttft_ms=300,               # 빠른 응답
        expected_tps=80,
    ),
    "gemini-1.5-flash": ModelConfig(
        model_id="gemini-1.5-flash",
        max_tokens=8192,
        cost_per_1k_tokens=0.075,  # $0.075 per 1M input tokens
        tier=ModelTier.STANDARD,
        provider=ModelProvider.GEMINI,
        supports_long_context=True,
        supports_structured_output=True,
        supports_context_caching=True,
        cached_cost_per_1k_tokens=0.01875,  # 캐싱 시 75% 할인
        expected_ttft_ms=100,               # 가장 빠른 응답
        expected_tps=200,                   # 가장 높은 처리량
    ),
    "gemini-1.5-flash-8b": ModelConfig(
        model_id="gemini-1.5-flash-8b",
        max_tokens=4096,
        cost_per_1k_tokens=0.0375,  # $0.0375 per 1M input tokens
        tier=ModelTier.ECONOMY,
        provider=ModelProvider.GEMINI,
        supports_long_context=True,
        supports_structured_output=True,
        supports_context_caching=True,
        cached_cost_per_1k_tokens=0.009375, # 캐싱 시 75% 할인
        expected_ttft_ms=80,                # 초고속 응답 (Pre-Routing용)
        expected_tps=250,                   # 최고 처리량
    ),
    
    # ──────────────────────────────────────────────────────────
    # Premium Tier - 전체 워크플로우 생성용 (Fallback)
    # ──────────────────────────────────────────────────────────
    "claude-3.5-sonnet": ModelConfig(
        model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        max_tokens=8192,
        cost_per_1k_tokens=3.0,  # $3.00 per 1K tokens (input)
        tier=ModelTier.PREMIUM,
        provider=ModelProvider.ANTHROPIC,
        expected_ttft_ms=400,
        expected_tps=60,
    ),
    "gpt-4o": ModelConfig(
        model_id="gpt-4o-2024-08-06",
        max_tokens=4096,
        cost_per_1k_tokens=2.5,  # $2.50 per 1K tokens (input)
        tier=ModelTier.PREMIUM,
        provider=ModelProvider.OPENAI,
        expected_ttft_ms=350,
        expected_tps=70,
    ),
    
    # ──────────────────────────────────────────────────────────
    # Standard Tier - 복잡한 Co-design용
    # ──────────────────────────────────────────────────────────
    "claude-3-sonnet": ModelConfig(
        model_id="anthropic.claude-3-sonnet-20240229-v1:0",
        max_tokens=4096,
        cost_per_1k_tokens=3.0,  # $3.00 per 1K tokens (input)
        tier=ModelTier.STANDARD,
        provider=ModelProvider.ANTHROPIC,
        expected_ttft_ms=450,
        expected_tps=55,
    ),
    
    # ──────────────────────────────────────────────────────────
    # Economy Tier - 단순 Co-design용
    # ──────────────────────────────────────────────────────────
    "claude-3-haiku": ModelConfig(
        model_id="anthropic.claude-3-haiku-20240307-v1:0",
        max_tokens=4096,
        cost_per_1k_tokens=0.25,  # $0.25 per 1K tokens (input)
        tier=ModelTier.ECONOMY,
        provider=ModelProvider.ANTHROPIC,
        expected_ttft_ms=200,
        expected_tps=120,
    ),
    "llama-3.1-70b": ModelConfig(
        model_id="meta.llama3-1-70b-instruct-v1:0",
        max_tokens=2048,
        cost_per_1k_tokens=0.99,  # $0.99 per 1K tokens (input)
        tier=ModelTier.ECONOMY,
        provider=ModelProvider.META,
        expected_ttft_ms=300,
        expected_tps=80,
    ),
}


# ============================================================
# 구조적 복잡성 키워드 (Gemini 우선 할당 트리거)
# ============================================================
STRUCTURAL_KEYWORDS = [
    # Loop/반복 관련
    "반복", "루프", "loop", "for each", "foreach", "iterate", "순회",
    # Map/병렬 관련
    "병렬", "parallel", "map", "동시", "분산", "concurrent", "distributed",
    # 조건부 분기 관련
    "조건", "분기", "if", "else", "switch", "condition", "branch",
    # 에러 처리 관련
    "재시도", "retry", "catch", "에러", "예외", "fallback",
    # 복잡한 구조 관련
    "서브그래프", "subgraph", "중첩", "nested", "워크플로우 호출",
]

# 장기 컨텍스트 키워드 (Gemini 장기 컨텍스트 활용 트리거)
LONG_CONTEXT_KEYWORDS = [
    "이전", "아까", "전에", "과거", "히스토리", "history", "기억",
    "3단계 전", "이전 버전", "되돌려", "복원", "맥락", "context",
]


def _detect_structural_complexity(user_request: str) -> bool:
    """구조적 복잡성(Loop/Map/Parallel) 필요 여부 감지 (키워드 기반)"""
    request_lower = user_request.lower()
    return any(keyword in request_lower for keyword in STRUCTURAL_KEYWORDS)


def _detect_long_context_need(user_request: str) -> bool:
    """장기 컨텍스트 참조 필요 여부 감지 (키워드 기반)"""
    request_lower = user_request.lower()
    return any(keyword in request_lower for keyword in LONG_CONTEXT_KEYWORDS)


def _is_gemini_available() -> bool:
    """Gemini API 사용 가능 여부 확인"""
    # 환경변수 또는 Secrets Manager에서 API 키 확인
    api_key = os.getenv("GOOGLE_API_KEY")
    if api_key:
        return True
    # Secrets Manager 확인은 실제 호출 시 수행
    return os.getenv("GEMINI_SECRET_NAME") is not None


# ============================================================
# [① Pre-Routing Classifier] Semantic Intent Detection
# ============================================================
# 키워드 매칭의 한계를 극복하기 위해 저렴한 모델로 의도 분류
# "이전 기능은 무시하고 루프 없이 짜줘" 같은 부정 문맥 처리

# 부정어 패턴 (키워드를 무효화)
NEGATION_PATTERNS = [
    "없이", "말고", "제외", "무시", "빼고", "안 ", "하지마", "하지 마",
    "without", "except", "exclude", "ignore", "don't", "do not", "no ",
]


def _is_negated_keyword(request_lower: str, keyword: str) -> bool:
    """
    키워드가 부정 문맥에서 사용되었는지 확인
    
    예: "루프 없이 짜줘" → '루프' 키워드는 부정됨
    """
    keyword_pos = request_lower.find(keyword)
    if keyword_pos == -1:
        return False
    
    # 키워드 앞 30자 내에 부정어가 있는지 확인
    context_start = max(0, keyword_pos - 30)
    context_before = request_lower[context_start:keyword_pos]
    
    # 키워드 뒤 15자 내에 부정어가 있는지 확인
    context_after = request_lower[keyword_pos:keyword_pos + len(keyword) + 15]
    
    for negation in NEGATION_PATTERNS:
        if negation in context_before or negation in context_after:
            return True
    
    return False


def _detect_structural_complexity_semantic(user_request: str) -> tuple:
    """
    [① Semantic Detection] 구조적 복잡성 감지 (부정 문맥 고려)
    
    Returns:
        (needs_structure: bool, confidence: float, detected_keywords: list)
    """
    request_lower = user_request.lower()
    detected = []
    negated = []
    
    for keyword in STRUCTURAL_KEYWORDS:
        if keyword in request_lower:
            if _is_negated_keyword(request_lower, keyword):
                negated.append(keyword)
            else:
                detected.append(keyword)
    
    # 유효한 키워드가 있는지 확인
    needs_structure = len(detected) > 0
    
    # 신뢰도 계산: 부정된 키워드가 많으면 신뢰도 낮춤
    if detected:
        confidence = len(detected) / (len(detected) + len(negated))
    else:
        confidence = 0.0
    
    return needs_structure, confidence, detected


def _detect_long_context_semantic(user_request: str) -> tuple:
    """
    [① Semantic Detection] 장기 컨텍스트 필요 감지 (부정 문맥 고려)
    
    Returns:
        (needs_long_context: bool, confidence: float, detected_keywords: list)
    """
    request_lower = user_request.lower()
    detected = []
    negated = []
    
    for keyword in LONG_CONTEXT_KEYWORDS:
        if keyword in request_lower:
            if _is_negated_keyword(request_lower, keyword):
                negated.append(keyword)
            else:
                detected.append(keyword)
    
    needs_long_context = len(detected) > 0
    
    if detected:
        confidence = len(detected) / (len(detected) + len(negated))
    else:
        confidence = 0.0
    
    return needs_long_context, confidence, detected


async def classify_intent_with_llm(user_request: str) -> Dict[str, Any]:
    """
    [① Pre-Routing Classifier] LLM 기반 의도 분류
    
    gemini-1.5-flash-8b를 사용하여 10원 미만의 비용으로
    사용자 요청의 진짜 의도를 분류합니다.
    
    Returns:
        {
            "needs_structure": bool,
            "needs_long_context": bool,
            "complexity_tier": "premium" | "standard" | "economy",
            "confidence": float,
            "reasoning": str
        }
    """
    # Pre-Routing 비활성화 시 키워드 기반 분류 사용
    if os.getenv("DISABLE_PRE_ROUTING", "false").lower() == "true":
        needs_struct, struct_conf, _ = _detect_structural_complexity_semantic(user_request)
        needs_ctx, ctx_conf, _ = _detect_long_context_semantic(user_request)
        return {
            "needs_structure": needs_struct,
            "needs_long_context": needs_ctx,
            "complexity_tier": "premium" if needs_struct else "standard",
            "confidence": max(struct_conf, ctx_conf, 0.5),
            "reasoning": "Keyword-based classification (Pre-Routing disabled)"
        }
    
    # Gemini Flash 8B로 빠른 분류 (비용: ~$0.00004 per request)
    try:
        from src.services.llm.gemini_service import get_gemini_flash_8b_service
        
        classifier_prompt = f"""다음 사용자 요청을 분석하여 JSON으로 응답하세요.

사용자 요청: "{user_request}"

분석 항목:
1. needs_structure: 반복(for_each), 병렬(parallel), 조건분기(if/else) 등 구조적 패턴이 필요한가?
2. needs_long_context: 이전 대화나 과거 작업을 참조해야 하는가?
3. complexity_tier: 작업 복잡도 ("premium": 전체 설계, "standard": 부분 수정, "economy": 단순 변경)

중요: "루프 없이", "이전 기능 무시" 등 부정 표현에 주의하세요.

JSON 형식으로만 응답:
{{"needs_structure": bool, "needs_long_context": bool, "complexity_tier": str, "reasoning": str}}"""
        
        service = get_gemini_flash_8b_service()
        response = service.invoke_model(
            user_prompt=classifier_prompt,
            max_output_tokens=256,
            temperature=0.1  # 결정적 응답
        )
        
        import json
        result = json.loads(response)
        result["confidence"] = 0.9  # LLM 분류는 높은 신뢰도
        return result
        
    except Exception as e:
        logger.warning(f"Pre-Routing Classifier failed, using keyword fallback: {e}")
        # Fallback to semantic keyword detection
        needs_struct, struct_conf, _ = _detect_structural_complexity_semantic(user_request)
        needs_ctx, ctx_conf, _ = _detect_long_context_semantic(user_request)
        return {
            "needs_structure": needs_struct,
            "needs_long_context": needs_ctx,
            "complexity_tier": "premium" if needs_struct else "standard",
            "confidence": max(struct_conf, ctx_conf, 0.5),
            "reasoning": f"Keyword fallback due to error: {str(e)[:50]}"
        }

def estimate_request_complexity(
    canvas_mode: Literal["agentic-designer", "co-design"],
    current_workflow: Dict[str, Any],
    user_request: str,
    recent_changes: list = None
) -> ModelTier:
    """
    요청 복잡도를 분석하여 적절한 모델 티어를 결정합니다.
    
    Args:
        canvas_mode: Canvas 모드 ("agentic-designer" 또는 "co-design")
        current_workflow: 현재 워크플로우 상태
        user_request: 사용자 요청
        recent_changes: 최근 변경사항
    
    Returns:
        ModelTier: 권장 모델 티어
    """
    
    # Agentic Designer 모드는 항상 Premium 모델 사용
    if canvas_mode == "agentic-designer":
        logger.info("Agentic Designer mode detected - using PREMIUM tier")
        return ModelTier.PREMIUM
    
    # Co-design 모드에서는 복잡도 분석
    nodes = current_workflow.get("nodes", [])
    edges = current_workflow.get("edges", [])
    node_count = len(nodes)
    edge_count = len(edges)
    
    # 복잡도 점수 계산
    complexity_score = 0
    
    # 워크플로우 크기 기반 점수
    if node_count > 20:
        complexity_score += 3
    elif node_count > 10:
        complexity_score += 2
    elif node_count > 5:
        complexity_score += 1
    
    # 연결 복잡도
    if edge_count > node_count * 1.5:  # 복잡한 연결 구조
        complexity_score += 2
    
    # 사용자 요청 복잡도 분석
    request_lower = user_request.lower()
    complex_keywords = [
        "전체", "완전히", "처음부터", "새로", "리팩토링", "재설계",
        "최적화", "성능", "보안", "에러처리", "예외처리", "로깅",
        "병렬", "분산", "스케일링", "아키텍처"
    ]
    
    for keyword in complex_keywords:
        if keyword in request_lower:
            complexity_score += 1
    
    # 최근 변경사항이 많으면 복잡도 증가
    if recent_changes and len(recent_changes) > 5:
        complexity_score += 1
    
    # 점수에 따른 티어 결정
    if complexity_score >= 5:
        tier = ModelTier.PREMIUM
    elif complexity_score >= 3:
        tier = ModelTier.STANDARD
    else:
        tier = ModelTier.ECONOMY
    
    logger.info(f"Complexity analysis: score={complexity_score}, tier={tier.value}")
    return tier

def select_optimal_model(
    canvas_mode: Literal["agentic-designer", "co-design"],
    current_workflow: Dict[str, Any],
    user_request: str,
    recent_changes: list = None,
    budget_constraint: ModelTier = None,
    prefer_gemini: bool = True,
    require_low_latency: bool = None,
    enable_context_caching: bool = None
) -> ModelConfig:
    """
    최적의 모델을 선택합니다.
    
    [2026년 고도화 전략]
    1. Semantic Intent Detection: 부정 문맥 고려한 의도 분류
    2. Context Caching 최적화: 동일 세션 시 캐싱 지원 모델 우선
    3. Latency 기반 라우팅: 실시간 협업 시 TTFT 최적화
    4. Gemini 불가 시 → Claude/GPT Fallback
    
    Args:
        canvas_mode: Canvas 모드
        current_workflow: 현재 워크플로우
        user_request: 사용자 요청
        recent_changes: 최근 변경사항
        budget_constraint: 예산 제약 (최대 허용 티어)
        prefer_gemini: Gemini 우선 선택 여부 (기본값: True)
        require_low_latency: 낮은 지연 시간 필수 여부 (None=자동 감지)
        enable_context_caching: 컨텍스트 캐싱 활성화 여부 (None=자동 감지)
    
    Returns:
        ModelConfig: 선택된 모델 설정
    """
    
    # 복잡도 분석
    recommended_tier = estimate_request_complexity(
        canvas_mode, current_workflow, user_request, recent_changes
    )
    
    # 예산 제약 적용
    if budget_constraint and budget_constraint.value < recommended_tier.value:
        logger.warning(f"Budget constraint applied: {recommended_tier.value} -> {budget_constraint.value}")
        recommended_tier = budget_constraint
    
    # 환경 변수로 모델 강제 지정 가능
    forced_model = os.getenv("FORCE_MODEL_ID")
    if forced_model and forced_model in AVAILABLE_MODELS:
        logger.info(f"Using forced model: {forced_model}")
        return AVAILABLE_MODELS[forced_model]
    
    # ──────────────────────────────────────────────────────────
    # [① Semantic Intent Detection] 부정 문맥 고려
    # ──────────────────────────────────────────────────────────
    needs_structure, struct_confidence, struct_keywords = _detect_structural_complexity_semantic(user_request)
    needs_long_context, ctx_confidence, ctx_keywords = _detect_long_context_semantic(user_request)
    
    logger.debug(f"Semantic detection: structure={needs_structure}(conf={struct_confidence:.2f}, keywords={struct_keywords}), "
                f"long_context={needs_long_context}(conf={ctx_confidence:.2f})")
    
    # ──────────────────────────────────────────────────────────
    # [② Context Caching 자동 감지]
    # ──────────────────────────────────────────────────────────
    if enable_context_caching is None:
        # 동일 워크플로우 반복 수정 시 캐싱 활성화
        node_count = len(current_workflow.get("nodes", []))
        enable_context_caching = (
            canvas_mode == "co-design" and 
            node_count > 5 and 
            recent_changes and len(recent_changes) >= 2
        )
    
    # ──────────────────────────────────────────────────────────
    # [③ Latency 요구사항 자동 감지]
    # ──────────────────────────────────────────────────────────
    if require_low_latency is None:
        # Co-design 모드에서는 기본적으로 낮은 지연 시간 필요
        require_low_latency = canvas_mode == "co-design"
    
    max_ttft_ms = 200 if require_low_latency else 500
    
    # ──────────────────────────────────────────────────────────
    # Gemini Native 라우팅 전략 (고도화)
    # ──────────────────────────────────────────────────────────
    gemini_available = _is_gemini_available()
    
    if prefer_gemini and gemini_available:
        # Case 1: 구조적 복잡성 (신뢰도 높을 때만) 또는 Agentic Designer
        if (needs_structure and struct_confidence >= 0.6) or canvas_mode == "agentic-designer":
            logger.info(f"Gemini routing: structural_complexity={needs_structure}(conf={struct_confidence:.2f}), "
                       f"mode={canvas_mode} → gemini-2.0-flash")
            return AVAILABLE_MODELS["gemini-2.0-flash"]
        
        # Case 2: 장기 컨텍스트 필요 (신뢰도 높을 때만)
        if needs_long_context and ctx_confidence >= 0.6:
            # 캐싱 활성화 시 Flash 사용 (비용 절감)
            if enable_context_caching:
                logger.info(f"Gemini routing: long_context + caching → gemini-1.5-flash")
                return AVAILABLE_MODELS["gemini-1.5-flash"]
            logger.info(f"Gemini routing: long_context_need=True → gemini-1.5-pro")
            return AVAILABLE_MODELS["gemini-1.5-pro"]
        
        # Case 3: Co-design 실시간 협업 → Latency 최적화
        if canvas_mode == "co-design":
            if require_low_latency:
                # TTFT 기준으로 모델 선택
                if recommended_tier == ModelTier.ECONOMY:
                    logger.info(f"Gemini routing: co-design low-latency economy → gemini-1.5-flash-8b (TTFT={AVAILABLE_MODELS['gemini-1.5-flash-8b'].expected_ttft_ms}ms)")
                    return AVAILABLE_MODELS["gemini-1.5-flash-8b"]
                else:
                    logger.info(f"Gemini routing: co-design low-latency → gemini-1.5-flash (TTFT={AVAILABLE_MODELS['gemini-1.5-flash'].expected_ttft_ms}ms)")
                    return AVAILABLE_MODELS["gemini-1.5-flash"]
            else:
                if recommended_tier == ModelTier.ECONOMY:
                    logger.info("Gemini routing: co-design economy → gemini-1.5-flash-8b")
                    return AVAILABLE_MODELS["gemini-1.5-flash-8b"]
                else:
                    logger.info("Gemini routing: co-design → gemini-1.5-flash")
                    return AVAILABLE_MODELS["gemini-1.5-flash"]
        
        # Case 4: 기본 Gemini 선택 (티어 + 캐싱 고려)
        if recommended_tier == ModelTier.PREMIUM:
            return AVAILABLE_MODELS["gemini-2.0-flash"]
        elif recommended_tier == ModelTier.STANDARD:
            return AVAILABLE_MODELS["gemini-1.5-flash"]
        else:
            return AVAILABLE_MODELS["gemini-1.5-flash-8b"]
    
    # ──────────────────────────────────────────────────────────
    # Fallback: 기존 Claude/GPT 라우팅
    # ──────────────────────────────────────────────────────────
    logger.info(f"Fallback routing: gemini_available={gemini_available}, prefer_gemini={prefer_gemini}")
    
    # 티어별 모델 선택 (Gemini 제외)
    tier_models = {
        model_name: config 
        for model_name, config in AVAILABLE_MODELS.items() 
        if config.tier == recommended_tier and config.provider != ModelProvider.GEMINI
    }
    
    if not tier_models:
        # Fallback to economy tier (Gemini 제외)
        logger.warning(f"No models found for tier {recommended_tier.value}, falling back to economy")
        tier_models = {
            model_name: config 
            for model_name, config in AVAILABLE_MODELS.items() 
            if config.tier == ModelTier.ECONOMY and config.provider != ModelProvider.GEMINI
        }
    
    if not tier_models:
        # 최종 Fallback: Claude Haiku
        logger.warning("No suitable model found, using claude-3-haiku as final fallback")
        return AVAILABLE_MODELS["claude-3-haiku"]
    
    # 가장 비용 효율적인 모델 선택 (같은 티어 내에서)
    selected_model_name = min(tier_models.keys(), 
                             key=lambda x: tier_models[x].cost_per_1k_tokens)
    selected_model = tier_models[selected_model_name]
    
    logger.info(f"Selected model: {selected_model_name} (tier: {selected_model.tier.value}, "
               f"provider: {selected_model.provider.value}, "
               f"cost: ${selected_model.cost_per_1k_tokens}/1K tokens)")
    
    return selected_model


def select_gemini_model(
    for_structure: bool = False,
    for_long_context: bool = False,
    for_realtime: bool = False,
    tier: ModelTier = ModelTier.STANDARD
) -> Optional[ModelConfig]:
    """
    Gemini 모델 직접 선택
    
    Args:
        for_structure: 구조적 추론용 (Loop/Map/Parallel)
        for_long_context: 장기 컨텍스트 활용
        for_realtime: 실시간 응답 필요
        tier: 선호 티어
    
    Returns:
        ModelConfig 또는 None (Gemini 불가 시)
    """
    if not _is_gemini_available():
        logger.warning("Gemini not available")
        return None
    
    if for_structure or for_long_context:
        return AVAILABLE_MODELS["gemini-1.5-pro"]
    
    if for_realtime:
        return AVAILABLE_MODELS["gemini-1.5-flash"]
    
    # 티어 기반 선택
    if tier == ModelTier.PREMIUM:
        return AVAILABLE_MODELS["gemini-1.5-pro"]
    elif tier == ModelTier.STANDARD:
        return AVAILABLE_MODELS["gemini-1.5-flash"]
    else:
        return AVAILABLE_MODELS["gemini-1.5-flash-8b"]


def get_model_for_canvas_mode(
    canvas_mode: Literal["agentic-designer", "co-design"],
    current_workflow: Dict[str, Any] = None,
    user_request: str = "",
    recent_changes: list = None,
    prefer_gemini: bool = True
) -> str:
    """
    Canvas 모드에 따른 모델 ID를 반환합니다.
    
    Args:
        canvas_mode: Canvas 모드
        current_workflow: 현재 워크플로우 상태
        user_request: 사용자 요청
        recent_changes: 최근 변경사항
        prefer_gemini: Gemini 우선 선택 여부
    
    Returns:
        str: 모델 ID
    """
    
    if current_workflow is None:
        current_workflow = {"nodes": [], "edges": []}
    
    # 예산 제약 설정 (환경 변수로 제어 가능)
    budget_tier_name = os.getenv("MAX_MODEL_TIER", "premium").lower()
    budget_constraint = None
    
    if budget_tier_name == "economy":
        budget_constraint = ModelTier.ECONOMY
    elif budget_tier_name == "standard":
        budget_constraint = ModelTier.STANDARD
    # premium이면 제약 없음
    
    # Gemini 우선 설정 (환경변수로 오버라이드 가능)
    gemini_pref = os.getenv("PREFER_GEMINI", "true").lower() in {"true", "1", "yes", "on"}
    
    selected_model = select_optimal_model(
        canvas_mode=canvas_mode,
        current_workflow=current_workflow,
        user_request=user_request,
        recent_changes=recent_changes,
        budget_constraint=budget_constraint,
        prefer_gemini=prefer_gemini and gemini_pref
    )
    
    return selected_model.model_id


def get_model_config_for_canvas_mode(
    canvas_mode: Literal["agentic-designer", "co-design"],
    current_workflow: Dict[str, Any] = None,
    user_request: str = "",
    recent_changes: list = None,
    prefer_gemini: bool = True
) -> ModelConfig:
    """
    Canvas 모드에 따른 전체 모델 설정을 반환합니다.
    
    Returns:
        ModelConfig: 선택된 모델의 전체 설정
    """
    
    if current_workflow is None:
        current_workflow = {"nodes": [], "edges": []}
    
    budget_tier_name = os.getenv("MAX_MODEL_TIER", "premium").lower()
    budget_constraint = None
    
    if budget_tier_name == "economy":
        budget_constraint = ModelTier.ECONOMY
    elif budget_tier_name == "standard":
        budget_constraint = ModelTier.STANDARD
    
    gemini_pref = os.getenv("PREFER_GEMINI", "true").lower() in {"true", "1", "yes", "on"}
    
    return select_optimal_model(
        canvas_mode=canvas_mode,
        current_workflow=current_workflow,
        user_request=user_request,
        recent_changes=recent_changes,
        budget_constraint=budget_constraint,
        prefer_gemini=prefer_gemini and gemini_pref
    )


# ============================================================
# 편의 함수들
# ============================================================

def get_agentic_designer_model(user_request: str = "") -> str:
    """
    Agentic Designer용 모델 ID 반환
    
    구조적 복잡성 감지 시 Gemini 1.5 Pro 우선 사용
    """
    return get_model_for_canvas_mode("agentic-designer", user_request=user_request)


def get_agentic_designer_config(user_request: str = "") -> ModelConfig:
    """Agentic Designer용 전체 모델 설정 반환"""
    return get_model_config_for_canvas_mode("agentic-designer", user_request=user_request)


def get_codesign_model(workflow: Dict[str, Any], request: str, changes: list = None) -> str:
    """
    Co-design용 모델 ID 반환
    
    실시간 협업을 위해 Gemini 1.5 Flash 우선 사용
    """
    return get_model_for_canvas_mode("co-design", workflow, request, changes)


def get_codesign_config(workflow: Dict[str, Any], request: str, changes: list = None) -> ModelConfig:
    """Co-design용 전체 모델 설정 반환"""
    return get_model_config_for_canvas_mode("co-design", workflow, request, changes)


def is_gemini_model(model_id: str) -> bool:
    """주어진 모델 ID가 Gemini 모델인지 확인"""
    return "gemini" in model_id.lower()


def get_model_provider(model_id: str) -> ModelProvider:
    """모델 ID로부터 제공자 확인"""
    for config in AVAILABLE_MODELS.values():
        if config.model_id == model_id:
            return config.provider
    # 기본 추론
    if "gemini" in model_id.lower():
        return ModelProvider.GEMINI
    elif "claude" in model_id.lower() or "anthropic" in model_id.lower():
        return ModelProvider.ANTHROPIC
    elif "gpt" in model_id.lower() or "openai" in model_id.lower():
        return ModelProvider.OPENAI
    elif "llama" in model_id.lower() or "meta" in model_id.lower():
        return ModelProvider.META
    return ModelProvider.ANTHROPIC  # Default fallback