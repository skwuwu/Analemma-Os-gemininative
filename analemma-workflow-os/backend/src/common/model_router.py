"""
Model Router v2.3: Dynamic Model Selection Based on Canvas Mode

Selects appropriate models based on task complexity for cost optimization.
- Agentic Designer (full generation): High-performance models (Gemini 1.5 Pro, Claude 3.5 Sonnet, GPT-4o)
- Co-design (partial modification): Lightweight models (Gemini 1.5 Flash, Claude Haiku)

Gemini Native Integration:
- Structural complexity (Loop/Map) resolution: Gemini 1.5 Pro priority allocation
- Extended context (1M+ tokens): Gemini 1.5 Pro/Flash utilization
- Real-time collaboration: Gemini 1.5 Flash (sub-second response)

v2.3 Critical Fixes:
- (1) Double negation detection improvement: LLM-based intent analysis replacing hardcoded filters
- (2) Adaptive TTFT Routing: Real-time metrics-based dynamic routing
- (3) Soft/Hard budget policy: Flexible performance adjustment when budget is insufficient
"""

import os
import time
import logging
from typing import Dict, Any, Literal, Optional, List, Tuple
from enum import Enum
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)


class ModelTier(Enum):
    """Model performance tier"""
    PREMIUM = "premium"      # Highest performance, high cost (full workflow generation)
    STANDARD = "standard"    # Medium performance, medium cost (complex Co-design)
    ECONOMY = "economy"      # Basic performance, low cost (simple Co-design)


class ThinkingLevel(Enum):
    """
    Gemini Thinking Mode Level
    
    Dynamically adjusts AI reasoning depth for complex planning/design tasks.
    - MINIMAL: Simple changes, typo fixes, etc. (1K tokens)
    - STANDARD: Typical Co-design tasks (2K tokens)
    - DEEP: Complex structural changes, multi-node additions (4K tokens)
    - MAXIMUM: Full workflow generation, architecture design (8K tokens)
    - ULTRA: Extremely complex distributed system design (16K tokens)
    """
    MINIMAL = "minimal"      # 1024
    STANDARD = "standard"    # 2048
    DEEP = "deep"            # 4096
    MAXIMUM = "maximum"      # 8192
    ULTRA = "ultra"          # 16384


# ThinkingLevel
THINKING_BUDGET_MAP = {
    ThinkingLevel.MINIMAL: 1024,
    ThinkingLevel.STANDARD: 2048,
    ThinkingLevel.DEEP: 4096,
    ThinkingLevel.MAXIMUM: 8192,
    ThinkingLevel.ULTRA: 16384,
}


class ModelProvider(Enum):
    """Model provider"""
    GEMINI = "gemini"        # Google Gemini (1M+ context, structural reasoning)
    ANTHROPIC = "anthropic"  # Anthropic Claude (Bedrock)
    OPENAI = "openai"        # OpenAI GPT (direct API)
    META = "meta"            # Meta Llama (Bedrock)


@dataclass
class ModelConfig:
    """
    Model configuration
    
    [2026 Enhancement]
    - supports_context_caching: Gemini Context Caching API support (up to 90% input cost reduction)
    - expected_ttft_ms: Time To First Token - baseline value
    - cached_cost_per_1k_tokens: Cost when caching is applied
    
    [v2.3 Adaptive Routing]
    - _recent_ttft_samples: Recent TTFT measurements (for dynamic routing)
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
    supports_thinking: bool = False           # Thinking Mode (Chain of Thought) 지원
    # [② Context Caching 지원]
    supports_context_caching: bool = False   # Gemini Context Caching API 지원
    cached_cost_per_1k_tokens: float = 0.0   # 캐싱 적용 시 비용 (90% 절감)
    # [③ Latency 지표 - 기준값]
    expected_ttft_ms: int = 500              # 첫 번째 토큰 생성 시간 (ms) - 기준값
    expected_tps: int = 50                   # 초당 토큰 생성 속도 (tokens/sec)


# ============================================================
# [(2) Adaptive TTFT Routing] Real-time TTFT Metrics Tracking
# ============================================================

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ ⚠️ Lambda Stateless Environment Considerations                            │
# │                                                                           │
# │ Currently _ttft_tracker resides in-memory. AWS Lambda containers          │
# │ are destroyed after execution, so previous TTFT records are lost          │
# │ on subsequent invocations.                                                 │
# │                                                                           │
# │ ▸ Demo/Development environment: Current implementation is sufficient      │
# │ ▸ Production environment: Choose from the following options to extend     │
# │                                                                           │
# │   1. ElastiCache (Redis): Share TTFT metrics with short TTL (5min)        │
# │      - Pros: Millisecond-level latency, instant sharing between Lambdas   │
# │      - Cons: Additional cost, VPC configuration required                  │
# │                                                                           │
# │   2. DynamoDB: Automatic expiration using TTL attribute                   │
# │      - Pros: Serverless, low management overhead                          │
# │      - Cons: Higher latency compared to Redis (10-20ms)                   │
# │                                                                           │
# │   3. CloudWatch Metrics: Publish TTFT in EMF format                       │
# │      - Pros: Integrates with existing monitoring, no additional infra     │
# │      - Cons: Query latency (1-2min), unsuitable for real-time routing     │
# │                                                                           │
# │ For distributed environment extension, implement the following interface: │
# │   TTFTStorage.save(model_id, ttft_ms)                                     │
# │   TTFTStorage.load(model_id) -> List[int]                                 │
# └───────────────────────────────────────────────────────────────────────────┘

class TTFTStorageBackend:
    """
    분산 환경용 TTFT 저장소 인터페이스 (Abstract Base)
    
    프로덕션 배포 시 이 인터페이스를 구현하여 주입하면
    Lambda 간 TTFT 메트릭을 공유할 수 있습니다.
    
    구현 예시:
        class RedisTTFTStorage(TTFTStorageBackend):
            def save(self, model_id: str, ttft_ms: int) -> None:
                self.redis.lpush(f"ttft:{model_id}", ttft_ms)
                self.redis.ltrim(f"ttft:{model_id}", 0, 19)  # 최근 20개만 유지
                self.redis.expire(f"ttft:{model_id}", 300)    # 5분 TTL
            
            def load(self, model_id: str) -> List[int]:
                return [int(x) for x in self.redis.lrange(f"ttft:{model_id}", 0, -1)]
    """
    
    def save(self, model_id: str, ttft_ms: int) -> None:
        """TTFT 측정값 저장 (분산 스토리지에)"""
        raise NotImplementedError("Implement this for distributed storage")
    
    def load(self, model_id: str) -> List[int]:
        """최근 TTFT 측정값 로드 (분산 스토리지에서)"""
        raise NotImplementedError("Implement this for distributed storage")
    
    def is_available(self) -> bool:
        """스토리지 연결 상태 확인"""
        return False


class AdaptiveTTFTTracker:
    """
    실시간 TTFT(Time To First Token) 메트릭 추적기
    
    CloudWatch/Cloud Monitoring 없이도 인메모리에서 최근 측정값을 기반으로
    동적으로 모델 선택 가중치를 조절합니다.
    
    사용 예:
        # 호출 후 TTFT 기록
        tracker.record_ttft("gemini-2.0-flash", measured_ttft_ms=145)
        
        # 동적 TTFT 조회
        effective_ttft = tracker.get_effective_ttft("gemini-2.0-flash")
    """
    
    def __init__(self, window_size: int = 20, decay_factor: float = 0.9, 
                 distributed_backend: TTFTStorageBackend = None):
        """
        Args:
            window_size: 추적할 최근 샘플 수 (기본 20개)
            decay_factor: 오래된 샘플 가중치 감쇠 (0.0~1.0)
            distributed_backend: 분산 환경용 스토리지 백엔드 (Optional)
        """
        self._samples: Dict[str, deque] = {}
        self._window_size = window_size
        self._decay_factor = decay_factor
        self._last_update: Dict[str, float] = {}
        self._distributed_backend = distributed_backend
    
    def _sync_from_distributed(self, model_id: str) -> None:
        """분산 스토리지에서 최근 샘플 동기화 (Lambda 콜드 스타트 시)"""
        if self._distributed_backend and self._distributed_backend.is_available():
            try:
                remote_samples = self._distributed_backend.load(model_id)
                if remote_samples and model_id not in self._samples:
                    self._samples[model_id] = deque(remote_samples, maxlen=self._window_size)
                    logger.debug(f"Synced {len(remote_samples)} TTFT samples from distributed storage for {model_id}")
            except Exception as e:
                logger.warning(f"Failed to sync from distributed storage: {e}")
    
    def record_ttft(self, model_id: str, measured_ttft_ms: int) -> None:
        """Record TTFT measurement (in-memory + distributed storage)"""
        if model_id not in self._samples:
            self._samples[model_id] = deque(maxlen=self._window_size)
        
        self._samples[model_id].append(measured_ttft_ms)
        self._last_update[model_id] = time.time()
        
        # Save to distributed storage as well (async recommended but sync implemented)
        if self._distributed_backend and self._distributed_backend.is_available():
            try:
                self._distributed_backend.save(model_id, measured_ttft_ms)
            except Exception as e:
                logger.warning(f"Failed to save TTFT to distributed storage: {e}")
        
        logger.debug(f"TTFT recorded: {model_id}={measured_ttft_ms}ms (samples={len(self._samples[model_id])})")
    
    def get_effective_ttft(self, model_id: str, base_ttft_ms: int) -> int:
        """
        동적 TTFT 계산 (최근 측정값 가중 평균)
        
        Args:
            model_id: 모델 ID
            base_ttft_ms: ModelConfig의 기준 TTFT
            
        Returns:
            효과적 TTFT (ms) - 최근 측정값이 없으면 기준값 반환
        """
        # Sync from distributed storage on Lambda cold start
        self._sync_from_distributed(model_id)
        
        if model_id not in self._samples or len(self._samples[model_id]) == 0:
            return base_ttft_ms
        
        samples = list(self._samples[model_id])
        
        # Exponentially weighted average (higher weight for recent values)
        weighted_sum = 0.0
        weight_total = 0.0
        
        for i, sample in enumerate(samples):
            weight = self._decay_factor ** (len(samples) - 1 - i)
            weighted_sum += sample * weight
            weight_total += weight
        
        if weight_total == 0:
            return base_ttft_ms
        
        effective = int(weighted_sum / weight_total)
        
        # Clamp if deviation from baseline is too large (outlier prevention)
        max_deviation = base_ttft_ms * 3  # Allow up to 3x baseline
        min_deviation = base_ttft_ms // 3  # Allow down to 1/3 baseline
        
        return max(min_deviation, min(effective, max_deviation))
    
    def get_model_health(self, model_id: str, base_ttft_ms: int) -> str:
        """
        모델 상태 판단
        
        Returns:
            "healthy" | "degraded" | "unhealthy" | "unknown"
        """
        if model_id not in self._samples or len(self._samples[model_id]) < 3:
            return "unknown"
        
        effective = self.get_effective_ttft(model_id, base_ttft_ms)
        ratio = effective / base_ttft_ms
        
        if ratio <= 1.2:
            return "healthy"
        elif ratio <= 2.0:
            return "degraded"
        else:
            return "unhealthy"
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """모든 모델의 TTFT 메트릭 반환"""
        result = {}
        for model_id, samples in self._samples.items():
            if samples:
                result[model_id] = {
                    "sample_count": len(samples),
                    "latest_ttft_ms": samples[-1] if samples else None,
                    "avg_ttft_ms": sum(samples) / len(samples),
                    "min_ttft_ms": min(samples),
                    "max_ttft_ms": max(samples),
                    "last_update": self._last_update.get(model_id)
                }
        return result


# 전역 TTFT 추적기 인스턴스
_ttft_tracker = AdaptiveTTFTTracker()


def record_model_ttft(model_id: str, ttft_ms: int) -> None:
    """
    모델 호출 후 TTFT 기록 (외부 API)
    
    사용법:
        start = time.time()
        response = model.generate(...)  # 첫 번째 청크 수신
        ttft_ms = int((time.time() - start) * 1000)
        record_model_ttft("gemini-2.0-flash", ttft_ms)
    """
    _ttft_tracker.record_ttft(model_id, ttft_ms)


def get_ttft_metrics() -> Dict[str, Dict[str, Any]]:
    """현재 TTFT 메트릭 조회 (모니터링용)"""
    return _ttft_tracker.get_all_metrics()


# ============================================================
# [(3) Soft/Hard Budget Policy] Budget Policy
# ============================================================
class BudgetPolicy(Enum):
    """
    Budget constraint policy
    
    - HARD: Immediately switch to lower model when budget exceeded (strict cost control)
    - SOFT: Adjust performance with token limits and warn when budget exceeded (quality priority)
    - ADAPTIVE: Automatically decide based on task importance
    """
    HARD = "hard"        # Strict budget control, accept performance degradation
    SOFT = "soft"        # Flexible budget, adjust with token limits
    ADAPTIVE = "adaptive"  # Automatically decide based on task complexity


@dataclass
class BudgetAdjustmentResult:
    """
    Budget adjustment result
    
    Provides detailed information about what adjustments were made when Soft policy is applied
    
    [(3) Critical Fix] system_prompt_addition field added:
    - Instructions to add to system prompt when SOFT policy is applied
    - Prevents response cut-off due to token limits
    """
    original_model: str
    adjusted_model: str
    original_max_tokens: int
    adjusted_max_tokens: int
    original_thinking_budget: int
    adjusted_thinking_budget: int
    policy_applied: BudgetPolicy
    warning_message: Optional[str] = None
    cost_reduction_percent: float = 0.0
    # [③ Critical Fix] 시스템 프롬프트에 동적 추가할 지침
    system_prompt_addition: Optional[str] = None
    
    @property
    def was_adjusted(self) -> bool:
        return (
            self.original_model != self.adjusted_model or
            self.original_max_tokens != self.adjusted_max_tokens or
            self.original_thinking_budget != self.adjusted_thinking_budget
        )
    
    def get_augmented_system_prompt(self, original_prompt: str) -> str:
        """
        [(3) Critical Fix] Augment system prompt based on budget adjustment
        
        When SOFT policy is applied, dynamically adds "respond briefly" instructions
        to the model to prevent response cut-off mid-sentence.
        
        Args:
            original_prompt: Original system prompt
            
        Returns:
            Augmented system prompt
        """
        if not self.system_prompt_addition:
            return original_prompt
        
        # Add budget constraint instruction at the end of prompt
        return f"""{original_prompt}

{self.system_prompt_addition}"""


def _ensure_output_headroom(
    thinking_budget: int,
    max_tokens: int,
    min_output_ratio: float = 0.4
) -> Tuple[int, int]:
    """
    [Critical Fix] Thinking Budget vs Max Tokens 충돌 방지 가드레일
    
    Gemini Thinking Mode에서 thinking_budget이 max_tokens보다 크거나
    비슷하면 모델이 '생각'만 하고 실제 '답변(Output)'을 낼 공간이 없어집니다.
    
    이 함수는 max_tokens의 일정 비율 이상을 Output용으로 확보하도록
    thinking_budget을 자동으로 조절합니다.
    
    예시:
        max_tokens=2048, thinking_budget=1500, min_output_ratio=0.4
        → Output 공간 = 2048 - 1500 = 548 (26.8%)
        → 26.8% < 40% 이므로 조절 필요
        → adjusted_thinking = 2048 * (1 - 0.4) = 1228
        → Output 공간 = 2048 - 1228 = 820 (40%)
    
    Args:
        thinking_budget: 원래 Thinking Budget
        max_tokens: 원래 Max Tokens
        min_output_ratio: Output용으로 확보할 최소 비율 (기본 40%)
    
    Returns:
        (조정된 thinking_budget, 조정된 max_tokens)
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ Token 분배 다이어그램                                                    │
    │                                                                         │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │                        max_tokens                               │   │
    │  ├────────────────────────────────────┬────────────────────────────┤   │
    │  │       Thinking Budget              │       Output Space         │   │
    │  │       (최대 60%)                    │       (최소 40%)            │   │
    │  └────────────────────────────────────┴────────────────────────────┘   │
    │                                                                         │
    │  조정 규칙:                                                              │
    │  - thinking_budget <= max_tokens * (1 - min_output_ratio)               │
    │  - 예: max_tokens=2048 → thinking_budget <= 1228                        │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    if max_tokens <= 0:
        return thinking_budget, max_tokens
    
    # Calculate maximum allowed Thinking Budget
    max_thinking_budget = int(max_tokens * (1 - min_output_ratio))
    
    # Ensure minimum Thinking Budget (1024 tokens)
    max_thinking_budget = max(1024, max_thinking_budget)
    
    # Check if Thinking Budget adjustment is needed
    if thinking_budget > max_thinking_budget:
        original_thinking = thinking_budget
        thinking_budget = max_thinking_budget
        
        # Output space calculation
        output_space = max_tokens - thinking_budget
        output_ratio = output_space / max_tokens * 100
        
        logger.info(
            f"Thinking/Output headroom adjusted: "
            f"thinking {original_thinking}→{thinking_budget}, "
            f"output_space={output_space} ({output_ratio:.1f}%)"
        )
    
    return thinking_budget, max_tokens


def apply_budget_constraint(
    recommended_model: 'ModelConfig',
    recommended_thinking_budget: int,
    budget_constraint: ModelTier,
    policy: BudgetPolicy = BudgetPolicy.ADAPTIVE,
    task_complexity_score: int = 0
) -> Tuple['ModelConfig', int, BudgetAdjustmentResult]:
    """
    [③ Critical Fix] 예산 제약을 유연하게 적용
    
    HARD 정책: 단순히 하위 모델 선택 (기존 동작)
    SOFT 정책: 동일 모델 유지하되 토큰 제한으로 비용 절감
    ADAPTIVE 정책: 작업 복잡도에 따라 자동 결정
    
    Args:
        recommended_model: 권장된 모델
        recommended_thinking_budget: 권장된 Thinking Budget
        budget_constraint: 예산 티어 제약
        policy: 적용할 정책
        task_complexity_score: 작업 복잡도 점수 (ADAPTIVE 정책용)
    
    Returns:
        (조정된 ModelConfig, 조정된 Thinking Budget, 조정 결과 상세)
    """
    # Budget constraint check
    tier_order = {ModelTier.ECONOMY: 0, ModelTier.STANDARD: 1, ModelTier.PREMIUM: 2}
    
    if tier_order.get(recommended_model.tier, 0) <= tier_order.get(budget_constraint, 2):
        # Within constraint - no adjustment needed
        return recommended_model, recommended_thinking_budget, BudgetAdjustmentResult(
            original_model=recommended_model.model_id,
            adjusted_model=recommended_model.model_id,
            original_max_tokens=recommended_model.max_tokens,
            adjusted_max_tokens=recommended_model.max_tokens,
            original_thinking_budget=recommended_thinking_budget,
            adjusted_thinking_budget=recommended_thinking_budget,
            policy_applied=policy
        )
    
    # ADAPTIVE policy: Decide HARD/SOFT based on complexity
    if policy == BudgetPolicy.ADAPTIVE:
        # Complex tasks (score >= 5) use SOFT, simple tasks use HARD
        policy = BudgetPolicy.SOFT if task_complexity_score >= 5 else BudgetPolicy.HARD
    
    # HARD policy: Switch to lower tier model
    if policy == BudgetPolicy.HARD:
        # Find model matching budget tier
        target_models = [
            (name, config) for name, config in AVAILABLE_MODELS.items()
            if config.tier == budget_constraint and config.provider == recommended_model.provider
        ]
        
        if not target_models:
            # If no same provider, pick any model
            target_models = [
                (name, config) for name, config in AVAILABLE_MODELS.items()
                if config.tier == budget_constraint
            ]
        
        if target_models:
            # Select the cheapest model
            _, adjusted_model = min(target_models, key=lambda x: x[1].cost_per_1k_tokens)
        else:
            adjusted_model = AVAILABLE_MODELS.get("claude-3-haiku", recommended_model)
        
        # Thinking budget proportionally reduced
        budget_ratio = adjusted_model.max_tokens / recommended_model.max_tokens
        adjusted_thinking = int(recommended_thinking_budget * budget_ratio)
        adjusted_thinking = max(1024, min(adjusted_thinking, adjusted_model.max_tokens // 2))
        
        # ═══════════════════════════════════════════════════════════════════
        # [Critical Fix] Thinking Budget vs Max Tokens 충돌 방지
        # - thinking_budget이 max_tokens보다 크면 생각만 하고 답변이 없음
        # - 최소 Output 공간 보장: max_tokens의 40% 이상은 Output용으로 확보
        # ═══════════════════════════════════════════════════════════════════
        adjusted_max_tokens = adjusted_model.max_tokens
        adjusted_thinking, adjusted_max_tokens = _ensure_output_headroom(
            thinking_budget=adjusted_thinking,
            max_tokens=adjusted_max_tokens,
            min_output_ratio=0.4  # 최소 40%는 Output용
        )
        
        cost_reduction = (1 - adjusted_model.cost_per_1k_tokens / recommended_model.cost_per_1k_tokens) * 100
        
        return adjusted_model, adjusted_thinking, BudgetAdjustmentResult(
            original_model=recommended_model.model_id,
            adjusted_model=adjusted_model.model_id,
            original_max_tokens=recommended_model.max_tokens,
            adjusted_max_tokens=adjusted_max_tokens,
            original_thinking_budget=recommended_thinking_budget,
            adjusted_thinking_budget=adjusted_thinking,
            policy_applied=BudgetPolicy.HARD,
            warning_message=f"⚠️ Budget constraint: Switched {recommended_model.model_id} → {adjusted_model.model_id}. Performance may degrade for complex tasks.",
            cost_reduction_percent=cost_reduction
        )
    
    # SOFT policy: Same model, reduce cost through token limits
    else:  # BudgetPolicy.SOFT
        # Calculate cost ratio
        target_cost_ratio = {
            ModelTier.ECONOMY: 0.3,
            ModelTier.STANDARD: 0.6,
            ModelTier.PREMIUM: 1.0
        }.get(budget_constraint, 1.0)
        
        # max_tokens reduction (cost proportional to output tokens)
        adjusted_max_tokens = int(recommended_model.max_tokens * target_cost_ratio)
        adjusted_max_tokens = max(1024, adjusted_max_tokens)  # Minimum 1K
        
        # Thinking budget also reduced
        adjusted_thinking = int(recommended_thinking_budget * target_cost_ratio)
        adjusted_thinking = max(1024, adjusted_thinking)
        
        # ═══════════════════════════════════════════════════════════════════
        # [Critical Fix] Thinking Budget vs Max Tokens 충돌 방지
        # - thinking_budget이 max_tokens보다 크면 생각만 하고 답변이 없음
        # - 최소 Output 공간 보장: max_tokens의 40% 이상은 Output용으로 확보
        # ═══════════════════════════════════════════════════════════════════
        adjusted_thinking, adjusted_max_tokens = _ensure_output_headroom(
            thinking_budget=adjusted_thinking,
            max_tokens=adjusted_max_tokens,
            min_output_ratio=0.4  # 최소 40%는 Output용
        )
        
        cost_reduction = (1 - target_cost_ratio) * 100
        
        # ===============================================================
        # [(3) Critical Fix] Generate system prompt instructions for SOFT policy
        # - If only max_tokens is reduced, the model response gets cut off mid-sentence
        # - Dynamically add "respond briefly" instructions to system prompt
        # ===============================================================
        budget_constraint_prompt = f"""⚠️ [System Budget Constraint Notice]
Due to current budget constraints, response tokens are limited to {adjusted_max_tokens}.
Please follow these instructions strictly:

1. **Be concise with essentials only**: Answer only the core requested content without elaboration.
2. **Priority-based output**: Output the most important content first.
3. **Minimize code**: Include only essential logic when code is needed.
4. **Maintain complete sentences**: Plan your conclusion first to avoid mid-sentence cut-off.
5. **Ensure JSON structure completion**: Always include closing brackets when outputting JSON.

The goal is to provide complete and useful responses within the limited tokens."""
        
        return recommended_model, adjusted_thinking, BudgetAdjustmentResult(
            original_model=recommended_model.model_id,
            adjusted_model=recommended_model.model_id,
            original_max_tokens=recommended_model.max_tokens,
            adjusted_max_tokens=adjusted_max_tokens,
            original_thinking_budget=recommended_thinking_budget,
            adjusted_thinking_budget=adjusted_thinking,
            policy_applied=BudgetPolicy.SOFT,
            warning_message=f"⚠️ Budget constraint: Output tokens {recommended_model.max_tokens}→{adjusted_max_tokens}, Thinking {recommended_thinking_budget}→{adjusted_thinking} limited.",
            cost_reduction_percent=cost_reduction,
            system_prompt_addition=budget_constraint_prompt  # [(3) Fix] Add prompt instructions
        )


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
        cached_cost_per_1k_tokens=0.01875,  # 75% discount with caching
        expected_ttft_ms=150,               # Very fast response
        expected_tps=150,                   # High throughput
    ),
    "gemini-2.0-flash-thinking-exp": ModelConfig(
        model_id="gemini-2.0-flash-thinking-exp",
        max_tokens=8192,
        cost_per_1k_tokens=0.075,  # Same as flash
        tier=ModelTier.PREMIUM,
        provider=ModelProvider.GEMINI,
        supports_long_context=True,
        supports_structured_output=True,
        supports_context_caching=True,
        supports_thinking=True,             # Thinking Mode 지원
        cached_cost_per_1k_tokens=0.01875,  # 75% discount with caching
        expected_ttft_ms=200,               # Slightly slower due to thinking
        expected_tps=120,                   # High throughput maintained
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
        cached_cost_per_1k_tokens=0.3125,   # 75% discount with caching
        expected_ttft_ms=300,               # Fast response
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
        cached_cost_per_1k_tokens=0.01875,  # 75% discount with caching
        expected_ttft_ms=100,               # Fastest response
        expected_tps=200,                   # Highest throughput
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
        cached_cost_per_1k_tokens=0.009375, # 75% discount with caching
        expected_ttft_ms=80,                # Ultra-fast response (for Pre-Routing)
        expected_tps=250,                   # Maximum throughput
    ),
    
    # ──────────────────────────────────────────────────────────
    # Premium Tier - 전체 워크플로우 생성용 (Fallback)
    # ──────────────────────────────────────────────────────────
    # Claude 3.5 Sonnet (실제로는 Haiku로 폴백됨 - 구현되지 않음)
    # "claude-3.5-sonnet": ModelConfig(
    #     model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
    #     max_tokens=8192,
    #     cost_per_1k_tokens=3.0,  # $3.00 per 1K tokens (input)
    #     tier=ModelTier.PREMIUM,
    #     provider=ModelProvider.ANTHROPIC,
    #     expected_ttft_ms=400,
    #     expected_tps=60,
    # ),
    # "gpt-4o": ModelConfig(  # TODO: OpenAI API 구현 필요
    #     model_id="gpt-4o-2024-08-06",
    #     max_tokens=4096,
    #     cost_per_1k_tokens=2.5,  # $2.50 per 1K tokens (input)
    #     tier=ModelTier.PREMIUM,
    #     provider=ModelProvider.OPENAI,
    #     expected_ttft_ms=350,
    #     expected_tps=70,
    # ),
    
    # ──────────────────────────────────────────────────────────
    # Standard Tier - 복잡한 Co-design용
    # ──────────────────────────────────────────────────────────
    # Claude 3 Sonnet (실제로는 Haiku로 폴백됨 - 구현되지 않음)
    # "claude-3-sonnet": ModelConfig(
    #     model_id="anthropic.claude-3-sonnet-20240229-v1:0",
    #     max_tokens=4096,
    #     cost_per_1k_tokens=3.0,  # $3.00 per 1K tokens (input)
    #     tier=ModelTier.STANDARD,
    #     provider=ModelProvider.ANTHROPIC,
    #     expected_ttft_ms=450,
    #     expected_tps=55,
    # ),
    
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
    # "llama-3.1-70b": ModelConfig(  # TODO: Meta Llama Bedrock 구현 필요
    #     model_id="meta.llama3-1-70b-instruct-v1:0",
    #     max_tokens=2048,
    #     cost_per_1k_tokens=0.99,  # $0.99 per 1K tokens (input)
    #     tier=ModelTier.ECONOMY,
    #     provider=ModelProvider.META,
    #     expected_ttft_ms=300,
    #     expected_tps=80,
    # ),
}


# ============================================================
# Structural Complexity Keywords (Gemini Priority Allocation Trigger)
# ============================================================
STRUCTURAL_KEYWORDS = [
    # Loop/Iteration related
    "repeat", "loop", "for each", "foreach", "iterate", "traverse",
    # Map/Parallel related
    "parallel", "map", "concurrent", "distributed", "simultaneous",
    # Conditional branching related
    "condition", "branch", "if", "else", "switch",
    # Error handling related
    "retry", "catch", "error", "exception", "fallback",
    # Complex structure related
    "subgraph", "nested", "workflow call",
]

# Long context keywords (Gemini Long Context Utilization Trigger)
LONG_CONTEXT_KEYWORDS = [
    "previous", "earlier", "before", "past", "history", "remember",
    "3 steps ago", "previous version", "revert", "restore", "context",
]


def _detect_structural_complexity(user_request: str) -> bool:
    """Detect if structural complexity (Loop/Map/Parallel) is needed (keyword-based)"""
    request_lower = user_request.lower()
    return any(keyword in request_lower for keyword in STRUCTURAL_KEYWORDS)


def _detect_long_context_need(user_request: str) -> bool:
    """Detect if long context reference is needed (keyword-based)"""
    request_lower = user_request.lower()
    return any(keyword in request_lower for keyword in LONG_CONTEXT_KEYWORDS)


def _is_gemini_available() -> bool:
    """Gemini API 사용 가능 여부 확인 (Vertex AI SDK 기반)"""
    try:
        # Vertex AI 초기화 가능 여부 확인
        from src.services.llm.gemini_service import _init_vertexai
        return _init_vertexai()
    except Exception as e:
        logger.debug(f"Gemini availability check failed: {e}")
        # Fallback: 환경변수 기반 기본 확인
        project_id = os.getenv("GCP_PROJECT_ID")
        has_creds = (
            os.getenv("GCP_SERVICE_ACCOUNT_KEY") or
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or
            os.getenv("GCP_SA_SECRET_NAME")
        )
        return bool(project_id and has_creds)


# ============================================================
# [(1) Pre-Routing Classifier] Semantic Intent Detection
# ============================================================
# Overcome keyword matching limitations by classifying intent with inexpensive model
# Handles negation contexts like "ignore previous features and write without loops"

# Negation patterns (invalidate keywords)
NEGATION_PATTERNS = [
    "without", "except", "exclude", "ignore", "don't", "do not", "no ",
    "skip", "remove", "disable", "avoid", "not use",
]

# [① Critical Fix] 이중 부정 패턴 (부정을 다시 긍정으로)
DOUBLE_NEGATION_PATTERNS = [
    # 한국어 이중 부정
    ("않으면 안", True),   # "하지 않으면 안 돼" = 해야 한다
    ("없으면 안", True),   # "없으면 안 돼" = 있어야 한다
    ("아니면 안", True),   # "아니면 안 돼" = 맞아야 한다
    ("않을 수 없", True),  # "않을 수 없다" = 해야 한다
    ("없을 수 없", True),  # "없을 수 없다" = 있어야 한다
    ("안 하면 안", True),  # "안 하면 안 돼" = 해야 한다
    # 영어 이중 부정
    ("can't not", True),
    ("cannot not", True),
    ("not without", True),  # "not without loops" = loops 필요
    ("never without", True),
]

# Sarcasm/Emphasis patterns (emphasize keyword usage)
EMPHASIS_PATTERNS = [
    "must", "always", "definitely", "absolutely", "required",
    "essential", "necessary", "critical", "important", "mandatory",
]


def _is_negated_keyword(request_lower: str, keyword: str) -> bool:
    """
    [① Critical Fix] 키워드가 부정 문맥에서 사용되었는지 확인
    
    개선 사항:
    - 이중 부정 감지 ("않으면 안 돼" = 긍정)
    - 반어법/강조 표현 감지
    - 컨텍스트 윈도우 확장
    
    예시:
    - "루프 없이 짜줘" → 부정 (True)
    - "루프를 쓰지 않으면 안 돼" → 이중부정=긍정 (False)
    - "반드시 루프를 써야 해" → 강조=긍정 (False)
    """
    keyword_pos = request_lower.find(keyword)
    if keyword_pos == -1:
        return False
    
    # 키워드 주변 컨텍스트 추출 (앞 50자, 뒤 30자)
    context_start = max(0, keyword_pos - 50)
    context_end = min(len(request_lower), keyword_pos + len(keyword) + 30)
    context = request_lower[context_start:context_end]
    
    # ═══════════════════════════════════════════════════════════════════
    # Step 1: 이중 부정 체크 (이중 부정 = 긍정이므로 False 반환)
    # ═══════════════════════════════════════════════════════════════════
    for pattern, _ in DOUBLE_NEGATION_PATTERNS:
        if pattern in context:
            logger.debug(f"Double negation detected: '{pattern}' in context of '{keyword}'")
            return False  # 이중 부정 = 긍정 → 키워드는 부정되지 않음
    
    # ═══════════════════════════════════════════════════════════════════
    # Step 2: 강조 표현 체크 (강조 = 긍정이므로 False 반환)
    # ═══════════════════════════════════════════════════════════════════
    context_before = request_lower[context_start:keyword_pos]
    for emphasis in EMPHASIS_PATTERNS:
        if emphasis in context_before:
            logger.debug(f"Emphasis detected: '{emphasis}' before '{keyword}'")
            return False  # 강조 표현 = 긍정
    
    # ═══════════════════════════════════════════════════════════════════
    # Step 3: 단순 부정어 체크
    # ═══════════════════════════════════════════════════════════════════
    context_after = request_lower[keyword_pos:keyword_pos + len(keyword) + 20]
    
    for negation in NEGATION_PATTERNS:
        if negation in context_before or negation in context_after:
            # ═══════════════════════════════════════════════════════════════════
            # [② Critical Fix] 맥락적 한계 해결 - 문장 종결 어미까지 분석
            # "루프 없이 짜는 건 좋지 않아"와 같이 부정어가 문장 끝에 걸리는 경우
            # remaining[:20]에서 remaining[:50]으로 확장하고, 종결 어미 패턴 분석
            # ═══════════════════════════════════════════════════════════════════
            remaining = request_lower[keyword_pos + len(keyword):]
            
            # 문장 끝까지 분석 (마침표, 물음표, 느낌표 또는 최대 50자)
            sentence_end_markers = [".", "?", "!", "\n"]
            sentence_end_pos = len(remaining)
            for marker in sentence_end_markers:
                pos = remaining.find(marker)
                if pos != -1 and pos < sentence_end_pos:
                    sentence_end_pos = pos
            
            # 종결 어미까지 포함하여 분석 (최대 50자)
            remaining_to_analyze = remaining[:min(sentence_end_pos + 1, 50)]
            
            # 이중 부정 패턴 체크 (확장된 윈도우)
            if any(dn[0].split()[-1] in remaining_to_analyze for dn in DOUBLE_NEGATION_PATTERNS):
                return False  # 이중 부정 가능성
            
            # [② Critical Fix] 종결 어미 부정 패턴 추가 분석
            # "좋지 않아", "안 돼", "어렵다" 등이 뒤에 오면 이중 부정일 가능성
            ENDING_NEGATION_PATTERNS = [
                "좋지 않", "좋지않", "바람직하지 않", "적절하지 않",
                "안 돼", "안돼", "안 된다", "안된다",
                "힘들", "어렵", "불가능",
                "not good", "not ideal", "not recommended",
                "bad idea", "should not", "shouldn't",
            ]
            
            if any(ending in remaining_to_analyze for ending in ENDING_NEGATION_PATTERNS):
                logger.debug(f"Ending negation pattern detected after '{keyword}': implies positive intent")
                return False  # 종결 부정 = 이중 부정 = 긍정
            
            logger.debug(f"Negation detected: '{negation}' near '{keyword}'")
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
        
        # [① Critical Fix] 이중 부정 및 반어법 분석 지침 강화
        # [② Critical Fix] 문장 종결 어미까지 완벽 분석 지침 추가
        classifier_prompt = f"""다음 사용자 요청을 분석하여 JSON으로 응답하세요.

사용자 요청: "{user_request}"

## 분석 항목
1. needs_structure: 반복(for_each), 병렬(parallel), 조건분기(if/else) 등 구조적 패턴이 **실제로** 필요한가?
2. needs_long_context: 이전 대화나 과거 작업을 **실제로** 참조해야 하는가?
3. complexity_tier: 작업 복잡도 ("premium": 전체 설계, "standard": 부분 수정, "economy": 단순 변경)

## ⚠️ 특별 주의: 이중 부정 및 반어법 분석
다음 패턴들을 정확히 분석하세요:

### 이중 부정 = 긍정 (기능이 필요함)
- "루프를 쓰지 않으면 안 돼" → needs_structure: true (루프 필요)
- "병렬 처리 없으면 안 된다" → needs_structure: true (병렬 필요)
- "이전 맥락 없이는 안 돼" → needs_long_context: true (맥락 필요)

### 단순 부정 = 부정 (기능 불필요)
- "루프 없이 짜줘" → needs_structure: false (루프 불필요)
- "이전 기능 무시해" → needs_long_context: false (맥락 불필요)

### 강조 표현 = 긍정 (기능이 필요함)
- "반드시 루프를 써야 해" → needs_structure: true
- "꼭 이전 버전 참고해" → needs_long_context: true

### ⚠️ [중요] 문장 종결 어미까지 완벽 분석
부정어가 문장 중간에 있더라도, **문장 끝의 종결 어미**를 반드시 확인하세요:
- "루프 없이 짜는 건 좋지 않아" → needs_structure: true (이중 부정: 없이 + 좋지 않아 = 필요)
- "병렬 처리 빼면 안 될 것 같아" → needs_structure: true (이중 부정: 빼면 + 안 될 = 필요)
- "이전 버전 무시하면 문제 생겨" → needs_long_context: true (부정 결과가 나쁨 = 필요)

종결 어미 예시:
- 긍정 종결: ~해야 해, ~필요해, ~좋아, ~맞아
- 부정 종결: ~하면 안 돼, ~없으면 안 돼, ~좋지 않아, ~문제야

JSON 형식으로만 응답 (설명 없이):
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
    budget_policy: BudgetPolicy = BudgetPolicy.ADAPTIVE,
    prefer_gemini: bool = True,
    require_low_latency: bool = None,
    enable_context_caching: bool = None,
    use_adaptive_ttft: bool = True
) -> ModelConfig:
    """
    최적의 모델을 선택합니다.
    
    [v2.3 고도화 전략]
    1. Semantic Intent Detection: 부정 문맥 + 이중 부정 고려한 의도 분류
    2. Context Caching 최적화: 동일 세션 시 캐싱 지원 모델 우선
    3. Adaptive TTFT Routing: 실시간 측정 기반 동적 라우팅
    4. Soft/Hard Budget Policy: 유연한 예산 정책
    5. Gemini 불가 시 → Claude/GPT Fallback
    
    Args:
        canvas_mode: Canvas 모드
        current_workflow: 현재 워크플로우
        user_request: 사용자 요청
        recent_changes: 최근 변경사항
        budget_constraint: 예산 제약 (최대 허용 티어)
        budget_policy: 예산 정책 (HARD/SOFT/ADAPTIVE)
        prefer_gemini: Gemini 우선 선택 여부 (기본값: True)
        require_low_latency: 낮은 지연 시간 필수 여부 (None=자동 감지)
        enable_context_caching: 컨텍스트 캐싱 활성화 여부 (None=자동 감지)
        use_adaptive_ttft: 동적 TTFT 라우팅 사용 여부
    
    Returns:
        ModelConfig: 선택된 모델 설정
    """
    
    # ──────────────────────────────────────────────────────────
    # [Design Mode Special Handling] Thinking 지원 모델 우선 선택
    # ──────────────────────────────────────────────────────────
    if canvas_mode in ["agentic-designer", "co-design"]:
        # 디자인 모드에서는 Thinking Mode가 유용하므로 thinking 지원 모델 우선 선택
        thinking_models = ["gemini-2.0-flash-thinking-exp", "gemini-1.5-pro"]
        
        for model_name in thinking_models:
            if model_name in AVAILABLE_MODELS:
                model_config = AVAILABLE_MODELS[model_name]
                # 예산 제약 확인
                if budget_constraint and model_config.tier.value > budget_constraint.value:
                    continue
                # Gemini 우선 정책 확인
                if not prefer_gemini and "gemini" in model_name:
                    continue
                    
                logger.info(f"Design mode: Selected thinking-capable model {model_name} for {canvas_mode}")
                return model_config
        
        logger.warning(f"Design mode: No thinking-capable models available, falling back to standard selection")
    
    # 복잡도 분석
    recommended_tier = estimate_request_complexity(
        canvas_mode, current_workflow, user_request, recent_changes
    )
    
    # 환경 변수로 모델 강제 지정 가능
    forced_model = os.getenv("FORCE_MODEL_ID")
    if forced_model and forced_model in AVAILABLE_MODELS:
        logger.info(f"Using forced model: {forced_model}")
        return AVAILABLE_MODELS[forced_model]
    
    # ──────────────────────────────────────────────────────────
    # [① Semantic Intent Detection] 부정 문맥 + 이중 부정 고려
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
    # [③ Latency 요구사항 자동 감지 + Adaptive TTFT]
    # ──────────────────────────────────────────────────────────
    if require_low_latency is None:
        # Co-design 모드에서는 기본적으로 낮은 지연 시간 필요
        require_low_latency = canvas_mode == "co-design"
    
    max_ttft_ms = 200 if require_low_latency else 500
    
    # ──────────────────────────────────────────────────────────
    # Gemini Native 라우팅 전략 (고도화)
    # ──────────────────────────────────────────────────────────
    gemini_available = _is_gemini_available()
    
    selected_model = None
    
    if prefer_gemini and gemini_available:
        # Case 1: 구조적 복잡성 (신뢰도 높을 때만) 또는 Agentic Designer
        if (needs_structure and struct_confidence >= 0.6) or canvas_mode == "agentic-designer":
            selected_model = AVAILABLE_MODELS["gemini-2.0-flash"]
            logger.info(f"Gemini routing: structural_complexity={needs_structure}(conf={struct_confidence:.2f}), "
                       f"mode={canvas_mode} → gemini-2.0-flash")
        
        # Case 2: 장기 컨텍스트 필요 (신뢰도 높을 때만)
        elif needs_long_context and ctx_confidence >= 0.6:
            if enable_context_caching:
                selected_model = AVAILABLE_MODELS["gemini-1.5-flash"]
                logger.info(f"Gemini routing: long_context + caching → gemini-1.5-flash")
            else:
                selected_model = AVAILABLE_MODELS["gemini-1.5-pro"]
                logger.info(f"Gemini routing: long_context_need=True → gemini-1.5-pro")
        
        # Case 3: Co-design 실시간 협업 → Latency 최적화
        elif canvas_mode == "co-design":
            # [② Adaptive TTFT] 동적 TTFT 기반 모델 선택
            if require_low_latency and use_adaptive_ttft:
                # 후보 모델들의 실제 TTFT 비교
                candidates = ["gemini-1.5-flash", "gemini-1.5-flash-8b"]
                best_model = None
                best_ttft = float('inf')
                
                for model_name in candidates:
                    model_config = AVAILABLE_MODELS[model_name]
                    effective_ttft = _ttft_tracker.get_effective_ttft(
                        model_name, model_config.expected_ttft_ms
                    )
                    health = _ttft_tracker.get_model_health(model_name, model_config.expected_ttft_ms)
                    
                    # 건강하지 않은 모델은 페널티 부여
                    if health == "unhealthy":
                        effective_ttft *= 2
                    elif health == "degraded":
                        effective_ttft *= 1.3
                    
                    logger.debug(f"Adaptive TTFT: {model_name} effective={effective_ttft}ms, health={health}")
                    
                    if effective_ttft < best_ttft and effective_ttft <= max_ttft_ms * 1.5:
                        best_ttft = effective_ttft
                        best_model = model_name
                
                if best_model:
                    selected_model = AVAILABLE_MODELS[best_model]
                    logger.info(f"Adaptive TTFT routing: {best_model} (effective_ttft={best_ttft}ms)")
                else:
                    # TTFT 조건 만족하는 모델 없음 → Flash 사용
                    selected_model = AVAILABLE_MODELS["gemini-1.5-flash"]
            else:
                # 기존 로직
                if recommended_tier == ModelTier.ECONOMY:
                    selected_model = AVAILABLE_MODELS["gemini-1.5-flash-8b"]
                else:
                    selected_model = AVAILABLE_MODELS["gemini-1.5-flash"]
        
        # Case 4: 기본 Gemini 선택 (티어 + 캐싱 고려)
        else:
            if recommended_tier == ModelTier.PREMIUM:
                selected_model = AVAILABLE_MODELS["gemini-2.0-flash"]
            elif recommended_tier == ModelTier.STANDARD:
                selected_model = AVAILABLE_MODELS["gemini-1.5-flash"]
            else:
                selected_model = AVAILABLE_MODELS["gemini-1.5-flash-8b"]
    
    # ──────────────────────────────────────────────────────────
    # Fallback: 기존 Claude/GPT 라우팅
    # ──────────────────────────────────────────────────────────
    if selected_model is None:
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
            selected_model = AVAILABLE_MODELS["claude-3-haiku"]
        else:
            # 가장 비용 효율적인 모델 선택 (같은 티어 내에서)
            selected_model_name = min(tier_models.keys(), 
                                     key=lambda x: tier_models[x].cost_per_1k_tokens)
            selected_model = tier_models[selected_model_name]
    
    logger.info(f"Selected model: {selected_model.model_id} (tier: {selected_model.tier.value}, "
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


# ============================================================
# Thinking Level Control (동적 사고 예산 조정)
# ============================================================

def calculate_thinking_budget(
    canvas_mode: str = "co-design",
    current_workflow: Dict[str, Any] = None,
    user_request: str = "",
    has_structural_patterns: bool = None,
    conflict_detected: bool = False,
    recent_changes: list = None,
    override_level: ThinkingLevel = None
) -> tuple:
    """
    작업 복잡도에 따라 Gemini Thinking Mode의 토큰 예산을 동적으로 계산합니다.
    
    복잡한 계획/설계 작업에서 AI의 사고 깊이를 적절히 조절하여:
    - 단순 작업: 빠른 응답 (토큰 절약)
    - 복잡한 작업: 깊은 추론 (품질 향상)
    
    Args:
        canvas_mode: Canvas 모드 ("agentic-designer" 또는 "co-design")
        current_workflow: 현재 워크플로우 상태 (nodes, edges)
        user_request: 사용자 요청 텍스트
        has_structural_patterns: 구조적 패턴(Loop/Map/Parallel) 필요 여부 (None=자동 감지)
        conflict_detected: 지침 충돌 감지 여부
        recent_changes: 최근 변경사항 목록
        override_level: 강제 지정할 ThinkingLevel (None=자동 계산)
    
    Returns:
        (thinking_budget_tokens: int, thinking_level: ThinkingLevel, reasoning: str)
    
    Examples:
        >>> budget, level, reason = calculate_thinking_budget(
        ...     canvas_mode="agentic-designer",
        ...     current_workflow={"nodes": [], "edges": []},
        ...     user_request="이메일 자동화 워크플로우를 만들어줘"
        ... )
        >>> print(f"{level.value}: {budget} tokens - {reason}")
        maximum: 8192 tokens - Agentic Designer mode requires deep reasoning
    """
    if current_workflow is None:
        current_workflow = {"nodes": [], "edges": []}
    
    # 강제 지정된 레벨이 있으면 바로 반환
    if override_level is not None:
        budget = THINKING_BUDGET_MAP.get(override_level, 2048)
        return budget, override_level, f"Override level: {override_level.value}"
    
    # 기본 파라미터 추출
    nodes = current_workflow.get("nodes", [])
    edges = current_workflow.get("edges", [])
    node_count = len(nodes)
    edge_count = len(edges)
    
    # 복잡도 점수 초기화
    complexity_score = 0
    reasoning_parts = []
    
    # ════════════════════════════════════════════════════════════════════
    # 1. Canvas 모드 기반 기본 점수
    # ════════════════════════════════════════════════════════════════════
    if canvas_mode == "agentic-designer":
        complexity_score += 4
        reasoning_parts.append("Agentic Designer mode (+4)")
    elif canvas_mode == "co-design":
        complexity_score += 1
        reasoning_parts.append("Co-design mode (+1)")
    
    # ════════════════════════════════════════════════════════════════════
    # 2. 워크플로우 규모 기반 점수
    # ════════════════════════════════════════════════════════════════════
    if node_count >= 30:
        complexity_score += 3
        reasoning_parts.append(f"Large workflow ({node_count} nodes, +3)")
    elif node_count >= 15:
        complexity_score += 2
        reasoning_parts.append(f"Medium workflow ({node_count} nodes, +2)")
    elif node_count >= 5:
        complexity_score += 1
        reasoning_parts.append(f"Small workflow ({node_count} nodes, +1)")
    
    # 복잡한 연결 구조 (엣지가 노드 수의 1.5배 이상)
    if node_count > 0 and edge_count > node_count * 1.5:
        complexity_score += 1
        reasoning_parts.append(f"Complex connections ({edge_count} edges, +1)")
    
    # ════════════════════════════════════════════════════════════════════
    # 3. 구조적 패턴 감지 (Loop/Map/Parallel)
    # ════════════════════════════════════════════════════════════════════
    if has_structural_patterns is None:
        # 자동 감지
        needs_structure, struct_confidence, struct_keywords = _detect_structural_complexity_semantic(user_request)
        has_structural_patterns = needs_structure and struct_confidence >= 0.5
        if has_structural_patterns:
            reasoning_parts.append(f"Structural patterns detected: {struct_keywords} (+2)")
    
    if has_structural_patterns:
        complexity_score += 2
    
    # ════════════════════════════════════════════════════════════════════
    # 4. 지침 충돌 감지
    # ════════════════════════════════════════════════════════════════════
    if conflict_detected:
        complexity_score += 1
        reasoning_parts.append("Instruction conflict detected (+1)")
    
    # ════════════════════════════════════════════════════════════════════
    # 5. 사용자 요청 복잡도 분석
    # ════════════════════════════════════════════════════════════════════
    request_lower = user_request.lower()
    
    # 고복잡도 키워드
    high_complexity_keywords = [
        "전체", "완전히", "처음부터", "새로", "리팩토링", "재설계", "아키텍처",
        "최적화", "성능", "보안", "에러처리", "병렬", "분산", "스케일링",
        "통합", "마이그레이션", "refactor", "redesign", "architecture"
    ]
    
    high_matches = [kw for kw in high_complexity_keywords if kw in request_lower]
    if len(high_matches) >= 3:
        complexity_score += 2
        reasoning_parts.append(f"High complexity keywords: {high_matches[:3]} (+2)")
    elif len(high_matches) >= 1:
        complexity_score += 1
        reasoning_parts.append(f"Complexity keywords: {high_matches[:2]} (+1)")
    
    # ════════════════════════════════════════════════════════════════════
    # 6. 최근 변경사항 패턴 분석
    # ════════════════════════════════════════════════════════════════════
    if recent_changes:
        change_count = len(recent_changes)
        if change_count >= 10:
            complexity_score += 2
            reasoning_parts.append(f"Many recent changes ({change_count}, +2)")
        elif change_count >= 5:
            complexity_score += 1
            reasoning_parts.append(f"Recent changes ({change_count}, +1)")
    
    # ════════════════════════════════════════════════════════════════════
    # 7. ThinkingLevel 결정
    # ════════════════════════════════════════════════════════════════════
    if complexity_score >= 8:
        level = ThinkingLevel.ULTRA
    elif complexity_score >= 6:
        level = ThinkingLevel.MAXIMUM
    elif complexity_score >= 4:
        level = ThinkingLevel.DEEP
    elif complexity_score >= 2:
        level = ThinkingLevel.STANDARD
    else:
        level = ThinkingLevel.MINIMAL
    
    budget = THINKING_BUDGET_MAP.get(level, 2048)
    reasoning = f"Score={complexity_score}: " + ", ".join(reasoning_parts) if reasoning_parts else f"Score={complexity_score}: Default"
    
    logger.info(f"Thinking budget calculated: {level.value}={budget} tokens ({reasoning})")
    
    return budget, level, reasoning


def get_thinking_config_for_workflow(
    canvas_mode: str,
    current_workflow: Dict[str, Any] = None,
    user_request: str = "",
    enable_thinking: bool = True
) -> Dict[str, Any]:
    """
    워크플로우 컨텍스트에 맞는 Thinking Mode 설정을 반환합니다.
    
    Gemini API 호출 시 바로 사용할 수 있는 형태로 반환합니다.
    
    Args:
        canvas_mode: Canvas 모드
        current_workflow: 현재 워크플로우
        user_request: 사용자 요청
        enable_thinking: Thinking Mode 활성화 여부
    
    Returns:
        {
            "enable_thinking": bool,
            "thinking_budget_tokens": int,
            "thinking_level": str,
            "reasoning": str
        }
    """
    if not enable_thinking:
        return {
            "enable_thinking": False,
            "thinking_budget_tokens": 0,
            "thinking_level": "disabled",
            "reasoning": "Thinking mode disabled"
        }
    
    budget, level, reasoning = calculate_thinking_budget(
        canvas_mode=canvas_mode,
        current_workflow=current_workflow,
        user_request=user_request
    )
    
    return {
        "enable_thinking": True,
        "thinking_budget_tokens": budget,
        "thinking_level": level.value,
        "reasoning": reasoning
    }