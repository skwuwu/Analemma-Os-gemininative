"""
Kernel Middleware - 자동 슬롭 필터 인터셉터
===========================================

LLM 노드 출력을 자동으로 가로채어 품질 검증을 수행하는 커널 미들웨어.

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                     LLM NODE OUTPUT                              │
    └─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │           KERNEL MIDDLEWARE INTERCEPTOR                          │
    │  ┌─────────────────────────────────────────────────────────┐    │
    │  │  post_process_node()                                    │    │
    │  │  - EntropyAnalyzer.analyze()                            │    │
    │  │  - SlopDetector.detect()                                │    │
    │  └─────────────────────────────────────────────────────────┘    │
    └─────────────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
       ┌─────────┐        ┌──────────┐        ┌──────────┐
       │  PASS   │        │ ESCALATE │        │  REJECT  │
       │ (Clean) │        │ (Stage2) │        │ (Regen)  │
       └─────────┘        └──────────┘        └──────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │           DISTILLATION TRIGGER (정보 증류)                       │
    │  - low_entropy_segments 타격                                     │
    │  - 구간별 재생성 인터럽트                                          │
    └─────────────────────────────────────────────────────────────────┘

Kernel Level: RING_1_QUALITY
"""

import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Union
from enum import Enum
from datetime import datetime
import logging

from .entropy_analyzer import EntropyAnalyzer, EntropyAnalysisResult, ContentDomain
from .slop_detector import SlopDetector, SlopDetectionResult
from .quality_gate import QualityGate, QualityVerdict, QualityGateResult

logger = logging.getLogger(__name__)


class InterceptorAction(Enum):
    """인터셉터 액션 타입"""
    PASS = "PASS"                                        # 통과
    PASS_WITH_BACKGROUND_DISTILL = "PASS_WITH_BACKGROUND_DISTILL"  # 통과 + 백그라운드 증류
    ESCALATE_STAGE2 = "ESCALATE_STAGE2"                  # Stage 2 LLM 검증으로 에스컬레이션
    DISTILL = "DISTILL"                                  # 정보 증류 (부분 재생성)
    REGENERATE = "REGENERATE"                            # 전체 재생성
    REJECT = "REJECT"                                    # 거부


@dataclass
class DistillationTarget:
    """정보 증류 대상 구간"""
    segment_text: str
    segment_index: int
    entropy_score: float
    suggested_action: str
    distillation_prompt: str


@dataclass
class BackgroundDistillationTask:
    """
    비동기 증류 태스크
    
    원본을 먼저 반환하고 백그라운드에서 증류 수행.
    웹소켓으로 실시간 교정 경험 제공.
    """
    task_id: str
    workflow_id: str
    node_id: str
    original_text: str
    distillation_targets: List[DistillationTarget]
    priority: int = 1  # 1 = 낮음, 5 = 높음
    created_at: str = ""
    status: str = "pending"  # pending, processing, completed, failed
    distilled_text: Optional[str] = None
    websocket_channel: Optional[str] = None  # 실시간 전송용 채널
    
    def to_dict(self) -> Dict:
        return {
            'task_id': self.task_id,
            'workflow_id': self.workflow_id,
            'node_id': self.node_id,
            'original_length': len(self.original_text),
            'target_count': len(self.distillation_targets),
            'priority': self.priority,
            'status': self.status,
            'created_at': self.created_at,
            'websocket_channel': self.websocket_channel
        }


@dataclass
class DistillationBudgetConfig:
    """
    증류 예산 가드레일 설정
    
    많은 저엔트로피 구간이 발견되면 구간별 증류보다
    전체 재생성이 더 비용 효율적일 수 있음.
    """
    # 이 수 초과 시 REGENERATE로 업그레이드
    max_distillation_targets: int = 3
    
    # 이 비율 초과 시 REGENERATE (텍스트 대비 증류 대상 비율)
    max_distillation_ratio: float = 0.4  # 40%
    
    # 증류 1회당 추정 비용 ($)
    estimated_cost_per_distillation: float = 0.0005
    
    # 전체 재생성 추정 비용 ($)
    estimated_cost_per_regeneration: float = 0.002
    
    # 백그라운드 증류 활성화 임계값 (이 점수 이상이면 백그라운드)
    background_distill_threshold: float = 0.5
    
    def should_upgrade_to_regenerate(self, target_count: int, total_text_length: int) -> bool:
        """증류 대신 전체 재생성이 더 효율적인지 판단"""
        # 타겟 수 초과
        if target_count > self.max_distillation_targets:
            return True
        
        # 비용 비교
        distill_cost = target_count * self.estimated_cost_per_distillation
        if distill_cost > self.estimated_cost_per_regeneration:
            return True
        
        return False
    
    def should_use_background_distill(self, combined_score: float) -> bool:
        """백그라운드 증류 사용 여부 판단"""
        # 품질이 양호하지만 개선 여지가 있을 때
        return combined_score >= self.background_distill_threshold


@dataclass
class InterceptorResult:
    """인터셉터 처리 결과"""
    action: InterceptorAction
    original_output: str
    processed_output: Optional[str]

    # 분석 결과
    entropy_result: Optional[EntropyAnalysisResult]
    slop_result: Optional[SlopDetectionResult]

    # 정보 증류 대상
    distillation_targets: List[DistillationTarget] = field(default_factory=list)

    # 백그라운드 증류 태스크 (비동기 처리용)
    background_task: Optional[BackgroundDistillationTask] = None

    # 메타데이터
    processing_time_ms: float = 0.0
    node_id: str = ""
    workflow_id: str = ""
    timestamp: str = ""

    # Stage 2 결과 (있는 경우)
    stage2_verdict: Optional[QualityVerdict] = None
    stage2_score: Optional[int] = None

    # 가드레일 정보
    guardrail_upgrade: bool = False  # DISTILL에서 REGENERATE로 업그레이드 되었는지
    guardrail_reason: str = ""

    # 품질 점수 (main.py에서 참조)
    combined_score: float = 0.0  # entropy_score와 slop_score의 가중 평균
    recommendation: str = ""  # 액션에 따른 권장 사항
    
    def to_dict(self) -> Dict:
        result = {
            'action': self.action.value,
            'original_length': len(self.original_output),
            'processed_length': len(self.processed_output) if self.processed_output else 0,
            'entropy': self.entropy_result.to_dict() if self.entropy_result else None,
            'slop': self.slop_result.to_dict() if self.slop_result else None,
            'combined_score': self.combined_score,
            'recommendation': self.recommendation,
            'distillation_targets': [
                {
                    'segment': dt.segment_text[:50] + '...' if len(dt.segment_text) > 50 else dt.segment_text,
                    'entropy': dt.entropy_score,
                    'action': dt.suggested_action
                }
                for dt in self.distillation_targets
            ],
            'metadata': {
                'processing_time_ms': round(self.processing_time_ms, 2),
                'node_id': self.node_id,
                'workflow_id': self.workflow_id,
                'timestamp': self.timestamp
            },
            'stage2': {
                'verdict': self.stage2_verdict.value if self.stage2_verdict else None,
                'score': self.stage2_score
            } if self.stage2_verdict else None,
            'guardrail': {
                'upgraded': self.guardrail_upgrade,
                'reason': self.guardrail_reason
            } if self.guardrail_upgrade else None
        }

        # 백그라운드 태스크 정보
        if self.background_task:
            result['background_distillation'] = self.background_task.to_dict()

        return result


class KernelMiddlewareInterceptor:
    """
    커널 미들웨어 인터셉터
    
    LLM 노드 출력을 자동으로 가로채어 품질 검증 수행.
    사용자 호출 없이 커널이 자동 실행.
    
    Kernel Level: RING_1_QUALITY
    
    Usage (커널 파이프라인 내부):
        interceptor = KernelMiddlewareInterceptor(
            domain=ContentDomain.TECHNICAL_REPORT,
            llm_verifier=my_llm_call_function
        )
        
        # 노드 실행 후 자동 호출
        result = interceptor.post_process_node(
            node_output=llm_response,
            node_id="generate_report",
            workflow_id="WF-12345",
            context=state
        )
        
        if result.action == InterceptorAction.DISTILL:
            # 정보 증류 실행
            for target in result.distillation_targets:
                regenerated = call_llm(target.distillation_prompt)
    """
    
    # 정보 증류 프롬프트 템플릿
    DISTILLATION_PROMPT_TEMPLATE = """다음 구간의 정보 밀도가 낮습니다. 
원본 컨텍스트를 기반으로 더 구체적이고 데이터 기반의 내용으로 다시 작성하세요.

[저품질 구간]
{segment}

[요구사항]
- 상투적 표현 제거 ("In conclusion", "It is important to note" 등)
- 구체적 수치, 데이터, 예시 포함
- 모호한 헤징 표현 제거 ("may or may not", "could potentially" 등)
- 정보 밀도를 높이되 길이는 비슷하게 유지

[개선된 버전]:"""

    def __init__(
        self,
        domain: ContentDomain = ContentDomain.GENERAL_TEXT,
        slop_threshold: float = 0.5,
        enable_distillation: bool = True,
        enable_background_distillation: bool = True,
        enable_stage2: bool = True,
        llm_verifier: Optional[Callable] = None,
        llm_distiller: Optional[Callable] = None,
        auto_regenerate_threshold: float = 0.25,  # 이 점수 이하면 자동 재생성
        distillation_entropy_threshold: float = 2.5,  # 이 엔트로피 이하 구간은 증류 대상
        budget_config: Optional[DistillationBudgetConfig] = None,
        websocket_callback: Optional[Callable] = None  # 비동기 증류 결과 전송용
    ):
        """
        Args:
            domain: 콘텐츠 도메인
            slop_threshold: 슬롭 판정 임계값
            enable_distillation: 정보 증류 활성화
            enable_background_distillation: 백그라운드 증류 활성화
            enable_stage2: Stage 2 LLM 검증 활성화
            llm_verifier: Stage 2 검증용 LLM 함수
            llm_distiller: 정보 증류용 LLM 함수
            auto_regenerate_threshold: 자동 재생성 임계값
            distillation_entropy_threshold: 증류 대상 엔트로피 임계값
            budget_config: 증류 예산 가드레일 설정
            websocket_callback: 비동기 증류 결과 웹소켓 전송 콜백
        """
        self.domain = domain
        self.enable_distillation = enable_distillation
        self.enable_background_distillation = enable_background_distillation
        self.enable_stage2 = enable_stage2
        self.auto_regenerate_threshold = auto_regenerate_threshold
        self.distillation_entropy_threshold = distillation_entropy_threshold
        
        # 증류 예산 가드레일
        self.budget_config = budget_config or DistillationBudgetConfig()
        
        # 웹소켓 콜백
        self.websocket_callback = websocket_callback
        
        # 백그라운드 태스크 큐 (간단한 인메모리 큐)
        self.background_queue: List[BackgroundDistillationTask] = []
        
        # 분석기 초기화
        self.entropy_analyzer = EntropyAnalyzer(domain=domain)
        self.slop_detector = SlopDetector(slop_threshold=slop_threshold)
        self.quality_gate = QualityGate(
            domain=domain,
            slop_threshold=slop_threshold,
            llm_verifier=llm_verifier
        )
        
        # LLM 함수
        self.llm_verifier = llm_verifier
        self.llm_distiller = llm_distiller

    @staticmethod
    def _get_recommendation(action: InterceptorAction) -> str:
        """액션에 따른 권장 사항 반환"""
        recommendations = {
            InterceptorAction.PASS: "Quality check passed. No action required.",
            InterceptorAction.PASS_WITH_BACKGROUND_DISTILL: "Quality acceptable. Background improvement scheduled.",
            InterceptorAction.ESCALATE_STAGE2: "Quality uncertain. Escalate to Stage 2 LLM verification.",
            InterceptorAction.DISTILL: "Low entropy segments detected. Information distillation recommended.",
            InterceptorAction.REGENERATE: "Quality too low. Full regeneration recommended.",
            InterceptorAction.REJECT: "Content rejected due to excessive slop or low quality.",
        }
        return recommendations.get(action, "Unknown action.")

    def post_process_node(
        self,
        node_output: str,
        node_id: str = "",
        workflow_id: str = "",
        context: Optional[Dict] = None
    ) -> InterceptorResult:
        """
        노드 출력 후처리 (커널 파이프라인에서 자동 호출)
        
        Args:
            node_output: LLM 노드의 출력 텍스트
            node_id: 현재 노드 ID
            workflow_id: 워크플로우 ID
            context: 워크플로우 상태 컨텍스트
            
        Returns:
            InterceptorResult: 처리 결과 및 권장 액션
        """
        start_time = time.time()
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # 빈 출력 처리
        if not node_output or len(node_output.strip()) < 10:
            return self._create_pass_result(node_output, node_id, workflow_id, timestamp, 0)
        
        # ============================================================
        # STAGE 1: Local Heuristic Analysis (비용 $0)
        # ============================================================
        entropy_result = self.entropy_analyzer.analyze(node_output)
        slop_result = self.slop_detector.detect(node_output)
        
        # 통합 점수 계산
        entropy_score = min(1.0, entropy_result.normalized_word_entropy / 6.0)
        slop_score = 1.0 - slop_result.slop_score
        combined_score = (0.5 * entropy_score + 0.5 * slop_score)
        
        processing_time = (time.time() - start_time) * 1000
        
        # ============================================================
        # DECISION TREE
        # ============================================================
        
        # Case 1: 품질이 너무 낮음 → 전체 재생성
        if combined_score < self.auto_regenerate_threshold:
            logger.warning(f"[Kernel] Node {node_id}: Quality too low ({combined_score:.3f}), recommending REGENERATE")
            return InterceptorResult(
                action=InterceptorAction.REGENERATE,
                original_output=node_output,
                processed_output=None,
                entropy_result=entropy_result,
                slop_result=slop_result,
                processing_time_ms=processing_time,
                node_id=node_id,
                workflow_id=workflow_id,
                timestamp=timestamp,
                combined_score=combined_score,
                recommendation=self._get_recommendation(InterceptorAction.REGENERATE)
            )
        
        # Case 2: 저엔트로피 구간 존재 → 정보 증류
        if self.enable_distillation and entropy_result.low_entropy_segments:
            distillation_targets = self._create_distillation_targets(
                entropy_result.low_entropy_segments,
                context
            )
            
            if distillation_targets:
                # ============================================================
                # 증류 예산 가드레일: 타겟이 너무 많으면 REGENERATE로 업그레이드
                # ============================================================
                if self.budget_config.should_upgrade_to_regenerate(
                    len(distillation_targets),
                    len(node_output)
                ):
                    logger.info(
                        f"[Kernel] Node {node_id}: Too many distillation targets "
                        f"({len(distillation_targets)}), upgrading to REGENERATE"
                    )
                    return InterceptorResult(
                        action=InterceptorAction.REGENERATE,
                        original_output=node_output,
                        processed_output=None,
                        entropy_result=entropy_result,
                        slop_result=slop_result,
                        distillation_targets=distillation_targets,
                        processing_time_ms=processing_time,
                        node_id=node_id,
                        workflow_id=workflow_id,
                        timestamp=timestamp,
                        guardrail_upgrade=True,
                        guardrail_reason=f"Distillation targets ({len(distillation_targets)}) > max ({self.budget_config.max_distillation_targets})",
                        combined_score=combined_score,
                        recommendation=self._get_recommendation(InterceptorAction.REGENERATE)
                    )
                
                # ============================================================
                # 백그라운드 증류: 품질이 양호하면 원본 반환 + 백그라운드 개선
                # ============================================================
                if (self.enable_background_distillation and 
                    self.budget_config.should_use_background_distill(combined_score)):
                    
                    background_task = self._create_background_task(
                        node_output,
                        distillation_targets,
                        node_id,
                        workflow_id,
                        timestamp
                    )
                    
                    logger.info(
                        f"[Kernel] Node {node_id}: Quality acceptable ({combined_score:.3f}), "
                        f"scheduling background distillation (task: {background_task.task_id})"
                    )
                    
                    return InterceptorResult(
                        action=InterceptorAction.PASS_WITH_BACKGROUND_DISTILL,
                        original_output=node_output,
                        processed_output=node_output,  # 원본 먼저 반환
                        entropy_result=entropy_result,
                        slop_result=slop_result,
                        distillation_targets=distillation_targets,
                        background_task=background_task,
                        processing_time_ms=processing_time,
                        node_id=node_id,
                        workflow_id=workflow_id,
                        timestamp=timestamp,
                        combined_score=combined_score,
                        recommendation=self._get_recommendation(InterceptorAction.PASS_WITH_BACKGROUND_DISTILL)
                    )
                
                # 동기 증류 필요
                logger.info(f"[Kernel] Node {node_id}: Found {len(distillation_targets)} low-entropy segments, triggering DISTILL")
                return InterceptorResult(
                    action=InterceptorAction.DISTILL,
                    original_output=node_output,
                    processed_output=None,
                    entropy_result=entropy_result,
                    slop_result=slop_result,
                    distillation_targets=distillation_targets,
                    processing_time_ms=processing_time,
                    node_id=node_id,
                    workflow_id=workflow_id,
                    timestamp=timestamp,
                    combined_score=combined_score,
                    recommendation=self._get_recommendation(InterceptorAction.DISTILL)
                )
        
        # Case 3: 불확실 구간 → Stage 2 에스컬레이션
        if self.enable_stage2 and 0.35 <= combined_score <= 0.65:
            if self.llm_verifier:
                logger.info(f"[Kernel] Node {node_id}: Uncertain quality ({combined_score:.3f}), escalating to Stage 2")
                
                stage2_result = self._run_stage2_verification(node_output)
                final_action = InterceptorAction.PASS if stage2_result['verdict'] == QualityVerdict.PASS else InterceptorAction.REGENERATE

                return InterceptorResult(
                    action=final_action,
                    original_output=node_output,
                    processed_output=node_output if stage2_result['verdict'] == QualityVerdict.PASS else None,
                    entropy_result=entropy_result,
                    slop_result=slop_result,
                    processing_time_ms=(time.time() - start_time) * 1000,
                    node_id=node_id,
                    workflow_id=workflow_id,
                    timestamp=timestamp,
                    stage2_verdict=stage2_result['verdict'],
                    stage2_score=stage2_result['score'],
                    combined_score=combined_score,
                    recommendation=self._get_recommendation(final_action)
                )
            else:
                # Stage 2 없이 ESCALATE 반환
                return InterceptorResult(
                    action=InterceptorAction.ESCALATE_STAGE2,
                    original_output=node_output,
                    processed_output=None,
                    entropy_result=entropy_result,
                    slop_result=slop_result,
                    processing_time_ms=processing_time,
                    node_id=node_id,
                    workflow_id=workflow_id,
                    timestamp=timestamp,
                    combined_score=combined_score,
                    recommendation=self._get_recommendation(InterceptorAction.ESCALATE_STAGE2)
                )
        
        # Case 4: 품질 양호 → PASS
        logger.debug(f"[Kernel] Node {node_id}: Quality check PASSED ({combined_score:.3f})")
        return InterceptorResult(
            action=InterceptorAction.PASS,
            original_output=node_output,
            processed_output=node_output,
            entropy_result=entropy_result,
            slop_result=slop_result,
            processing_time_ms=processing_time,
            node_id=node_id,
            workflow_id=workflow_id,
            timestamp=timestamp,
            combined_score=combined_score,
            recommendation=self._get_recommendation(InterceptorAction.PASS)
        )
    
    def _create_distillation_targets(
        self,
        low_entropy_segments: List[str],
        context: Optional[Dict]
    ) -> List[DistillationTarget]:
        """정보 증류 대상 생성"""
        targets = []
        
        for i, segment in enumerate(low_entropy_segments):
            # 각 구간의 엔트로피 재계산
            words = self.entropy_analyzer._tokenize_words(segment)
            entropy = self.entropy_analyzer._calculate_entropy(words)
            
            # 증류 프롬프트 생성
            distillation_prompt = self.DISTILLATION_PROMPT_TEMPLATE.format(
                segment=segment
            )
            
            targets.append(DistillationTarget(
                segment_text=segment,
                segment_index=i,
                entropy_score=entropy,
                suggested_action="REWRITE_WITH_DATA",
                distillation_prompt=distillation_prompt
            ))
        
        return targets
    
    def _run_stage2_verification(self, text: str) -> Dict:
        """Stage 2 LLM 검증 실행"""
        if not self.llm_verifier:
            return {'verdict': QualityVerdict.UNCERTAIN, 'score': 0}
        
        try:
            # 텍스트 길이 제한 (비용 절감)
            truncated = text[:2000] if len(text) > 2000 else text
            
            system_prompt = """You are a strict quality evaluator. Rate information density 1-10.
Score 7+: APPROVE. Score <7: REJECT. Be harsh - most AI output is slop."""
            
            user_prompt = f"""Rate this text (1-10):
---
{truncated}
---
Reply: APPROVE: [score] or REJECT: [score]"""
            
            response = self.llm_verifier(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model="gemini-1.5-flash-8b",
                max_tokens=50
            )
            
            # 응답 파싱
            import re
            if 'APPROVE' in response.upper():
                match = re.search(r'(\d+)', response)
                score = int(match.group(1)) if match else 7
                return {'verdict': QualityVerdict.PASS, 'score': score}
            else:
                match = re.search(r'(\d+)', response)
                score = int(match.group(1)) if match else 5
                return {'verdict': QualityVerdict.FAIL, 'score': score}
                
        except Exception as e:
            logger.error(f"Stage 2 verification failed: {e}")
            return {'verdict': QualityVerdict.UNCERTAIN, 'score': 0}
    
    def execute_distillation(
        self,
        original_text: str,
        targets: List[DistillationTarget]
    ) -> str:
        """
        정보 증류 실행 (저엔트로피 구간 재생성)
        
        Args:
            original_text: 원본 텍스트
            targets: 증류 대상 목록
            
        Returns:
            개선된 텍스트
        """
        if not self.llm_distiller:
            logger.warning("No LLM distiller configured, returning original text")
            return original_text
        
        result_text = original_text
        
        for target in targets:
            try:
                # 증류 실행
                improved_segment = self.llm_distiller(
                    system_prompt="You are a technical writer. Improve information density.",
                    user_prompt=target.distillation_prompt,
                    model="gemini-1.5-flash",
                    max_tokens=200
                )
                
                # 원본 구간을 개선된 버전으로 교체
                if target.segment_text in result_text:
                    result_text = result_text.replace(
                        target.segment_text,
                        improved_segment.strip(),
                        1  # 첫 번째 매칭만 교체
                    )
                    
            except Exception as e:
                logger.error(f"Distillation failed for segment {target.segment_index}: {e}")
        
        return result_text
    
    def _create_pass_result(
        self,
        output: str,
        node_id: str,
        workflow_id: str,
        timestamp: str,
        processing_time: float
    ) -> InterceptorResult:
        """PASS 결과 생성 헬퍼"""
        return InterceptorResult(
            action=InterceptorAction.PASS,
            original_output=output,
            processed_output=output,
            entropy_result=None,
            slop_result=None,
            processing_time_ms=processing_time,
            node_id=node_id,
            workflow_id=workflow_id,
            timestamp=timestamp,
            combined_score=1.0,  # 빈 출력 또는 짧은 출력은 기본 통과
            recommendation=self._get_recommendation(InterceptorAction.PASS)
        )
    
    def _create_background_task(
        self,
        original_text: str,
        targets: List[DistillationTarget],
        node_id: str,
        workflow_id: str,
        timestamp: str
    ) -> BackgroundDistillationTask:
        """백그라운드 증류 태스크 생성"""
        import uuid
        
        task = BackgroundDistillationTask(
            task_id=f"distill-{uuid.uuid4().hex[:8]}",
            workflow_id=workflow_id,
            node_id=node_id,
            original_text=original_text,
            distillation_targets=targets,
            priority=len(targets),  # 타겟이 많을수록 우선순위 높음
            created_at=timestamp,
            status="pending",
            websocket_channel=f"ws://{workflow_id}/{node_id}/distillation"
        )
        
        # 태스크 큐에 추가
        self.background_queue.append(task)
        
        return task
    
    async def process_background_queue(self):
        """
        백그라운드 증류 큐 처리 (비동기 워커)
        
        Usage:
            import asyncio
            asyncio.create_task(interceptor.process_background_queue())
        """
        while True:
            if not self.background_queue:
                await asyncio.sleep(0.5)
                continue
            
            # 우선순위 정렬 (높은 것 먼저)
            self.background_queue.sort(key=lambda t: -t.priority)
            task = self.background_queue.pop(0)
            
            try:
                task.status = "processing"
                logger.info(f"[BackgroundDistill] Processing task {task.task_id}")
                
                # 증류 실행
                distilled_text = self.execute_distillation(
                    task.original_text,
                    task.distillation_targets
                )
                
                task.distilled_text = distilled_text
                task.status = "completed"
                
                # 웹소켓으로 결과 전송
                if self.websocket_callback:
                    await self._send_distillation_result(task)
                
                logger.info(f"[BackgroundDistill] Task {task.task_id} completed")
                
            except Exception as e:
                task.status = "failed"
                logger.error(f"[BackgroundDistill] Task {task.task_id} failed: {e}")
    
    async def _send_distillation_result(self, task: BackgroundDistillationTask):
        """웹소켓으로 증류 결과 전송"""
        if not self.websocket_callback:
            return
        
        try:
            message = {
                'type': 'distillation_complete',
                'task_id': task.task_id,
                'node_id': task.node_id,
                'workflow_id': task.workflow_id,
                'original_length': len(task.original_text),
                'distilled_length': len(task.distilled_text) if task.distilled_text else 0,
                'distilled_text': task.distilled_text,
                'segments_improved': len(task.distillation_targets)
            }
            
            await self.websocket_callback(task.websocket_channel, message)
            
        except Exception as e:
            logger.error(f"Failed to send distillation result via websocket: {e}")
    
    def get_pending_tasks(self) -> List[BackgroundDistillationTask]:
        """대기 중인 백그라운드 태스크 조회"""
        return [t for t in self.background_queue if t.status == "pending"]
    
    def cancel_task(self, task_id: str) -> bool:
        """백그라운드 태스크 취소"""
        for i, task in enumerate(self.background_queue):
            if task.task_id == task_id and task.status == "pending":
                self.background_queue.pop(i)
                return True
        return False


# asyncio import 추가 (파일 상단에 없으면)
try:
    import asyncio
except ImportError:
    asyncio = None


# ============================================================
# 커널 파이프라인 통합 유틸리티
# ============================================================

def create_kernel_interceptor(
    workflow_state: Dict,
    llm_verifier: Optional[Callable] = None,
    llm_distiller: Optional[Callable] = None
) -> KernelMiddlewareInterceptor:
    """
    워크플로우 상태에서 인터셉터 생성
    
    Usage:
        interceptor = create_kernel_interceptor(state, my_llm_function)
    """
    # 워크플로우에서 도메인 추론
    domain_hint = workflow_state.get('content_domain', 'general_text')
    domain_map = {
        'technical': ContentDomain.TECHNICAL_REPORT,
        'technical_report': ContentDomain.TECHNICAL_REPORT,
        'creative': ContentDomain.CREATIVE_WRITING,
        'code': ContentDomain.CODE_DOCUMENTATION,
        'api': ContentDomain.API_RESPONSE,
        'workflow': ContentDomain.WORKFLOW_OUTPUT,
    }
    domain = domain_map.get(domain_hint, ContentDomain.GENERAL_TEXT)
    
    return KernelMiddlewareInterceptor(
        domain=domain,
        llm_verifier=llm_verifier,
        llm_distiller=llm_distiller
    )


def register_node_interceptor(node_handler: Callable) -> Callable:
    """
    노드 핸들러에 인터셉터 자동 등록 데코레이터
    
    Usage:
        @register_node_interceptor
        def my_llm_node_handler(state):
            response = call_llm(...)
            return response
    """
    def wrapper(state: Dict, *args, **kwargs) -> Dict:
        # 노드 실행
        result = node_handler(state, *args, **kwargs)
        
        # LLM 응답 추출
        llm_output = result.get('llm_response') or result.get('response') or ''
        
        if not llm_output or not isinstance(llm_output, str):
            return result
        
        # 인터셉터 생성 및 실행
        interceptor = create_kernel_interceptor(state)
        intercept_result = interceptor.post_process_node(
            node_output=llm_output,
            node_id=state.get('current_node_id', 'unknown'),
            workflow_id=state.get('workflow_id', 'unknown'),
            context=state
        )
        
        # 결과 주입
        result['_kernel_quality_check'] = intercept_result.to_dict()
        result['_kernel_action'] = intercept_result.action.value
        
        # DISTILL 액션인 경우 증류 대상 정보 추가
        if intercept_result.action == InterceptorAction.DISTILL:
            result['_distillation_required'] = True
            result['_distillation_targets'] = [
                {
                    'segment': t.segment_text,
                    'prompt': t.distillation_prompt
                }
                for t in intercept_result.distillation_targets
            ]
        
        return result
    
    return wrapper
