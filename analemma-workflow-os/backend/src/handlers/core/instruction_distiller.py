# -*- coding: utf-8 -*-
"""
Instruction Distiller Lambda

HITL 단계에서 사용자가 수정한 결과물을 분석하여 
암묵적 지침을 추출하는 비동기 증류 파이프라인입니다.

트리거: HITL 승인 완료 이벤트 (EventBridge 또는 SNS)
프로세스:
  1. S3에서 original_output과 user_corrected_output 로드
  2. LLM(Haiku)이 두 텍스트의 차이점(diff) 분석
  3. 추출된 지침을 DistilledInstructions 테이블에 저장
  4. 다음 실행부터 해당 지침 자동 반영
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
DISTILLED_INSTRUCTIONS_TABLE = os.environ.get("DISTILLED_INSTRUCTIONS_TABLE", "DistilledInstructions")
S3_BUCKET = os.environ.get("WORKFLOW_STATE_BUCKET", "")
BEDROCK_REGION = os.environ.get("AWS_REGION", "us-east-1")
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY", "")

# Fallback Model (Bedrock Haiku - 레거시 호환)
HAIKU_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

# Gemini Native 설정
GEMINI_FLASH_MODEL = "gemini-1.5-flash"  # 비용 효율적
GEMINI_2_FLASH_MODEL = "gemini-2.0-flash"  # 고성능
USE_GEMINI_NATIVE = os.environ.get("USE_GEMINI_NATIVE", "true").lower() == "true"

# 지침 가중치 설정
DEFAULT_INSTRUCTION_WEIGHT = Decimal("1.0")
MIN_INSTRUCTION_WEIGHT = Decimal("0.1")
BASE_WEIGHT_DECAY = Decimal("0.3")  # 기본 재수정 시 가중치 감소량 (동적 조절됨)
MAX_INSTRUCTIONS_PER_NODE = 10  # 노드당 최대 지침 수
COMPRESSED_INSTRUCTION_COUNT = 3  # 압축 후 최대 지침 수
FEW_SHOT_EXAMPLE_MAX_LENGTH = 500  # Few-shot 예시 최대 길이

# Context Caching 설정
CONTEXT_CACHE_TTL_SECONDS = 3600  # 1시간
CONTEXT_CACHE_MIN_TOKENS = 32000  # 최소 토큰 수 (캐싱 조건)
ENABLE_CONTEXT_CACHING = os.environ.get("ENABLE_CONTEXT_CACHING", "true").lower() == "true"

# 지침 구조 (레거시 Python dict)
INSTRUCTION_SCHEMA_LEGACY = {
    "text": str,  # 지침 텍스트
    "weight": Decimal,  # 가중치 (1.0 ~ 0.1)
    "created_at": str,  # 생성 시간
    "last_used": str,  # 마지막 사용 시간
    "usage_count": int,  # 사용 횟수
    "is_active": bool  # 활성화 여부
}

# ═══════════════════════════════════════════════════════════════════════════════
# Gemini Structured Output JSON Schema (Response Schema)
# ═══════════════════════════════════════════════════════════════════════════════
GEMINI_DISTILLATION_SCHEMA = {
    "type": "object",
    "properties": {
        "instructions": {
            "type": "array",
            "description": "추출된 지침 목록 (구체적이고 추상화된 규칙)",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "지침 텍스트"},
                    "category": {
                        "type": "string",
                        "enum": ["style", "content", "format", "tone", "prohibition"],
                        "description": "지침 유형"
                    },
                    "confidence": {
                        "type": "number",
                        "description": "추출 신뢰도 (0.0~1.0)"
                    }
                },
                "required": ["text", "category"]
            }
        },
        "semantic_diff_score": {
            "type": "number",
            "description": "원본과 수정본 간의 의미적 차이 점수 (0.0=동일, 1.0=완전히 다름)"
        },
        "reasoning": {
            "type": "string",
            "description": "수정 패턴을 분석한 이유 및 근거"
        },
        "is_typo_only": {
            "type": "boolean",
            "description": "단순 오타 수정만 있는 경우 true"
        }
    },
    "required": ["instructions", "semantic_diff_score", "reasoning", "is_typo_only"]
}

# 지침 충돌 검사용 스키마
GEMINI_CONFLICT_CHECK_SCHEMA = {
    "type": "object",
    "properties": {
        "has_conflicts": {"type": "boolean"},
        "conflicts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "instruction_a": {"type": "string"},
                    "instruction_b": {"type": "string"},
                    "conflict_type": {
                        "type": "string",
                        "enum": ["contradiction", "redundancy", "ambiguity"]
                    },
                    "resolution": {"type": "string"}
                },
                "required": ["instruction_a", "instruction_b", "conflict_type", "resolution"]
            }
        },
        "resolved_instructions": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["has_conflicts", "resolved_instructions"]
}

# 지능형 지침 압축(Compression)용 스키마
GEMINI_COMPRESSION_SCHEMA = {
    "type": "object",
    "properties": {
        "compressed_instructions": {
            "type": "array",
            "description": "압축된 핵심 지침 (3개 이내)",
            "items": {"type": "string"},
            "maxItems": 3
        },
        "preserved_meaning": {
            "type": "boolean",
            "description": "원본 지침들의 핵심 의미가 보존되었는지"
        },
        "compression_ratio": {
            "type": "number",
            "description": "압축 비율 (0.0~1.0, 낮을수록 더 많이 압축)"
        }
    },
    "required": ["compressed_instructions", "preserved_meaning"]
}

# Few-shot 예시 추출용 스키마
GEMINI_FEWSHOT_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "is_good_example": {
            "type": "boolean",
            "description": "이 수정 사례가 Few-shot 예시로 적합한지"
        },
        "example_quality_score": {
            "type": "number",
            "description": "예시 품질 점수 (0.0~1.0)"
        },
        "condensed_original": {
            "type": "string",
            "description": "압축된 원본 출력 (최대 200자)"
        },
        "condensed_corrected": {
            "type": "string",
            "description": "압축된 수정본 (최대 200자)"
        },
        "key_difference": {
            "type": "string",
            "description": "핵심 차이점 요약"
        }
    },
    "required": ["is_good_example", "example_quality_score"]
}

# AWS 클라이언트
dynamodb = boto3.resource("dynamodb")
instructions_table = dynamodb.Table(DISTILLED_INSTRUCTIONS_TABLE)
s3_client = boto3.client("s3")

# Bedrock 클라이언트 (Fallback)
bedrock_config = Config(
    retries={"max_attempts": 2, "mode": "standard"},
    read_timeout=30,
    connect_timeout=5,
)
bedrock_client = boto3.client(
    "bedrock-runtime",
    region_name=BEDROCK_REGION,
    config=bedrock_config
)

# Gemini 클라이언트 (Lazy Initialization)
_gemini_client = None

# Context Cache 저장소 (workflow_id#node_id -> cache_name)
_context_cache_registry: Dict[str, Dict[str, Any]] = {}


def _get_gemini_client():
    """Gemini 클라이언트 싱글톤"""
    global _gemini_client
    if _gemini_client is None and USE_GEMINI_NATIVE:
        try:
            import google.generativeai as genai
            api_key = GEMINI_API_KEY or _get_gemini_api_key_from_secrets()
            if api_key:
                genai.configure(api_key=api_key)
                _gemini_client = genai
                logger.info("Gemini client initialized for Instruction Distiller")
            else:
                logger.warning("GOOGLE_API_KEY not configured, falling back to Bedrock")
        except ImportError:
            logger.warning("google-generativeai not installed, falling back to Bedrock")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini client: {e}")
    return _gemini_client


def _get_gemini_api_key_from_secrets() -> Optional[str]:
    """AWS Secrets Manager에서 Gemini API 키 조회"""
    try:
        secret_name = os.environ.get("GEMINI_SECRET_NAME", "backend-workflow-dev-gemini_api_key")
        secrets_client = boto3.client("secretsmanager", region_name=BEDROCK_REGION)
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret.get("api_key", "")
    except Exception as e:
        logger.warning(f"Failed to get Gemini API key from Secrets Manager: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# ① Context Caching 기반 '지침 인덱스' 최적화
# ═══════════════════════════════════════════════════════════════════════════════

def _get_or_create_instruction_cache(
    owner_id: str,
    workflow_id: str,
    node_id: str,
    instructions: List[str],
    few_shot_example: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    지침을 Gemini Context Cache에 등록하고 cache_name 반환
    
    동일 워크플로우 반복 실행 시:
    - 매번 지침을 텍스트로 주입하는 대신 캐시된 System Instruction 사용
    - 토큰 비용 절감 + '커널 레벨 규칙'으로 강력한 인지
    
    Returns:
        cache_name: 캐시 참조 이름 (실패 시 None)
    """
    if not ENABLE_CONTEXT_CACHING:
        return None
    
    gemini = _get_gemini_client()
    if not gemini:
        return None
    
    cache_key = f"{owner_id}#{workflow_id}#{node_id}"
    
    # 이미 캐시가 있으면 반환
    if cache_key in _context_cache_registry:
        cached = _context_cache_registry[cache_key]
        # TTL 체크 (만료되었으면 재생성)
        if datetime.now(timezone.utc).timestamp() < cached.get("expires_at", 0):
            return cached.get("cache_name")
    
    try:
        # System Instruction 구성
        system_instruction = _build_cached_system_instruction(instructions, few_shot_example)
        
        # 캐시 생성 (Gemini 1.5+ Context Caching API)
        # Note: 실제 API는 google.generativeai.caching 모듈 사용
        try:
            from google.generativeai import caching
            
            cache = caching.CachedContent.create(
                model=GEMINI_2_FLASH_MODEL,
                display_name=f"instructions_{workflow_id}_{node_id}",
                system_instruction=system_instruction,
                ttl=f"{CONTEXT_CACHE_TTL_SECONDS}s",
            )
            
            cache_name = cache.name
            
            # 레지스트리에 등록
            _context_cache_registry[cache_key] = {
                "cache_name": cache_name,
                "expires_at": datetime.now(timezone.utc).timestamp() + CONTEXT_CACHE_TTL_SECONDS,
                "instruction_hash": hashlib.md5(system_instruction.encode()).hexdigest(),
            }
            
            logger.info(f"Context cache created: {cache_name} for {cache_key}")
            return cache_name
            
        except ImportError:
            logger.debug("Gemini caching module not available")
            return None
        except Exception as e:
            logger.warning(f"Context cache creation failed: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Error in context caching: {e}")
        return None


def _build_cached_system_instruction(
    instructions: List[str],
    few_shot_example: Optional[Dict[str, str]] = None
) -> str:
    """
    캐싱용 System Instruction 구성
    
    지침 + Few-shot 예시를 구조화된 형태로 결합
    """
    parts = []
    
    # 핵심 규칙 헤더
    parts.append("# 사용자 맞춤 규칙 (최우선 준수)")
    parts.append("")
    
    # 지침 목록
    parts.append("<user_rules>")
    for i, inst in enumerate(instructions, 1):
        parts.append(f"  <rule priority=\"{i}\">{inst}</rule>")
    parts.append("</user_rules>")
    
    # Few-shot 예시 (있는 경우)
    if few_shot_example:
        parts.append("")
        parts.append("# 참조 예시 (이 패턴을 따르세요)")
        parts.append("<example>")
        parts.append(f"  <bad>{few_shot_example.get('original', '')}</bad>")
        parts.append(f"  <good>{few_shot_example.get('corrected', '')}</good>")
        if few_shot_example.get('key_difference'):
            parts.append(f"  <explanation>{few_shot_example['key_difference']}</explanation>")
        parts.append("</example>")
    
    parts.append("")
    parts.append("위 규칙들을 모든 응답에서 엄격히 준수하십시오.")
    
    return "\n".join(parts)


def invalidate_instruction_cache(owner_id: str, workflow_id: str, node_id: str) -> None:
    """
    지침이 업데이트되었을 때 캐시 무효화
    
    새로운 지침이 추출되면 기존 캐시를 삭제하여
    다음 호출에서 새 캐시가 생성되도록 합니다.
    """
    cache_key = f"{owner_id}#{workflow_id}#{node_id}"
    
    if cache_key in _context_cache_registry:
        cached = _context_cache_registry.pop(cache_key)
        
        # Gemini 캐시 삭제 시도
        try:
            from google.generativeai import caching
            cache = caching.CachedContent.get(cached.get("cache_name", ""))
            cache.delete()
            logger.info(f"Context cache invalidated: {cache_key}")
        except Exception as e:
            logger.debug(f"Cache deletion skipped: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ② Few-shot 예시 자동 추출
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_few_shot_example(
    original_output: str,
    corrected_output: str,
    node_id: str
) -> Optional[Dict[str, Any]]:
    """
    수정 사례에서 Few-shot 예시 추출
    
    '백 마디 지침보다 하나의 실제 예시'가 모델 이행률을 비약적으로 높입니다.
    Gemini를 사용하여 예시의 품질을 평가하고, 핵심만 압축합니다.
    
    Returns:
        {
            "original": "압축된 원본",
            "corrected": "압축된 수정본",
            "key_difference": "핵심 차이점",
            "quality_score": 0.0~1.0
        }
    """
    gemini = _get_gemini_client()
    if not gemini:
        # Fallback: 단순 잘라내기
        return {
            "original": original_output[:FEW_SHOT_EXAMPLE_MAX_LENGTH],
            "corrected": corrected_output[:FEW_SHOT_EXAMPLE_MAX_LENGTH],
            "key_difference": "",
            "quality_score": 0.5
        }
    
    try:
        prompt = f"""다음 AI 출력과 사용자 수정본을 분석하여 Few-shot 학습 예시로 적합한지 평가하세요.

## 원본 AI 출력:
```
{original_output[:1500]}
```

## 사용자 수정본:
```
{corrected_output[:1500]}
```

## 평가 기준:
1. 수정 사항이 명확하고 일관된가?
2. 예시로 사용했을 때 다른 상황에도 적용 가능한가?
3. 핵심 차이점이 무엇인가?

적합하다면 is_good_example=true, 원본과 수정본을 각각 200자 이내로 압축하세요."""
        
        model = gemini.GenerativeModel(
            model_name=GEMINI_FLASH_MODEL,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": GEMINI_FEWSHOT_EXTRACTION_SCHEMA,
                "max_output_tokens": 500,
                "temperature": 0.2,
            }
        )
        
        response = model.generate_content(prompt)
        result = json.loads(response.text)
        
        if result.get("is_good_example", False):
            return {
                "original": result.get("condensed_original", original_output[:200]),
                "corrected": result.get("condensed_corrected", corrected_output[:200]),
                "key_difference": result.get("key_difference", ""),
                "quality_score": result.get("example_quality_score", 0.5)
            }
        
        logger.info(f"Example not suitable for few-shot: quality={result.get('example_quality_score', 0)}")
        return None
        
    except Exception as e:
        logger.warning(f"Few-shot extraction failed: {e}")
        return None


def _get_best_few_shot_example(pk: str, node_id: str) -> Optional[Dict[str, str]]:
    """
    저장된 Few-shot 예시 중 가장 품질이 높은 것 조회
    """
    try:
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": f"LATEST#{node_id}"}
        )
        
        if "Item" not in response:
            return None
        
        latest_sk = response["Item"].get("latest_instruction_sk")
        if not latest_sk:
            return None
        
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": latest_sk}
        )
        
        if "Item" in response:
            return response["Item"].get("few_shot_example")
            
    except Exception as e:
        logger.warning(f"Failed to get few-shot example: {e}")
    
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ③ 지능형 지침 압축 (Instruction Compression)
# ═══════════════════════════════════════════════════════════════════════════════

def _compress_instructions(
    instructions: List[str],
    target_count: int = COMPRESSED_INSTRUCTION_COUNT
) -> List[str]:
    """
    지침들을 의미를 유지하면서 압축
    
    10개 이상의 지침은 프롬프트를 지저분하게 만듭니다.
    Gemini에게 '가장 짧고 강력한 3개의 문장'으로 압축하도록 지시합니다.
    
    Args:
        instructions: 원본 지침 목록
        target_count: 목표 지침 수 (기본 3개)
    
    Returns:
        압축된 지침 목록
    """
    if len(instructions) <= target_count:
        return instructions
    
    gemini = _get_gemini_client()
    if not gemini:
        # Fallback: 앞에서부터 target_count개만 사용
        return instructions[:target_count]
    
    try:
        prompt = f"""다음 {len(instructions)}개의 지침을 {target_count}개의 핵심 문장으로 압축하세요.

## 원본 지침:
{json.dumps(instructions, ensure_ascii=False, indent=2)}

## 압축 규칙:
1. 모든 지침의 핵심 의미를 보존할 것
2. 상충되는 지침은 더 중요한 것을 우선할 것
3. 유사한 지침은 하나로 통합할 것
4. 각 문장은 명확하고 실행 가능해야 함
5. 결과는 정확히 {target_count}개 이하의 문장

가장 짧고 강력한 {target_count}개의 문장으로 압축하세요."""
        
        model = gemini.GenerativeModel(
            model_name=GEMINI_FLASH_MODEL,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": GEMINI_COMPRESSION_SCHEMA,
                "max_output_tokens": 400,
                "temperature": 0.2,
            }
        )
        
        response = model.generate_content(prompt)
        result = json.loads(response.text)
        
        compressed = result.get("compressed_instructions", [])
        preserved = result.get("preserved_meaning", True)
        ratio = result.get("compression_ratio", 0.3)
        
        if compressed and preserved:
            logger.info(f"Instructions compressed: {len(instructions)} → {len(compressed)} (ratio={ratio:.2f})")
            return compressed[:target_count]
        
        logger.warning("Compression did not preserve meaning, using original")
        return instructions[:target_count]
        
    except Exception as e:
        logger.warning(f"Instruction compression failed: {e}")
        return instructions[:target_count]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    HITL 승인 이벤트를 처리하여 지침을 증류합니다.
    
    이벤트 구조:
    {
        "detail-type": "HITL Approval Completed",
        "detail": {
            "execution_id": "exec-123",
            "node_id": "generate_email",
            "workflow_id": "wf-456",
            "owner_id": "user-789",
            "original_output_ref": "s3://bucket/original.json",
            "corrected_output_ref": "s3://bucket/corrected.json",
            "approval_timestamp": "2026-01-04T12:00:00Z"
        }
    }
    """
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)[:500]}")
        
        detail = event.get("detail", {})
        
        # 필수 필드 검증
        required_fields = ["execution_id", "node_id", "original_output_ref", "corrected_output_ref"]
        for field in required_fields:
            if field not in detail:
                logger.warning(f"Missing required field: {field}")
                return {"statusCode": 400, "body": f"Missing: {field}"}
        
        execution_id = detail["execution_id"]
        node_id = detail["node_id"]
        workflow_id = detail.get("workflow_id", "unknown")
        owner_id = detail.get("owner_id", "unknown")
        
        # S3에서 원본 및 수정본 로드
        original_output = _load_from_s3_ref(detail["original_output_ref"])
        corrected_output = _load_from_s3_ref(detail["corrected_output_ref"])
        
        if not original_output or not corrected_output:
            logger.warning("Failed to load outputs from S3")
            return {"statusCode": 400, "body": "Failed to load outputs"}
        
        # 기존 지침 조회 (통합을 위해)
        existing_instructions = get_active_instructions(owner_id, workflow_id, node_id)
        
        # 차이점 분석 및 지침 추출 (Gemini Native 또는 Fallback)
        distillation_result = _distill_instructions(
            original_output=original_output,
            corrected_output=corrected_output,
            node_id=node_id,
            workflow_id=workflow_id,
            owner_id=owner_id,
            existing_instructions=existing_instructions
        )
        
        distilled_instructions = distillation_result.get("instructions", [])
        semantic_diff_score = distillation_result.get("semantic_diff_score", 0.5)
        is_typo_only = distillation_result.get("is_typo_only", False)
        
        # 단순 오타 수정이면 지침 추출 스킵
        if is_typo_only:
            logger.info(f"Typo-only correction detected for {node_id}, skipping distillation")
            return {"statusCode": 200, "body": "Typo-only correction, no instructions distilled"}
        
        # ② Few-shot 예시 추출 (높은 품질의 수정 사례만)
        few_shot_example = None
        if distilled_instructions and semantic_diff_score >= 0.3:
            few_shot_example = _extract_few_shot_example(
                original_output=original_output,
                corrected_output=corrected_output,
                node_id=node_id
            )
            if few_shot_example:
                logger.info(f"Few-shot example extracted (quality={few_shot_example.get('quality_score', 0):.2f})")
        
        if distilled_instructions:
            # DynamoDB에 저장 (동적 가중치 + Few-shot 예시)
            _save_distilled_instructions(
                workflow_id=workflow_id,
                node_id=node_id,
                owner_id=owner_id,
                instructions=distilled_instructions,
                execution_id=execution_id,
                semantic_diff_score=semantic_diff_score,
                few_shot_example=few_shot_example
            )
            
            # ① Context Cache 무효화 (새 지침 반영을 위해)
            invalidate_instruction_cache(owner_id, workflow_id, node_id)
            
            logger.info(f"Distilled {len(distilled_instructions)} instructions for {node_id} (diff_score={semantic_diff_score:.2f})")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "distilled_count": len(distilled_instructions),
                    "instructions": distilled_instructions,
                    "semantic_diff_score": semantic_diff_score,
                    "reasoning": distillation_result.get("reasoning", ""),
                    "has_few_shot": few_shot_example is not None
                })
            }
        
        return {"statusCode": 200, "body": "No significant differences found"}
        
    except Exception as e:
        logger.error(f"Error in instruction distillation: {e}", exc_info=True)
        return {"statusCode": 500, "body": str(e)}


def _load_from_s3_ref(s3_ref: str) -> Optional[str]:
    """
    S3 참조에서 콘텐츠 로드
    s3://bucket/key 형식 지원
    """
    try:
        if s3_ref.startswith("s3://"):
            parts = s3_ref[5:].split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        else:
            bucket = S3_BUCKET
            key = s3_ref
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        
        # JSON인 경우 파싱 후 문자열로 변환
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                # output 필드가 있으면 추출
                return data.get("output", json.dumps(data, ensure_ascii=False))
            return str(data)
        except json.JSONDecodeError:
            return content
            
    except Exception as e:
        logger.error(f"Failed to load from S3: {s3_ref}, error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 핵심 증류 함수 (Gemini Native + Fallback)
# ═══════════════════════════════════════════════════════════════════════════════

def _distill_instructions(
    original_output: str,
    corrected_output: str,
    node_id: str,
    workflow_id: str,
    owner_id: str,
    existing_instructions: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Gemini Native를 사용한 세만틱 증류 (Semantic Distillation)
    
    사용자의 수정 행위에서 반복 적용 가능한 스타일 가이드와 금지 규칙을 추출합니다.
    Gemini의 Structured Output으로 파싱 에러 0%를 달성합니다.
    
    Args:
        original_output: 원본 AI 출력
        corrected_output: 사용자 수정본
        node_id: 노드 ID
        workflow_id: 워크플로우 ID  
        owner_id: 소유자 ID
        existing_instructions: 기존 활성 지침 (통합용)
    
    Returns:
        {
            "instructions": ["지침1", "지침2", ...],
            "semantic_diff_score": 0.0~1.0,
            "reasoning": "분석 근거",
            "is_typo_only": bool
        }
    """
    gemini = _get_gemini_client()
    
    if gemini:
        result = _distill_with_gemini(original_output, corrected_output, node_id, workflow_id, existing_instructions)
        if result:
            return result
        logger.warning("Gemini distillation failed, falling back to Bedrock")
    
    # Fallback: Bedrock Haiku
    return _distill_with_bedrock(original_output, corrected_output, node_id, workflow_id, existing_instructions)


def _distill_with_gemini(
    original_output: str,
    corrected_output: str,
    node_id: str,
    workflow_id: str,
    existing_instructions: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    """
    Gemini 1.5/2.0 Flash를 사용한 세만틱 증류
    
    특징:
    - Structured Output으로 JSON 파싱 에러 제거
    - 의도 추론 (왜 이렇게 고쳤는가)
    - 오타 vs 의도적 변경 구분
    - 의미적 차이 점수로 동적 가중치 조절
    """
    gemini = _get_gemini_client()
    if not gemini:
        return None
    
    # 세만틱 증류 시스템 지침
    system_instruction = """당신은 사용자의 수정 패턴을 분석하여 AI 응답 개선 지침을 추출하는 전문가입니다.

## 핵심 원칙
1. 사용자의 수정 행위는 AI의 '실수'를 교정하는 행위입니다.
2. 수정된 텍스트에서 **반복적으로 적용 가능한 스타일 가이드**와 **금지 규칙**을 세만틱하게 추출하십시오.
3. 특정 고유 명사(인명, 회사명, 날짜 등)는 추상화된 속성으로 치환하십시오.
   - 예: "홍길동에게 메일 써줘" → "수신자의 이름을 본문에 포함할 것"
   - 예: "2026년 1월 15일" → "구체적인 날짜를 명시할 것"
4. 단순 오타 수정과 의도적 스타일 변경을 명확히 구분하십시오.

## 지침 카테고리
- style: 문체, 어조, 말투 관련
- content: 내용 추가/삭제/수정 관련
- format: 형식, 구조, 레이아웃 관련
- tone: 격식, 친근함, 전문성 정도
- prohibition: 금지 사항, 피해야 할 표현

## 출력 요구사항
- instructions: 최대 10개의 구체적이고 실행 가능한 지침
- semantic_diff_score: 원본과 수정본의 의미적 차이 (0.0=거의 동일, 1.0=완전히 다름)
- reasoning: 왜 이러한 지침을 추출했는지 분석 근거
- is_typo_only: 변경이 오타 수정뿐이면 true"""
    
    # 기존 지침이 있으면 컨텍스트에 포함
    existing_context = ""
    if existing_instructions:
        existing_context = f"\n\n## 기존 활성 지침 (참고용, 중복 피하고 보완할 것):\n" + "\n".join([f"- {inst}" for inst in existing_instructions])
    
    user_prompt = f"""## 분석 대상
- 노드: {node_id}
- 워크플로우: {workflow_id}

## 원본 AI 출력:
```
{original_output[:3000]}
```

## 사용자 수정본:
```
{corrected_output[:3000]}
```
{existing_context}

위 두 텍스트를 비교 분석하여 향후 AI 응답 개선을 위한 지침을 추출해주세요."""
    
    try:
        # Gemini 모델 선택 (2.0 Flash 우선, 없으면 1.5 Flash)
        model_name = GEMINI_2_FLASH_MODEL
        try:
            model = gemini.GenerativeModel(
                model_name=model_name,
                system_instruction=system_instruction,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": GEMINI_DISTILLATION_SCHEMA,
                    "max_output_tokens": 1000,
                    "temperature": 0.3,  # 일관된 추출을 위해 낮은 온도
                }
            )
        except Exception:
            # Fallback to 1.5 Flash
            model_name = GEMINI_FLASH_MODEL
            model = gemini.GenerativeModel(
                model_name=model_name,
                system_instruction=system_instruction,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": GEMINI_DISTILLATION_SCHEMA,
                    "max_output_tokens": 1000,
                    "temperature": 0.3,
                }
            )
        
        response = model.generate_content(user_prompt)
        
        # Structured Output 직접 파싱 (에러 가능성 0%)
        result = json.loads(response.text)
        
        # instructions 배열에서 text만 추출
        instructions = []
        for inst in result.get("instructions", []):
            if isinstance(inst, dict):
                text = inst.get("text", "").strip()
                if text and len(text) > 5:
                    instructions.append(text)
            elif isinstance(inst, str) and len(inst.strip()) > 5:
                instructions.append(inst.strip())
        
        # 기존 지침과 통합 (중복 제거)
        if existing_instructions:
            instructions = _consolidate_instructions_native(existing_instructions, instructions, node_id)
        
        logger.info(f"Gemini distillation successful: {len(instructions)} instructions, model={model_name}")
        
        return {
            "instructions": instructions[:MAX_INSTRUCTIONS_PER_NODE],
            "semantic_diff_score": result.get("semantic_diff_score", 0.5),
            "reasoning": result.get("reasoning", ""),
            "is_typo_only": result.get("is_typo_only", False)
        }
        
    except Exception as e:
        logger.error(f"Gemini distillation error: {e}")
        return None


def _distill_with_bedrock(
    original_output: str,
    corrected_output: str,
    node_id: str,
    workflow_id: str,
    existing_instructions: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Bedrock Haiku Fallback 증류
    
    Gemini 실패 시 기존 Haiku 기반 로직 사용
    """
    # 기존 _extract_new_instructions 호출
    new_instructions = _extract_new_instructions(original_output, corrected_output, node_id, workflow_id)
    
    if not new_instructions:
        return {
            "instructions": [],
            "semantic_diff_score": 0.0,
            "reasoning": "No significant differences found",
            "is_typo_only": True
        }
    
    # 기존 지침과 통합
    if existing_instructions:
        consolidated = _consolidate_instructions(existing_instructions, new_instructions, node_id)
    else:
        consolidated = new_instructions
    
    return {
        "instructions": consolidated[:MAX_INSTRUCTIONS_PER_NODE],
        "semantic_diff_score": 0.5,  # Haiku는 점수 제공 안 함, 기본값
        "reasoning": "Extracted via Bedrock Haiku fallback",
        "is_typo_only": False
    }


def _consolidate_instructions_native(
    existing: List[str],
    new_instructions: List[str],
    node_id: str
) -> List[str]:
    """
    Gemini를 사용한 지능형 지침 통합
    
    단순 중복 제거가 아닌 의미적 통합 수행
    """
    gemini = _get_gemini_client()
    if not gemini or not existing or not new_instructions:
        # Fallback: 단순 합집합
        combined = list(set(existing + new_instructions))
        return combined[:MAX_INSTRUCTIONS_PER_NODE]
    
    try:
        prompt = f"""기존 지침과 신규 지침을 통합하여 최적화된 지침 목록을 만들어주세요.

## 기존 지침:
{json.dumps(existing, ensure_ascii=False)}

## 신규 지침:
{json.dumps(new_instructions, ensure_ascii=False)}

## 규칙:
1. 상충되는 내용은 신규 지침 우선
2. 의미적으로 동일한 지침은 하나로 통합
3. 최대 {MAX_INSTRUCTIONS_PER_NODE}개 이내

통합된 지침을 JSON 문자열 배열로 출력하세요."""
        
        model = gemini.GenerativeModel(
            model_name=GEMINI_FLASH_MODEL,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "max_output_tokens": 500,
                "temperature": 0.2,
            }
        )
        
        response = model.generate_content(prompt)
        result = json.loads(response.text)
        
        if isinstance(result, list):
            return [inst.strip() for inst in result if isinstance(inst, str) and len(inst.strip()) > 5][:MAX_INSTRUCTIONS_PER_NODE]
    
    except Exception as e:
        logger.warning(f"Native consolidation failed: {e}")
    
    # Fallback
    return list(set(existing + new_instructions))[:MAX_INSTRUCTIONS_PER_NODE]


def _check_instruction_conflicts(
    instructions: List[str]
) -> Tuple[bool, List[str]]:
    """
    지침 간 충돌 검사 (할루시네이션 방지)
    
    서로 상충되는 지침이 있는지 검사하고 해결합니다.
    예: "격식체 사용" vs "친근한 말투 사용"
    
    Returns:
        (has_conflicts, resolved_instructions)
    """
    if len(instructions) < 2:
        return False, instructions
    
    gemini = _get_gemini_client()
    if not gemini:
        return False, instructions  # 검사 불가, 그대로 반환
    
    try:
        prompt = f"""다음 지침 목록에서 서로 상충되는 지침이 있는지 검사하고 해결해주세요.

## 지침 목록:
{json.dumps(instructions, ensure_ascii=False)}

충돌이 있으면 has_conflicts=true, 없으면 false로 응답하세요.
충돌이 있는 경우 resolved_instructions에 해결된 지침 목록을 제공하세요."""
        
        model = gemini.GenerativeModel(
            model_name=GEMINI_FLASH_MODEL,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": GEMINI_CONFLICT_CHECK_SCHEMA,
                "max_output_tokens": 600,
                "temperature": 0.1,
            }
        )
        
        response = model.generate_content(prompt)
        result = json.loads(response.text)
        
        has_conflicts = result.get("has_conflicts", False)
        resolved = result.get("resolved_instructions", instructions)
        
        if has_conflicts:
            logger.info(f"Instruction conflicts detected and resolved: {len(result.get('conflicts', []))} conflicts")
        
        return has_conflicts, resolved
        
    except Exception as e:
        logger.warning(f"Conflict check failed: {e}")
        return False, instructions


def _extract_new_instructions(
    original_output: str,
    corrected_output: str,
    node_id: str,
    workflow_id: str
) -> List[str]:
    """
    LLM을 사용하여 원본과 수정본의 차이에서 신규 지침 추출
    """
    # 프롬프트 구성
    prompt = f"""당신은 사용자의 수정 패턴을 분석하여 향후 AI 응답 개선을 위한 지침을 추출하는 전문가입니다.

## 원본 AI 출력:
{original_output[:2000]}

## 사용자 수정본:
{corrected_output[:2000]}

## 분석 대상 노드: {node_id}
## 워크플로우: {workflow_id}

위 두 텍스트를 비교하여 사용자가 원하는 스타일/내용의 차이점을 추출해주세요.
각 지침은 구체적이고 실행 가능해야 합니다.

중요 보안 지침:
- 특정 인명, 주소, 연락처 등 개인정보(PII)는 일반적인 규칙으로 추상화하십시오.
- 예: "홍길동에게 메일 써줘" → "수신자의 이름을 본문에 포함할 것"

다음 형식으로 JSON 배열만 출력하세요 (설명 없이):
["지침1", "지침2", "지침3"]

예시:
["정중하고 격식체를 사용할 것", "구체적인 수치와 날짜를 포함할 것", "인사말을 먼저 작성할 것"]

JSON 배열:"""
    
    try:
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 500,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }
        
        response = bedrock_client.invoke_model(
            body=json.dumps(payload),
            modelId=HAIKU_MODEL_ID
        )
        
        result = json.loads(response["body"].read())
        if "content" in result and result["content"]:
            response_text = result["content"][0].get("text", "").strip()
            
            # JSON 배열 파싱
            try:
                # ```json ... ``` 형식 처리
                if "```" in response_text:
                    json_match = response_text.split("```")[1]
                    if json_match.startswith("json"):
                        json_match = json_match[4:]
                    response_text = json_match.strip()
                
                instructions = json.loads(response_text)
                if isinstance(instructions, list):
                    # 유효한 문자열만 필터링
                    return [
                        inst.strip() for inst in instructions 
                        if isinstance(inst, str) and len(inst.strip()) > 5
                    ][:MAX_INSTRUCTIONS_PER_NODE]
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse instructions JSON: {response_text[:200]}")
                
    except ClientError as e:
        logger.error(f"Bedrock invocation failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in distillation: {e}")
    
    return []


def _consolidate_instructions(
    existing_instructions: List[str],
    new_raw_instructions: List[str],
    node_id: str
) -> List[str]:
    """
    기존 지침과 신규 지침을 분석하여 최적의 통합 지침 생성
    """
    if not new_raw_instructions:
        return existing_instructions
    
    if not existing_instructions:
        return new_raw_instructions
    
    prompt = f"""당신은 AI 가이드라인 최적화 전문가입니다. 
기존 지침과 사용자의 새로운 요구사항을 통합하여 하나의 정교한 지침 목록을 만들어주세요.

## 기존 지침:
{json.dumps(existing_instructions, ensure_ascii=False)}

## 신규 추가 사항:
{json.dumps(new_raw_instructions, ensure_ascii=False)}

## 제약 사항:
1. 서로 상충되는 내용이 있다면 '신규 추가 사항'을 우선하십시오.
2. 특정 인명, 주소, 연락처 등 개인정보(PII)는 일반적인 규칙으로 추상화하십시오.
3. 최대 {MAX_INSTRUCTIONS_PER_NODE}개 이내의 불렛포인트로 작성하십시오.
4. 출력은 반드시 JSON 문자열 배열 형식이어야 합니다.

통합 지침:"""
    
    try:
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 600,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }
        
        response = bedrock_client.invoke_model(
            body=json.dumps(payload),
            modelId=HAIKU_MODEL_ID
        )
        
        result = json.loads(response["body"].read())
        if "content" in result and result["content"]:
            response_text = result["content"][0].get("text", "").strip()
            
            # JSON 배열 파싱
            try:
                # ```json ... ``` 형식 처리
                if "```" in response_text:
                    json_match = response_text.split("```")[1]
                    if json_match.startswith("json"):
                        json_match = json_match[4:]
                    response_text = json_match.strip()
                
                instructions = json.loads(response_text)
                if isinstance(instructions, list):
                    # 유효한 문자열만 필터링
                    return [
                        inst.strip() for inst in instructions 
                        if isinstance(inst, str) and len(inst.strip()) > 5
                    ][:MAX_INSTRUCTIONS_PER_NODE]
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse consolidated instructions JSON: {response_text[:200]}")
                # 파싱 실패 시 기존 + 신규 단순 결합
                combined = existing_instructions + new_raw_instructions
                return list(set(combined))[:MAX_INSTRUCTIONS_PER_NODE]
                
    except ClientError as e:
        logger.error(f"Bedrock invocation failed in consolidation: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in consolidation: {e}")
    
    # 실패 시 기존 지침 유지
    return existing_instructions


def _save_distilled_instructions(
    workflow_id: str,
    node_id: str,
    owner_id: str,
    instructions: List[str],
    execution_id: str,
    semantic_diff_score: float = 0.5,
    few_shot_example: Optional[Dict[str, Any]] = None
) -> None:
    """
    증류된 지침을 DynamoDB에 저장 (동적 가중치 시스템 적용)
    
    Gemini가 판단한 semantic_diff_score에 따라 가중치 감쇠량을 동적 조절합니다:
    - 높은 diff_score (의미 변화 큼) → 강한 감쇠 (기존 지침 신뢰도 하락)
    - 낮은 diff_score (미세 조정) → 약한 감쇠
    
    Args:
        semantic_diff_score: 0.0~1.0 (Gemini가 산출한 의미적 차이 점수)
        few_shot_example: Few-shot 예시 (② 기능)
    """
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d%H%M%S")
    
    pk = f"{owner_id}#{workflow_id}"
    sk = f"{node_id}#{timestamp}"
    
    # 동적 가중치 감쇠량 계산: diff_score가 높을수록 강한 감쇠
    # 0.0 → 0.1 감쇠, 0.5 → 0.3 감쇠, 1.0 → 0.5 감쇠
    dynamic_decay = Decimal(str(round(0.1 + (semantic_diff_score * 0.4), 2)))
    
    try:
        # 기존 지침 조회 및 가중치 decay 적용
        existing_weighted_instructions = _get_weighted_instructions(pk, node_id)
        
        # 기존 Few-shot 예시 조회 (더 좋은 것으로 교체 여부 결정)
        existing_few_shot = _get_best_few_shot_example(pk, node_id)
        
        # 동적 Decay 적용: semantic_diff_score 기반
        decayed_instructions = []
        for inst in existing_weighted_instructions:
            current_weight = Decimal(str(inst["weight"]))
            new_weight = current_weight - dynamic_decay
            if new_weight >= MIN_INSTRUCTION_WEIGHT:
                inst["weight"] = new_weight
                inst["last_decay_reason"] = f"semantic_diff={semantic_diff_score:.2f}"
                decayed_instructions.append(inst)
            else:
                logger.info(f"Instruction dropped below threshold: {inst.get('text', '')[:50]}...")
            # MIN_INSTRUCTION_WEIGHT 이하이면 제거 (is_active = False)
        
        # 통합 지침을 구조화된 형태로 변환
        structured_instructions = []
        for inst_text in instructions:
            structured_instructions.append({
                "text": inst_text,
                "weight": DEFAULT_INSTRUCTION_WEIGHT,
                "created_at": now.isoformat(),
                "last_used": now.isoformat(),
                "usage_count": 0,
                "is_active": True
            })
        
        # 새 지침 저장 (decay된 기존 + 새 통합)
        final_instructions = decayed_instructions + structured_instructions
        
        # ② Few-shot 예시 결정: 품질이 더 높은 것 선택
        final_few_shot = None
        if few_shot_example:
            new_quality = few_shot_example.get("quality_score", 0)
            existing_quality = existing_few_shot.get("quality_score", 0) if existing_few_shot else 0
            
            if new_quality > existing_quality:
                final_few_shot = few_shot_example
                logger.info(f"Few-shot example updated: quality {existing_quality:.2f} → {new_quality:.2f}")
            else:
                final_few_shot = existing_few_shot
        elif existing_few_shot:
            final_few_shot = existing_few_shot
        
        item = {
            "pk": pk,
            "sk": sk,
            "node_id": node_id,
            "workflow_id": workflow_id,
            "owner_id": owner_id,
            "instructions": [inst["text"] for inst in final_instructions],  # 레거시 호환
            "weighted_instructions": _convert_to_dynamodb_format(final_instructions),
            "source_execution_id": execution_id,
            "created_at": now.isoformat(),
            "is_active": True,
            "version": 1,
            "usage_count": 0,
        }
        
        # ② Few-shot 예시 저장
        if final_few_shot:
            item["few_shot_example"] = final_few_shot
        
        instructions_table.put_item(Item=item)
        
        # 해당 노드의 최신 활성 지침 인덱스 업데이트
        _update_latest_instruction_index(pk, node_id, sk)
        
        logger.info(f"Saved distilled instructions with weights: pk={pk}, sk={sk}, count={len(final_instructions)}, has_few_shot={final_few_shot is not None}")
        
    except ClientError as e:
        logger.error(f"DynamoDB error saving instructions: {e}")
        raise


def _merge_and_deduplicate_instructions(
    existing: List[dict], 
    new_instructions: List[str]
) -> List[str]:
    """
    기존 지침과 새 지침을 병합하고 의미적 중복을 제거합니다.
    """
    existing_texts = {inst.get("text", "").strip().lower() for inst in existing}
    
    merged = [inst.get("text", "") for inst in existing]
    
    for new_inst in new_instructions:
        normalized = new_inst.strip().lower()
        # 간단한 중복 검사 (정확 일치)
        if normalized not in existing_texts:
            merged.append(new_inst)
            existing_texts.add(normalized)
    
    return merged


def _deduplicate_weighted_instructions(instructions: List[dict]) -> List[dict]:
    """가중치 기반 지침 중복 제거 (높은 가중치 우선)"""
    seen = {}
    
    for inst in instructions:
        text = inst.get("text", "").strip().lower()
        if text not in seen:
            seen[text] = inst
        elif inst.get("weight", 0) > seen[text].get("weight", 0):
            seen[text] = inst
    
    # 가중치 순으로 정렬
    return sorted(seen.values(), key=lambda x: x.get("weight", 0), reverse=True)


def _convert_to_dynamodb_format(instructions: List[dict]) -> List[dict]:
    """DynamoDB 저장을 위해 Decimal 변환"""
    result = []
    for inst in instructions:
        item = dict(inst)
        if "weight" in item:
            item["weight"] = Decimal(str(item["weight"]))
        result.append(item)
    return result



def _get_weighted_instructions(pk: str, node_id: str) -> List[Dict[str, Any]]:
    """
    특정 노드의 최신 가중치 기반 지침 목록 조회
    (내부 로직용 - Decay 계산 시 사용)
    """
    try:
        # 최신 지침 인덱스 조회
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": f"LATEST#{node_id}"}
        )
        
        if "Item" not in response:
            return []
        
        latest_sk = response["Item"].get("latest_instruction_sk")
        if not latest_sk:
            return []
        
        # 실제 지침 조회
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": latest_sk}
        )
        
        if "Item" in response:
            return response["Item"].get("weighted_instructions", [])
            
    except ClientError as e:
        logger.warning(f"Failed to get weighted instructions: {e}")
    
    return []

def _update_latest_instruction_index(pk: str, node_id: str, latest_sk: str) -> None:
    """
    노드별 최신 지침 인덱스 업데이트
    """
    try:
        instructions_table.put_item(
            Item={
                "pk": pk,
                "sk": f"LATEST#{node_id}",
                "latest_instruction_sk": latest_sk,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    except ClientError as e:
        logger.warning(f"Failed to update latest index: {e}")


def get_active_instructions(
    owner_id: str,
    workflow_id: str,
    node_id: str
) -> List[str]:
    """
    특정 노드에 대한 활성 지침 조회 (외부 호출용)
    가중치가 MIN_INSTRUCTION_WEIGHT 이상인 지침만 반환합니다.
    
    Returns:
        활성화된 지침 목록 (가중치 순)
    """
    pk = f"{owner_id}#{workflow_id}"
    
    try:
        # 최신 지침 인덱스 조회
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": f"LATEST#{node_id}"}
        )
        
        if "Item" not in response:
            return []
        
        latest_sk = response["Item"].get("latest_instruction_sk")
        if not latest_sk:
            return []
        
        # 실제 지침 조회
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": latest_sk}
        )
        
        if "Item" in response and response["Item"].get("is_active"):
            # 가중치 기반 필터링
            weighted = response["Item"].get("weighted_instructions", [])
            
            if weighted:
                # 가중치가 충분한 지침만 선택
                valid_instructions = [
                    inst["text"] for inst in weighted
                    if Decimal(str(inst.get("weight", 0))) >= MIN_INSTRUCTION_WEIGHT
                ]
                
                # 사용 횟수 증가 (비동기)
                _increment_usage_count(pk, latest_sk)
                
                return valid_instructions
            
            # 레거시 형식 호환
            instructions = response["Item"].get("instructions", [])
            _increment_usage_count(pk, latest_sk)
            return instructions
            
    except ClientError as e:
        logger.error(f"Error getting active instructions: {e}")
    
    return []


def _increment_usage_count(pk: str, sk: str) -> None:
    """사용 횟수 증가 (비동기, 실패 무시)"""
    try:
        instructions_table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression="SET usage_count = usage_count + :inc, total_applications = total_applications + :inc",
            ExpressionAttributeValues={":inc": 1}
        )
    except Exception:
        pass


def record_instruction_feedback(
    owner_id: str,
    workflow_id: str,
    node_id: str,
    is_positive: bool,
    instruction_text: Optional[str] = None
) -> None:
    """
    지침 적용 결과에 대한 피드백을 기록하고 가중치를 조정합니다.
    
    - is_positive=True: 지침이 효과적이었음 (가중치 유지 또는 증가)
    - is_positive=False: 사용자가 다시 수정함 (가중치 감소)
    
    Args:
        owner_id: 사용자 ID
        workflow_id: 워크플로우 ID
        node_id: 노드 ID
        is_positive: 긍정적 피드백 여부
        instruction_text: 특정 지침 텍스트 (없으면 전체 적용)
    """
    pk = f"{owner_id}#{workflow_id}"
    
    try:
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": f"LATEST#{node_id}"}
        )
        
        if "Item" not in response:
            return
        
        latest_sk = response["Item"].get("latest_instruction_sk")
        if not latest_sk:
            return
        
        response = instructions_table.get_item(
            Key={"pk": pk, "sk": latest_sk}
        )
        
        if "Item" not in response:
            return
        
        item = response["Item"]
        weighted = item.get("weighted_instructions", [])
        
        if not weighted:
            return
        
        updated = False
        for inst in weighted:
            # 특정 지침만 업데이트하거나, 전체 업데이트
            if instruction_text and inst.get("text") != instruction_text:
                continue
            
            current_weight = Decimal(str(inst.get("weight", DEFAULT_INSTRUCTION_WEIGHT)))
            
            if is_positive:
                # 긍정적 피드백: 가중치 소폭 증가 (최대 1.5)
                new_weight = min(current_weight + Decimal("0.1"), Decimal("1.5"))
            else:
                # 부정적 피드백: 가중치 감소
                inst["rework_count"] = inst.get("rework_count", 0) + 1
                new_weight = max(current_weight - BASE_WEIGHT_DECAY, MIN_INSTRUCTION_WEIGHT)
            
            inst["weight"] = new_weight
            updated = True
        
        if updated:
            # 성공률 업데이트
            total = item.get("total_applications", 1)
            current_success = item.get("success_rate", Decimal("0"))
            
            if is_positive:
                new_success_rate = ((current_success * (total - 1)) + 1) / total
            else:
                new_success_rate = (current_success * (total - 1)) / total
            
            instructions_table.update_item(
                Key={"pk": pk, "sk": latest_sk},
                UpdateExpression="SET weighted_instructions = :wi, success_rate = :sr",
                ExpressionAttributeValues={
                    ":wi": weighted,
                    ":sr": Decimal(str(round(new_success_rate, 2)))
                }
            )
            
            logger.info(f"Updated instruction weights for {node_id}: positive={is_positive}")
            
    except ClientError as e:
        logger.error(f"Error recording instruction feedback: {e}")


def merge_instructions_into_prompt(
    base_prompt: str,
    owner_id: str,
    workflow_id: str,
    node_id: str,
    injection_strategy: str = "system",
    check_conflicts: bool = True,
    compress_instructions: bool = True,
    include_few_shot: bool = True,
    use_context_cache: bool = True
) -> str:
    """
    기본 프롬프트에 증류된 지침을 구조화하여 병합
    
    Gemini Native Extra-Mile 최적화:
    - ① Context Caching: 동일 워크플로우 반복 시 토큰 비용 절감
    - ② Few-shot 예시: 실제 수정 사례를 <example> 태그로 주입
    - ③ 지침 압축: 10개 이상의 지침을 3개로 압축
    - 지침 충돌 검사 (할루시네이션 방지)
    
    Args:
        injection_strategy: "system" (권장), "prefix", "suffix"
        check_conflicts: 지침 간 충돌 검사 여부
        compress_instructions: 지침 압축 활성화 (③ 기능)
        include_few_shot: Few-shot 예시 포함 (② 기능)
        use_context_cache: Context Caching 사용 (① 기능)
    
    Returns:
        강화된 프롬프트 또는 cache_name (Context Caching 사용 시)
    """
    instructions = get_active_instructions(owner_id, workflow_id, node_id)
    
    if not instructions:
        return base_prompt
    
    pk = f"{owner_id}#{workflow_id}"
    
    # 지침 충돌 검사 (선택적)
    if check_conflicts and len(instructions) > 1:
        has_conflicts, resolved = _check_instruction_conflicts(instructions)
        if has_conflicts:
            instructions = resolved
            logger.info(f"Resolved instruction conflicts for {node_id}")
    
    # ③ 지능형 지침 압축 (10개 이상이면 3개로 압축)
    if compress_instructions and len(instructions) > COMPRESSED_INSTRUCTION_COUNT:
        instructions = _compress_instructions(instructions, COMPRESSED_INSTRUCTION_COUNT)
        logger.info(f"Compressed instructions to {len(instructions)} for {node_id}")
    
    # ② Few-shot 예시 조회
    few_shot_example = None
    if include_few_shot:
        few_shot_example = _get_best_few_shot_example(pk, node_id)
    
    # ① Context Caching 시도 (캐시 가능한 경우)
    if use_context_cache and ENABLE_CONTEXT_CACHING:
        cache_name = _get_or_create_instruction_cache(
            owner_id=owner_id,
            workflow_id=workflow_id,
            node_id=node_id,
            instructions=instructions,
            few_shot_example=few_shot_example
        )
        if cache_name:
            # 캐시가 있으면 System Instruction은 캐시에 있으므로 base_prompt만 반환
            # 호출자는 cache_name을 사용하여 Gemini 호출 시 fromCache 파라미터 사용
            logger.info(f"Using cached instructions: {cache_name}")
            # 메타데이터로 캐시 정보 전달 (마커 사용)
            return f"{{{{CONTEXT_CACHE:{cache_name}}}}}\n{base_prompt}"
    
    # 캐시 미사용 시 직접 프롬프트 구성
    # XML 태그로 지침 구조화 (Gemini/Claude 최적화)
    instruction_block = "\n".join([f"  <rule>{inst}</rule>" for inst in instructions])
    
    # ② Few-shot 예시 블록 구성
    few_shot_block = ""
    if few_shot_example:
        few_shot_block = f"""
<example type="correction">
  <original>{few_shot_example.get('original', '')}</original>
  <corrected>{few_shot_example.get('corrected', '')}</corrected>
  <explanation>{few_shot_example.get('key_difference', '위처럼 수정하세요')}</explanation>
</example>
"""
    
    if injection_strategy == "system":
        enhanced_prompt = f"""<user_preferences>
{instruction_block}
</user_preferences>
{few_shot_block}
{base_prompt}

위의 <user_preferences> 내의 규칙을 최우선으로 준수하여 답변하십시오.{' <example>의 패턴을 참고하세요.' if few_shot_example else ''}"""
    
    elif injection_strategy == "prefix":
        enhanced_prompt = f"""<user_preferences>
{instruction_block}
</user_preferences>
{few_shot_block}
---

{base_prompt}"""
    
    else:  # suffix (기존 방식, 레거시 호환)
        enhanced_prompt = f"""{base_prompt}

## 사용자 맞춤 지침 (자동 적용):
다음 지침을 반드시 따라주세요:
{instruction_block}
{few_shot_block}
"""
    
    return enhanced_prompt


def _format_instructions_block(instructions: List[str]) -> str:
    """
    지침을 구조화된 블록으로 포맷팅합니다.
    XML 태그와 번호 매김을 사용하여 LLM의 이행률을 높입니다.
    """
    formatted_lines = []
    formatted_lines.append("당신은 다음의 사용자 맞춤 지침을 반드시 따라야 합니다:")
    formatted_lines.append("")
    
    for i, inst in enumerate(instructions, 1):
        formatted_lines.append(f"{i}. {inst}")
    
    formatted_lines.append("")
    formatted_lines.append("위 지침을 모든 응답에 적용하세요.")
    
    return "\n".join(formatted_lines)
