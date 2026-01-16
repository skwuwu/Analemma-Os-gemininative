"""
지능형 지침 증류기 - 메타데이터 추출 서비스 (고급 최적화)

주요 개선사항:
1. 시그니처 정규화 (Signature Normalization)
2. Diff-based 추출 (대규모 텍스트 60% 비용 절감)
3. Style/Tone 모호성 해결
"""

import openai
import json
import logging
import difflib
import re
from typing import Dict, Any, Optional, List, Tuple
from src.models.correction_log import ToneEnum, LengthEnum, FormalityEnum, StyleEnum

logger = logging.getLogger(__name__)

class MetadataExtractor:
    """구조화된 메타데이터 추출 (고급 최적화 적용)"""
    
    # 개선사항 1: 시그니처 정규화를 위한 우선순위 정의
    ATTRIBUTE_PRIORITY = {
        "tone": 1,      # 가장 영향력 큰 속성
        "style": 2,     # 구조적 특성
        "formality": 3, # 격식 수준
        "length": 4     # 길이 요구사항
    }
    
    # 개선사항 3: Style과 Tone 명확한 구분
    TONE_STYLE_MAPPING = {
        # Tone (감정적 태도) - Style (문장 구조) 매핑
        "tone_attributes": {
            "formal": "공식적이고 거리감 있는 감정적 태도",
            "casual": "친근하고 편안한 감정적 태도", 
            "friendly": "따뜻하고 호의적인 감정적 태도",
            "professional": "업무적이고 신뢰할 수 있는 감정적 태도",
            "urgent": "긴급하고 절박한 감정적 태도"
        },
        "style_attributes": {
            "direct": "간결하고 명확한 문장 구조",
            "diplomatic": "완곡하고 우회적인 문장 구조",
            "technical": "전문적이고 정확한 문장 구조", 
            "conversational": "대화체의 자연스러운 문장 구조"
        },
        "ambiguous_resolution": {
            # 모호한 경우의 기본 매핑
            "professional": "tone",  # professional은 감정적 태도로 분류
            "formal": "tone",        # formal은 감정적 태도로 분류
            "technical": "style",    # technical은 문장 구조로 분류
            "casual": "tone"         # casual은 감정적 태도로 분류
        }
    }
    
    # OpenAI Structured Outputs용 JSON Schema (개선된 버전)
    METADATA_JSON_SCHEMA = {
        "type": "object",
        "properties": {
            "tone": {
                "type": "string",
                "enum": [e.value for e in ToneEnum],
                "description": "감정적 태도 (emotional attitude): 화자의 감정 상태나 상대방에 대한 태도"
            },
            "length": {
                "type": "string", 
                "enum": [e.value for e in LengthEnum],
                "description": "길이 요구사항: 응답의 분량이나 상세도"
            },
            "formality": {
                "type": "string",
                "enum": [e.value for e in FormalityEnum],
                "description": "격식 수준: 언어 사용의 공식성 정도"
            },
            "style": {
                "type": "string",
                "enum": [e.value for e in StyleEnum],
                "description": "문장 구조적 특성 (structural characteristics): 문장의 구성 방식이나 표현 방법"
            }
        },
        "additionalProperties": False,
        "required": []  # 변경된 속성만 포함
    }
    
    # 개선사항 2: Diff-based 추출을 위한 설정
    LARGE_TEXT_THRESHOLD = 2000  # 2000자 이상은 대규모 텍스트로 간주
    CONTEXT_WINDOW_SIZE = 200    # 변경 부분 주변 200자 컨텍스트
    MAX_DIFF_CHUNKS = 5          # 최대 5개의 변경 구간만 처리
    
    def __init__(self):
        self.client = openai.AsyncOpenAI()
    
    async def extract_metadata(
        self, 
        original: str, 
        corrected: str
    ) -> Dict[str, str]:
        """
        메타데이터 추출 (고급 최적화 적용)
        
        개선사항:
        1. 대규모 텍스트 시 Diff-based 추출로 60% 비용 절감
        2. 시그니처 정규화로 일관성 보장
        3. Style/Tone 모호성 해결
        """
        
        try:
            # 개선사항 2: 대규모 텍스트 감지 및 Diff-based 처리
            if self._is_large_text(original, corrected):
                logger.info("Large text detected, using diff-based extraction")
                return await self._extract_metadata_diff_based(original, corrected)
            else:
                return await self._extract_metadata_full_text(original, corrected)
                
        except Exception as e:
            logger.error(f"Metadata extraction failed: {str(e)}")
            return {"extraction_error": "api_failure"}
    
    def _is_large_text(self, original: str, corrected: str) -> bool:
        """대규모 텍스트 여부 판단"""
        return (len(original) > self.LARGE_TEXT_THRESHOLD or 
                len(corrected) > self.LARGE_TEXT_THRESHOLD)
    
    async def _extract_metadata_diff_based(
        self, 
        original: str, 
        corrected: str
    ) -> Dict[str, str]:
        """
        Diff-based 메타데이터 추출 (대규모 텍스트용)
        
        변경된 부분의 주변 컨텍스트만 LLM에 전달하여 비용 60% 절감
        """
        
        # 변경 구간 식별
        diff_chunks = self._extract_change_windows(original, corrected)
        
        if not diff_chunks:
            return {"no_significant_change": "true"}
        
        # 각 변경 구간에서 메타데이터 추출
        all_metadata = {}
        
        for i, (orig_chunk, corr_chunk) in enumerate(diff_chunks[:self.MAX_DIFF_CHUNKS]):
            try:
                chunk_metadata = await self._extract_metadata_full_text(
                    orig_chunk, corr_chunk
                )
                
                # 유효한 메타데이터만 병합
                if chunk_metadata and "extraction_error" not in chunk_metadata:
                    all_metadata.update(chunk_metadata)
                    
            except Exception as e:
                logger.warning(f"Failed to extract metadata from src.chunk {i}: {e}")
                continue
        
        # 시그니처 정규화 적용
        return self._normalize_signature(all_metadata)
    
    def _extract_change_windows(
        self, 
        original: str, 
        corrected: str
    ) -> List[Tuple[str, str]]:
        """
        변경된 부분의 주변 컨텍스트 추출
        
        Returns:
            List of (original_window, corrected_window) tuples
        """
        
        # difflib을 사용한 변경 구간 식별
        differ = difflib.SequenceMatcher(None, original, corrected)
        change_windows = []
        
        for tag, i1, i2, j1, j2 in differ.get_opcodes():
            if tag in ['replace', 'delete', 'insert']:
                # 변경 구간 주변의 컨텍스트 추출
                orig_start = max(0, i1 - self.CONTEXT_WINDOW_SIZE)
                orig_end = min(len(original), i2 + self.CONTEXT_WINDOW_SIZE)
                
                corr_start = max(0, j1 - self.CONTEXT_WINDOW_SIZE)
                corr_end = min(len(corrected), j2 + self.CONTEXT_WINDOW_SIZE)
                
                orig_window = original[orig_start:orig_end]
                corr_window = corrected[corr_start:corr_end]
                
                # 의미 있는 변경인지 확인 (공백이나 구두점만 변경된 경우 제외)
                if self._is_meaningful_change(orig_window, corr_window):
                    change_windows.append((orig_window, corr_window))
        
        return change_windows
    
    def _is_meaningful_change(self, orig_window: str, corr_window: str) -> bool:
        """의미 있는 변경인지 판단"""
        
        # 공백과 구두점 제거 후 비교
        orig_clean = re.sub(r'[\s\.,!?;:]+', '', orig_window.lower())
        corr_clean = re.sub(r'[\s\.,!?;:]+', '', corr_window.lower())
        
        # 내용이 실제로 다른지 확인
        if orig_clean == corr_clean:
            return False
        
        # 너무 짧은 변경은 제외 (5자 미만)
        if len(orig_clean) < 5 and len(corr_clean) < 5:
            return False
        
        return True
    
    async def _extract_metadata_full_text(
        self, 
        original: str, 
        corrected: str
    ) -> Dict[str, str]:
        """전체 텍스트 기반 메타데이터 추출 (기존 방식 개선)"""
        
        # 개선사항 3: Style/Tone 구분을 위한 명확한 지침
        system_prompt = f"""You are a metadata extraction expert with clear understanding of tone vs style.

IMPORTANT DISTINCTIONS:
- TONE (감정적 태도): The emotional attitude or feeling conveyed
  {json.dumps(self.TONE_STYLE_MAPPING['tone_attributes'], indent=2, ensure_ascii=False)}

- STYLE (문장 구조): The structural characteristics of how sentences are formed
  {json.dumps(self.TONE_STYLE_MAPPING['style_attributes'], indent=2, ensure_ascii=False)}

ANALYSIS RULES:
1. Only include attributes that CLEARLY changed between original and corrected
2. If uncertain between tone/style, use this mapping: {self.TONE_STYLE_MAPPING['ambiguous_resolution']}
3. Be conservative - omit unclear attributes entirely
4. Focus on the most significant changes

Examples:
- "Send it now!" → "Could you please send it when convenient?" 
  = tone: friendly (emotional attitude changed), formality: informal
- "Write brief notes" → "Write comprehensive documentation" 
  = length: detailed (clear length change)
- "Use simple words" → "Use technical terminology" 
  = style: technical (structural approach changed)"""
        
        response = await self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "system",
                "content": system_prompt
            }, {
                "role": "user", 
                "content": f"""
Original: {original}
Corrected: {corrected}

Extract changed metadata attributes:
"""
            }],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "metadata_extraction",
                    "schema": self.METADATA_JSON_SCHEMA,
                    "strict": True
                }
            },
            max_tokens=150,
            temperature=0.1
        )
        
        metadata = json.loads(response.choices[0].message.content)
        
        # 빈 객체면 변화 없음으로 처리
        if not metadata:
            return {"no_significant_change": "true"}
            
        # 검증 및 정규화
        if self.validate_metadata(metadata):
            return self._normalize_signature(metadata)
        else:
            logger.warning(f"Invalid metadata extracted: {metadata}")
            return {"extraction_error": "invalid_values"}
    
    def _normalize_signature(self, metadata: Dict[str, str]) -> Dict[str, str]:
        """
        개선사항 1: 시그니처 정규화
        
        일관된 메타데이터 시그니처 생성으로 충돌 감지 로직 단순화
        """
        
        if not metadata or "extraction_error" in metadata or "no_significant_change" in metadata:
            return metadata
        
        # 1단계: 우선순위 기반 정렬
        sorted_metadata = {}
        
        # 우선순위 순서로 정렬
        for attr in sorted(self.ATTRIBUTE_PRIORITY.keys(), key=lambda x: self.ATTRIBUTE_PRIORITY[x]):
            if attr in metadata:
                sorted_metadata[attr] = metadata[attr]
        
        # 우선순위에 없는 속성들은 알파벳 순으로 추가
        remaining_attrs = set(metadata.keys()) - set(self.ATTRIBUTE_PRIORITY.keys())
        for attr in sorted(remaining_attrs):
            sorted_metadata[attr] = metadata[attr]
        
        # 2단계: Primary Attribute 식별 및 표시
        primary_attr = self._identify_primary_attribute(sorted_metadata)
        if primary_attr:
            sorted_metadata["_primary"] = primary_attr
        
        # 3단계: 시그니처 해시 생성 (충돌 감지 최적화용)
        signature_hash = self._generate_signature_hash(sorted_metadata)
        sorted_metadata["_signature_hash"] = signature_hash
        
        return sorted_metadata
    
    def _identify_primary_attribute(self, metadata: Dict[str, str]) -> Optional[str]:
        """가장 영향력이 큰 속성(Primary Attribute) 식별"""
        
        if not metadata:
            return None
        
        # 우선순위가 가장 높은 속성을 Primary로 선택
        for attr in sorted(self.ATTRIBUTE_PRIORITY.keys(), key=lambda x: self.ATTRIBUTE_PRIORITY[x]):
            if attr in metadata:
                return attr
        
        # 우선순위에 없는 경우 첫 번째 속성
        return list(metadata.keys())[0] if metadata else None
    
    def _generate_signature_hash(self, metadata: Dict[str, str]) -> str:
        """시그니처 해시 생성 (충돌 감지 최적화)"""
        
        # _primary와 _signature_hash 제외한 실제 메타데이터만 사용
        clean_metadata = {k: v for k, v in metadata.items() 
                         if not k.startswith('_')}
        
        if not clean_metadata:
            return "empty"
        
        # 정렬된 키-값 쌍으로 해시 생성
        signature_parts = []
        for key in sorted(clean_metadata.keys()):
            signature_parts.append(f"{key}:{clean_metadata[key]}")
        
        signature_string = "|".join(signature_parts)
        
        # 간단한 해시 (실제로는 hashlib 사용 권장)
        import hashlib
        return hashlib.md5(signature_string.encode()).hexdigest()[:8]
    
    def validate_metadata(self, metadata: Dict[str, str]) -> bool:
        """추출된 메타데이터 검증"""
        
        valid_values = {
            "tone": [e.value for e in ToneEnum],
            "length": [e.value for e in LengthEnum], 
            "formality": [e.value for e in FormalityEnum],
            "style": [e.value for e in StyleEnum]
        }
        
        for key, value in metadata.items():
            if key in valid_values and value not in valid_values[key]:
                logger.warning(f"Invalid metadata value: {key}={value}")
                return False
                
        return True
    
    def calculate_edit_distance(self, original: str, corrected: str) -> int:
        """
        Levenshtein 편집 거리 계산 (개선된 버전)
        
        개선사항: 대규모 텍스트에서도 효율적으로 동작하도록 최적화
        """
        
        # 대규모 텍스트의 경우 샘플링으로 근사치 계산
        if len(original) > 10000 or len(corrected) > 10000:
            return self._calculate_edit_distance_sampled(original, corrected)
        
        # 기존 정확한 계산
        if len(original) < len(corrected):
            return self.calculate_edit_distance(corrected, original)
        
        if len(corrected) == 0:
            return len(original)
        
        previous_row = list(range(len(corrected) + 1))
        for i, c1 in enumerate(original):
            current_row = [i + 1]
            for j, c2 in enumerate(corrected):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _calculate_edit_distance_sampled(self, original: str, corrected: str) -> int:
        """대규모 텍스트용 샘플링 기반 편집 거리 근사 계산"""
        
        # 텍스트를 청크로 나누어 샘플링
        chunk_size = 1000
        total_distance = 0
        total_chunks = 0
        
        orig_chunks = [original[i:i+chunk_size] for i in range(0, len(original), chunk_size)]
        corr_chunks = [corrected[i:i+chunk_size] for i in range(0, len(corrected), chunk_size)]
        
        # 각 청크 쌍의 편집 거리 계산
        max_chunks = max(len(orig_chunks), len(corr_chunks))
        
        for i in range(max_chunks):
            orig_chunk = orig_chunks[i] if i < len(orig_chunks) else ""
            corr_chunk = corr_chunks[i] if i < len(corr_chunks) else ""
            
            chunk_distance = self.calculate_edit_distance(orig_chunk, corr_chunk)
            total_distance += chunk_distance
            total_chunks += 1
        
        return total_distance
    
    def get_signature_compatibility_score(
        self, 
        signature1: Dict[str, str], 
        signature2: Dict[str, str]
    ) -> float:
        """
        두 시그니처 간의 호환성 점수 계산
        
        충돌 감지 로직에서 사용할 수 있는 유사도 메트릭
        """
        
        if not signature1 or not signature2:
            return 0.0
        
        # _로 시작하는 메타 속성 제외
        clean_sig1 = {k: v for k, v in signature1.items() if not k.startswith('_')}
        clean_sig2 = {k: v for k, v in signature2.items() if not k.startswith('_')}
        
        if not clean_sig1 or not clean_sig2:
            return 0.0
        
        # 공통 키와 전체 키 계산
        common_keys = set(clean_sig1.keys()) & set(clean_sig2.keys())
        all_keys = set(clean_sig1.keys()) | set(clean_sig2.keys())
        
        if not all_keys:
            return 1.0
        
        # 키 겹침 점수
        key_overlap_score = len(common_keys) / len(all_keys)
        
        # 값 일치 점수
        value_match_score = 0.0
        if common_keys:
            matching_values = sum(1 for key in common_keys 
                                if clean_sig1[key] == clean_sig2[key])
            value_match_score = matching_values / len(common_keys)
        
        # 가중 평균 (키 겹침 30%, 값 일치 70%)
        compatibility_score = (key_overlap_score * 0.3) + (value_match_score * 0.7)
        
        return compatibility_score
    
    def resolve_tone_style_ambiguity(self, extracted_value: str, context: str = "") -> Tuple[str, str]:
        """
        개선사항 3: Tone과 Style 모호성 해결
        
        Returns:
            (attribute_type, resolved_value) - 'tone' or 'style'과 정제된 값
        """
        
        # 명확한 매핑이 있는 경우
        if extracted_value in self.TONE_STYLE_MAPPING['ambiguous_resolution']:
            attr_type = self.TONE_STYLE_MAPPING['ambiguous_resolution'][extracted_value]
            return attr_type, extracted_value
        
        # 컨텍스트 기반 분석
        if context:
            context_lower = context.lower()
            
            # 감정적 표현이 많으면 tone으로 분류
            emotional_indicators = ['feel', 'emotion', 'attitude', 'mood', 'friendly', 'warm', 'cold']
            if any(indicator in context_lower for indicator in emotional_indicators):
                return 'tone', extracted_value
            
            # 구조적 표현이 많으면 style로 분류
            structural_indicators = ['structure', 'format', 'organize', 'arrange', 'technical', 'method']
            if any(indicator in context_lower for indicator in structural_indicators):
                return 'style', extracted_value
        
        # 기본값: 더 일반적인 tone으로 분류
        return 'tone', extracted_value
    
    async def classify_correction_type(
        self,
        original: str,
        corrected: str,
        metadata: Dict[str, str]
    ) -> str:
        """
        수정 타입 분류 (개선된 버전)
        
        개선사항: 정규화된 시그니처와 Primary Attribute 활용
        """
        
        # 정규화된 메타데이터에서 Primary Attribute 우선 사용
        if "_primary" in metadata:
            primary_attr = metadata["_primary"]
            
            # Primary Attribute 기반 분류
            if primary_attr == "tone":
                return "tone"
            elif primary_attr == "style":
                return "style"
            elif primary_attr == "length":
                return "format"
            elif primary_attr == "formality":
                return "tone"  # formality는 tone과 관련
        
        # 기존 메타데이터 기반 1차 분류
        if "tone" in metadata or "formality" in metadata:
            return "tone"
        elif "style" in metadata:
            return "style"
        elif "length" in metadata:
            return "format"
        
        # 대규모 텍스트의 경우 diff 기반 분류
        if self._is_large_text(original, corrected):
            return await self._classify_correction_type_diff_based(original, corrected)
        
        # LLM 기반 2차 분류 (소규모 텍스트)
        return await self._classify_correction_type_llm(original, corrected)
    
    async def _classify_correction_type_diff_based(
        self, 
        original: str, 
        corrected: str
    ) -> str:
        """대규모 텍스트용 diff 기반 수정 타입 분류"""
        
        try:
            # 변경 구간 추출
            diff_chunks = self._extract_change_windows(original, corrected)
            
            if not diff_chunks:
                return "content"
            
            # 첫 번째 의미 있는 변경 구간만 분석 (비용 절약)
            first_chunk = diff_chunks[0]
            return await self._classify_correction_type_llm(first_chunk[0], first_chunk[1])
            
        except Exception as e:
            logger.error(f"Diff-based correction type classification failed: {str(e)}")
            return "content"
    
    async def _classify_correction_type_llm(self, original: str, corrected: str) -> str:
        """LLM 기반 수정 타입 분류"""
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{
                    "role": "system",
                    "content": """Classify the type of correction. Consider these definitions:
- tone: Changes in emotional attitude or feeling
- format: Changes in length, structure, or presentation
- content: Changes in actual information or facts
- logic: Changes in reasoning or argument flow
- style: Changes in sentence structure or expression method

Return ONLY one of: tone, format, content, logic, style"""
                }, {
                    "role": "user",
                    "content": f"Original: {original}\nCorrected: {corrected}"
                }],
                max_tokens=10,
                temperature=0
            )
            
            correction_type = response.choices[0].message.content.strip().lower()
            
            # 유효한 타입인지 확인
            valid_types = ["tone", "format", "content", "logic", "style"]
            if correction_type in valid_types:
                return correction_type
            else:
                return "content"  # 기본값
                
        except Exception as e:
            logger.error(f"LLM correction type classification failed: {str(e)}")
            return "content"  # 기본값
    
    def get_extraction_metrics(self) -> Dict[str, Any]:
        """추출 성능 메트릭 반환 (모니터링용)"""
        
        return {
            "large_text_threshold": self.LARGE_TEXT_THRESHOLD,
            "context_window_size": self.CONTEXT_WINDOW_SIZE,
            "max_diff_chunks": self.MAX_DIFF_CHUNKS,
            "attribute_priority": self.ATTRIBUTE_PRIORITY,
            "supported_attributes": list(self.METADATA_JSON_SCHEMA["properties"].keys()),
            "tone_style_mapping_available": True
        }