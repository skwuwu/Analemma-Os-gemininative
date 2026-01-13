"""
GeminiService - Google Gemini 1.5 Pro/Flash Native Integration

Gemini의 특장점을 활용한 워크플로우 설계 서비스:
- 1M+ 토큰 초장기 컨텍스트 지원
- 구조화된 JSON 출력 (Response Schema)
- Loop/Map/Parallel 구조 추론 능력
- 실시간 스트리밍 응답

GitHub Secrets의 GOOGLE_API_KEY를 사용하여 Google AI API 직접 호출
"""

import os
import json
import logging
import time
from typing import Any, Dict, Generator, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


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


def _get_api_key() -> str:
    """Google AI API 키 조회 (Secrets Manager 또는 환경변수)"""
    # 1. 환경변수에서 직접 조회
    api_key = os.getenv("GOOGLE_API_KEY")
    if api_key:
        return api_key
    
    # 2. AWS Secrets Manager에서 조회 (Lambda 환경)
    secret_name = os.getenv("GEMINI_SECRET_NAME", "backend-workflow-dev-gemini_api_key")
    try:
        import boto3
        client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret.get("api_key", "")
    except Exception as e:
        logger.warning(f"Failed to get API key from Secrets Manager: {e}")
        return ""


def get_gemini_client():
    """Gemini 클라이언트 Lazy Initialization"""
    global _gemini_client
    if _gemini_client is None:
        try:
            import google.generativeai as genai
            api_key = _get_api_key()
            if not api_key:
                logger.error("GOOGLE_API_KEY not configured")
                return None
            genai.configure(api_key=api_key)
            _gemini_client = genai
            logger.info("GeminiService: Client initialized successfully")
        except ImportError:
            logger.error("google-generativeai package not installed")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Gemini client: {e}")
            return None
    return _gemini_client


def _is_mock_mode() -> bool:
    """Mock 모드 여부 확인"""
    return os.getenv("MOCK_MODE", "true").strip().lower() in {"true", "1", "yes", "on"}


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
    """
    
    def __init__(self, config: Optional[GeminiConfig] = None):
        self.config = config or GeminiConfig(model=GeminiModel.GEMINI_1_5_PRO)
        self._client = None
    
    @property
    def client(self):
        """Lazy client initialization"""
        if self._client is None:
            self._client = get_gemini_client()
        return self._client
    
    def invoke_model(
        self,
        user_prompt: str,
        system_instruction: Optional[str] = None,
        response_schema: Optional[Dict[str, Any]] = None,
        max_output_tokens: Optional[int] = None,
        temperature: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Gemini 모델 동기 호출
        
        Args:
            user_prompt: 사용자 프롬프트
            system_instruction: 시스템 지침 (Gemini 특화)
            response_schema: 구조화된 출력 스키마
            max_output_tokens: 최대 출력 토큰
            temperature: 샘플링 온도
        
        Returns:
            Gemini 응답 딕셔너리
        """
        if _is_mock_mode():
            logger.info("MOCK_MODE: Returning synthetic Gemini response")
            return _mock_gemini_response(use_tools=response_schema is not None)
        
        client = self.client
        if not client:
            raise RuntimeError("Gemini client not initialized")
        
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
        
        try:
            response = model.generate_content(user_prompt)
            return self._parse_response(response)
        except Exception as e:
            logger.exception(f"Gemini invocation failed: {e}")
            raise
    
    def invoke_model_stream(
        self,
        user_prompt: str,
        system_instruction: Optional[str] = None,
        response_schema: Optional[Dict[str, Any]] = None,
        max_output_tokens: Optional[int] = None,
        temperature: Optional[float] = None
    ) -> Generator[str, None, None]:
        """
        Gemini 모델 스트리밍 호출
        
        Yields:
            JSONL 형식의 응답 청크
        """
        if _is_mock_mode():
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
            return
        
        client = self.client
        if not client:
            yield '{"type": "error", "data": "Gemini client not initialized"}\n'
            return
        
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
                logger.exception(f"Gemini streaming failed: {e}")
                yield f'{{"type": "error", "data": "{error_message}"}}\n'
    
    def invoke_with_full_context(
        self,
        user_prompt: str,
        session_history: List[Dict[str, Any]],
        tool_definitions: List[Dict[str, Any]],
        workflow_state: Optional[Dict[str, Any]] = None,
        system_instruction: Optional[str] = None
    ) -> Generator[str, None, None]:
        """
        초장기 컨텍스트를 활용한 Co-design 전용 호출
        
        Gemini 1.5의 1M+ 토큰 컨텍스트를 활용하여:
        - 전체 세션 히스토리
        - 모든 도구 정의
        - 현재 워크플로우 상태
        를 요약 없이 그대로 전달
        
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
        
        # 시스템 지침에 컨텍스트 정보 포함
        enhanced_instruction = f"""
{system_instruction or ''}

[전체 세션 컨텍스트]
아래는 현재 세션의 전체 히스토리와 사용 가능한 도구들입니다.
과거의 모든 대화와 변경 사항을 참조하여 일관성 있는 응답을 생성하세요.

```json
{json.dumps(full_context, ensure_ascii=False, indent=2)}
```
"""
        
        # 스트리밍 호출
        yield from self.invoke_model_stream(
            user_prompt=user_prompt,
            system_instruction=enhanced_instruction,
            max_output_tokens=8192,
            temperature=0.7
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
