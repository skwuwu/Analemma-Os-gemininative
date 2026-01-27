# -*- coding: utf-8 -*-
"""
Bedrock Client - 통합 LLM 클라이언트

Bedrock/Gemini 모델 호출을 위한 통합 클라이언트입니다.
기존 agentic_designer_handler.py에서 분리되었습니다.

Features:
- Bedrock (Claude) 동기/스트리밍 호출
- Gemini Native 스트리밍 호출
- Mock 모드 지원
- UTF-8 멀티바이트 문자 안전 처리
"""

import json
import codecs
import logging
import os
import time
from typing import Any, Dict, Generator, Iterator, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


# ============================================================================
# 상수 정의
# ============================================================================

# 모델 ID
# Claude 3.5 Sonnet v2 (2024년 10월 출시): anthropic.claude-3-5-sonnet-20241022-v2:0
# Note: Cross-region inference profiles use format us.anthropic.* but require explicit ARN or model access
MODEL_HAIKU = os.getenv("HAIKU_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
MODEL_SONNET = os.getenv("SONNET_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0")
MODEL_GEMINI_PRO = os.getenv("GEMINI_PRO_MODEL_ID", "gemini-1.5-pro")
MODEL_GEMINI_FLASH = os.getenv("GEMINI_FLASH_MODEL_ID", "gemini-1.5-flash")

# 스트리밍 UX 설정
STREAMING_UI_DELAY_MS = int(os.getenv("STREAMING_UI_DELAY_MS", "50"))
MOCK_UI_DELAY_MS = int(os.getenv("MOCK_UI_DELAY_MS", "100"))


# ============================================================================
# Mock 헬퍼
# ============================================================================

def is_mock_mode() -> bool:
    """Mock 모드 여부 확인"""
    return os.getenv("MOCK_MODE", "true").strip().lower() in {"true", "1", "yes", "on"}


def get_mock_workflow() -> Dict[str, Any]:
    """테스트용 Mock 워크플로우 생성"""
    return {
        "name": "Mock Workflow",
        "nodes": [
            {"id": "start", "type": "operator", "position": {"x": 150, "y": 50}, "data": {"label": "Start", "blockId": "start"}},
            {"id": "mock_llm", "type": "aiModel", "prompt_content": "이것은 목업 응답입니다.", "temperature": 0.1, "max_tokens": 128, "position": {"x": 150, "y": 150}, "data": {"label": "Mock LLM", "model": "mock"}},
            {"id": "end", "type": "operator", "position": {"x": 150, "y": 250}, "data": {"label": "End", "blockId": "end"}},
        ],
        "edges": [
            {"id": "e-start-mock_llm", "type": "edge", "source": "start", "target": "mock_llm"},
            {"id": "e-mock_llm-end", "type": "edge", "source": "mock_llm", "target": "end"},
        ],
    }


def get_mock_text_response() -> Dict[str, Any]:
    """Mock 텍스트 응답"""
    return {"content": [{"text": "이것은 목업 응답입니다. 연결 테스트가 성공했습니다!"}]}


# ============================================================================
# Bedrock 클라이언트
# ============================================================================

_bedrock_client = None


def get_bedrock_client():
    """Bedrock 클라이언트 lazy loading"""
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
    return _bedrock_client


def invoke_bedrock_model(
    model_id: str,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 1024
) -> Dict[str, Any]:
    """
    Bedrock 모델 동기 호출 (Claude/Gemini)
    
    Args:
        model_id: 모델 ID
        system_prompt: 시스템 프롬프트
        user_prompt: 사용자 프롬프트
        max_tokens: 최대 토큰 수
        
    Returns:
        모델 응답 딕셔너리
    """
    if is_mock_mode():
        return get_mock_text_response()
    
    try:
        is_gemini = "gemini" in model_id.lower()
        
        if is_gemini:
            payload = {
                "text_generation_config": {"max_output_tokens": max_tokens},
                "contents": [{"role": "user", "parts": [{"text": user_prompt or " "}]}],
            }
            if system_prompt:
                payload["system_instruction"] = {"parts": [{"text": system_prompt}]}
        else:
            payload = {
                "max_tokens": max_tokens,
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": user_prompt or " "}],
            }
            if system_prompt:
                payload["system"] = system_prompt
        
        client = get_bedrock_client()
        response = client.invoke_model(body=json.dumps(payload), modelId=model_id)
        return json.loads(response.get("body").read())
        
    except ClientError as e:
        logger.exception(f"Bedrock invoke_model failed: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Bedrock response JSON: {e}")
        raise ValueError(f"Invalid JSON response from Bedrock: {e}")


def invoke_bedrock_stream(
    system_prompt: str,
    user_request: str,
    model_id: str = None,
    chunk_size: int = 512
) -> Generator[str, None, None]:
    """
    통합 LLM 스트리밍 호출 (Gemini 우선, Claude 폴백)
    
    Args:
        system_prompt: 시스템 프롬프트
        user_request: 사용자 요청
        model_id: 모델 ID (gemini-*, claude-*)
        chunk_size: 청크 크기
        
    Yields:
        JSONL 형식의 응답 청크
        
    Technical Notes:
        - Gemini Native API를 우선 호출
        - Gemini 실패 시 Bedrock Claude로 자동 폴백
        - Incremental UTF-8 decoder로 멀티바이트 문자(한글 등) 깨짐 방지
        - UI delay로 렌더링 가독성 확보
    """
    model_to_use = model_id or MODEL_GEMINI_FLASH  # Gemini를 기본값으로 변경
    
    # Mock 모드 처리
    if is_mock_mode():
        logger.info("MOCK_MODE: Streaming synthetic response")
        mock_wf = get_mock_workflow()
        ui_delay = MOCK_UI_DELAY_MS / 1000.0
        
        for node in mock_wf.get("nodes", []):
            yield json.dumps({"type": "node", "data": node}) + "\n"
            if ui_delay > 0:
                time.sleep(ui_delay)
        
        for edge in mock_wf.get("edges", []):
            yield json.dumps({"type": "edge", "data": edge}) + "\n"
            if ui_delay > 0:
                time.sleep(ui_delay)
        
        yield json.dumps({"type": "status", "data": "done"}) + "\n"
        return
    
    # Note: Gemini 모델은 상위 레이어(codesign_assistant)에서 직접 gemini_service 호출
    # bedrock_client는 Claude fallback 전용
    if "gemini" in model_to_use.lower():
        logger.warning(f"Gemini model requested in bedrock_client (unexpected). Falling back to Claude.")
        model_to_use = MODEL_SONNET
    
    # Claude (Bedrock) 호출
    logger.info(f"Using Bedrock Claude model: {model_to_use}")
    client = get_bedrock_client()
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": int(os.getenv("STREAM_MAX_TOKENS", "4096")),
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_request}],
    }
    
    try:
        response = client.invoke_model_with_response_stream(
            modelId=model_to_use,
            body=json.dumps(payload)
        )
    except Exception as e:
        logger.exception(f"invoke_model_with_response_stream failed: {e}")
        # Fallback to synchronous
        resp = invoke_bedrock_model(model_to_use, system_prompt, user_request, max_tokens=4096)
        out_text = json.dumps(resp, ensure_ascii=False)
        for i in range(0, len(out_text), chunk_size):
            yield out_text[i:i + chunk_size]
        return
    
    stream = response.get("body")
    if not stream:
        logger.error("No streaming body from Bedrock")
        return
    
    # UTF-8 디코더 (멀티바이트 문자 안전 처리)
    decoder = codecs.getincrementaldecoder("utf-8")()
    buffer = ""
    ui_delay = STREAMING_UI_DELAY_MS / 1000.0
    
    for event in stream:
        try:
            chunk = event.get("chunk")
            if not chunk:
                continue
            chunk_bytes = chunk.get("bytes")
            if not chunk_bytes:
                continue
            text = decoder.decode(chunk_bytes)
        except Exception as e:
            logger.debug(f"Skipping non-decodable chunk: {e}")
            continue
        
        buffer += text
        
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            if not line.strip():
                continue
            
            try:
                json.loads(line)
                yield line + "\n"
                if ui_delay > 0:
                    time.sleep(ui_delay)
            except json.JSONDecodeError:
                logger.debug(f"Discarding non-JSON chunk: {line}")
    
    # 남은 버퍼 플러시
    try:
        tail = decoder.decode(b"", final=True)
        buffer += tail
    except Exception:
        pass
    
    if buffer.strip():
        try:
            json.loads(buffer)
            yield buffer + "\n"
        except json.JSONDecodeError:
            logger.debug(f"Discarding final non-JSON buffer: {buffer}")


def extract_text_from_response(response: Dict[str, Any]) -> Optional[str]:
    """Bedrock 응답에서 텍스트 추출"""
    if isinstance(response, dict) and "content" in response:
        blocks = response.get("content", [])
        if blocks and isinstance(blocks[0], dict):
            return blocks[0].get("text")
    return None
