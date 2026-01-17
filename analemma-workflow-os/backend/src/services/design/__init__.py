# -*- coding: utf-8 -*-
"""
Design Services Package

This package contains AI-powered design assistance services:
- codesign_assistant: Natural language to workflow co-design
- designer_service: Core workflow design logic
- prompts: LLM system prompt definitions
"""

from src.services.design.codesign_assistant import (
    stream_codesign_response,
    explain_workflow,
)

from src.services.design.designer_service import (
    DesignerService,
    get_designer_service,
    analyze_request,
    stream_workflow_jsonl,
    build_text_response,
)

from src.services.design.prompts import (
    SYSTEM_PROMPT,
    PATCH_SYSTEM_PROMPT,
    ANALYSIS_PROMPT,
    RESPONSE_SYSTEM_PROMPT,
    get_gemini_system_prompt,
)

__all__ = [
    # Co-design
    "stream_codesign_response",
    "explain_workflow",
    # Designer Service
    "DesignerService",
    "get_designer_service",
    "analyze_request",
    "stream_workflow_jsonl",
    "build_text_response",
    # Prompts
    "SYSTEM_PROMPT",
    "PATCH_SYSTEM_PROMPT",
    "ANALYSIS_PROMPT",
    "RESPONSE_SYSTEM_PROMPT",
    "get_gemini_system_prompt",
]
