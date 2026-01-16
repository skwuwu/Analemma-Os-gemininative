# -*- coding: utf-8 -*-
"""
Models package for backend application.
Contains Pydantic models for checkpoints, plan briefing, task context and related features.
"""

from src.models.checkpoint import (
    CheckpointStatus,
    ExecutionCheckpoint,
    RollbackRequest,
    BranchInfo,
)

from src.models.plan_briefing import (
    RiskLevel,
    PlanStep,
    DraftResult,
    PlanBriefing,
)

from src.models.task_context import (
    TaskStatus,
    ArtifactType,
    ArtifactPreview,
    AgentThought,
    PendingDecision,
    TaskContext,
    convert_technical_status,
    get_friendly_error_message,
    TECHNICAL_TO_TASK_STATUS,
    ERROR_MESSAGE_MAP,
)

__all__ = [
    # Checkpoint models
    "CheckpointStatus",
    "ExecutionCheckpoint",
    "RollbackRequest",
    "BranchInfo",
    # Plan briefing models
    "RiskLevel",
    "PlanStep",
    "DraftResult",
    "PlanBriefing",
    # Task context models
    "TaskStatus",
    "ArtifactType",
    "ArtifactPreview",
    "AgentThought",
    "PendingDecision",
    "TaskContext",
    "convert_technical_status",
    "get_friendly_error_message",
    "TECHNICAL_TO_TASK_STATUS",
    "ERROR_MESSAGE_MAP",
]
