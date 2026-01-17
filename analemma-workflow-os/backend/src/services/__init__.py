# -*- coding: utf-8 -*-
"""
Services package for backend application.
Contains business logic services for checkpoints, plan briefing, and related features.

Note: Services are imported lazily to avoid AWS credential issues during testing.
"""


def get_plan_briefing_service():
    """Lazy load PlanBriefingService"""
    from src.services.plan_briefing_service import PlanBriefingService
    return PlanBriefingService


def get_draft_generator():
    """Lazy load DraftResultGenerator"""
    from src.services.draft_generator import DraftResultGenerator
    return DraftResultGenerator


def get_checkpoint_service():
    """Lazy load CheckpointService"""
    from src.services.checkpoint_service import CheckpointService
    return CheckpointService


def get_time_machine_service():
    """Lazy load TimeMachineService"""
    from src.services.time_machine_service import TimeMachineService
    return TimeMachineService


def get_task_service():
    """Lazy load TaskService"""
    from src.services.task_service import TaskService
    return TaskService


def get_context_aware_logger():
    """Lazy load ContextAwareLogger"""
    from src.services.task_service import ContextAwareLogger
    return ContextAwareLogger


__all__ = [
    "get_plan_briefing_service",
    "get_draft_generator",
    "get_checkpoint_service",
    "get_time_machine_service",
    "get_task_service",
    "get_context_aware_logger",
]
