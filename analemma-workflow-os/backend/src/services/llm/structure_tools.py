"""
Structure Tools - Workflow Structural Tools Definition

Gemini is maximized for structural reasoning by defining tools:
- Loop: Sequential processing structure
- Map: Parallel distributed processing structure
- Parallel: Concurrent execution structure
- Conditional: Conditional branching structure
- SubGraph: Nested workflow structure

Each tool is defined in JSON Schema format to guide Gemini in generating accurate structures.
"""

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum
from pydantic import BaseModel, Field


class StructureType(Enum):
    """Workflow structure types"""
    LOOP = "loop"
    MAP = "map"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    SUBGRAPH = "subgraph"
    RETRY = "retry"
    CATCH = "catch"
    WAIT_FOR_APPROVAL = "wait_for_approval"


class StructureDefinition(BaseModel):
    """Structure tool definition"""
    name: str
    type: StructureType
    description: str
    schema: Dict[str, Any]
    examples: List[Dict[str, Any]]
    use_cases: List[str]
    ui_hints: Dict[str, Any] = Field(default_factory=dict)


# ============================================================
# 1. Loop structure - Sequential processing
# ============================================================
LOOP_STRUCTURE = StructureDefinition(
    name="loop",
    type=StructureType.LOOP,
    description="A structure that sequentially processes a collection of data, applying the same processing logic to each item.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., loop_1)"
            },
            "type": {
                "type": "string",
                "const": "for_each",
                "description": "Node type (fixed value: for_each)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["items_path", "body_nodes"],
                "properties": {
                    "items_path": {
                        "type": "string",
                        "description": "State path of the item array to iterate (e.g., state.items, state.users)"
                    },
                    "item_key": {
                        "type": "string",
                        "default": "current_item",
                        "description": "State key to store the current item in each iteration"
                    },
                    "index_key": {
                        "type": "string",
                        "default": "current_index",
                        "description": "State key to store the current index"
                    },
                    "body_nodes": {
                        "type": "array",
                        "description": "List of node IDs to execute within the loop",
                        "items": {"type": "string"}
                    },
                    "max_iterations": {
                        "type": "integer",
                        "default": 1000,
                        "description": "Maximum number of iterations (prevents infinite loops)"
                    },
                    "continue_on_error": {
                        "type": "boolean",
                        "default": False,
                        "description": "Whether to continue to the next item if an error occurs"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "loop_process_items",
            "type": "for_each",
            "data": {"label": "Process Items Loop", "description": "Process each item in the collection"},
            "config": {
                "items_path": "state.items",
                "item_key": "current_item",
                "body_nodes": ["process_item", "log_result"],
                "continue_on_error": True
            },
            "position": {"x": 200, "y": 150}
        }
    ],
    use_cases=[
        "Process each item in a collection sequentially",
        "Handle a list of files one by one",
        "Process array data from API responses",
        "Process each record in batch operations"
    ],
    ui_hints={
        "suggested_width": 200,
        "color_code": "#4CAF50",
        "icon": "loop"
    }
)


# ============================================================
# 2. Map structure - Parallel distributed processing
# ============================================================
MAP_STRUCTURE = StructureDefinition(
    name="map",
    type=StructureType.MAP,
    description="A structure that processes a collection of data in parallel. Compatible with AWS Step Functions Distributed Map.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., map_1)"
            },
            "type": {
                "type": "string",
                "const": "distributed_map",
                "description": "Node type (fixed value: distributed_map)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["items_path", "processor_node"],
                "properties": {
                    "items_path": {
                        "type": "string",
                        "description": "State path of the item array for parallel processing"
                    },
                    "processor_node": {
                        "type": "string",
                        "description": "Node ID to process each item"
                    },
                    "max_concurrency": {
                        "type": "integer",
                        "default": 40,
                        "description": "Maximum concurrent executions (1-1000)"
                    },
                    "tolerated_failure_percentage": {
                        "type": "number",
                        "default": 0,
                        "description": "Allowed failure percentage (0-100)"
                    },
                    "tolerated_failure_count": {
                        "type": "integer",
                        "default": 0,
                        "description": "Allowed failure count"
                    },
                    "result_path": {
                        "type": "string",
                        "default": "$.map_results",
                        "description": "State path to store results"
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "Batch size (process in batches if specified)"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "map_process_documents",
            "type": "distributed_map",
            "data": {"label": "Document Parallel Processing", "description": "Analyze large volumes of documents simultaneously"},
            "config": {
                "items_path": "state.documents",
                "processor_node": "analyze_document",
                "max_concurrency": 100,
                "tolerated_failure_percentage": 5,
                "result_path": "$.analysis_results"
            },
            "position": {"x": 300, "y": 200}
        }
    ],
    use_cases=[
        "Process large images simultaneously",
        "Execute thousands of API calls in parallel",
        "Perform distributed data analysis",
        "Handle large-scale ETL pipelines"
    ]
)


# ============================================================
# 3. Parallel structure - Concurrent execution (different tasks)
# ============================================================
PARALLEL_STRUCTURE = StructureDefinition(
    name="parallel",
    type=StructureType.PARALLEL,
    description="A structure that executes multiple different tasks simultaneously. Each branch runs independently.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., parallel_1)"
            },
            "type": {
                "type": "string",
                "const": "parallel",
                "description": "Node type (fixed value: parallel)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["branches"],
                "properties": {
                    "branches": {
                        "type": "array",
                        "description": "Branch definitions for parallel execution",
                        "items": {
                            "type": "object",
                            "required": ["id", "nodes"],
                            "properties": {
                                "id": {"type": "string", "description": "Branch ID"},
                                "name": {"type": "string", "description": "Branch name"},
                                "nodes": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "List of node IDs to execute in this branch"
                                }
                            }
                        }
                    },
                    "wait_for_all": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether to wait for all branches to complete"
                    },
                    "result_merge_strategy": {
                        "type": "string",
                        "enum": ["merge", "array", "first"],
                        "default": "merge",
                        "description": "Result merge strategy"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "parallel_fetch_data",
            "type": "parallel",
            "data": {"label": "Concurrent Data Collection", "description": "Collect data from multiple sources simultaneously"},
            "config": {
                "branches": [
                    {"id": "branch_api", "name": "API Data", "nodes": ["fetch_api"]},
                    {"id": "branch_db", "name": "DB Data", "nodes": ["query_db"]},
                    {"id": "branch_cache", "name": "Cache Data", "nodes": ["check_cache"]}
                ],
                "wait_for_all": True,
                "result_merge_strategy": "merge"
            },
            "position": {"x": 250, "y": 100}
        }
    ],
    use_cases=[
        "Collect data from multiple APIs simultaneously",
        "Query databases and caches concurrently",
        "Send notifications to multiple services at once",
        "Integrate data from multiple sources"
    ]
)


# ============================================================
# 4. Conditional structure - Conditional branching
# ============================================================
CONDITIONAL_STRUCTURE = StructureDefinition(
    name="conditional",
    type=StructureType.CONDITIONAL,
    description="A structure that branches to different paths based on conditions. Supports if-else and switch-case patterns.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., cond_1)"
            },
            "type": {
                "type": "string",
                "const": "route_condition",
                "description": "Node type (fixed value: route_condition)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["conditions"],
                "properties": {
                    "conditions": {
                        "type": "array",
                        "description": "List of conditions (evaluated in order)",
                        "items": {
                            "type": "object",
                            "required": ["id", "expression", "target_node"],
                            "properties": {
                                "id": {"type": "string", "description": "Condition ID"},
                                "name": {"type": "string", "description": "Condition name"},
                                "expression": {
                                    "type": "string",
                                    "description": "Python expression (e.g., state.score > 80)"
                                },
                                "target_node": {
                                    "type": "string",
                                    "description": "Node ID to move to if condition is true"
                                }
                            }
                        }
                    },
                    "default_node": {
                        "type": "string",
                        "description": "Default node ID if all conditions are false"
                    },
                    "evaluation_mode": {
                        "type": "string",
                        "enum": ["first_match", "all_match"],
                        "default": "first_match",
                        "description": "Evaluation mode (first_match: first true, all_match: all true)"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "cond_quality_check",
            "type": "route_condition",
            "data": {"label": "Quality Check Branch", "description": "Branch based on score to different processing paths"},
            "config": {
                "conditions": [
                    {"id": "high_quality", "name": "High Quality", "expression": "state.score >= 90", "target_node": "approve"},
                    {"id": "medium_quality", "name": "Medium Quality", "expression": "state.score >= 70", "target_node": "review"},
                    {"id": "low_quality", "name": "Low Quality", "expression": "state.score < 70", "target_node": "reject"}
                ],
                "default_node": "manual_review",
                "evaluation_mode": "first_match"
            },
            "position": {"x": 350, "y": 200}
        }
    ],
    use_cases=[
        "Approval/rejection branching based on quality scores",
        "Processing path branching based on user permissions",
        "Branching based on data validation",
        "A/B test branching"
    ]
)


# ============================================================
# 5. Retry structure - Retry processing
# ============================================================
RETRY_STRUCTURE = StructureDefinition(
    name="retry",
    type=StructureType.RETRY,
    description="A structure that automatically retries on failure. Supports exponential backoff and maximum retry counts.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., retry_1)"
            },
            "type": {
                "type": "string",
                "const": "retry_wrapper",
                "description": "Node type (fixed value: retry_wrapper)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["target_node"],
                "properties": {
                    "target_node": {
                        "type": "string",
                        "description": "Node ID to retry"
                    },
                    "max_attempts": {
                        "type": "integer",
                        "default": 3,
                        "description": "Maximum retry attempts"
                    },
                    "initial_interval_seconds": {
                        "type": "number",
                        "default": 1,
                        "description": "Wait time before first retry (seconds)"
                    },
                    "max_interval_seconds": {
                        "type": "number",
                        "default": 60,
                        "description": "Maximum wait time (seconds)"
                    },
                    "backoff_rate": {
                        "type": "number",
                        "default": 2.0,
                        "description": "Exponential backoff rate"
                    },
                    "retryable_errors": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of retryable error types (empty means all errors)"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "retry_api_call",
            "type": "retry_wrapper",
            "data": {"label": "API Call Retry", "description": "Retry up to 5 times on failure"},
            "config": {
                "target_node": "external_api_call",
                "max_attempts": 5,
                "initial_interval_seconds": 2,
                "backoff_rate": 2.0,
                "retryable_errors": ["TimeoutError", "ConnectionError"]
            },
            "position": {"x": 200, "y": 250}
        }
    ],
    use_cases=[
        "Handle unreliable external API calls",
        "Manage network timeouts",
        "Respond to temporary service outages",
        "Handle rate limit exceedances"
    ]
)


# ============================================================
# 6. Catch structure - Error handling
# ============================================================
CATCH_STRUCTURE = StructureDefinition(
    name="catch",
    type=StructureType.CATCH,
    description="A structure that branches to alternative paths on error. Implements try-catch patterns.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., catch_1)"
            },
            "type": {
                "type": "string",
                "const": "error_handler",
                "description": "Node type (fixed value: error_handler)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["try_nodes"],
                "properties": {
                    "try_nodes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of node IDs to attempt"
                    },
                    "catch_handlers": {
                        "type": "array",
                        "description": "Error handler list",
                        "items": {
                            "type": "object",
                            "required": ["error_types", "handler_node"],
                            "properties": {
                                "error_types": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "List of error types to handle"
                                },
                                "handler_node": {
                                    "type": "string",
                                    "description": "Error handling node ID"
                                }
                            }
                        }
                    },
                    "finally_node": {
                        "type": "string",
                        "description": "Cleanup node ID to always execute (optional)"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "catch_processing",
            "type": "error_handler",
            "data": {"label": "Error Handling", "description": "Recover from errors during processing"},
            "config": {
                "try_nodes": ["process_data", "save_result"],
                "catch_handlers": [
                    {"error_types": ["ValidationError"], "handler_node": "validation_fallback"},
                    {"error_types": ["TimeoutError", "ConnectionError"], "handler_node": "network_fallback"}
                ],
                "finally_node": "cleanup"
            },
            "position": {"x": 300, "y": 300}
        }
    ],
    use_cases=[
        "Execute alternative logic on data processing failures",
        "Handle external service outages with fallbacks",
        "Rollback on transaction failures",
        "Ensure resource cleanup"
    ]
)


# ============================================================
# 7. SubGraph structure - Reusable nested workflow
# ============================================================
SUBGRAPH_STRUCTURE = StructureDefinition(
    name="subgraph",
    type=StructureType.SUBGRAPH,
    description="""A reusable nested workflow structure.
Modularizes complex workflows and can be used recursively within other structures (Loop, Parallel, etc.).
SubGraph exchanges data between parent and child through independent state mapping.""",
    schema={
        "type": "object",
        "required": ["id", "type"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., subgraph_1, group_analysis)"
            },
            "type": {
                "type": "string",
                "enum": ["group", "subgraph"],
                "description": "Node type (group or subgraph)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "SubGraph description"}
                }
            },
            "config": {
                "type": "object",
                "properties": {
                    "subgraph_ref": {
                        "type": "string",
                        "description": "SubGraph ID to reference (lookup from subgraphs dictionary)"
                    },
                    "subgraph_inline": {
                        "type": "object",
                        "description": "Inline subgraph definition (includes nodes, edges)",
                        "properties": {
                            "nodes": {
                                "type": "array",
                                "description": "List of nodes inside the subgraph",
                                "items": {"type": "object"}
                            },
                            "edges": {
                                "type": "array",
                                "description": "List of edges inside the subgraph",
                                "items": {"type": "object"}
                            }
                        }
                    },
                    "skill_ref": {
                        "type": "string",
                        "description": "Skill ID to reference (lookup from SkillRepository)"
                    },
                    "input_mapping": {
                        "type": "object",
                        "description": "Parent state → child input mapping (e.g., {'parent_key': 'child_key'})",
                        "additionalProperties": {"type": "string"}
                    },
                    "output_mapping": {
                        "type": "object",
                        "description": "Child output → parent state mapping (e.g., {'child_key': 'parent_key'})",
                        "additionalProperties": {"type": "string"}
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "default": 300,
                        "description": "Subgraph execution timeout (seconds)"
                    },
                    "error_handling": {
                        "type": "string",
                        "enum": ["fail", "ignore", "fallback"],
                        "default": "fail",
                        "description": "Error handling mode"
                    },
                    "max_depth": {
                        "type": "integer",
                        "default": 5,
                        "description": "Maximum recursion depth (prevents infinite nesting)"
                    }
                }
            },
            "metadata": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "SubGraph display name"},
                    "version": {"type": "string", "description": "SubGraph version"},
                    "author": {"type": "string", "description": "Author"},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags for search/classification"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "subgraph_data_pipeline",
            "type": "group",
            "data": {"label": "Data Pipeline", "description": "Data collection and cleansing sub-workflow"},
            "config": {
                "subgraph_ref": "data_pipeline_v1",
                "input_mapping": {
                    "raw_data": "input_data",
                    "config": "pipeline_config"
                },
                "output_mapping": {
                    "processed_data": "result",
                    "metadata": "processing_metadata"
                },
                "timeout_seconds": 600,
                "error_handling": "fallback"
            },
            "metadata": {
                "name": "Data Pipeline",
                "version": "1.0.0",
                "tags": ["data", "etl", "pipeline"]
            },
            "position": {"x": 200, "y": 150}
        },
        {
            "id": "inline_analysis_group",
            "type": "subgraph",
            "data": {"label": "Inline Analysis Group", "description": "Inline subgraph for document analysis"},
            "config": {
                "subgraph_inline": {
                    "nodes": [
                        {"id": "extract", "type": "operator", "config": {"code": "state['text'] = extract_text(state['doc'])"}},
                        {"id": "analyze", "type": "llm_chat", "config": {"prompt_content": "Analyze: {{text}}"}}
                    ],
                    "edges": [
                        {"source": "extract", "target": "analyze"}
                    ]
                },
                "input_mapping": {"document": "doc"},
                "output_mapping": {"analysis_result": "result"},
                "max_depth": 3
            },
            "position": {"x": 300, "y": 200}
        },
        {
            "id": "nested_loop_with_subgraph",
            "type": "group",
            "data": {"label": "Nested Processing", "description": "SubGraph example used within a Loop"},
            "config": {
                "skill_ref": "document_processor_skill",
                "input_mapping": {"item": "current_document"},
                "output_mapping": {"processed": "result"}
            },
            "position": {"x": 400, "y": 250}
        }
    ],
    use_cases=[
        "Modularize complex workflows into reusable components",
        "Encapsulate compound processing logic within Loop/Parallel structures",
        "Apply skill-based reusable workflow patterns",
        "Share workflow components across teams",
        "Manage versioned workflow fragments",
        "Handle recursive data processing (e.g., tree structure traversal)"
    ],
    ui_hints={
        "suggested_width": 280,
        "color_code": "#9C27B0",
        "icon": "folder_special",
        "expandable": True,
        "show_internal_preview": True
    }
)


# ============================================================
# 8. Wait for Approval structure - Human intervention (HITL)
# ============================================================
WAIT_FOR_APPROVAL_STRUCTURE = StructureDefinition(
    name="wait_for_approval",
    type=StructureType.WAIT_FOR_APPROVAL,
    description="A structure that waits for human approval at points where AI judgment is uncertain. Core element of Glassbox AI.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "Unique node ID (e.g., approval_1)"
            },
            "type": {
                "type": "string",
                "const": "wait_for_approval",
                "description": "Node type (fixed value: wait_for_approval)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node name displayed in UI"},
                    "description": {"type": "string", "description": "Node description"}
                }
            },
            "config": {
                "type": "object",
                "required": ["approval_message", "timeout_seconds"],
                "properties": {
                    "approval_message": {
                        "type": "string",
                        "description": "Approval request message to display to user"
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "default": 3600,
                        "description": "Approval wait timeout (default 1 hour)"
                    },
                    "approver_roles": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of user roles allowed to approve"
                    },
                    "auto_approve_conditions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "expression": {"type": "string", "description": "Auto-approval condition expression"},
                                "action": {"type": "string", "enum": ["approve", "reject"], "description": "Action on auto-approval"}
                            }
                        },
                        "description": "List of auto-approval conditions"
                    },
                    "escalation_rules": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "after_seconds": {"type": "integer", "description": "Escalation wait time"},
                                "escalate_to": {"type": "string", "description": "Escalation target role"}
                            }
                        },
                        "description": "Escalation rules"
                    }
                }
            },
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                }
            }
        }
    },
    examples=[
        {
            "id": "approval_high_value_transaction",
            "type": "wait_for_approval",
            "data": {"label": "High-Value Transaction Approval", "description": "Transactions over threshold require manager approval"},
            "config": {
                "approval_message": "A high-value transaction has occurred. Do you approve?",
                "timeout_seconds": 7200,
                "approver_roles": ["manager", "supervisor"],
                "auto_approve_conditions": [
                    {"expression": "state.amount < 50000", "action": "approve"}
                ],
                "escalation_rules": [
                    {"after_seconds": 3600, "escalate_to": "senior_manager"}
                ]
            },
            "position": {"x": 300, "y": 250}
        }
    ],
    use_cases=[
        "Require human approval for decisions exceeding defined thresholds",
        "Gate access to sensitive data or operations",
        "Validate AI outputs with low confidence scores",
        "Ensure regulatory compliance before proceeding",
        "Confirm critical business decisions"
    ],
    ui_hints={
        "suggested_width": 250,
        "color_code": "#FF9800",
        "icon": "user_check"
    }
)


# ============================================================
# 도구 레지스트리
# ============================================================

ALL_STRUCTURE_DEFINITIONS: List[StructureDefinition] = [
    LOOP_STRUCTURE,
    MAP_STRUCTURE,
    PARALLEL_STRUCTURE,
    CONDITIONAL_STRUCTURE,
    RETRY_STRUCTURE,
    CATCH_STRUCTURE,
    SUBGRAPH_STRUCTURE,
    WAIT_FOR_APPROVAL_STRUCTURE,
]


def get_all_structure_tools() -> List[Dict[str, Any]]:
    """
    Returns all structural tool definitions in JSON format for Gemini/LLM calls
    """
    tools = []
    for struct in ALL_STRUCTURE_DEFINITIONS:
        tools.append({
            "name": struct.name,
            "type": struct.type.value,
            "description": struct.description,
            "schema": struct.schema,
            "examples": struct.examples,
            "use_cases": struct.use_cases,
        })
    return tools


def get_structure_tool(name: str) -> Optional[StructureDefinition]:
    """Lookup structure tool by name"""
    for struct in ALL_STRUCTURE_DEFINITIONS:
        if struct.name == name:
            return struct
    return None


def get_structure_tools_by_type(type_: StructureType) -> List[StructureDefinition]:
    """Lookup structure tools by type"""
    return [s for s in ALL_STRUCTURE_DEFINITIONS if s.type == type_]


def get_gemini_system_instruction(mode: str = "detailed") -> str:
    """
    Generate structure tool documentation for Gemini system instructions
    
    Args:
        mode: "detailed" (includes full schema) or "brief" (names and purposes only)
    
    Returns:
        System instruction text
    """
    # Meta-instruction for pattern-based reasoning
    meta_instruction = """
**IMPORTANT: Pattern-Based Reasoning**
The Use Cases provided below are representative examples only. You should analyze 
the logical patterns (Iterative, Parallel, Conditional, Error Handling, etc.) of workflows 
and actively use structural tools when appropriate, even in situations not explicitly 
covered by the examples. Focus on the underlying pattern, not the specific domain.
"""
    
    if mode == "brief":
        # Summary mode: names and purposes only
        tools_doc = []
        for struct in ALL_STRUCTURE_DEFINITIONS:
            doc = f"""
### {struct.name.upper()} ({struct.type.value})
{struct.description}

**Use Cases:**
{chr(10).join(f'- {uc}' for uc in struct.use_cases)}
"""
            tools_doc.append(doc)
        
        return f"""
# Analemma OS Workflow Structure Tools (Summary Mode)

This document summarizes the structural tools available for workflow design.
Each tool implements a specific control flow pattern. Refer to detailed schema separately.

{meta_instruction}

{''.join(tools_doc)}

---
**Key Rules:**
1. Consider using Loop or Map structure first when processing data collections.
2. Use Parallel structure for concurrent execution of independent tasks.
3. Use Conditional structure when branching is needed.
4. Always pair external API calls with Retry structure.
5. Apply Catch structure to all error-prone sections.
6. Use Wait for Approval structure when AI confidence is low.
"""
    
    else:
        # Detailed mode: includes full schema
        tools_doc = []
        for struct in ALL_STRUCTURE_DEFINITIONS:
            doc = f"""
### {struct.name.upper()} ({struct.type.value})
{struct.description}

**Use Cases:**
{chr(10).join(f'- {uc}' for uc in struct.use_cases)}

**JSON Schema:**
```json
{json.dumps(struct.schema, ensure_ascii=False, indent=2)}
```

**Example:**
```json
{json.dumps(struct.examples[0], ensure_ascii=False, indent=2)}
```
"""
            tools_doc.append(doc)
        
        return f"""
# Analemma OS Workflow Structure Tools

This document defines the structural tools available for workflow design.
Each tool implements a specific control flow pattern and must strictly follow the JSON schema.

{meta_instruction}

{''.join(tools_doc)}

---
**Key Rules:**
1. Consider using Loop or Map structure first when processing data collections.
2. Use Parallel structure for concurrent execution of independent tasks.
3. Use Conditional structure when branching is needed.
4. Always pair external API calls with Retry structure.
5. Apply Catch structure to all error-prone sections.
6. Use Wait for Approval structure when AI confidence is low.
"""


def validate_structure_node(node: Dict[str, Any], all_node_ids: Optional[List[str]] = None) -> List[str]:
    """
    Validate a structure node
    
    Args:
        node: Node to validate
        all_node_ids: List of all node IDs in the workflow (for referential integrity)
    
    Returns:
        List of validation error messages (empty list if valid)
    """
    errors = []
    
    node_type = node.get("type")
    if not node_type:
        errors.append("Node is missing the 'type' field.")
        return errors
    
    # Structure node type mapping
    type_to_struct = {
        "for_each": LOOP_STRUCTURE,
        "distributed_map": MAP_STRUCTURE,
        "parallel": PARALLEL_STRUCTURE,
        "route_condition": CONDITIONAL_STRUCTURE,
        "retry_wrapper": RETRY_STRUCTURE,
        "error_handler": CATCH_STRUCTURE,
        "group": SUBGRAPH_STRUCTURE,
        "subgraph": SUBGRAPH_STRUCTURE,  # supports both group and subgraph
        "wait_for_approval": WAIT_FOR_APPROVAL_STRUCTURE,
    }
    
    struct = type_to_struct.get(node_type)
    if not struct:
        # Regular node, skip structure validation
        return errors
    
    # Required field validation
    required_fields = struct.schema.get("required", [])
    for field in required_fields:
        if field not in node:
            errors.append(f"{struct.name} node is missing required field '{field}'.")
    
    # Config required field validation
    config = node.get("config", {})
    config_schema = struct.schema.get("properties", {}).get("config", {})
    config_required = config_schema.get("required", [])
    
    for field in config_required:
        if field not in config:
            errors.append(f"{struct.name} node config is missing required field '{field}'.")
    
    # Referential integrity validation (if all_node_ids provided)
    if all_node_ids:
        # body_nodes validation (Loop, Map, etc.)
        body_nodes = config.get("body_nodes", [])
        if isinstance(body_nodes, list):
            for node_id in body_nodes:
                if node_id not in all_node_ids:
                    errors.append(f"{struct.name} node body_nodes references non-existent node ID '{node_id}'.")
        
        # target_node validation (Conditional, Retry, etc.)
        target_node = config.get("target_node")
        if target_node and target_node not in all_node_ids:
            errors.append(f"{struct.name} node target_node references non-existent node ID '{target_node}'.")
        
        # default_node validation
        default_node = config.get("default_node")
        if default_node and default_node not in all_node_ids:
            errors.append(f"{struct.name} node default_node references non-existent node ID '{default_node}'.")
        
        # conditions target_node validation
        conditions = config.get("conditions", [])
        if isinstance(conditions, list):
            for i, cond in enumerate(conditions):
                target = cond.get("target_node")
                if target and target not in all_node_ids:
                    errors.append(f"{struct.name} node conditions[{i}] target_node references non-existent node ID '{target}'.")
        
        # branches nodes validation
        branches = config.get("branches", [])
        if isinstance(branches, list):
            for i, branch in enumerate(branches):
                nodes = branch.get("nodes", [])
                if isinstance(nodes, list):
                    for node_id in nodes:
                        if node_id not in all_node_ids:
                            errors.append(f"{struct.name} node branches[{i}] references non-existent node ID '{node_id}'.")
        
        # try_nodes validation
        try_nodes = config.get("try_nodes", [])
        if isinstance(try_nodes, list):
            for node_id in try_nodes:
                if node_id not in all_node_ids:
                    errors.append(f"{struct.name} node try_nodes references non-existent node ID '{node_id}'.")
        
        # catch_handlers handler_node validation
        catch_handlers = config.get("catch_handlers", [])
        if isinstance(catch_handlers, list):
            for i, handler in enumerate(catch_handlers):
                handler_node = handler.get("handler_node")
                if handler_node and handler_node not in all_node_ids:
                    errors.append(f"{struct.name} node catch_handlers[{i}] handler_node references non-existent node ID '{handler_node}'.")
        
        # finally_node validation
        finally_node = config.get("finally_node")
        if finally_node and finally_node not in all_node_ids:
            errors.append(f"{struct.name} node finally_node references non-existent node ID '{finally_node}'.")
    
    return errors


# ============================================================
# Recursive depth validation utility
# ============================================================

MAX_NESTING_DEPTH = 10  # Global maximum nesting depth


def validate_nesting_depth(
    workflow: Dict[str, Any],
    max_depth: int = MAX_NESTING_DEPTH
) -> List[str]:
    """
    Validates the nesting depth of structure nodes in a workflow.
    
    Checks depth limits to prevent infinite recursion in complex nested structures
    like Loop inside Parallel, SubGraph inside Parallel, etc.
    
    Args:
        workflow: Complete workflow definition (includes nodes, edges, subgraphs)
        max_depth: Maximum allowed nesting depth (default: 10)
    
    Returns:
        List of validation error messages (empty list if valid)
    """
    errors = []
    nodes = workflow.get("nodes", [])
    subgraphs = workflow.get("subgraphs", {})
    
    # Node ID → node definition mapping
    node_map = {n.get("id"): n for n in nodes if n.get("id")}
    
    # Structure node types
    structure_types = {
        "for_each", "distributed_map", "parallel", "route_condition",
        "retry_wrapper", "error_handler", "group", "subgraph", "wait_for_approval"
    }
    
    def calculate_depth(node_id: str, visited: set, current_depth: int) -> int:
        """
        Recursively calculates the nesting depth of a node.
        """
        if node_id in visited:
            # Circular reference detected
            return current_depth
        
        node = node_map.get(node_id)
        if not node:
            return current_depth
        
        node_type = node.get("type", "")
        config = node.get("config", {})
        
        # Increase depth for structure nodes
        if node_type in structure_types:
            current_depth += 1
            visited = visited | {node_id}
            
            # Check depth limit
            if current_depth > max_depth:
                errors.append(
                    f"Node '{node_id}' nesting depth ({current_depth}) exceeds "
                    f"maximum allowed depth ({max_depth})."
                )
                return current_depth
            
            # Calculate child node depths
            child_node_ids = []
            
            # body_nodes (Loop, Map)
            child_node_ids.extend(config.get("body_nodes", []))
            
            # branches (Parallel)
            for branch in config.get("branches", []):
                child_node_ids.extend(branch.get("nodes", []))
            
            # try_nodes (Catch)
            child_node_ids.extend(config.get("try_nodes", []))
            
            # target_node, default_node (Conditional, Retry)
            if config.get("target_node"):
                child_node_ids.append(config["target_node"])
            if config.get("default_node"):
                child_node_ids.append(config["default_node"])
            
            # conditions target_node (Conditional)
            for cond in config.get("conditions", []):
                if cond.get("target_node"):
                    child_node_ids.append(cond["target_node"])
            
            # catch_handlers handler_node (Catch)
            for handler in config.get("catch_handlers", []):
                if handler.get("handler_node"):
                    child_node_ids.append(handler["handler_node"])
            
            # finally_node (Catch)
            if config.get("finally_node"):
                child_node_ids.append(config["finally_node"])
            
            # Recursively check child nodes
            max_child_depth = current_depth
            for child_id in child_node_ids:
                child_depth = calculate_depth(child_id, visited, current_depth)
                max_child_depth = max(max_child_depth, child_depth)
            
            # Check SubGraph inline definition
            if node_type in ("group", "subgraph"):
                subgraph_inline = config.get("subgraph_inline", {})
                if subgraph_inline:
                    inline_nodes = subgraph_inline.get("nodes", [])
                    inline_node_map = {n.get("id"): n for n in inline_nodes if n.get("id")}
                    
                    # Temporarily extend node_map
                    original_node_map = node_map.copy()
                    node_map.update(inline_node_map)
                    
                    for inline_node in inline_nodes:
                        inline_id = inline_node.get("id")
                        if inline_id:
                            child_depth = calculate_depth(inline_id, visited, current_depth)
                            max_child_depth = max(max_child_depth, child_depth)
                    
                    # Restore node_map
                    node_map.clear()
                    node_map.update(original_node_map)
                
                # Check referenced subgraph
                subgraph_ref = config.get("subgraph_ref")
                if subgraph_ref and subgraph_ref in subgraphs:
                    ref_subgraph = subgraphs[subgraph_ref]
                    ref_nodes = ref_subgraph.get("nodes", [])
                    ref_node_map = {n.get("id"): n for n in ref_nodes if n.get("id")}
                    
                    original_node_map = node_map.copy()
                    node_map.update(ref_node_map)
                    
                    for ref_node in ref_nodes:
                        ref_id = ref_node.get("id")
                        if ref_id:
                            child_depth = calculate_depth(ref_id, visited, current_depth)
                            max_child_depth = max(max_child_depth, child_depth)
                    
                    node_map.clear()
                    node_map.update(original_node_map)
            
            return max_child_depth
        
        return current_depth
    
    # Start depth calculation from all top-level nodes
    for node in nodes:
        node_id = node.get("id")
        if node_id:
            calculate_depth(node_id, set(), 0)
    
    return errors


def get_structure_type_for_node(node_type: str) -> Optional[StructureDefinition]:
    """
    Returns the structure definition for a given node type.
    
    Args:
        node_type: Node type string
    
    Returns:
        StructureDefinition or None (for regular nodes)
    """
    type_to_struct = {
        "for_each": LOOP_STRUCTURE,
        "distributed_map": MAP_STRUCTURE,
        "parallel": PARALLEL_STRUCTURE,
        "route_condition": CONDITIONAL_STRUCTURE,
        "retry_wrapper": RETRY_STRUCTURE,
        "error_handler": CATCH_STRUCTURE,
        "group": SUBGRAPH_STRUCTURE,
        "subgraph": SUBGRAPH_STRUCTURE,
        "wait_for_approval": WAIT_FOR_APPROVAL_STRUCTURE,
    }
    return type_to_struct.get(node_type)
