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
    """워크플로우 구조 타입"""
    LOOP = "loop"
    MAP = "map"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    SUBGRAPH = "subgraph"
    RETRY = "retry"
    CATCH = "catch"
    WAIT_FOR_APPROVAL = "wait_for_approval"


class StructureDefinition(BaseModel):
    """구조 도구 정의"""
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
# 8. Wait for Approval 구조 - 인간 개입 (HITL)
# ============================================================
WAIT_FOR_APPROVAL_STRUCTURE = StructureDefinition(
    name="wait_for_approval",
    type=StructureType.WAIT_FOR_APPROVAL,
    description="AI가 판단하기 어려운 구간에서 인간의 승인을 기다리는 구조. Glassbox AI의 핵심 요소입니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: approval_1)"
            },
            "type": {
                "type": "string",
                "const": "wait_for_approval",
                "description": "노드 타입 (고정값: wait_for_approval)"
            },
            "data": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "UI에 표시될 노드 이름"},
                    "description": {"type": "string", "description": "노드 설명"}
                }
            },
            "config": {
                "type": "object",
                "required": ["approval_message", "timeout_seconds"],
                "properties": {
                    "approval_message": {
                        "type": "string",
                        "description": "사용자에게 표시할 승인 요청 메시지"
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "default": 3600,
                        "description": "승인 대기 시간 초과 (기본 1시간)"
                    },
                    "approver_roles": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "승인 가능한 사용자 역할 목록"
                    },
                    "auto_approve_conditions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "expression": {"type": "string", "description": "자동 승인 조건 표현식"},
                                "action": {"type": "string", "enum": ["approve", "reject"], "description": "자동 승인 시 액션"}
                            }
                        },
                        "description": "자동 승인 조건 목록"
                    },
                    "escalation_rules": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "after_seconds": {"type": "integer", "description": "에스컬레이션 대기 시간"},
                                "escalate_to": {"type": "string", "description": "에스컬레이션 대상 역할"}
                            }
                        },
                        "description": "에스컬레이션 규칙"
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
            "data": {"label": "고액 거래 승인", "description": "100만원 이상 거래는 관리자 승인 필요"},
            "config": {
                "approval_message": "고액 거래가 발생했습니다. 승인하시겠습니까?",
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
        "고액 거래 승인",
        "민감한 데이터 접근 승인",
        "AI 신뢰도가 낮은 판단 승인",
        "규정 준수 확인 승인",
        "중요한 의사결정 승인"
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
    모든 구조적 도구 정의를 Gemini/LLM 호출용 JSON 형식으로 반환
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
    """이름으로 구조 도구 조회"""
    for struct in ALL_STRUCTURE_DEFINITIONS:
        if struct.name == name:
            return struct
    return None


def get_structure_tools_by_type(type_: StructureType) -> List[StructureDefinition]:
    """타입으로 구조 도구 조회"""
    return [s for s in ALL_STRUCTURE_DEFINITIONS if s.type == type_]


def get_gemini_system_instruction(mode: str = "detailed") -> str:
    """
    Gemini 시스템 지침용 구조 도구 문서 생성
    
    Args:
        mode: "detailed" (전체 스키마 포함) 또는 "brief" (이름과 용도만 포함)
    
    Returns:
        시스템 지침 텍스트
    """
    if mode == "brief":
        # 요약 모드: 이름과 용도만 포함
        tools_doc = []
        for struct in ALL_STRUCTURE_DEFINITIONS:
            doc = f"""
### {struct.name.upper()} ({struct.type.value})
{struct.description}

**사용 사례:**
{chr(10).join(f'- {uc}' for uc in struct.use_cases)}
"""
            tools_doc.append(doc)
        
        return f"""
# Analemma OS 워크플로우 구조 도구 (요약 모드)

이 문서는 워크플로우 설계 시 사용할 수 있는 구조적 도구들을 요약합니다.
각 도구는 특정 제어 흐름 패턴을 구현하며, 상세 스키마는 별도 참조하세요.

{''.join(tools_doc)}

---
**중요 규칙:**
1. 데이터 컬렉션 처리 시 Loop 또는 Map 구조 사용을 먼저 검토하세요.
2. 독립적인 여러 작업은 Parallel 구조로 동시 실행하세요.
3. 조건 분기가 필요하면 Conditional 구조를 사용하세요.
4. 외부 API 호출에는 Retry 구조를 함께 사용하세요.
5. 모든 에러 발생 가능 구간에 Catch 구조를 적용하세요.
6. AI 신뢰도가 낮은 구간에는 Wait for Approval 구조를 사용하세요.
"""
    
    else:
        # 상세 모드: 전체 스키마 포함
        tools_doc = []
        for struct in ALL_STRUCTURE_DEFINITIONS:
            doc = f"""
### {struct.name.upper()} ({struct.type.value})
{struct.description}

**사용 사례:**
{chr(10).join(f'- {uc}' for uc in struct.use_cases)}

**JSON 스키마:**
```json
{json.dumps(struct.schema, ensure_ascii=False, indent=2)}
```

**예시:**
```json
{json.dumps(struct.examples[0], ensure_ascii=False, indent=2)}
```
"""
            tools_doc.append(doc)
        
        return f"""
# Analemma OS 워크플로우 구조 도구

이 문서는 워크플로우 설계 시 사용할 수 있는 구조적 도구들을 정의합니다.
각 도구는 특정 제어 흐름 패턴을 구현하며, JSON 스키마를 엄격히 따라야 합니다.

{''.join(tools_doc)}

---
**중요 규칙:**
1. 데이터 컬렉션 처리 시 Loop 또는 Map 구조 사용을 먼저 검토하세요.
2. 독립적인 여러 작업은 Parallel 구조로 동시 실행하세요.
3. 조건 분기가 필요하면 Conditional 구조를 사용하세요.
4. 외부 API 호출에는 Retry 구조를 함께 사용하세요.
5. 모든 에러 발생 가능 구간에 Catch 구조를 적용하세요.
6. AI 신뢰도가 낮은 구간에는 Wait for Approval 구조를 사용하세요.
"""


def validate_structure_node(node: Dict[str, Any], all_node_ids: Optional[List[str]] = None) -> List[str]:
    """
    구조 노드의 유효성 검증
    
    Args:
        node: 검증할 노드
        all_node_ids: 전체 워크플로우의 노드 ID 목록 (참조 무결성 검증용)
    
    Returns:
        검증 오류 메시지 목록 (빈 리스트면 유효)
    """
    errors = []
    
    node_type = node.get("type")
    if not node_type:
        errors.append("노드에 type 필드가 없습니다.")
        return errors
    
    # 구조 노드 타입 매핑
    type_to_struct = {
        "for_each": LOOP_STRUCTURE,
        "distributed_map": MAP_STRUCTURE,
        "parallel": PARALLEL_STRUCTURE,
        "route_condition": CONDITIONAL_STRUCTURE,
        "retry_wrapper": RETRY_STRUCTURE,
        "error_handler": CATCH_STRUCTURE,
        "group": SUBGRAPH_STRUCTURE,
        "wait_for_approval": WAIT_FOR_APPROVAL_STRUCTURE,
    }
    
    struct = type_to_struct.get(node_type)
    if not struct:
        # 일반 노드이므로 구조 검증 스킵
        return errors
    
    # 필수 필드 검증
    required_fields = struct.schema.get("required", [])
    for field in required_fields:
        if field not in node:
            errors.append(f"{struct.name} 노드에 필수 필드 '{field}'가 없습니다.")
    
    # config 필수 필드 검증
    config = node.get("config", {})
    config_schema = struct.schema.get("properties", {}).get("config", {})
    config_required = config_schema.get("required", [])
    
    for field in config_required:
        if field not in config:
            errors.append(f"{struct.name} 노드의 config에 필수 필드 '{field}'가 없습니다.")
    
    # 참조 무결성 검증 (all_node_ids가 제공된 경우)
    if all_node_ids:
        # body_nodes 검증 (Loop, Map 등)
        body_nodes = config.get("body_nodes", [])
        if isinstance(body_nodes, list):
            for node_id in body_nodes:
                if node_id not in all_node_ids:
                    errors.append(f"{struct.name} 노드의 body_nodes에 존재하지 않는 노드 ID '{node_id}'가 참조되었습니다.")
        
        # target_node 검증 (Conditional, Retry 등)
        target_node = config.get("target_node")
        if target_node and target_node not in all_node_ids:
            errors.append(f"{struct.name} 노드의 target_node에 존재하지 않는 노드 ID '{target_node}'가 참조되었습니다.")
        
        # default_node 검증
        default_node = config.get("default_node")
        if default_node and default_node not in all_node_ids:
            errors.append(f"{struct.name} 노드의 default_node에 존재하지 않는 노드 ID '{default_node}'가 참조되었습니다.")
        
        # conditions의 target_node 검증
        conditions = config.get("conditions", [])
        if isinstance(conditions, list):
            for i, cond in enumerate(conditions):
                target = cond.get("target_node")
                if target and target not in all_node_ids:
                    errors.append(f"{struct.name} 노드의 conditions[{i}] target_node에 존재하지 않는 노드 ID '{target}'가 참조되었습니다.")
        
        # branches의 nodes 검증
        branches = config.get("branches", [])
        if isinstance(branches, list):
            for i, branch in enumerate(branches):
                nodes = branch.get("nodes", [])
                if isinstance(nodes, list):
                    for node_id in nodes:
                        if node_id not in all_node_ids:
                            errors.append(f"{struct.name} 노드의 branches[{i}]에 존재하지 않는 노드 ID '{node_id}'가 참조되었습니다.")
        
        # try_nodes 검증
        try_nodes = config.get("try_nodes", [])
        if isinstance(try_nodes, list):
            for node_id in try_nodes:
                if node_id not in all_node_ids:
                    errors.append(f"{struct.name} 노드의 try_nodes에 존재하지 않는 노드 ID '{node_id}'가 참조되었습니다.")
        
        # catch_handlers의 handler_node 검증
        catch_handlers = config.get("catch_handlers", [])
        if isinstance(catch_handlers, list):
            for i, handler in enumerate(catch_handlers):
                handler_node = handler.get("handler_node")
                if handler_node and handler_node not in all_node_ids:
                    errors.append(f"{struct.name} 노드의 catch_handlers[{i}] handler_node에 존재하지 않는 노드 ID '{handler_node}'가 참조되었습니다.")
        
        # finally_node 검증
        finally_node = config.get("finally_node")
        if finally_node and finally_node not in all_node_ids:
            errors.append(f"{struct.name} 노드의 finally_node에 존재하지 않는 노드 ID '{finally_node}'가 참조되었습니다.")
    
    return errors
