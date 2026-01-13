"""
Structure Tools - 워크플로우 구조적 도구 정의

Gemini의 구조적 추론 능력을 극대화하기 위한 도구 정의:
- Loop: 반복 처리 구조
- Map: 병렬 분산 처리 구조
- Parallel: 동시 실행 구조
- Conditional: 조건부 분기 구조
- SubGraph: 중첩 워크플로우 구조

각 도구는 JSON Schema 형식으로 정의되어 Gemini가
정확한 구조를 생성하도록 유도합니다.
"""

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class StructureType(Enum):
    """워크플로우 구조 타입"""
    LOOP = "loop"
    MAP = "map"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    SUBGRAPH = "subgraph"
    RETRY = "retry"
    CATCH = "catch"


@dataclass
class StructureDefinition:
    """구조 도구 정의"""
    name: str
    type: StructureType
    description: str
    schema: Dict[str, Any]
    examples: List[Dict[str, Any]]
    use_cases: List[str]


# ============================================================
# 1. Loop 구조 - 반복 처리
# ============================================================
LOOP_STRUCTURE = StructureDefinition(
    name="loop",
    type=StructureType.LOOP,
    description="데이터 컬렉션을 순차적으로 반복 처리하는 구조. 각 아이템에 대해 동일한 처리 로직을 적용합니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: loop_1)"
            },
            "type": {
                "type": "string",
                "const": "for_each",
                "description": "노드 타입 (고정값: for_each)"
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
                "required": ["items_path", "body_nodes"],
                "properties": {
                    "items_path": {
                        "type": "string",
                        "description": "반복할 아이템 배열의 상태 경로 (예: state.items, state.users)"
                    },
                    "item_key": {
                        "type": "string",
                        "default": "current_item",
                        "description": "각 반복에서 현재 아이템을 저장할 상태 키"
                    },
                    "index_key": {
                        "type": "string",
                        "default": "current_index",
                        "description": "현재 인덱스를 저장할 상태 키"
                    },
                    "body_nodes": {
                        "type": "array",
                        "description": "반복 내에서 실행할 노드 ID 목록",
                        "items": {"type": "string"}
                    },
                    "max_iterations": {
                        "type": "integer",
                        "default": 1000,
                        "description": "최대 반복 횟수 (무한 루프 방지)"
                    },
                    "continue_on_error": {
                        "type": "boolean",
                        "default": False,
                        "description": "오류 발생 시 다음 아이템으로 계속 진행 여부"
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
            "id": "loop_process_users",
            "type": "for_each",
            "data": {"label": "사용자 처리 루프", "description": "각 사용자에 대해 이메일 발송"},
            "config": {
                "items_path": "state.users",
                "item_key": "current_user",
                "body_nodes": ["send_email", "log_result"],
                "continue_on_error": True
            },
            "position": {"x": 200, "y": 150}
        }
    ],
    use_cases=[
        "사용자 목록을 순회하며 각각에 대해 처리",
        "파일 목록을 하나씩 처리",
        "API 응답의 배열 데이터를 순차 처리",
        "배치 작업에서 각 레코드를 순서대로 처리"
    ]
)


# ============================================================
# 2. Map 구조 - 병렬 분산 처리
# ============================================================
MAP_STRUCTURE = StructureDefinition(
    name="map",
    type=StructureType.MAP,
    description="데이터 컬렉션을 병렬로 동시 처리하는 구조. AWS Step Functions의 Distributed Map과 호환됩니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: map_1)"
            },
            "type": {
                "type": "string",
                "const": "distributed_map",
                "description": "노드 타입 (고정값: distributed_map)"
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
                "required": ["items_path", "processor_node"],
                "properties": {
                    "items_path": {
                        "type": "string",
                        "description": "병렬 처리할 아이템 배열의 상태 경로"
                    },
                    "processor_node": {
                        "type": "string",
                        "description": "각 아이템을 처리할 노드 ID"
                    },
                    "max_concurrency": {
                        "type": "integer",
                        "default": 40,
                        "description": "최대 동시 실행 수 (1-1000)"
                    },
                    "tolerated_failure_percentage": {
                        "type": "number",
                        "default": 0,
                        "description": "허용 가능한 실패 비율 (0-100)"
                    },
                    "tolerated_failure_count": {
                        "type": "integer",
                        "default": 0,
                        "description": "허용 가능한 실패 횟수"
                    },
                    "result_path": {
                        "type": "string",
                        "default": "$.map_results",
                        "description": "결과를 저장할 상태 경로"
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "배치 크기 (지정 시 배치 단위로 처리)"
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
            "data": {"label": "문서 병렬 처리", "description": "대량의 문서를 동시에 분석"},
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
        "대량의 이미지를 동시에 처리",
        "수천 개의 API 호출을 병렬로 실행",
        "분산 데이터 분석 작업",
        "대규모 ETL 파이프라인"
    ]
)


# ============================================================
# 3. Parallel 구조 - 동시 실행 (서로 다른 작업)
# ============================================================
PARALLEL_STRUCTURE = StructureDefinition(
    name="parallel",
    type=StructureType.PARALLEL,
    description="서로 다른 여러 작업을 동시에 실행하는 구조. 각 브랜치는 독립적으로 실행됩니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: parallel_1)"
            },
            "type": {
                "type": "string",
                "const": "parallel",
                "description": "노드 타입 (고정값: parallel)"
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
                "required": ["branches"],
                "properties": {
                    "branches": {
                        "type": "array",
                        "description": "병렬로 실행할 브랜치 정의",
                        "items": {
                            "type": "object",
                            "required": ["id", "nodes"],
                            "properties": {
                                "id": {"type": "string", "description": "브랜치 ID"},
                                "name": {"type": "string", "description": "브랜치 이름"},
                                "nodes": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "이 브랜치에서 실행할 노드 ID 목록"
                                }
                            }
                        }
                    },
                    "wait_for_all": {
                        "type": "boolean",
                        "default": True,
                        "description": "모든 브랜치 완료를 대기할지 여부"
                    },
                    "result_merge_strategy": {
                        "type": "string",
                        "enum": ["merge", "array", "first"],
                        "default": "merge",
                        "description": "결과 병합 전략"
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
            "data": {"label": "데이터 동시 수집", "description": "여러 소스에서 동시에 데이터 수집"},
            "config": {
                "branches": [
                    {"id": "branch_api", "name": "API 데이터", "nodes": ["fetch_api"]},
                    {"id": "branch_db", "name": "DB 데이터", "nodes": ["query_db"]},
                    {"id": "branch_cache", "name": "캐시 데이터", "nodes": ["check_cache"]}
                ],
                "wait_for_all": True,
                "result_merge_strategy": "merge"
            },
            "position": {"x": 250, "y": 100}
        }
    ],
    use_cases=[
        "여러 API에서 동시에 데이터 수집",
        "데이터베이스와 캐시를 동시에 조회",
        "여러 서비스에 동시 알림 발송",
        "다중 소스 데이터 통합"
    ]
)


# ============================================================
# 4. Conditional 구조 - 조건부 분기
# ============================================================
CONDITIONAL_STRUCTURE = StructureDefinition(
    name="conditional",
    type=StructureType.CONDITIONAL,
    description="조건에 따라 다른 경로로 분기하는 구조. if-else, switch-case 패턴을 지원합니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: cond_1)"
            },
            "type": {
                "type": "string",
                "const": "route_condition",
                "description": "노드 타입 (고정값: route_condition)"
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
                "required": ["conditions"],
                "properties": {
                    "conditions": {
                        "type": "array",
                        "description": "조건 목록 (순서대로 평가)",
                        "items": {
                            "type": "object",
                            "required": ["id", "expression", "target_node"],
                            "properties": {
                                "id": {"type": "string", "description": "조건 ID"},
                                "name": {"type": "string", "description": "조건 이름"},
                                "expression": {
                                    "type": "string",
                                    "description": "Python 표현식 (예: state.score > 80)"
                                },
                                "target_node": {
                                    "type": "string",
                                    "description": "조건이 참일 때 이동할 노드 ID"
                                }
                            }
                        }
                    },
                    "default_node": {
                        "type": "string",
                        "description": "모든 조건이 거짓일 때 이동할 기본 노드 ID"
                    },
                    "evaluation_mode": {
                        "type": "string",
                        "enum": ["first_match", "all_match"],
                        "default": "first_match",
                        "description": "평가 모드 (first_match: 첫 번째 일치, all_match: 모든 일치)"
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
            "data": {"label": "품질 검사 분기", "description": "점수에 따라 다른 처리 경로로 분기"},
            "config": {
                "conditions": [
                    {"id": "high_quality", "name": "고품질", "expression": "state.score >= 90", "target_node": "approve"},
                    {"id": "medium_quality", "name": "중품질", "expression": "state.score >= 70", "target_node": "review"},
                    {"id": "low_quality", "name": "저품질", "expression": "state.score < 70", "target_node": "reject"}
                ],
                "default_node": "manual_review",
                "evaluation_mode": "first_match"
            },
            "position": {"x": 350, "y": 200}
        }
    ],
    use_cases=[
        "품질 점수에 따른 승인/반려 분기",
        "사용자 권한에 따른 처리 경로 분기",
        "데이터 유효성에 따른 분기",
        "A/B 테스트 분기"
    ]
)


# ============================================================
# 5. Retry 구조 - 재시도 처리
# ============================================================
RETRY_STRUCTURE = StructureDefinition(
    name="retry",
    type=StructureType.RETRY,
    description="실패 시 자동으로 재시도하는 구조. 지수 백오프 및 최대 재시도 횟수를 설정할 수 있습니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: retry_1)"
            },
            "type": {
                "type": "string",
                "const": "retry_wrapper",
                "description": "노드 타입 (고정값: retry_wrapper)"
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
                "required": ["target_node"],
                "properties": {
                    "target_node": {
                        "type": "string",
                        "description": "재시도할 노드 ID"
                    },
                    "max_attempts": {
                        "type": "integer",
                        "default": 3,
                        "description": "최대 재시도 횟수"
                    },
                    "initial_interval_seconds": {
                        "type": "number",
                        "default": 1,
                        "description": "첫 번째 재시도 전 대기 시간 (초)"
                    },
                    "max_interval_seconds": {
                        "type": "number",
                        "default": 60,
                        "description": "최대 대기 시간 (초)"
                    },
                    "backoff_rate": {
                        "type": "number",
                        "default": 2.0,
                        "description": "지수 백오프 비율"
                    },
                    "retryable_errors": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "재시도할 에러 타입 목록 (비어있으면 모든 에러)"
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
            "data": {"label": "API 호출 재시도", "description": "실패 시 최대 5회 재시도"},
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
        "불안정한 외부 API 호출",
        "네트워크 타임아웃 처리",
        "일시적인 서비스 장애 대응",
        "Rate Limit 초과 시 재시도"
    ]
)


# ============================================================
# 6. Catch 구조 - 에러 핸들링
# ============================================================
CATCH_STRUCTURE = StructureDefinition(
    name="catch",
    type=StructureType.CATCH,
    description="에러 발생 시 대체 경로로 분기하는 구조. try-catch 패턴을 구현합니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: catch_1)"
            },
            "type": {
                "type": "string",
                "const": "error_handler",
                "description": "노드 타입 (고정값: error_handler)"
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
                "required": ["try_nodes"],
                "properties": {
                    "try_nodes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "시도할 노드 ID 목록"
                    },
                    "catch_handlers": {
                        "type": "array",
                        "description": "에러 핸들러 목록",
                        "items": {
                            "type": "object",
                            "required": ["error_types", "handler_node"],
                            "properties": {
                                "error_types": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "처리할 에러 타입 목록"
                                },
                                "handler_node": {
                                    "type": "string",
                                    "description": "에러 처리 노드 ID"
                                }
                            }
                        }
                    },
                    "finally_node": {
                        "type": "string",
                        "description": "항상 실행할 정리 노드 ID (선택사항)"
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
            "data": {"label": "에러 핸들링", "description": "처리 중 에러 발생 시 복구"},
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
        "데이터 처리 실패 시 대체 로직 실행",
        "외부 서비스 장애 시 폴백 처리",
        "트랜잭션 실패 시 롤백",
        "리소스 정리 보장"
    ]
)


# ============================================================
# 7. SubGraph 구조 - 중첩 워크플로우
# ============================================================
SUBGRAPH_STRUCTURE = StructureDefinition(
    name="subgraph",
    type=StructureType.SUBGRAPH,
    description="다른 워크플로우를 중첩 호출하는 구조. 재사용 가능한 워크플로우 모듈화를 지원합니다.",
    schema={
        "type": "object",
        "required": ["id", "type", "config"],
        "properties": {
            "id": {
                "type": "string",
                "description": "노드 고유 ID (예: subgraph_1)"
            },
            "type": {
                "type": "string",
                "const": "group",
                "description": "노드 타입 (고정값: group)"
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
                "required": ["subgraph_id"],
                "properties": {
                    "subgraph_id": {
                        "type": "string",
                        "description": "호출할 서브그래프(워크플로우) ID"
                    },
                    "input_mapping": {
                        "type": "object",
                        "description": "입력 매핑 (현재 상태 → 서브그래프 입력)"
                    },
                    "output_mapping": {
                        "type": "object",
                        "description": "출력 매핑 (서브그래프 출력 → 현재 상태)"
                    },
                    "inline_definition": {
                        "type": "object",
                        "description": "인라인 서브그래프 정의 (외부 참조 대신 직접 정의)"
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
            "id": "subgraph_notification",
            "type": "group",
            "data": {"label": "알림 발송 서브그래프", "description": "재사용 가능한 알림 워크플로우 호출"},
            "config": {
                "subgraph_id": "notification_workflow_v2",
                "input_mapping": {
                    "recipient": "state.user_email",
                    "message": "state.notification_message"
                },
                "output_mapping": {
                    "state.notification_sent": "result.success"
                }
            },
            "position": {"x": 400, "y": 200}
        }
    ],
    use_cases=[
        "공통 알림 로직 재사용",
        "데이터 검증 파이프라인 모듈화",
        "복잡한 워크플로우의 논리적 분리",
        "팀 간 워크플로우 공유"
    ]
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


def get_gemini_system_instruction() -> str:
    """
    Gemini 시스템 지침용 구조 도구 문서 생성
    """
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
"""


def validate_structure_node(node: Dict[str, Any]) -> List[str]:
    """
    구조 노드의 유효성 검증
    
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
    
    return errors
