"""
Graph DSL: Workflow JSON schema definition and validation

Core module of Co-design Assistant that formalizes workflow structures.
"""
import json
from typing import Any, Dict, List, Literal, Optional

# Fallback for when Pydantic is not available
try:
    from pydantic import BaseModel, Field, ValidationError, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    
    class BaseModel:
        """Pydantic fallback"""
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        
        def model_dump(self) -> Dict[str, Any]:
            return self.__dict__.copy()
    
    class ValidationError(Exception):
        pass
    
    def Field(*args, **kwargs):
        return kwargs.get("default")
    
    def field_validator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator


class Position(BaseModel):
    """Node position information"""
    x: float
    y: float


class NodeConfig(BaseModel):
    """Flexible node configuration (various fields by node type)"""
    # operator
    code: Optional[str] = None
    sets: Optional[Dict[str, Any]] = None
    
    # llm_chat
    prompt_content: Optional[str] = None
    model: Optional[str] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    system_prompt: Optional[str] = None
    
    # api_call
    url: Optional[str] = None
    method: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None
    json_body: Optional[Dict[str, Any]] = Field(default=None, alias="json")
    timeout: Optional[int] = None
    
    # db_query
    query: Optional[str] = None
    connection_string: Optional[str] = None
    
    # for_each
    items: Optional[List[Any]] = None
    item_key: Optional[str] = None
    
    # route_draft_quality
    threshold: Optional[float] = None
    
    # group / subgraph
    subgraph_id: Optional[str] = None
    subgraph_ref: Optional[str] = None  # 참조할 서브그래프 ID
    subgraph_inline: Optional[Dict[str, Any]] = None  # 인라인 서브그래프 정의
    skill_ref: Optional[str] = None  # Skill 참조
    input_mapping: Optional[Dict[str, str]] = None  # 부모→자식 상태 매핑
    output_mapping: Optional[Dict[str, str]] = None  # 자식→부모 상태 매핑
    error_handling: Optional[str] = None  # "fail", "ignore", "fallback"
    max_depth: Optional[int] = None  # 최대 재귀 깊이
    
    # for_each / distributed_map 공통
    items_path: Optional[str] = None  # 반복할 아이템 경로
    body_nodes: Optional[List[str]] = None  # 루프 내 실행할 노드 ID 목록
    max_iterations: Optional[int] = None  # 최대 반복 횟수
    continue_on_error: Optional[bool] = None  # 에러 시 계속 진행
    
    # parallel
    branches: Optional[List[Dict[str, Any]]] = None  # 병렬 브랜치 정의
    wait_for_all: Optional[bool] = None  # 모든 브랜치 완료 대기
    
    # route_condition (conditional)
    conditions: Optional[List[Dict[str, Any]]] = None  # 조건 목록
    default_node: Optional[str] = None  # 기본 대상 노드
    
    # retry_wrapper
    target_node: Optional[str] = None  # 재시도 대상 노드
    max_attempts: Optional[int] = None  # 최대 재시도 횟수
    backoff_rate: Optional[float] = None  # 백오프 배율
    
    # error_handler (catch)
    try_nodes: Optional[List[str]] = None  # 시도할 노드 목록
    catch_handlers: Optional[List[Dict[str, Any]]] = None  # 에러 핸들러 목록
    finally_node: Optional[str] = None  # 항상 실행할 노드
    
    # wait_for_approval
    approval_message: Optional[str] = None  # 승인 요청 메시지
    timeout_seconds: Optional[int] = None  # 타임아웃 초
    approver_roles: Optional[List[str]] = None  # 승인 가능 역할


class WorkflowNode(BaseModel):
    """Workflow node definition"""
    id: str
    type: Literal[
        "operator", "llm_chat", "api_call", "db_query", "for_each", 
        "route_draft_quality", "group", "subgraph", "aiModel",
        "parallel", "distributed_map", "route_condition", 
        "retry_wrapper", "error_handler", "wait_for_approval"
    ]
    label: Optional[str] = None
    position: Position
    config: Optional[NodeConfig] = None
    data: Optional[Dict[str, Any]] = None  # For frontend compatibility
    
    # Subgraph relationships
    subgraph_id: Optional[str] = None  # For group nodes (internal subgraph reference)
    parent_id: Optional[str] = None    # For nodes within groups (parent group reference)


class WorkflowEdge(BaseModel):
    """워크플로우 엣지 정의"""
    id: str
    source: str
    target: str
    source_handle: Optional[str] = None
    target_handle: Optional[str] = None
    label: Optional[str] = None
    condition: Optional[str] = None  # 조건부 분기용
    type: Optional[str] = None  # edge 타입 (예: "smoothstep")


class SubgraphMetadata(BaseModel):
    """서브그래프(그룹) 메타데이터"""
    id: str
    name: str
    description: Optional[str] = None
    nodes: List[WorkflowNode]
    edges: List[WorkflowEdge]
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None


class Workflow(BaseModel):
    """Graph DSL 루트 스키마"""
    name: Optional[str] = None
    description: Optional[str] = None
    nodes: List[WorkflowNode]
    edges: List[WorkflowEdge]
    subgraphs: Optional[Dict[str, SubgraphMetadata]] = None
    metadata: Optional[Dict[str, Any]] = None


# ──────────────────────────────────────────────────────────────
# 스키마 검증 함수
# ──────────────────────────────────────────────────────────────

def validate_workflow(workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    워크플로우 구조 검증
    
    Args:
        workflow: 워크플로우 JSON 딕셔너리
        
    Returns:
        에러 목록 [{"level": "error|warning", "message": "...", "node_id": "..."}]
    """
    errors: List[Dict[str, Any]] = []
    
    # 1. 기본 구조 검증 (Pydantic 모델 사용)
    if PYDANTIC_AVAILABLE:
        try:
            # 노드/엣지 배열 추출
            nodes = workflow.get("nodes", [])
            edges = workflow.get("edges", [])
            
            # 각 노드의 position 필드가 올바른 형식인지 확인
            for i, node in enumerate(nodes):
                if "position" not in node:
                    errors.append({
                        "level": "error",
                        "message": f"노드 #{i}에 position 필드가 없습니다.",
                        "node_id": node.get("id")
                    })
                elif not isinstance(node["position"], dict):
                    errors.append({
                        "level": "error",
                        "message": f"노드 '{node.get('id')}'의 position이 객체가 아닙니다.",
                        "node_id": node.get("id")
                    })
                    
        except Exception as e:
            errors.append({
                "level": "error",
                "message": f"워크플로우 구조 검증 실패: {str(e)}",
                "node_id": None
            })
    
    # 기본 필드 존재 확인
    if "nodes" not in workflow:
        errors.append({
            "level": "error",
            "message": "워크플로우에 'nodes' 필드가 없습니다.",
            "node_id": None
        })
        return errors
        
    if "edges" not in workflow:
        errors.append({
            "level": "error",
            "message": "워크플로우에 'edges' 필드가 없습니다.",
            "node_id": None
        })
        return errors
    
    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])
    
    # 2. 노드 ID 중복 검사
    node_ids = [n.get("id") for n in nodes if n.get("id")]
    seen_ids = set()
    for node_id in node_ids:
        if node_id in seen_ids:
            errors.append({
                "level": "error",
                "message": f"중복된 노드 ID: {node_id}",
                "node_id": node_id
            })
        seen_ids.add(node_id)
    
    # 3. 엣지 참조 무결성
    node_id_set = set(node_ids)
    for edge in edges:
        edge_id = edge.get("id", "unknown")
        source = edge.get("source")
        target = edge.get("target")
        
        if source and source not in node_id_set:
            errors.append({
                "level": "error",
                "message": f"엣지 '{edge_id}': 존재하지 않는 source 노드 '{source}'",
                "node_id": source
            })
        if target and target not in node_id_set:
            errors.append({
                "level": "error",
                "message": f"엣지 '{edge_id}': 존재하지 않는 target 노드 '{target}'",
                "node_id": target
            })
    
    # 4. 엣지 ID 중복 검사
    edge_ids = [e.get("id") for e in edges if e.get("id")]
    seen_edge_ids = set()
    for edge_id in edge_ids:
        if edge_id in seen_edge_ids:
            errors.append({
                "level": "warning",
                "message": f"중복된 엣지 ID: {edge_id}",
                "node_id": None
            })
        seen_edge_ids.add(edge_id)
    
    return errors


def normalize_workflow(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    워크플로우 정규화 (프론트엔드 ↔ 백엔드 변환)
    
    - position 필드 보장
    - data 필드와 config 필드 동기화
    - 기본값 설정
    """
    normalized = {
        "name": workflow.get("name"),
        "description": workflow.get("description"),
        "nodes": [],
        "edges": [],
        "subgraphs": workflow.get("subgraphs"),
        "metadata": workflow.get("metadata")
    }
    
    for node in workflow.get("nodes", []):
        normalized_node = {
            "id": node.get("id"),
            "type": node.get("type", "operator"),
            "label": node.get("label") or (node.get("data", {}) or {}).get("label"),
            "position": node.get("position", {"x": 150, "y": 50}),
            "config": node.get("config"),
            "data": node.get("data"),
            "subgraph_id": node.get("subgraph_id"),
            "parent_id": node.get("parent_id")
        }
        
        # data에서 config 동기화
        if node.get("data") and not node.get("config"):
            data = node["data"]
            normalized_node["config"] = {
                k: v for k, v in data.items() 
                if k not in ["label", "blockId", "id"]
            }
        
        normalized["nodes"].append(normalized_node)
    
    for edge in workflow.get("edges", []):
        normalized_edge = {
            "id": edge.get("id"),
            "source": edge.get("source"),
            "target": edge.get("target"),
            "source_handle": edge.get("source_handle") or edge.get("sourceHandle"),
            "target_handle": edge.get("target_handle") or edge.get("targetHandle"),
            "label": edge.get("label"),
            "condition": edge.get("condition"),
            "type": edge.get("type")
        }
        normalized["edges"].append(normalized_edge)
    
    return normalized


def workflow_to_frontend_format(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    백엔드 워크플로우를 프론트엔드 React Flow 형식으로 변환
    """
    frontend_nodes = []
    frontend_edges = []
    
    for node in workflow.get("nodes", []):
        frontend_node = {
            "id": node.get("id"),
            "type": node.get("type"),
            "position": node.get("position", {"x": 150, "y": 50}),
            "data": {
                "label": node.get("label") or node.get("id"),
                "blockId": node.get("id"),
                **(node.get("config") or {}),
                **(node.get("data") or {})
            }
        }
        frontend_nodes.append(frontend_node)
    
    for edge in workflow.get("edges", []):
        frontend_edge = {
            "id": edge.get("id"),
            "source": edge.get("source"),
            "target": edge.get("target"),
            "sourceHandle": edge.get("source_handle"),
            "targetHandle": edge.get("target_handle"),
            "label": edge.get("label"),
            "animated": True,
            "style": {"stroke": "hsl(263 70% 60%)", "strokeWidth": 2}
        }
        if edge.get("type"):
            frontend_edge["type"] = edge["type"]
        frontend_edges.append(frontend_edge)
    
    return {
        "nodes": frontend_nodes,
        "edges": frontend_edges
    }


def workflow_from_frontend_format(frontend_workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    프론트엔드 React Flow 형식을 백엔드 워크플로우로 변환
    """
    backend_nodes = []
    backend_edges = []
    
    for node in frontend_workflow.get("nodes", []):
        data = node.get("data", {}) or {}
        backend_node = {
            "id": node.get("id"),
            "type": node.get("type"),
            "label": data.get("label"),
            "position": node.get("position", {"x": 150, "y": 50}),
            "config": {
                k: v for k, v in data.items()
                if k not in ["label", "blockId", "id"]
            },
            "data": data
        }
        backend_nodes.append(backend_node)
    
    for edge in frontend_workflow.get("edges", []):
        backend_edge = {
            "id": edge.get("id"),
            "source": edge.get("source"),
            "target": edge.get("target"),
            "source_handle": edge.get("sourceHandle"),
            "target_handle": edge.get("targetHandle"),
            "label": edge.get("label"),
            "type": edge.get("type")
        }
        backend_edges.append(backend_edge)
    
    return {
        "nodes": backend_nodes,
        "edges": backend_edges
    }


# ──────────────────────────────────────────────────────────────
# JSON 스키마 정의 (외부 도구 호환용)
# ──────────────────────────────────────────────────────────────

WORKFLOW_JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Workflow",
    "type": "object",
    "required": ["nodes", "edges"],
    "properties": {
        "name": {"type": "string"},
        "description": {"type": "string"},
        "nodes": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type", "position"],
                "properties": {
                    "id": {"type": "string"},
                    "type": {
                        "type": "string",
                        "enum": ["operator", "llm_chat", "api_call", "db_query", "for_each", "route_draft_quality", "group", "aiModel"]
                    },
                    "label": {"type": "string"},
                    "position": {
                        "type": "object",
                        "properties": {
                            "x": {"type": "number"},
                            "y": {"type": "number"}
                        },
                        "required": ["x", "y"]
                    },
                    "config": {"type": "object"},
                    "data": {"type": "object"}
                }
            }
        },
        "edges": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "source", "target"],
                "properties": {
                    "id": {"type": "string"},
                    "source": {"type": "string"},
                    "target": {"type": "string"},
                    "label": {"type": "string"},
                    "condition": {"type": "string"}
                }
            }
        },
        "subgraphs": {"type": "object"},
        "metadata": {"type": "object"}
    }
}


# ──────────────────────────────────────────────────────────────
# [3단계] 워크플로우 복잡도 검증기 (Workflow Complexity Validator)
# ──────────────────────────────────────────────────────────────
# 
# 참고: Analemma-OS는 ASL을 직접 생성하지 않습니다.
# aws_step_functions.json은 고정된 오케스트레이터이며,
# workflow_config는 DynamicWorkflowBuilder를 통해 LangGraph로 해석됩니다.
#
# 이 검증기는 workflow_config의 구조적 문제를 사전에 감지합니다:
# - 순환 참조 (무한 루프 방지)
# - 고아 노드 (연결되지 않은 노드)
# - 과도한 복잡도 (실행 시간 예측)
# - 노드 ID 규칙 위반

# 워크플로우 복잡도 한계값
WORKFLOW_LIMITS = {
    "max_nodes": 500,  # orchestrator_service.py WorkflowConfigModel.nodes max_length
    "max_edges": 1000,  # orchestrator_service.py WorkflowConfigModel.edges max_length
    "max_node_id_length": 128,  # orchestrator_service.py NodeModel.id max_length
    "max_parallel_branches": 40,  # 실행 시간 예측을 위한 권장값
    "max_subgraph_depth": 5,  # 서브그래프 중첩 깊이 제한
    "max_config_size_bytes": 1_000_000,  # 1 MB (DynamoDB/Lambda 페이로드 고려)
}

# 의도적 루프를 허용하는 제어 노드 타입
# 이 타입의 노드를 통한 순환은 '설계 의도'로 간주하여 에러가 아닌 info로 처리
INTENTIONAL_LOOP_NODE_TYPES = {
    "for_each",      # 반복 처리
    "while",         # 조건부 반복
    "retry",         # 재시도 로직
    "loop",          # 명시적 루프
    "do_while",      # do-while 패턴
}


class WorkflowComplexityResult:
    """
    워크플로우 복잡도 검증 결과
    
    [③ Gemini 피드백 루프 결합]
    to_gemini_feedback() 메서드를 통해 검증 결과를
    Gemini Self-Correction 프롬프트로 변환할 수 있습니다.
    """
    
    def __init__(self):
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.infos: List[Dict[str, Any]] = []  # 의도적 루프 등 정보성 메시지
        self.is_valid: bool = True
        self.metrics: Dict[str, Any] = {}
    
    def add_error(self, code: str, message: str, node_id: str = None, suggestion: str = None):
        """에러 추가 (실행 차단)"""
        self.errors.append({
            "level": "error",
            "code": code,
            "message": message,
            "node_id": node_id,
            "suggestion": suggestion
        })
        self.is_valid = False
    
    def add_warning(self, code: str, message: str, node_id: str = None, suggestion: str = None):
        """경고 추가 (실행 가능하나 주의 필요)"""
        self.warnings.append({
            "level": "warning",
            "code": code,
            "message": message,
            "node_id": node_id,
            "suggestion": suggestion
        })
    
    def add_info(self, code: str, message: str, node_id: str = None):
        """정보 추가 (의도적 설계로 간주되는 패턴)"""
        self.infos.append({
            "level": "info",
            "code": code,
            "message": message,
            "node_id": node_id
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """결과를 딕셔너리로 변환"""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "infos": self.infos,
            "metrics": self.metrics
        }
    
    def to_gemini_feedback(self) -> str:
        """
        [③ Gemini 피드백 루프 결합]
        
        검증 결과를 Gemini Self-Correction 프롬프트 형식으로 변환.
        CodesignAssistant._attempt_self_correction()에서 활용됩니다.
        
        Returns:
            Gemini에게 전달할 피드백 문자열
        """
        if self.is_valid and not self.warnings:
            return ""
        
        lines = ["[워크플로우 검증 결과 - 수정 필요]"]
        
        if self.errors:
            lines.append("\n❌ 에러 (실행 차단):")
            for err in self.errors:
                lines.append(f"  - [{err['code']}] {err['message']}")
                if err.get('suggestion'):
                    lines.append(f"    → 수정 방법: {err['suggestion']}")
                if err.get('node_id'):
                    lines.append(f"    → 관련 노드: {err['node_id']}")
        
        if self.warnings:
            lines.append("\n⚠️ 경고 (개선 권장):")
            for warn in self.warnings:
                lines.append(f"  - [{warn['code']}] {warn['message']}")
                if warn.get('suggestion'):
                    lines.append(f"    → 권장 조치: {warn['suggestion']}")
        
        lines.append("\n위 문제를 해결한 수정된 워크플로우를 생성하세요.")
        
        return "\n".join(lines)
    
    def has_blocking_errors(self) -> bool:
        """실행을 차단하는 에러가 있는지 확인"""
        return len(self.errors) > 0


def validate_workflow_complexity(
    workflow: Dict[str, Any]
) -> WorkflowComplexityResult:
    """
    [3단계] 워크플로우 복잡도 검증
    
    DynamicWorkflowBuilder에서 LangGraph로 변환하기 전에
    workflow_config의 구조적 문제를 사전 감지합니다.
    
    검증 항목:
    1. 노드/엣지 수 제한 (Pydantic 스키마 기반)
    2. 노드 ID 규칙 (길이, 특수문자)
    3. 순환 참조 감지 (의도치 않은 무한 루프)
    4. 고아 노드 감지 (연결되지 않은 노드)
    5. 병렬 분기 수 (실행 시간 예측)
    6. 설정 크기 (페이로드 제한)
    
    Args:
        workflow: 워크플로우 JSON (nodes, edges)
        
    Returns:
        WorkflowComplexityResult
    """
    result = WorkflowComplexityResult()
    
    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])
    
    # 기본 메트릭 수집
    result.metrics = {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "max_nodes": WORKFLOW_LIMITS["max_nodes"],
        "max_edges": WORKFLOW_LIMITS["max_edges"],
    }
    
    # ────────────────────────────────────────────────
    # 1. 노드/엣지 수 제한 검증
    # ────────────────────────────────────────────────
    if len(nodes) > WORKFLOW_LIMITS["max_nodes"]:
        result.add_error(
            code="WORKFLOW_TOO_MANY_NODES",
            message=f"노드 수({len(nodes)})가 제한({WORKFLOW_LIMITS['max_nodes']})을 초과합니다.",
            suggestion="워크플로우를 여러 Skill로 분할하세요."
        )
    elif len(nodes) > WORKFLOW_LIMITS["max_nodes"] * 0.8:
        result.add_warning(
            code="WORKFLOW_NODE_COUNT_WARNING",
            message=f"노드 수({len(nodes)})가 제한의 80%를 초과합니다.",
            suggestion="워크플로우 분할을 고려하세요."
        )
    
    if len(edges) > WORKFLOW_LIMITS["max_edges"]:
        result.add_error(
            code="WORKFLOW_TOO_MANY_EDGES",
            message=f"엣지 수({len(edges)})가 제한({WORKFLOW_LIMITS['max_edges']})을 초과합니다.",
            suggestion="불필요한 연결을 정리하세요."
        )
    
    # ────────────────────────────────────────────────
    # 2. 노드 ID 규칙 검증
    # ────────────────────────────────────────────────
    for node in nodes:
        node_id = node.get("id", "")
        
        # 길이 검사
        if len(node_id) > WORKFLOW_LIMITS["max_node_id_length"]:
            result.add_error(
                code="NODE_ID_TOO_LONG",
                message=f"노드 ID '{node_id[:30]}...'가 최대 길이({WORKFLOW_LIMITS['max_node_id_length']}자)를 초과합니다.",
                node_id=node_id,
                suggestion="노드 ID를 짧게 줄이세요."
            )
        
        # 빈 ID 검사
        if not node_id or not node_id.strip():
            result.add_error(
                code="NODE_ID_EMPTY",
                message="노드에 ID가 없습니다.",
                suggestion="모든 노드에 고유한 ID를 부여하세요."
            )
        
        # 특수 문자 검사 (LangGraph 노드 이름 호환성)
        invalid_chars = set('<>{}[]|\\^~`')
        found_invalid = [c for c in node_id if c in invalid_chars]
        if found_invalid:
            result.add_warning(
                code="NODE_ID_INVALID_CHARS",
                message=f"노드 ID '{node_id}'에 권장하지 않는 문자가 있습니다: {found_invalid}",
                node_id=node_id,
                suggestion="알파벳, 숫자, 언더스코어, 하이픈 사용을 권장합니다."
            )
    
    # ────────────────────────────────────────────────
    # 3. 순환 참조 감지 (의도적 루프 vs 설계 실수 구분)
    # ────────────────────────────────────────────────
    intentional_cycles, accidental_cycles = _detect_cycles_with_intent(
        nodes, edges, INTENTIONAL_LOOP_NODE_TYPES
    )
    
    # 의도적 루프는 info로 기록 (에러/경고 아님)
    for cycle_info in intentional_cycles:
        cycle_str = " → ".join(cycle_info["path"])
        result.add_info(
            code="WORKFLOW_INTENTIONAL_LOOP",
            message=f"의도적 루프 감지 (제어 노드: {cycle_info['control_node']}): {cycle_str}",
            node_id=cycle_info["control_node"]
        )
    
    # 설계 실수로 인한 순환은 에러로 처리
    for cycle in accidental_cycles:
        cycle_str = " → ".join(cycle)
        result.add_error(
            code="WORKFLOW_ACCIDENTAL_CYCLE",
            message=f"의도치 않은 순환 참조 감지: {cycle_str}",
            suggestion="이 순환은 제어 노드(for_each, while 등)를 통하지 않습니다. 엣지 연결을 확인하세요."
        )
    
    result.metrics["intentional_cycle_count"] = len(intentional_cycles)
    result.metrics["accidental_cycle_count"] = len(accidental_cycles)
    
    # ────────────────────────────────────────────────
    # 4. 고아 노드 감지
    # ────────────────────────────────────────────────
    orphan_nodes = _find_orphan_nodes(nodes, edges)
    for orphan_id in orphan_nodes:
        result.add_warning(
            code="WORKFLOW_ORPHAN_NODE",
            message=f"노드 '{orphan_id}'가 다른 노드와 연결되어 있지 않습니다.",
            node_id=orphan_id,
            suggestion="이 노드를 워크플로우에 연결하거나 제거하세요."
        )
    
    result.metrics["orphan_count"] = len(orphan_nodes)
    
    # ────────────────────────────────────────────────
    # 5. 병렬 분기 수 검증
    # ────────────────────────────────────────────────
    parallel_branches = _count_parallel_branches(nodes, edges)
    max_branches = max(parallel_branches.values()) if parallel_branches else 0
    result.metrics["max_parallel_branches"] = max_branches
    
    for node_id, branch_count in parallel_branches.items():
        if branch_count > WORKFLOW_LIMITS["max_parallel_branches"]:
            result.add_warning(
                code="WORKFLOW_MANY_BRANCHES",
                message=f"노드 '{node_id}'에서 {branch_count}개의 분기가 있습니다. 실행 시간이 길어질 수 있습니다.",
                node_id=node_id,
                suggestion="병렬 분기를 여러 단계로 분할하는 것을 고려하세요."
            )
    
    # ────────────────────────────────────────────────
    # 6. 설정 크기 검증
    # ────────────────────────────────────────────────
    workflow_json = json.dumps(workflow, ensure_ascii=False)
    config_size = len(workflow_json.encode('utf-8'))
    result.metrics["config_size_bytes"] = config_size
    
    if config_size > WORKFLOW_LIMITS["max_config_size_bytes"]:
        result.add_error(
            code="WORKFLOW_CONFIG_TOO_LARGE",
            message=f"워크플로우 설정 크기({config_size:,} bytes)가 제한(1MB)을 초과합니다.",
            suggestion="노드 설정에서 대용량 데이터를 S3로 분리하세요."
        )
    elif config_size > WORKFLOW_LIMITS["max_config_size_bytes"] * 0.7:
        result.add_warning(
            code="WORKFLOW_CONFIG_SIZE_WARNING",
            message=f"워크플로우 설정 크기({config_size:,} bytes)가 제한의 70%를 초과합니다.",
            suggestion="대용량 데이터 분리를 고려하세요."
        )
    
    # ────────────────────────────────────────────────
    # 7. 복잡도 점수 계산
    # ────────────────────────────────────────────────
    complexity_score = _calculate_complexity_score(workflow)
    result.metrics["complexity_score"] = complexity_score
    result.metrics["complexity_level"] = (
        "high" if complexity_score > 100 else
        "medium" if complexity_score > 50 else
        "low"
    )
    
    # ────────────────────────────────────────────────
    # 8. 멱등성 키 체크 (② Idempotency Check)
    # ────────────────────────────────────────────────
    idempotency_issues = _check_idempotency_readiness(workflow)
    for issue in idempotency_issues:
        result.add_warning(
            code=issue["code"],
            message=issue["message"],
            node_id=issue.get("node_id"),
            suggestion=issue.get("suggestion")
        )
    
    result.metrics["idempotency_ready"] = len(idempotency_issues) == 0
    
    return result


def _calculate_complexity_score(workflow: Dict[str, Any]) -> int:
    """
    워크플로우 복잡도 점수 계산
    
    복잡도 요소:
    - 노드 수: 1점/노드
    - for_each/map: 5점 (반복 구조)
    - parallel: 3점 (병렬 분기)
    - group: 2점 (서브그래프)
    - 엣지 밀도: (엣지수/노드수) × 10
    """
    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])
    
    score = len(nodes)  # 기본 노드 수
    
    for node in nodes:
        node_type = node.get("type", "operator")
        if node_type == "for_each":
            score += 5
        elif node_type == "parallel":
            score += 3
        elif node_type == "group":
            score += 2
    
    # 엣지 밀도
    if nodes:
        edge_density = len(edges) / len(nodes)
        score += int(edge_density * 10)
    
    return score


def _detect_cycles_with_intent(
    nodes: List[Dict], 
    edges: List[Dict],
    intentional_types: set
) -> tuple:
    """
    [① 의도적 루프 vs 설계 실수 구분]
    
    DFS 기반 순환 참조 감지 + 의도성 판별
    
    Args:
        nodes: 노드 목록
        edges: 엣지 목록
        intentional_types: 의도적 루프를 허용하는 노드 타입 집합
        
    Returns:
        (intentional_cycles, accidental_cycles) 튜플
        - intentional_cycles: [{"path": [...], "control_node": "..."}]
        - accidental_cycles: [["A", "B", "C", "A"]]
    """
    # 노드 타입 맵 구축
    node_type_map = {n.get("id"): n.get("type", "operator") for n in nodes if n.get("id")}
    
    # 그래프 구축
    graph: Dict[str, List[str]] = {}
    node_ids = set(node_type_map.keys())
    
    for node_id in node_ids:
        graph[node_id] = []
    
    for edge in edges:
        source = edge.get("source")
        target = edge.get("target")
        if source in graph and target in node_ids:
            graph[source].append(target)
    
    intentional_cycles = []
    accidental_cycles = []
    visited = set()
    rec_stack = set()
    path = []
    
    def dfs(node: str):
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                result = dfs(neighbor)
                if result:
                    return result
            elif neighbor in rec_stack:
                # 순환 발견
                cycle_start = path.index(neighbor)
                cycle = path[cycle_start:] + [neighbor]
                return cycle
        
        path.pop()
        rec_stack.remove(node)
        return None
    
    for node_id in graph:
        if node_id not in visited:
            cycle = dfs(node_id)
            if cycle:
                # 순환 경로 내에 의도적 제어 노드가 있는지 확인
                control_nodes_in_cycle = [
                    nid for nid in cycle[:-1]  # 마지막은 시작점 중복
                    if node_type_map.get(nid) in intentional_types
                ]
                
                if control_nodes_in_cycle:
                    # 의도적 루프
                    intentional_cycles.append({
                        "path": cycle,
                        "control_node": control_nodes_in_cycle[0]
                    })
                else:
                    # 설계 실수로 인한 순환
                    accidental_cycles.append(cycle)
                
                # 리셋 후 계속 탐색
                visited.clear()
                rec_stack.clear()
                path.clear()
    
    return intentional_cycles, accidental_cycles


def _check_idempotency_readiness(workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    [② 멱등성(Idempotency) 체크]
    
    배포 안정성을 위한 멱등성 준비 상태 검증:
    1. 노드에 idempotency_key 또는 deterministic ID 패턴 확인
    2. 부작용(side-effect) 가능성이 있는 노드 식별
    
    Returns:
        문제 목록 [{"code": ..., "message": ..., "suggestion": ...}]
    """
    issues = []
    nodes = workflow.get("nodes", [])
    
    # 부작용 가능성이 높은 노드 타입
    side_effect_types = {"api_call", "db_query"}
    
    for node in nodes:
        node_id = node.get("id", "")
        node_type = node.get("type", "operator")
        config = node.get("config", {}) or node.get("data", {}) or {}
        
        # 부작용 노드에 멱등성 키가 없는 경우
        if node_type in side_effect_types:
            has_idempotency = (
                config.get("idempotency_key") or
                config.get("idempotent") or
                config.get("method", "").upper() in ["GET", "HEAD", "OPTIONS"]  # 안전한 HTTP 메서드
            )
            
            if not has_idempotency:
                issues.append({
                    "code": "IDEMPOTENCY_NOT_CONFIGURED",
                    "message": f"'{node_id}' 노드({node_type})에 멱등성 설정이 없습니다.",
                    "node_id": node_id,
                    "suggestion": "config에 idempotency_key를 추가하거나, 멱등한 API 메서드(GET)를 사용하세요."
                })
        
        # UUID 기반이 아닌 순차 ID 패턴 감지
        if node_id and node_id.isdigit():
            issues.append({
                "code": "SEQUENTIAL_NODE_ID",
                "message": f"노드 ID '{node_id}'가 순차 숫자입니다. 배포 간 충돌 위험이 있습니다.",
                "node_id": node_id,
                "suggestion": "UUID 또는 의미 있는 문자열 ID 사용을 권장합니다."
            })
    
    return issues


def _count_parallel_branches(nodes: List[Dict], edges: List[Dict]) -> Dict[str, int]:
    """
    각 노드에서 나가는 병렬 분기 수 계산
    """
    outgoing_count: Dict[str, int] = {}
    
    for edge in edges:
        source = edge.get("source")
        if source:
            outgoing_count[source] = outgoing_count.get(source, 0) + 1
    
    # Parallel 타입 노드만 필터링
    parallel_nodes = {
        n.get("id"): outgoing_count.get(n.get("id"), 0)
        for n in nodes
        if n.get("type") in ["parallel", "route_draft_quality", "conditional"]
    }
    
    return parallel_nodes


def _find_orphan_nodes(nodes: List[Dict], edges: List[Dict]) -> List[str]:
    """
    연결되지 않은 고아 노드 찾기
    """
    node_ids = {n.get("id") for n in nodes if n.get("id")}
    connected_nodes = set()
    
    for edge in edges:
        source = edge.get("source")
        target = edge.get("target")
        if source:
            connected_nodes.add(source)
        if target:
            connected_nodes.add(target)
    
    # 노드가 1개만 있으면 고아 아님
    if len(node_ids) <= 1:
        return []
    
    orphans = node_ids - connected_nodes
    return list(orphans)
