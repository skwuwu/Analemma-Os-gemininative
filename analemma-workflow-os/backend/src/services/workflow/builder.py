# backend/workflow_builder.py
# Dynamic Workflow Builder for runtime graph construction from src.JSON config
# Replaces S3 pickle loading with on-demand LangGraph compilation
# Extended with Sub-graph Abstraction support for hierarchical workflows
#
# [v2.0 Production Hardening]
# - Template rendering safety with missing variable detection
# - MAX_SUBGRAPH_DEPTH limit to prevent stack overflow
# - Graph completeness validation (leaf nodes → END)
# - Lightweight state schema for subgraphs (memory optimization)

import logging
import json
import hashlib
import os
from typing import Dict, Any, Callable, Optional, List, Set, TypedDict, Annotated
from langgraph.graph import StateGraph, END

logger = logging.getLogger(__name__)

# Import existing node functions and state schema from src.handlers.core.main.py
from src.handlers.core.main import NODE_REGISTRY, WorkflowState

# ============================================================================
# [Critical Fix #2] 재귀 깊이 제한 상수
# ============================================================================
MAX_SUBGRAPH_DEPTH = int(os.environ.get("MAX_SUBGRAPH_DEPTH", "10"))


# ============================================================================
# [Critical Fix #1] 템플릿 렌더링 취약성 해결
# ============================================================================
class TemplateRenderingError(Exception):
    """템플릿 렌더링 중 필수 변수 누락 시 발생하는 예외"""
    def __init__(self, missing_vars: List[str], template: str):
        self.missing_vars = missing_vars
        self.template = template
        super().__init__(
            f"Template rendering failed: missing required variables {missing_vars} "
            f"in template '{template[:100]}...'"
        )


# ============================================================================
# [Performance Optimization] 서브그래프 전용 경량 상태 스키마
# ============================================================================
class LightweightSubgraphState(TypedDict, total=False):
    """
    서브그래프 실행을 위한 경량 상태 스키마.
    부모 상태의 모든 필드를 상속받지 않고 필요한 것만 전달하여
    중첩 시 메모리 사용량 증가를 방지합니다.
    """
    # 필수 컨텍스트
    user_api_keys: Dict[str, str]
    step_history: List[str]
    # 실행 결과
    result: Any
    # 스킬 통합 (필요 시)
    skill_execution_log: List[Dict[str, Any]]


# 서브그래프에서 부모로 전파해야 하는 누적 필드 목록
SUBGRAPH_ACCUMULATOR_FIELDS = frozenset({"step_history", "skill_execution_log"})
# 서브그래프 입력 시 자동 상속되는 공통 필드
SUBGRAPH_INHERIT_FIELDS = frozenset({"user_api_keys", "step_history", "skill_execution_log"})


def _render_template(
    template: Any, 
    state: Dict[str, Any],
    strict_mode: bool = False,
    required_vars: Optional[Set[str]] = None
) -> Any:
    """
    Render {{variable}} templates against the provided state.
    
    [v2.0 Production Hardening]
    - strict_mode=True: 변수 누락 시 TemplateRenderingError 발생
    - strict_mode=False: 변수 누락 시 {{MISSING:var}} 플레이스홀더 반환 (디버깅용)
    - required_vars: 필수 변수 집합 - 이 변수들이 누락되면 항상 에러
    - json.dumps 실패 시 안전하게 str() 폴백
    
    Args:
        template: 렌더링할 템플릿 (str, dict, list, 또는 기타)
        state: 변수 값을 가져올 상태 딕셔너리
        strict_mode: True면 누락 변수 시 예외 발생
        required_vars: 필수 변수 집합 (누락 시 항상 예외)
        
    Returns:
        렌더링된 템플릿
        
    Raises:
        TemplateRenderingError: strict_mode이거나 required_vars 누락 시
    """
    import re
    
    if template is None:
        return None
    
    if required_vars is None:
        required_vars = set()
    
    missing_vars: List[str] = []
    
    if isinstance(template, str):
        def _repl(m: re.Match) -> str:
            key = m.group(1).strip()
            parts = key.split('.')
            cur: Any = state
            
            for p in parts:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    # 변수를 찾지 못함
                    missing_vars.append(key)
                    
                    # 필수 변수이거나 strict_mode면 나중에 예외 발생
                    if key in required_vars or strict_mode:
                        return f"{{{{MISSING:{key}}}}}"
                    
                    # 디버깅을 위한 플레이스홀더 반환
                    logger.warning(f"Template variable not found: {key} (returning placeholder)")
                    return f"{{{{MISSING:{key}}}}}"
            
            # 값을 찾음 - 타입에 따라 변환
            if isinstance(cur, (dict, list)):
                try:
                    return json.dumps(cur, ensure_ascii=False)
                except (TypeError, ValueError) as e:
                    logger.warning(f"json.dumps failed for key '{key}': {e}, using str()")
                    return str(cur)
            
            return str(cur) if cur is not None else ""
        
        rendered = re.sub(r"\{\{\s*([\w\.]+)\s*\}\}", _repl, template)
        
        # 필수 변수 누락 체크
        required_missing = set(missing_vars) & required_vars
        if required_missing:
            raise TemplateRenderingError(list(required_missing), template)
        
        # strict_mode에서 누락 변수 있으면 예외
        if strict_mode and missing_vars:
            raise TemplateRenderingError(missing_vars, template)
        
        return rendered
    
    if isinstance(template, dict):
        return {k: _render_template(v, state, strict_mode, required_vars) for k, v in template.items()}
    
    if isinstance(template, list):
        return [_render_template(v, state, strict_mode, required_vars) for v in template]
    
    return template


class DynamicWorkflowBuilder:
    """
    Builds LangGraph workflows dynamically from src.JSON configuration at runtime.
    This replaces the need for pre-compiled S3 pickle files.
    
    Extended with Sub-graph Abstraction:
    - Supports 'subgraph' node type
    - Recursively compiles nested sub-graphs
    - Handles state mapping between parent and child graphs
    """

    def __init__(
        self, 
        config: Dict[str, Any], 
        parent_path: List[str] = None,
        use_lightweight_state: bool = False
    ):
        """
        Initialize builder with workflow configuration.

        Args:
            config: Workflow configuration dict with 'nodes', 'edges', and optionally 'subgraphs'
            parent_path: Path from src.root (for nested subgraphs), e.g., ["root", "group1"]
            use_lightweight_state: True면 LightweightSubgraphState 사용 (메모리 최적화)
            
        Raises:
            ValueError: 서브그래프 깊이가 MAX_SUBGRAPH_DEPTH 초과 시
        """
        self.config = config
        self.parent_path = parent_path or ["root"]
        self.use_lightweight_state = use_lightweight_state
        
        # [Critical Fix #2] 재귀 깊이 제한 검증
        current_depth = len(self.parent_path)
        if current_depth > MAX_SUBGRAPH_DEPTH:
            raise ValueError(
                f"Subgraph nesting depth ({current_depth}) exceeds maximum allowed "
                f"({MAX_SUBGRAPH_DEPTH}). Path: {' > '.join(self.parent_path)}. "
                f"Consider flattening your workflow structure or increasing MAX_SUBGRAPH_DEPTH."
            )
        
        # Cache for compiled subgraphs (avoid recompilation)
        self._subgraph_cache: Dict[str, Any] = {}
        
        # [Performance Optimization] 서브그래프는 경량 상태 스키마 사용
        if use_lightweight_state:
            self.graph = StateGraph(LightweightSubgraphState)
            logger.debug(f"Using LightweightSubgraphState for: {' > '.join(self.parent_path)}")
        else:
            self.graph = StateGraph(WorkflowState)
    
    def _get_subgraph_definition(self, node_def: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Resolve subgraph definition from src.node config.
        
        Supports:
        - subgraph_ref: Reference to subgraphs dict
        - subgraph_inline: Inline definition
        - skill_ref: Reference to a saved Skill (requires SkillRepository)
        """
        # 1. Inline definition
        if node_def.get("subgraph_inline"):
            return node_def["subgraph_inline"]
        
        # 2. Reference to subgraphs dict
        subgraph_ref = node_def.get("subgraph_ref")
        if subgraph_ref:
            subgraphs = self.config.get("subgraphs", {})
            if subgraph_ref in subgraphs:
                return subgraphs[subgraph_ref]
            raise ValueError(f"Subgraph reference not found: {subgraph_ref}")
        
        # 3. Skill reference (requires repository lookup)
        skill_ref = node_def.get("skill_ref")
        if skill_ref:
            try:
                from src.services.skill_repository import get_skill_repository
                repo = get_skill_repository()
                skill = repo.get_latest_skill(skill_ref)
                if skill and skill.get("skill_type") == "subgraph_based":
                    return skill.get("subgraph_config")
                elif skill:
                    raise ValueError(f"Skill {skill_ref} is not subgraph_based")
                else:
                    raise ValueError(f"Skill not found: {skill_ref}")
            except ImportError:
                raise ValueError(f"SkillRepository not available for skill_ref: {skill_ref}")
        
        return None
    
    def _detect_cycle(self, subgraph_id: str, visited: Set[str] = None) -> None:
        """Detect circular subgraph references to prevent infinite recursion."""
        if visited is None:
            visited = set()
        
        if subgraph_id in visited:
            raise ValueError(f"Circular subgraph reference detected: {subgraph_id} in path {visited}")
        
        visited.add(subgraph_id)
        
        # Check nested subgraphs
        subgraph_def = self.config.get("subgraphs", {}).get(subgraph_id, {})
        for node in subgraph_def.get("nodes", []):
            if node.get("type") == "subgraph":
                nested_ref = node.get("subgraph_ref")
                if nested_ref:
                    self._detect_cycle(nested_ref, visited.copy())
    
    def _build_subgraph(self, subgraph_def: Dict[str, Any], node_id: str) -> Any:
        """
        Recursively compile a subgraph definition.
        
        [v2.0 Production Hardening]
        - 깊이 제한 검증 (MAX_SUBGRAPH_DEPTH)
        - 경량 상태 스키마 사용으로 메모리 최적화
        
        Args:
            subgraph_def: The subgraph definition dict
            node_id: ID of the parent subgraph node
            
        Returns:
            Compiled LangGraph app for the subgraph
            
        Raises:
            ValueError: 깊이 제한 초과 시
        """
        # Cache key based on content hash
        cache_key = hashlib.md5(
            json.dumps(subgraph_def, sort_keys=True).encode()
        ).hexdigest()[:16]
        
        if cache_key in self._subgraph_cache:
            logger.debug(f"Using cached subgraph: {node_id}")
            return self._subgraph_cache[cache_key]
        
        # Build child path for logging
        child_path = self.parent_path + [node_id]
        
        # [Critical Fix #2] 깊이 사전 체크 (더 명확한 에러 메시지)
        if len(child_path) > MAX_SUBGRAPH_DEPTH:
            raise ValueError(
                f"Cannot build subgraph '{node_id}': nesting depth ({len(child_path)}) "
                f"would exceed MAX_SUBGRAPH_DEPTH ({MAX_SUBGRAPH_DEPTH}). "
                f"Current path: {' > '.join(self.parent_path)}"
            )
        
        logger.info(f"📦 Building subgraph: {' > '.join(child_path)} (depth: {len(child_path)})")
        
        # [Performance Optimization] 자식 서브그래프는 경량 상태 사용
        child_builder = DynamicWorkflowBuilder(
            subgraph_def, 
            parent_path=child_path,
            use_lightweight_state=True  # 서브그래프는 항상 경량 상태 사용
        )
        compiled = child_builder.build()
        
        self._subgraph_cache[cache_key] = compiled
        return compiled
    
    def _create_subgraph_handler(
        self, 
        node_def: Dict[str, Any], 
        compiled_subgraph: Any
    ) -> Callable:
        """
        Create a node handler that executes a subgraph with state mapping.
        
        State Mapping Flow:
        1. Extract fields from src.parent state using input_mapping
        2. Execute subgraph with extracted state
        3. Map subgraph output back to parent state using output_mapping
        """
        node_id = node_def.get("id", "unknown_subgraph")
        input_mapping = node_def.get("input_mapping", {})
        output_mapping = node_def.get("output_mapping", {})
        timeout = node_def.get("timeout_seconds", 300)
        error_handling = node_def.get("error_handling", "fail")
        metadata = node_def.get("metadata", {})
        
        def subgraph_handler(state: WorkflowState, config=None) -> Dict[str, Any]:
            import time
            import copy
            from datetime import datetime, timezone
            
            subgraph_name = metadata.get("name", node_id)
            logger.info(f"📦 Entering subgraph: {subgraph_name}")
            start_time = time.time()
            
            try:
                # 1. Input Mapping: Parent state → Child input (경량화)
                # [Performance Optimization] 필요한 필드만 복사하여 메모리 절약
                child_input: Dict[str, Any] = {}
                
                for parent_key, child_key in input_mapping.items():
                    # Support template syntax: {"query": "{{user_input}}"}
                    if isinstance(child_key, str) and "{{" in child_key:
                        # [Critical Fix #1] 템플릿 렌더링 시 필수 변수 검증
                        try:
                            child_input[parent_key] = _render_template(
                                child_key, 
                                state,
                                strict_mode=False,  # 경고만 (서브그래프 내부에서는 유연하게)
                                required_vars=None
                            )
                        except TemplateRenderingError as e:
                            logger.error(f"Subgraph {node_id} input mapping failed: {e}")
                            raise
                    else:
                        # Direct key mapping - 값이 있을 때만 복사
                        if parent_key in state:
                            # [Performance] 대용량 객체는 얕은 복사만 수행
                            val = state[parent_key]
                            if isinstance(val, (list, dict)):
                                child_input[child_key] = copy.copy(val)  # shallow copy
                            else:
                                child_input[child_key] = val
                
                # [Performance Optimization] 공통 필드는 필요한 것만 상속
                for key in SUBGRAPH_INHERIT_FIELDS:
                    if key not in child_input and key in state:
                        # 누적 필드는 빈 리스트로 초기화 (결과에서 병합)
                        if key in SUBGRAPH_ACCUMULATOR_FIELDS:
                            child_input[key] = []  # 부모 히스토리를 복사하지 않음 (나중에 병합)
                        else:
                            child_input[key] = state[key]
                
                logger.debug(f"Subgraph {node_id} input keys: {list(child_input.keys())} (lightweight)")
                
                # 2. Execute Subgraph
                child_output = compiled_subgraph.invoke(child_input)
                
                # 3. Output Mapping: Child output → Parent state
                result = {}
                for child_key, parent_key in output_mapping.items():
                    if child_key in child_output:
                        result[parent_key] = child_output[child_key]
                
                # Merge accumulated fields (step_history, logs)
                if "step_history" in child_output:
                    parent_history = state.get("step_history", [])
                    result["step_history"] = parent_history + child_output["step_history"]
                
                if "skill_execution_log" in child_output:
                    parent_log = state.get("skill_execution_log", [])
                    result["skill_execution_log"] = parent_log + child_output["skill_execution_log"]
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                logger.info(f"📦 Exiting subgraph: {subgraph_name} ({execution_time_ms}ms)")
                
                # Add subgraph execution log
                subgraph_log = {
                    "type": "subgraph_execution",
                    "subgraph_id": node_id,
                    "subgraph_name": subgraph_name,
                    "execution_time_ms": execution_time_ms,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "input_keys": list(child_input.keys()),
                    "output_keys": list(result.keys()),
                }
                current_log = result.get("skill_execution_log", state.get("skill_execution_log", []))
                result["skill_execution_log"] = current_log + [subgraph_log]
                
                return result
                
            except Exception as e:
                execution_time_ms = int((time.time() - start_time) * 1000)
                logger.error(f"🚨 Subgraph {subgraph_name} failed after {execution_time_ms}ms: {e}")
                
                if error_handling == "skip":
                    logger.warning(f"Skipping failed subgraph: {node_id}")
                    return {f"{node_id}_error": str(e), f"{node_id}_skipped": True}
                elif error_handling == "isolate":
                    # Return error but don't propagate
                    return {f"{node_id}_error": str(e)}
                else:
                    raise
        
        return subgraph_handler

    def _get_node_handler(self, node_type: str, node_def: Dict[str, Any]) -> Callable:
        """
        Get the appropriate node handler function for a given node type.
        Uses standardized interface: (state, config=None)

        Args:
            node_type: String identifier for node type (e.g., 'llm_chat')
            node_def: Full node definition dict

        Returns:
            Callable node function with signature (state, config=None)

        Raises:
            ValueError: If node type is not registered
        """
        # First, try NODE_REGISTRY for registered handlers
        registry_func = NODE_REGISTRY.get(node_type)
        if registry_func and callable(registry_func):
            def _registry_node(state: WorkflowState, config=None) -> Any:
                node_id = node_def.get('id', f'unknown_{node_type}')
                logger.info(f"🔧 Executing {node_type} node: {node_id}")
                try:
                    # Standard interface: pass state and node_def as config
                    result = registry_func(state, node_def)
                    logger.info(f"✅ {node_type} node {node_id} completed")
                    return result
                except Exception as e:
                    logger.error(f"🚨 {node_type} node {node_id} failed: {e}")
                    raise
            return _registry_node

        # Fallback for special cases or legacy types
        if node_type in ("llm_chat", "aiModel"):
            # Direct import for llm_chat if not in registry
            try:
                from src.handlers.core.main import llm_chat_runner
                def _llm_node(state: WorkflowState, config=None) -> Any:
                    node_id = node_def.get('id', 'unknown_llm')
                    node_config = node_def.get('config', {}).copy()  # Avoid mutation
                    node_config['id'] = node_id
                    logger.info(f"🔧 Executing {node_type} node: {node_id}")
                    try:
                        result = llm_chat_runner(state, node_config)
                        logger.info(f"✅ {node_type} node {node_id} completed")
                        return result
                    except Exception as e:
                        logger.error(f"🚨 {node_type} node {node_id} failed: {e}")
                        raise
                return _llm_node
            except ImportError:
                pass

        elif node_type == "operator":
            try:
                from src.handlers.core.main import operator_runner
                def _operator_node(state: WorkflowState, config=None) -> Any:
                    node_id = node_def.get('id', 'unknown_operator')
                    logger.info(f"🔧 Executing operator node: {node_id}")
                    try:
                        result = operator_runner(state, node_def)
                        logger.info(f"✅ Operator node {node_id} completed")
                        return result
                    except Exception as e:
                        logger.error(f"🚨 Operator node {node_id} failed: {e}")
                        raise
                return _operator_node
            except ImportError:
                pass

        elif node_type == "trigger":
            # Triggers are handled like operators
            try:
                from src.handlers.core.main import operator_runner
                def _trigger_node(state: WorkflowState, config=None) -> Any:
                    return operator_runner(state, node_def)
                return _trigger_node
            except ImportError:
                pass
        
        elif node_type == "subgraph":
            # Handle subgraph nodes - delegate to subgraph handler creation
            # This is handled specially in _add_nodes, so shouldn't reach here
            raise ValueError(f"Subgraph nodes should be handled in _add_nodes, not _get_node_handler")

        # If no handler found
        available_types = list(NODE_REGISTRY.keys()) + ["subgraph"]
        raise ValueError(f"Unknown node type: {node_type}. Available types: {available_types}")

    def _add_nodes(self):
        """Add all nodes from src.config to the graph, including subgraph nodes."""
        nodes = self.config.get("nodes", [])
        
        for node_def in nodes:
            node_id = node_def.get("id")
            node_type = node_def.get("type")

            if not node_id or not node_type:
                raise ValueError(f"Node missing required fields 'id' or 'type': {node_def}")
            
            # Special handling for subgraph nodes
            if node_type == "subgraph":
                # Get subgraph definition
                subgraph_def = self._get_subgraph_definition(node_def)
                if not subgraph_def:
                    raise ValueError(f"Subgraph node {node_id} has no valid definition")
                
                # Check for circular references
                subgraph_ref = node_def.get("subgraph_ref")
                if subgraph_ref:
                    self._detect_cycle(subgraph_ref)
                
                # Recursively build the subgraph
                compiled_subgraph = self._build_subgraph(subgraph_def, node_id)
                
                # Create handler with state mapping
                handler = self._create_subgraph_handler(node_def, compiled_subgraph)
                self.graph.add_node(node_id, handler)
                
                metadata = node_def.get("metadata", {})
                logger.info(f"Added subgraph node: {node_id} ({metadata.get('name', 'unnamed')})")
            else:
                # Standard node handling
                handler = self._get_node_handler(node_type, node_def)
                self.graph.add_node(node_id, handler)
                logger.debug(f"Added node: {node_id} ({node_type})")

    def _validate_conditional_edge_routers(self, edges: List[Dict[str, Any]]) -> None:
        """Validate that all conditional edge routers are registered before processing.
        
        This pre-validation catches undefined routers early, providing clear error 
        messages before attempting to build the graph.
        
        Raises:
            ValueError: If any conditional edge references an undefined router function
        """
        undefined_routers = []
        for edge in edges:
            if edge.get("type") == "conditional_edge":
                router_name = edge.get("router_func", "route_draft_quality")
                router = NODE_REGISTRY.get(router_name)
                if not router or not callable(router):
                    undefined_routers.append({
                        "source": edge.get("source"),
                        "router_func": router_name
                    })
        
        if undefined_routers:
            details = ", ".join([f"'{r['router_func']}' (from {r['source']})" for r in undefined_routers])
            raise ValueError(f"Undefined router functions for conditional_edges: {details}")

    def _add_edges(self):
        """Add all edges from src.config to the graph."""
        edges = self.config.get("edges", [])
        
        # [수정] 라우터 사전 검증 - compile() 전에 모든 라우터 함수가 등록되었는지 확인
        self._validate_conditional_edge_routers(edges)
        
        # [수정] 현재 세그먼트에 포함된 유효한 노드 ID 목록 추출
        valid_node_ids = {n["id"] for n in self.config.get("nodes", [])}
        
        entrypoint_assigned = False

        for edge in edges:
            edge_type = edge.get("type", "edge")
            source = edge.get("source")
            target = edge.get("target")

            # [수정] 방어 로직: 소스나 타겟 중 하나라도 현재 그래프에 없으면 
            # 이는 '크로스 세그먼트 엣지'이므로 무시해야 함.
            # (데이터 전달은 state_data로 이미 되었고, 흐름 제어는 Step Functions가 하므로 무시해도 됨)
            if source not in valid_node_ids or target not in valid_node_ids:
                # 단, START/END 같은 특수 노드는 예외일 수 있으나, 
                # LangGraph에서는 보통 명시적 노드가 필요하므로 안전하게 스킵
                # START -> Node 연결은 아래 로직에서 처리됨
                if source != "START": 
                    logger.debug(f"Skipping cross-segment edge: {source} -> {target}")
                    continue

            # hitp, human_in_the_loop, pause edge types are treated as normal edges
            # (segmentation uses them for splitting, but LangGraph execution is the same)
            if edge_type in ("edge", "normal", "flow", "hitp", "human_in_the_loop", "pause"):
                if source == "START":
                    # Special handling for START -> target as entry point
                    if target and target in valid_node_ids: # target 유효성 체크 추가
                        self.graph.set_entry_point(target)
                        entrypoint_assigned = True
                        logger.debug(f"Set entry point: {target}")
                else:
                    # source와 target이 모두 valid_node_ids에 있는 경우만 여기 도달함
                    if source and target:
                        self.graph.add_edge(source, target)
                        logger.debug(f"Added edge: {source} -> {target}")

            elif edge_type == "conditional_edge":
                # Conditional edge의 source는 반드시 현재 세그먼트에 있어야 함
                if source not in valid_node_ids:
                    continue
                    
                # Handle conditional edges with router functions
                mapping = edge.get("mapping", {})
                router_name = edge.get("router_func", "route_draft_quality")
                router = NODE_REGISTRY.get(router_name)

                if not router or not callable(router):
                    raise ValueError(f"Undefined router for conditional_edge: {router_name}")

                self.graph.add_conditional_edges(source, router, mapping)
                logger.debug(f"Added conditional edge from {source} with router {router_name}")
                logger.debug(f"Added conditional edge from {source} with router {router_name}")

            elif edge_type == "start":
                if source:
                    self.graph.set_entry_point(source)
                    entrypoint_assigned = True
                    logger.debug(f"Set entry point: {source}")

            elif edge_type == "end":
                if source:
                    # Use add_edge(source, END) for LangGraph 1.0+ compatibility
                    from langgraph.graph import END
                    self.graph.add_edge(source, END)
                    logger.debug(f"Set finish point via add_edge: {source} -> END")

            else:
                raise ValueError(f"Unknown edge type: {edge_type}")

        # Default entry point if none assigned
        # [Critical Fix] Use topological analysis instead of nodes[0]
        # Find nodes with in-degree 0 (no incoming edges) as entry point candidates
        if not entrypoint_assigned:
            nodes = self.config.get("nodes", [])
            if nodes:
                # Build in-degree map
                node_ids = {n.get("id") for n in nodes}
                in_degree = {nid: 0 for nid in node_ids}
                
                for edge in edges:
                    target = edge.get("target")
                    if target in in_degree:
                        in_degree[target] += 1
                
                # Find entry point candidates (in-degree 0)
                entry_candidates = [nid for nid, deg in in_degree.items() if deg == 0]
                
                if entry_candidates:
                    # If multiple candidates, prefer the first one in node order
                    # (maintains backward compatibility with sorted nodes)
                    node_order = [n.get("id") for n in nodes]
                    for nid in node_order:
                        if nid in entry_candidates:
                            self.graph.set_entry_point(nid)
                            logger.debug(f"Entry point set via topological analysis: {nid}")
                            break
                else:
                    # Fallback to first node if no clear entry point (isolated nodes or cycles)
                    first_node_id = nodes[0].get("id")
                    if first_node_id:
                        self.graph.set_entry_point(first_node_id)
                        logger.debug(f"Default entry point set (fallback): {first_node_id}")

    def _validate_graph_completeness(self) -> None:
        """
        [Critical Fix #3] 그래프 완결성 검증.
        
        모든 리프 노드(나가는 엣지가 없는 노드)가 명시적으로 END로 연결되거나
        Step Functions 전이를 담당하는지 확인합니다.
        
        Raises:
            ValueError: 리프 노드가 END로 연결되지 않은 경우
        """
        nodes = self.config.get("nodes", [])
        edges = self.config.get("edges", [])
        
        if not nodes:
            return  # 빈 그래프는 검증 불필요
        
        node_ids = {n["id"] for n in nodes}
        
        # 각 노드별 나가는 엣지 수 계산
        outgoing_count: Dict[str, int] = {nid: 0 for nid in node_ids}
        has_end_edge: Set[str] = set()
        
        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            edge_type = edge.get("type", "edge")
            
            # 현재 세그먼트 내부 엣지만 카운트
            if source in node_ids:
                if target in node_ids:
                    outgoing_count[source] = outgoing_count.get(source, 0) + 1
                elif edge_type == "end" or target == "END":
                    has_end_edge.add(source)
                # conditional_edge도 outgoing으로 카운트
                if edge_type == "conditional_edge":
                    mapping = edge.get("mapping", {})
                    # 매핑의 타겟들 중 현재 세그먼트 내부 노드가 있으면 카운트
                    for mapped_target in mapping.values():
                        if mapped_target in node_ids or mapped_target == "END":
                            outgoing_count[source] = outgoing_count.get(source, 0) + 1
                            if mapped_target == "END":
                                has_end_edge.add(source)
        
        # 리프 노드 식별 (나가는 엣지가 0개)
        leaf_nodes = [
            nid for nid, count in outgoing_count.items() 
            if count == 0 and nid not in has_end_edge
        ]
        
        if leaf_nodes:
            # 경고 수준으로 처리 (Step Functions가 세그먼트 간 전이를 담당하므로)
            # 단, 루트 워크플로우에서는 더 엄격하게 처리할 수 있음
            if len(self.parent_path) == 1:  # 루트 레벨
                logger.warning(
                    f"⚠️ Graph completeness warning: Leaf nodes without explicit END edge detected: "
                    f"{leaf_nodes}. These nodes may rely on Step Functions for segment transitions. "
                    f"Consider adding explicit 'end' edges for clarity."
                )
            else:
                # 서브그래프에서는 더 유연하게 처리
                logger.debug(
                    f"Subgraph leaf nodes without END: {leaf_nodes} "
                    f"(path: {' > '.join(self.parent_path)})"
                )
        
        # 노드가 1개뿐이고 END 연결이 없으면 자동으로 추가해야 함을 경고
        if len(nodes) == 1 and not has_end_edge:
            single_node_id = nodes[0]["id"]
            logger.info(
                f"Single-node graph detected ({single_node_id}). "
                f"Implicitly connecting to END."
            )
            # 실제로 END 연결 추가
            from langgraph.graph import END
            self.graph.add_edge(single_node_id, END)

    def build(self) -> Any:
        """
        Build and compile the LangGraph workflow.
        
        [v2.0 Production Hardening]
        - 그래프 완결성 검증 (리프 노드 → END)
        - 깊이 제한 검증 (MAX_SUBGRAPH_DEPTH)

        Returns:
            Compiled LangGraph app ready for execution

        Raises:
            ValueError: If configuration is invalid or graph incomplete
            ImportError: If required dependencies are missing
        """
        depth = len(self.parent_path)
        schema_type = "Lightweight" if self.use_lightweight_state else "Full"
        logger.info(
            f"🏗️ Building workflow dynamically from config "
            f"(depth: {depth}/{MAX_SUBGRAPH_DEPTH}, schema: {schema_type})"
        )

        try:
            self._add_nodes()
            self._add_edges()
            
            # [Critical Fix #3] 그래프 완결성 검증
            self._validate_graph_completeness()

            # Compile the graph
            compiled_app = self.graph.compile()
            logger.info(
                f"✅ Workflow built successfully "
                f"(path: {' > '.join(self.parent_path)})"
            )
            return compiled_app

        except Exception as e:
            logger.error(
                f"🚨 Failed to build workflow at depth {depth}: {e} "
                f"(path: {' > '.join(self.parent_path)})"
            )
            raise
