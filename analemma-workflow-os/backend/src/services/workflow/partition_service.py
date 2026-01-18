import os
import json
import logging
import threading
from typing import Dict, Any, List, Set, Optional, Tuple, FrozenSet
from src.services.workflow.repository import WorkflowRepository

logger = logging.getLogger(__name__)

# ============================================================================
# [v2.0 Production Hardening] 상수 및 설정
# ============================================================================

# 최대 재귀 깊이 제한 (무한 루프 방지)
MAX_PARTITION_DEPTH = int(os.environ.get("MAX_PARTITION_DEPTH", "50"))

# 최대 노드 수 제한 (대규모 그래프 보호)
MAX_NODES_LIMIT = int(os.environ.get("MAX_NODES_LIMIT", "500"))

# LLM 노드 타입들 - 이 타입들을 만날 때마다 세그먼트를 분할합니다
LLM_NODE_TYPES: FrozenSet[str] = frozenset({
    "llm_chat",
    "openai_chat", 
    "anthropic_chat",
    "bedrock_chat",
    "claude_chat",
    "gpt_chat",
    "aiModel"  # 범용 AI 모델 노드 타입 추가
})

# HITP (Human in the Loop) 엣지 타입들
HITP_EDGE_TYPES: FrozenSet[str] = frozenset({"hitp", "human_in_the_loop", "pause"})

# 세그먼트 타입들
SEGMENT_TYPES: FrozenSet[str] = frozenset({
    "normal", "llm", "hitp", "isolated", "complete", "parallel_group", "aggregator"
})


# ============================================================================
# [Critical Fix #1] 사이클 감지 예외 및 DAG 검증
# ============================================================================

class CycleDetectedError(Exception):
    """그래프에서 사이클(순환 참조)이 감지되었을 때 발생하는 예외"""
    def __init__(self, cycle_path: List[str]):
        self.cycle_path = cycle_path
        super().__init__(
            f"Cycle detected in workflow graph: {' -> '.join(cycle_path)}. "
            f"Workflows must be DAGs (Directed Acyclic Graphs)."
        )


class PartitionDepthExceededError(Exception):
    """파티셔닝 재귀 깊이 초과 시 발생하는 예외"""
    def __init__(self, depth: int, max_depth: int):
        self.depth = depth
        self.max_depth = max_depth
        super().__init__(
            f"Partition recursion depth ({depth}) exceeded maximum ({max_depth}). "
            f"Consider simplifying the workflow or increasing MAX_PARTITION_DEPTH."
        )


class BranchTerminationError(Exception):
    """브랜치가 올바르게 종료되지 않았을 때 발생하는 예외"""
    def __init__(self, branch_id: str, message: str):
        self.branch_id = branch_id
        super().__init__(f"Branch '{branch_id}' termination error: {message}")


def validate_dag(
    nodes: Dict[str, Any], 
    outgoing_edges: Dict[str, List[Dict[str, Any]]]
) -> Tuple[bool, Optional[List[str]]]:
    """
    [Critical Fix #1] 그래프가 DAG(Directed Acyclic Graph)인지 검증합니다.
    
    Kahn's Algorithm (위상 정렬) 기반 사이클 감지.
    
    Args:
        nodes: 노드 ID -> 노드 정의 맵
        outgoing_edges: 노드 ID -> 나가는 엣지 리스트 맵
        
    Returns:
        Tuple[is_dag, cycle_path]: DAG이면 (True, None), 아니면 (False, cycle_path)
    """
    if not nodes:
        return True, None
    
    # 진입 차수(in-degree) 계산
    in_degree: Dict[str, int] = {nid: 0 for nid in nodes}
    
    for source_id, edges in outgoing_edges.items():
        for edge in edges:
            target = edge.get("target")
            if target and target in in_degree:
                in_degree[target] += 1
    
    # 진입 차수가 0인 노드들로 시작
    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    visited_count = 0
    
    while queue:
        node_id = queue.pop(0)
        visited_count += 1
        
        for edge in outgoing_edges.get(node_id, []):
            target = edge.get("target")
            if target and target in in_degree:
                in_degree[target] -= 1
                if in_degree[target] == 0:
                    queue.append(target)
    
    # 모든 노드를 방문하지 못했다면 사이클 존재
    if visited_count < len(nodes):
        # 사이클 경로 추적 (DFS로 실제 사이클 찾기)
        cycle_path = _find_cycle_path(nodes, outgoing_edges)
        return False, cycle_path
    
    return True, None


def _find_cycle_path(
    nodes: Dict[str, Any], 
    outgoing_edges: Dict[str, List[Dict[str, Any]]]
) -> List[str]:
    """DFS로 실제 사이클 경로를 추적합니다."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = {nid: WHITE for nid in nodes}
    parent: Dict[str, Optional[str]] = {nid: None for nid in nodes}
    
    def dfs(node_id: str, path: List[str]) -> Optional[List[str]]:
        color[node_id] = GRAY
        path.append(node_id)
        
        for edge in outgoing_edges.get(node_id, []):
            target = edge.get("target")
            if target and target in color:
                if color[target] == GRAY:
                    # 사이클 발견 - 경로 추출
                    cycle_start = path.index(target)
                    return path[cycle_start:] + [target]
                elif color[target] == WHITE:
                    result = dfs(target, path)
                    if result:
                        return result
        
        color[node_id] = BLACK
        path.pop()
        return None
    
    for nid in nodes:
        if color[nid] == WHITE:
            result = dfs(nid, [])
            if result:
                return result
    
    return ["unknown_cycle"]  # 폴백


def partition_workflow_advanced(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    고급 워크플로우 분할: HITP 엣지와 LLM 노드 기반으로 세그먼트를 생성합니다.
    
    [v2.0 Production Hardening]
    - DAG(Directed Acyclic Graph) 사전 검증
    - 재귀 깊이 제한 (MAX_PARTITION_DEPTH)
    - 합류점(Convergence Node) 강제 분할
    - 브랜치 종료 검증
    - Thread-safe ID 생성
    
    개선된 알고리즘:
    - 병합 지점(Merge Point) 감지 및 처리
    - 병렬 그룹(Parallel Group) 생성 및 재귀적 파티셔닝
    - Convergence Node 찾기 및 브랜치 제한
    - 재귀적 Node-to-Segment 매핑
    
    Raises:
        CycleDetectedError: 그래프에 사이클이 있는 경우
        PartitionDepthExceededError: 재귀 깊이 초과 시
        ValueError: 노드 수 제한 초과 시
    """
    nodes = {n["id"]: n for n in config.get("nodes", [])}
    edges = config.get("edges", []) if config.get("edges") else []
    
    # [Critical Fix] 노드 수 제한 검증
    if len(nodes) > MAX_NODES_LIMIT:
        raise ValueError(
            f"Workflow has {len(nodes)} nodes, exceeding maximum limit of {MAX_NODES_LIMIT}. "
            f"Consider splitting into subgraphs."
        )
    
    # [Performance Optimization] 엣지 맵 생성 (Pre-indexed)
    # 향후 워크플로우 저장 시점에 메타데이터로 추출하여 재사용 가능
    incoming_edges: Dict[str, List[Dict[str, Any]]] = {}
    outgoing_edges: Dict[str, List[Dict[str, Any]]] = {}
    
    for edge in edges:
        source = edge.get("source")
        target = edge.get("target") 
        if source:
            outgoing_edges.setdefault(source, []).append(edge)
        if target:
            incoming_edges.setdefault(target, []).append(edge)
    
    # [Critical Fix #1] DAG 검증 - 사이클 감지
    is_dag, cycle_path = validate_dag(nodes, outgoing_edges)
    if not is_dag:
        raise CycleDetectedError(cycle_path or ["unknown"])
    
    # [Critical Fix #2] 합류점 집합 - 이 노드들은 반드시 새 세그먼트 시작점이 됨
    # find_convergence_node로 찾은 모든 합류점을 미리 수집
    forced_segment_starts: Set[str] = set()
    
    # [Performance Optimization] Thread-safe ID 생성기
    class ThreadSafeIdGenerator:
        def __init__(self): 
            self._val = -1
            self._lock = threading.Lock()
        
        def next(self) -> int:
            with self._lock:
                self._val += 1
                return self._val
        
        @property
        def current(self) -> int:
            return self._val
    
    seg_id_gen = ThreadSafeIdGenerator()
    stats = {"llm": 0, "hitp": 0, "parallel_groups": 0, "branches": 0}
    
    # --- Helper: 합류 지점(Convergence Node) 찾기 ---
    def find_convergence_node(start_nodes: List[str]) -> Optional[str]:
        """
        브랜치들이 공통으로 도달하는 첫 번째 Merge Point를 찾습니다.
        in-degree > 1인 노드를 후보로 봅니다.
        
        [Critical Fix #2] 찾은 합류점은 forced_segment_starts에 등록되어
        반드시 새 세그먼트의 시작점이 됩니다.
        """
        queue = list(start_nodes)
        seen = set(queue)
        
        while queue:
            node_id = queue.pop(0)
            # Merge Point 후보 확인
            if len(incoming_edges.get(node_id, [])) > 1:
                if node_id not in start_nodes:
                    # [Critical Fix #2] 합류점은 반드시 새 세그먼트 시작점
                    forced_segment_starts.add(node_id)
                    logger.debug(f"Convergence node registered as forced segment start: {node_id}")
                    return node_id
            
            for out_edge in outgoing_edges.get(node_id, []):
                target = out_edge.get("target")
                if target and target not in seen:
                    seen.add(target)
                    queue.append(target)
        return None
    
    # --- [Critical Fix] 위상 정렬 헬퍼 ---
    def _topological_sort_nodes(nodes_map: Dict[str, Any], edges_list: List[Dict]) -> List[Dict[str, Any]]:
        """
        세그먼트 내 노드들을 위상 정렬하여 실행 순서대로 반환합니다.
        
        DynamicWorkflowBuilder는 nodes[0]을 entry point로 사용하므로,
        첫 번째 노드가 실제 시작 노드가 되어야 합니다.
        
        Args:
            nodes_map: {node_id: node_config} 매핑
            edges_list: 세그먼트 내부 엣지 리스트
            
        Returns:
            위상 정렬된 노드 설정 리스트
        """
        if len(nodes_map) <= 1:
            return list(nodes_map.values())
        
        # 세그먼트 내 노드 ID 집합
        node_ids = set(nodes_map.keys())
        
        # 인접 리스트 및 진입 차수(in-degree) 계산
        in_degree = {nid: 0 for nid in node_ids}
        adj = {nid: [] for nid in node_ids}
        
        for edge in edges_list:
            src = edge.get("source")
            tgt = edge.get("target")
            if src in node_ids and tgt in node_ids:
                adj[src].append(tgt)
                in_degree[tgt] += 1
        
        # Kahn's Algorithm: 진입 차수가 0인 노드부터 시작
        queue = [nid for nid in node_ids if in_degree[nid] == 0]
        sorted_ids = []
        
        while queue:
            # 안정적인 순서를 위해 정렬 (알파벳 순)
            queue.sort()
            node_id = queue.pop(0)
            sorted_ids.append(node_id)
            
            for neighbor in adj[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # 정렬되지 않은 노드가 있으면 (사이클 또는 연결 안됨) 원래 순서로 추가
        if len(sorted_ids) < len(node_ids):
            remaining = [nid for nid in nodes_map.keys() if nid not in sorted_ids]
            logger.warning(f"Some nodes not topologically sorted, appending in original order: {remaining}")
            sorted_ids.extend(remaining)
        
        result = [nodes_map[nid] for nid in sorted_ids]
        logger.debug(f"Topological sort result: {[n.get('id') for n in result]}")
        return result
    
    # --- Segment 생성 헬퍼 ---
    def create_segment(nodes_map, edges_list, s_type="normal", override_id=None, config=None):
        # 세그먼트 내부 엣지 추가
        if config:
            all_edges = config.get("edges", [])
            for edge in all_edges:
                source = edge.get("source")
                target = edge.get("target")
                if source in nodes_map and target in nodes_map:
                    if edge not in edges_list:  # 중복 방지
                        edges_list.append(edge)
        
        # [Critical Fix] 노드 순서를 위상 정렬하여 첫 번째 노드가 실제 시작 노드가 되도록 보장
        # DynamicWorkflowBuilder는 nodes[0]을 entry point로 사용하므로 순서가 중요함
        sorted_nodes = _topological_sort_nodes(nodes_map, edges_list)
        
        final_type = s_type
        if s_type == "normal":
            if any(n.get("hitp") in [True, "true"] for n in nodes_map.values()):
                final_type = "hitp"
        
        if final_type == "llm": 
            stats["llm"] += 1
        elif final_type == "hitp": 
            stats["hitp"] += 1
            
        return {
            "id": override_id if override_id is not None else seg_id_gen.next(),
            "nodes": sorted_nodes,  # [Critical Fix] 위상 정렬된 노드 사용
            "edges": list(edges_list),
            "type": final_type,
            "node_ids": [n["id"] for n in sorted_nodes]  # 정렬된 순서 반영
        }
    
    # --- 재귀적 파티셔닝 로직 ---
    visited_nodes: Set[str] = set()
    
    def run_partitioning(
        start_node_ids: List[str], 
        stop_at_nodes: Set[str] = None, 
        config=None,
        depth: int = 0  # [Critical Fix #1] 재귀 깊이 추적
    ) -> List[Dict[str, Any]]:
        """
        재귀적 파티셔닝 로직.
        
        [Critical Fix #1] depth 파라미터로 재귀 깊이 제한
        [Critical Fix #2] forced_segment_starts로 합류점 강제 분할
        """
        # [Critical Fix #1] 재귀 깊이 제한 검사
        if depth > MAX_PARTITION_DEPTH:
            raise PartitionDepthExceededError(depth, MAX_PARTITION_DEPTH)
        
        local_segments = []
        local_current_nodes = {}
        local_current_edges = []
        queue = list(start_node_ids)
        
        # [Critical Fix #1] 무한 루프 방지용 반복 카운터
        max_iterations = len(nodes) * 2  # 안전 마진
        iteration_count = 0
        
        def flush_local(seg_type="normal"):
            nonlocal local_current_nodes, local_current_edges
            if local_current_nodes or local_current_edges:
                seg = create_segment(local_current_nodes, local_current_edges, seg_type, config=config)
                local_segments.append(seg)
                local_current_nodes = {}
                local_current_edges = []
        
        while queue:
            # [Critical Fix #1] 무한 루프 방지
            iteration_count += 1
            if iteration_count > max_iterations:
                logger.error(
                    f"Partition iteration limit exceeded ({max_iterations}). "
                    f"Possible infinite loop. Queue: {queue[:5]}..."
                )
                raise PartitionDepthExceededError(iteration_count, max_iterations)
            
            node_id = queue.pop(0)
            
            # Stop Condition
            if node_id in visited_nodes: 
                continue
            if stop_at_nodes and node_id in stop_at_nodes: 
                continue
            
            # [Safety] 노드가 존재하는지 확인
            if node_id not in nodes:
                logger.warning(f"Node '{node_id}' referenced but not found in nodes map. Skipping.")
                continue
            
            node = nodes[node_id]
            
            # 트리거 조건 계산
            in_edges = incoming_edges.get(node_id, [])
            non_hitp_in = [e for e in in_edges if e.get("type") not in HITP_EDGE_TYPES]
            
            is_hitp_start = any(e.get("type") in HITP_EDGE_TYPES for e in in_edges)
            is_llm = node.get("type") in LLM_NODE_TYPES
            is_merge = len(non_hitp_in) > 1
            is_branch = len(outgoing_edges.get(node_id, [])) > 1
            
            # [Critical Fix #2] 합류점은 반드시 새 세그먼트 시작
            is_forced_start = node_id in forced_segment_starts
            
            # 세그먼트 분할 트리거 (is_forced_start 추가)
            if (is_hitp_start or is_llm or is_merge or is_branch or is_forced_start) and local_current_nodes:
                if node_id not in local_current_nodes:
                    flush_local("normal")
            
            # 병렬 그룹 처리
            if is_branch:
                flush_local("normal")  # 현재까지 저장
                
                # 분기점 노드 처리
                seg = create_segment({node_id: node}, [], "normal", config=config) 
                local_segments.append(seg)
                visited_nodes.add(node_id)
                
                out_edges = outgoing_edges.get(node_id, [])
                branch_targets = [e.get("target") for e in out_edges if e.get("target")]
                
                # 합류점 찾기 - [Critical Fix #2] 합류점이 forced_segment_starts에 등록됨
                convergence_node = find_convergence_node(branch_targets)
                stop_set = {convergence_node} if convergence_node else set()
                
                # 각 브랜치 실행
                branches_data = []
                for i, target in enumerate(branch_targets):
                    if target:
                        # [Critical Fix #1] 재귀 깊이 전달
                        branch_segs = run_partitioning(
                            [target], 
                            stop_at_nodes=stop_set, 
                            config=config,
                            depth=depth + 1
                        )
                        
                        # [Critical Fix #3] 브랜치 종료 검증
                        if branch_segs:
                            last_seg = branch_segs[-1]
                            # 브랜치 메타데이터 추가
                            branch_data = {
                                "branch_id": f"B{i}",
                                "partition_map": branch_segs,
                                "has_end": last_seg.get("next_mode") == "end",
                                "target_node": target
                            }
                            branches_data.append(branch_data)
                            stats["branches"] += 1
                        else:
                            # 빈 브랜치 경고
                            logger.warning(f"Branch {i} starting at {target} produced no segments")
                
                # Parallel Group 생성
                if branches_data:
                    stats["parallel_groups"] += 1
                    p_seg_id = seg_id_gen.next()
                    parallel_seg = {
                        "id": p_seg_id,
                        "type": "parallel_group",
                        "branches": branches_data,
                        "node_ids": [],
                        "branch_count": len(branches_data)  # [추가] 브랜치 수 메타데이터
                    }
                    local_segments.append(parallel_seg)
                    
                    # Aggregator 생성
                    agg_seg_id = seg_id_gen.next()
                    aggregator_seg = {
                        "id": agg_seg_id,
                        "type": "aggregator",
                        "nodes": [],
                        "edges": [],
                        "node_ids": [],
                        "convergence_node": convergence_node,  # 합류 노드 저장
                        "source_parallel_group": p_seg_id  # [추가] 원본 parallel_group 참조
                    }
                    local_segments.append(aggregator_seg)
                    
                    # Parallel Group의 next 설정
                    parallel_seg["next_mode"] = "default"
                    parallel_seg["default_next"] = agg_seg_id
                
                # 합류점이 있다면 큐에 추가
                if convergence_node and convergence_node not in visited_nodes:
                    queue.append(convergence_node)
                continue
            
            # 일반 노드 처리
            local_current_nodes[node_id] = node
            visited_nodes.add(node_id)
            
            # 특수 타입 처리
            if is_llm:
                flush_local("llm")
            elif is_hitp_start:
                flush_local("hitp")
            
            # 다음 노드 탐색
            for out_edge in outgoing_edges.get(node_id, []):
                tgt = out_edge.get("target")
                if tgt and tgt not in visited_nodes and tgt not in queue:
                    if not (stop_at_nodes and tgt in stop_at_nodes):
                        queue.append(tgt)
        
        flush_local()  # 남은 것 처리
        return local_segments
    
    # 시작 노드 찾기 및 실행
    start_nodes = [nid for nid in nodes if not incoming_edges.get(nid)]
    if not start_nodes and nodes: 
        start_nodes = [list(nodes.keys())[0]]
    
    segments = run_partitioning(start_nodes, config=config)
    
    # --- Pass 2: 재귀적 Node-to-Segment 매핑 ---
    node_to_seg_map = {}
    
    def map_nodes_recursive(seg_list):
        for seg in seg_list:
            for nid in seg.get("node_ids", []):
                node_to_seg_map[nid] = seg["id"]
            
            if seg["type"] == "parallel_group":
                for branch in seg["branches"]:
                    map_nodes_recursive(branch["partition_map"])
    
    map_nodes_recursive(segments)
    
    # --- Next Mode 설정 (재귀적) ---
    def process_links_recursive(seg_list: List[Dict[str, Any]], parent_aggregator_id: Optional[int] = None):
        """
        세그먼트 간 연결(next_mode) 설정.
        
        [Critical Fix #3] 브랜치 내부 세그먼트가 올바르게 종료되는지 검증.
        parent_aggregator_id가 주어지면, 브랜치 내 마지막 세그먼트는 이 aggregator로 연결되어야 함.
        """
        # Aggregator 세그먼트들의 ID 집합을 미리 파악
        aggregator_ids = {s["id"] for s in seg_list if s.get("type") == "aggregator"}
        
        for idx, seg in enumerate(seg_list):
            if seg["type"] == "parallel_group":
                # parallel_group 다음의 aggregator ID 찾기
                next_agg_id = seg.get("default_next")
                
                for branch in seg["branches"]:
                    branch_segs = branch.get("partition_map", [])
                    
                    # [Critical Fix #3] 브랜치 내부 재귀 처리 - aggregator ID 전달
                    process_links_recursive(branch_segs, parent_aggregator_id=next_agg_id)
                    
                    # [Critical Fix #3] 브랜치 마지막 세그먼트 검증
                    if branch_segs:
                        last_branch_seg = branch_segs[-1]
                        
                        # 마지막 세그먼트가 명시적 END가 아니고 next가 없으면
                        # aggregator로 암묵적 연결 설정
                        if last_branch_seg.get("next_mode") == "end" and next_agg_id:
                            # 비대칭 브랜치: 한 쪽은 끝나고 다른 쪽은 합류
                            # aggregator에서 이를 처리할 수 있도록 메타데이터 추가
                            last_branch_seg["implicit_aggregator_target"] = next_agg_id
                            branch["terminates_early"] = True
                            logger.debug(
                                f"Branch {branch['branch_id']} terminates early. "
                                f"Implicit aggregator target: {next_agg_id}"
                            )
                continue
            
            # Aggregator의 경우 convergence_node를 사용해 다음 세그먼트 연결
            if seg.get("type") == "aggregator":
                convergence_node = seg.get("convergence_node")
                if convergence_node and convergence_node in node_to_seg_map:
                    next_seg_id = node_to_seg_map[convergence_node]
                    seg["next_mode"] = "default"
                    seg["default_next"] = next_seg_id
                else:
                    # [Critical Fix #2] 합류점이 맵에 없으면 강제로 찾기 시도
                    if convergence_node:
                        # 합류점이 forced_segment_starts에 있는지 확인
                        if convergence_node in forced_segment_starts:
                            logger.error(
                                f"Aggregator {seg['id']} has convergence node '{convergence_node}' "
                                f"which is a forced segment start but not mapped. "
                                f"This indicates a partitioning logic error."
                            )
                        else:
                            logger.warning(
                                f"Aggregator {seg['id']} has convergence node '{convergence_node}' "
                                f"but it is not mapped to any segment. Treating as workflow end."
                            )
                    
                    seg["next_mode"] = "end"
                    seg["default_next"] = None
                continue
            
            exit_edges = []
            for nid in seg.get("node_ids", []):
                for out_edge in outgoing_edges.get(nid, []):
                    tgt = out_edge.get("target")
                    if tgt and tgt in node_to_seg_map:
                        tgt_seg = node_to_seg_map[tgt]
                        
                        # 타겟이 현재 세그먼트와 다르고
                        if tgt_seg != seg["id"]:
                            # 만약 타겟이 Aggregator라면, 브랜치 내부에서는 이를 연결하지 않음
                            # (ASL Map State가 끝나고 자연스럽게 넘어가도록 함)
                            if tgt_seg in aggregator_ids:
                                continue 
                                
                            exit_edges.append({"edge": out_edge, "target_segment": tgt_seg})
            
            if not exit_edges:
                # [Critical Fix #3] 부모 aggregator가 있으면 암묵적 연결
                if parent_aggregator_id is not None:
                    seg["next_mode"] = "implicit_aggregator"
                    seg["default_next"] = None
                    seg["parent_aggregator"] = parent_aggregator_id
                else:
                    seg["next_mode"] = "end"
                    seg["default_next"] = None
            elif len(exit_edges) == 1:
                seg["next_mode"] = "default"
                seg["default_next"] = exit_edges[0]["target_segment"]
            else:
                seg["next_mode"] = "conditional"
                seg["branches"] = [
                    {"condition": e["edge"].get("condition", "default"), "next": e["target_segment"]}
                    for e in exit_edges
                ]
    
    process_links_recursive(segments)
    
    # --- 재귀적 세그먼트 수 계산 ---
    def count_segments_recursive(seg_list):
        total = 0
        for seg in seg_list:
            total += 1
            if seg.get("type") == "parallel_group":
                for branch in seg.get("branches", []):
                    total += count_segments_recursive(branch.get("partition_map", []))
        return total
    
    total_segments = count_segments_recursive(segments)
    
    # [Performance Optimization] Pre-indexed 메타데이터 반환
    return {
        "partition_map": segments,
        "total_segments": total_segments, 
        "llm_segments": stats["llm"],
        "hitp_segments": stats["hitp"],
        # [v2.0] 추가 통계
        "parallel_groups": stats["parallel_groups"],
        "total_branches": stats["branches"],
        "forced_segment_starts": list(forced_segment_starts),
        # [Performance] Pre-indexed 데이터 (재사용 가능)
        "node_to_segment_map": node_to_seg_map,
        "metadata": {
            "max_partition_depth": MAX_PARTITION_DEPTH,
            "max_nodes_limit": MAX_NODES_LIMIT,
            "nodes_processed": len(visited_nodes),
            "total_nodes": len(nodes)
        }
    }


def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    PartitionWorkflow Lambda: 워크플로우를 지능적으로 분할합니다.
    
    [v2.0 Production Hardening]
    - DAG 검증 실패 시 명확한 에러 반환
    - 재귀 깊이 초과 시 에러 반환
    - 노드 수 제한 초과 시 에러 반환
    
    Input event:
        - workflow_config: 분할할 워크플로우 설정
        - ownerId: 소유자 ID (보안/로깅용)
        
    Output:
        - partition_result: partition_workflow_advanced() 결과
        - status: "success" | "error"
    """
    try:
        workflow_config = event.get("workflow_config")
        if not workflow_config:
            raise ValueError("workflow_config is required")
        
        owner_id = event.get("ownerId") or event.get("owner_id") or event.get("user_id")
        
        # 워크플로우 분할 실행
        partition_result = partition_workflow_advanced(workflow_config)
        
        logger.info(
            "Partitioned workflow for owner=%s: %d total segments "
            "(%d LLM, %d HITP, %d parallel groups, %d branches)", 
            owner_id,
            partition_result["total_segments"],
            partition_result["llm_segments"], 
            partition_result["hitp_segments"],
            partition_result.get("parallel_groups", 0),
            partition_result.get("total_branches", 0)
        )
        
        return {
            "status": "success",
            "partition_result": partition_result
        }
    
    except CycleDetectedError as e:
        logger.error(f"Cycle detected in workflow: {e.cycle_path}")
        return {
            "status": "error",
            "error_type": "CycleDetectedError",
            "error_message": str(e),
            "cycle_path": e.cycle_path
        }
    
    except PartitionDepthExceededError as e:
        logger.error(f"Partition depth exceeded: {e.depth}/{e.max_depth}")
        return {
            "status": "error",
            "error_type": "PartitionDepthExceededError",
            "error_message": str(e),
            "depth": e.depth,
            "max_depth": e.max_depth
        }
    
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return {
            "status": "error",
            "error_type": "ValidationError",
            "error_message": str(e)
        }
        
    except Exception as e:
        logger.exception("Failed to partition workflow")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "error_message": str(e)
        }
