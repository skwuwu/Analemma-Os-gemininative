"""
Logical Auditor: 워크플로우 논리적 검증 및 시뮬레이션

워크플로우의 구조적 무결성, 논리적 오류, 데이터 흐름을 검증합니다.
"""
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class LogicalAuditor:
    """워크플로우 논리적 검증 및 시뮬레이션"""
    
    def __init__(self, workflow: Dict[str, Any]):
        self.workflow = workflow
        self.nodes: Dict[str, Dict[str, Any]] = {
            n.get('id', f'unknown_{i}'): n 
            for i, n in enumerate(workflow.get('nodes', []))
        }
        self.edges: List[Dict[str, Any]] = workflow.get('edges', [])
        self.issues: List[Dict[str, Any]] = []
        
        # 그래프 구조 구축 (인접 리스트)
        self.adjacency: Dict[str, List[str]] = defaultdict(list)
        self.reverse_adjacency: Dict[str, List[str]] = defaultdict(list)
        
        for edge in self.edges:
            source = edge.get('source')
            target = edge.get('target')
            if source and target:
                self.adjacency[source].append(target)
                self.reverse_adjacency[target].append(source)
    
    def audit(self) -> List[Dict[str, Any]]:
        """
        전체 검증 수행
        
        Returns:
            이슈 목록 [{"level": "error|warning|info", "type": "...", "message": "...", ...}]
        """
        self.issues = []
        
        if not self.nodes:
            return self.issues
        
        self._check_orphan_nodes()
        self._check_cycles()
        self._check_unreachable_nodes()
        self._check_dead_ends()
        self._check_missing_configs()
        self._check_data_flow()
        self._check_duplicate_connections()
        
        return self.issues
    
    # ─────────────────────────────────────────────
    # 검증 규칙들
    # ─────────────────────────────────────────────
    
    def _check_orphan_nodes(self):
        """연결되지 않은 노드 감지"""
        connected_nodes: Set[str] = set()
        
        for edge in self.edges:
            if edge.get('source'):
                connected_nodes.add(edge['source'])
            if edge.get('target'):
                connected_nodes.add(edge['target'])
        
        for node_id in self.nodes:
            if node_id not in connected_nodes and len(self.nodes) > 1:
                node = self.nodes[node_id]
                self.issues.append({
                    "level": "warning",
                    "type": "orphan_node",
                    "message": f"노드 '{node_id}'가 어떤 엣지와도 연결되어 있지 않습니다.",
                    "affected_nodes": [node_id],
                    "suggestion": "이 노드를 워크플로우에 연결하거나, 불필요하다면 삭제하세요."
                })
    
    def _check_cycles(self):
        """
        무한 루프 감지 (DFS 기반 사이클 탐지)
        
        참고: for_each 같은 의도된 루프 노드는 제외
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {node_id: WHITE for node_id in self.nodes}
        cycle_nodes: List[str] = []
        
        def dfs(node: str, path: List[str]) -> bool:
            color[node] = GRAY
            path.append(node)
            
            for neighbor in self.adjacency.get(node, []):
                if neighbor not in color:
                    # 노드가 삭제되었거나 존재하지 않음
                    continue
                    
                if color[neighbor] == GRAY:
                    # 사이클 발견
                    cycle_start = path.index(neighbor)
                    cycle_nodes.extend(path[cycle_start:])
                    return True
                elif color[neighbor] == WHITE:
                    if dfs(neighbor, path):
                        return True
            
            color[node] = BLACK
            path.pop()
            return False
        
        for node_id in self.nodes:
            if color.get(node_id, WHITE) == WHITE:
                if dfs(node_id, []):
                    break
        
        if cycle_nodes:
            # for_each 노드가 포함된 사이클은 의도된 것일 수 있음
            has_foreach = any(
                self.nodes.get(nid, {}).get('type') == 'for_each'
                for nid in cycle_nodes
            )
            
            level = "warning" if has_foreach else "error"
            message = (
                f"루프가 감지되었습니다: {' → '.join(cycle_nodes)} → {cycle_nodes[0]}"
                if not has_foreach else
                f"for_each 노드를 포함한 루프가 감지되었습니다: {' → '.join(cycle_nodes)}"
            )
            
            self.issues.append({
                "level": level,
                "type": "cycle_detected" if has_foreach else "infinite_loop",
                "message": message,
                "affected_nodes": list(set(cycle_nodes)),
                "suggestion": "의도된 루프가 아니라면 사이클을 끊거나 종료 조건을 추가하세요."
            })
    
    def _check_unreachable_nodes(self):
        """시작점에서 도달할 수 없는 노드 감지"""
        # 진입 차수가 0인 노드를 시작점으로 가정
        start_nodes = [
            node_id for node_id in self.nodes
            if len(self.reverse_adjacency.get(node_id, [])) == 0
        ]
        
        if not start_nodes:
            # 모든 노드에 진입 엣지가 있다면 사이클만 존재
            if self.nodes:
                self.issues.append({
                    "level": "warning",
                    "type": "no_start_node",
                    "message": "시작 노드가 없습니다. 모든 노드가 다른 노드로부터 연결되어 있습니다.",
                    "affected_nodes": list(self.nodes.keys()),
                    "suggestion": "워크플로우의 진입점을 확인하세요."
                })
            return
        
        # DFS로 도달 가능한 노드 탐색 (stack.pop() = LIFO = DFS)
        reachable: Set[str] = set()
        stack = list(start_nodes)
        
        while stack:
            node = stack.pop()
            if node in reachable:
                continue
            reachable.add(node)
            stack.extend(self.adjacency.get(node, []))
        
        unreachable = set(self.nodes.keys()) - reachable
        for node_id in unreachable:
            self.issues.append({
                "level": "warning",
                "type": "unreachable_node",
                "message": f"노드 '{node_id}'에 시작점에서 도달할 수 없습니다.",
                "affected_nodes": [node_id],
                "suggestion": "시작 노드로부터 이 노드로 연결하는 엣지를 추가하세요."
            })
    
    def _check_dead_ends(self):
        """막다른 노드 감지 (종료 노드가 아닌데 후속 노드가 없는 경우)"""
        # 의도적인 종료 노드 타입
        terminal_types = {"route_draft_quality", "group"}
        
        for node_id, node in self.nodes.items():
            node_type = node.get('type', '')
            has_outgoing = len(self.adjacency.get(node_id, [])) > 0
            
            if not has_outgoing and node_type not in terminal_types:
                # 단일 노드 워크플로우인 경우 경고하지 않음
                if len(self.nodes) > 1:
                    has_incoming = len(self.reverse_adjacency.get(node_id, [])) > 0
                    if has_incoming:
                        # 중간 노드인데 출구가 없으면 정보 레벨로 알림
                        self.issues.append({
                            "level": "info",
                            "type": "dead_end",
                            "message": f"노드 '{node_id}'가 워크플로우의 끝점입니다.",
                            "affected_nodes": [node_id],
                            "suggestion": "의도된 종료점인지 확인하세요. 필요하다면 후속 노드를 연결하세요."
                        })
    
    def _check_missing_configs(self):
        """필수 설정 누락 감지"""
        required_configs: Dict[str, List[str]] = {
            "llm_chat": ["prompt_content"],
            "aiModel": ["prompt_content"],
            "api_call": ["url"],
            "db_query": ["query", "connection_string"],
            "for_each": ["items"],
            "route_draft_quality": ["threshold"]
        }
        
        for node_id, node in self.nodes.items():
            node_type = node.get('type', '')
            config = node.get('config', {}) or {}
            data = node.get('data', {}) or {}
            
            # config와 data 모두에서 필드 검색
            combined = {**data, **config}
            
            required = required_configs.get(node_type, [])
            missing = [
                field for field in required 
                if not combined.get(field)
            ]
            
            if missing:
                self.issues.append({
                    "level": "error",
                    "type": "missing_config",
                    "message": f"노드 '{node_id}'에 필수 설정이 누락되었습니다: {', '.join(missing)}",
                    "affected_nodes": [node_id],
                    "suggestion": f"'{missing[0]}' 설정을 추가하세요.",
                    "missing_fields": missing
                })
    
    def _check_data_flow(self):
        """데이터 흐름 분석 (간단한 휴리스틱)"""
        for node_id, node in self.nodes.items():
            node_type = node.get('type', '')
            config = node.get('config', {}) or {}
            data = node.get('data', {}) or {}
            
            combined = {**data, **config}
            
            # 템플릿 변수 사용 검사 (Jinja2 또는 ${} 형식)
            if node_type in ["llm_chat", "aiModel"]:
                prompt = combined.get('prompt_content', '')
                if isinstance(prompt, str):
                    # {{변수}}, {%...%}, ${변수} 패턴 검사
                    has_template = (
                        '{{' in prompt or 
                        '{%' in prompt or 
                        '${' in prompt or
                        '{state' in prompt
                    )
                    
                    if has_template:
                        # 입력 노드가 없으면 경고
                        if not self.reverse_adjacency.get(node_id):
                            self.issues.append({
                                "level": "warning",
                                "type": "potential_data_issue",
                                "message": f"노드 '{node_id}'가 템플릿 변수를 사용하지만 입력 노드가 없습니다.",
                                "affected_nodes": [node_id],
                                "suggestion": "데이터를 제공하는 노드를 연결하거나, 템플릿 변수를 확인하세요."
                            })
            
            # API 호출 노드의 URL 템플릿 검사
            if node_type == "api_call":
                url = combined.get('url', '')
                if isinstance(url, str) and ('{' in url or '${' in url):
                    if not self.reverse_adjacency.get(node_id):
                        self.issues.append({
                            "level": "warning",
                            "type": "potential_data_issue",
                            "message": f"노드 '{node_id}'의 URL에 템플릿 변수가 있지만 입력 노드가 없습니다.",
                            "affected_nodes": [node_id],
                            "suggestion": "URL 변수를 제공하는 노드를 연결하세요."
                        })
    
    def _check_duplicate_connections(self):
        """중복 연결 감지"""
        connection_set: Set[Tuple[str, str]] = set()
        
        for edge in self.edges:
            source = edge.get('source')
            target = edge.get('target')
            
            if source and target:
                key = (source, target)
                if key in connection_set:
                    self.issues.append({
                        "level": "warning",
                        "type": "duplicate_connection",
                        "message": f"'{source}'에서 '{target}'로의 중복 연결이 있습니다.",
                        "affected_nodes": [source, target],
                        "suggestion": "중복된 엣지 중 하나를 삭제하세요."
                    })
                connection_set.add(key)
    
    # ─────────────────────────────────────────────
    # 시뮬레이션 모드
    # ─────────────────────────────────────────────
    
    def simulate(self, mock_inputs: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        워크플로우 실행 시뮬레이션 (실제 API/LLM 호출 없이)
        
        Args:
            mock_inputs: 시뮬레이션에 사용할 초기 입력값
            
        Returns:
            {
                "success": bool,
                "trace": [...],  # 실행 추적
                "errors": [...],  # 발생한 오류
                "visited_nodes": [...],
                "coverage": float  # 0.0 ~ 1.0
            }
        """
        trace: List[Dict[str, Any]] = []
        errors: List[str] = []
        state: Dict[str, Any] = mock_inputs or {}
        
        # 시작 노드 찾기 (진입 엣지가 없는 노드)
        start_nodes = [
            node_id for node_id in self.nodes
            if len(self.reverse_adjacency.get(node_id, [])) == 0
        ]
        
        if not start_nodes:
            # 시작 노드가 없으면 첫 번째 노드 사용
            if self.nodes:
                start_nodes = [list(self.nodes.keys())[0]]
            else:
                return {
                    "success": False,
                    "trace": [],
                    "errors": ["시작 노드를 찾을 수 없습니다."],
                    "visited_nodes": [],
                    "coverage": 0.0
                }
        
        visited: Set[str] = set()
        queue = list(start_nodes)
        max_iterations = len(self.nodes) * 2  # 무한 루프 방지
        iterations = 0
        
        while queue and iterations < max_iterations:
            iterations += 1
            node_id = queue.pop(0)
            
            if node_id in visited:
                continue
            visited.add(node_id)
            
            node = self.nodes.get(node_id)
            if not node:
                errors.append(f"노드 '{node_id}'를 찾을 수 없습니다.")
                continue
            
            node_type = node.get('type', 'unknown')
            
            # 시뮬레이션 추적 기록
            trace_entry = {
                "node_id": node_id,
                "node_type": node_type,
                "step": len(trace) + 1,
                "simulated_output": self._simulate_node(node, state)
            }
            trace.append(trace_entry)
            
            # 다음 노드 추가
            next_nodes = self.adjacency.get(node_id, [])
            for next_node in next_nodes:
                if next_node not in visited:
                    queue.append(next_node)
        
        return {
            "success": len(errors) == 0,
            "trace": trace,
            "errors": errors,
            "visited_nodes": list(visited),
            "coverage": len(visited) / len(self.nodes) if self.nodes else 0.0
        }
    
    def _simulate_node(self, node: Dict[str, Any], state: Dict[str, Any]) -> str:
        """개별 노드 시뮬레이션"""
        node_type = node.get('type', 'unknown')
        node_id = node.get('id', 'unknown')
        
        simulations = {
            "operator": f"[시뮬레이션] Python 코드 실행됨",
            "llm_chat": f"[시뮬레이션] LLM 호출됨 → 응답 생성",
            "aiModel": f"[시뮬레이션] AI 모델 호출됨 → 응답 생성",
            "api_call": f"[시뮬레이션] API 호출됨 → 응답 수신",
            "db_query": f"[시뮬레이션] DB 쿼리 실행됨 → 결과 반환",
            "for_each": f"[시뮬레이션] 반복 처리 시작",
            "route_draft_quality": f"[시뮬레이션] 품질 라우팅 수행",
            "group": f"[시뮬레이션] 서브그래프 진입"
        }
        
        return simulations.get(node_type, f"[시뮬레이션] {node_type} 노드 실행됨")
    
    def get_execution_order(self) -> List[str]:
        """
        워크플로우 실행 순서 반환 (위상 정렬)
        
        Returns:
            노드 ID 순서 리스트
        """
        in_degree: Dict[str, int] = {node_id: 0 for node_id in self.nodes}
        
        for edge in self.edges:
            target = edge.get('target')
            if target and target in in_degree:
                in_degree[target] += 1
        
        # 진입 차수가 0인 노드부터 시작
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        order: List[str] = []
        
        while queue:
            node = queue.pop(0)
            order.append(node)
            
            for neighbor in self.adjacency.get(node, []):
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
        
        return order
    
    def get_affected_downstream(self, node_id: str) -> List[str]:
        """
        특정 노드의 변경이 영향을 미치는 하위 노드들 반환
        
        Args:
            node_id: 변경된 노드 ID
            
        Returns:
            영향받는 하위 노드 ID 리스트
        """
        affected: List[str] = []
        visited: Set[str] = set()
        queue = [node_id]
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            if current != node_id:  # 시작 노드는 제외
                affected.append(current)
            
            queue.extend(self.adjacency.get(current, []))
        
        return affected


# ──────────────────────────────────────────────────────────────
# API 엔드포인트용 함수
# ──────────────────────────────────────────────────────────────

def audit_workflow(workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    워크플로우 검증 API
    
    Args:
        workflow: 워크플로우 JSON
        
    Returns:
        이슈 목록
    """
    auditor = LogicalAuditor(workflow)
    return auditor.audit()


def simulate_workflow(
    workflow: Dict[str, Any], 
    mock_inputs: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    워크플로우 시뮬레이션 API
    
    Args:
        workflow: 워크플로우 JSON
        mock_inputs: 시뮬레이션 입력값
        
    Returns:
        시뮬레이션 결과
    """
    auditor = LogicalAuditor(workflow)
    return auditor.simulate(mock_inputs)


def get_workflow_analysis(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    워크플로우 종합 분석
    
    Args:
        workflow: 워크플로우 JSON
        
    Returns:
        종합 분석 결과
    """
    auditor = LogicalAuditor(workflow)
    
    issues = auditor.audit()
    simulation = auditor.simulate()
    execution_order = auditor.get_execution_order()
    
    # 이슈 요약
    error_count = sum(1 for i in issues if i.get('level') == 'error')
    warning_count = sum(1 for i in issues if i.get('level') == 'warning')
    info_count = sum(1 for i in issues if i.get('level') == 'info')
    
    return {
        "issues": issues,
        "issue_summary": {
            "errors": error_count,
            "warnings": warning_count,
            "info": info_count,
            "total": len(issues)
        },
        "simulation": simulation,
        "execution_order": execution_order,
        "node_count": len(auditor.nodes),
        "edge_count": len(auditor.edges),
        "is_valid": error_count == 0
    }


def lambda_handler(event, context):
    """Lambda 핸들러: 워크플로우 검증 API"""
    import json
    
    try:
        # HTTP API Gateway 이벤트에서 body 파싱
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)
        
        workflow = body.get('workflow', {})
        
        if not workflow:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'workflow is required'})
            }
        
        # 워크플로우 분석 실행
        analysis = get_workflow_analysis(workflow)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(analysis, ensure_ascii=False)
        }
        
    except Exception as e:
        logger.exception(f"Audit failed: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }
