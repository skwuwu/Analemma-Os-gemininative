"""
DesignerService - Workflow Design and LLM Streaming Service

Extracted from `agentic_designer.py` to separate business logic from handler.
Handles:
- Request intent analysis
- Workflow generation via LLM
- Streaming response processing
"""

import json
import codecs
import logging
import os
import re
import time
from typing import Dict, Any, Iterator, List, Optional, Tuple

# 공통 유틸리티 import
try:
    from src.common.constants import is_mock_mode as _common_is_mock_mode, LLMModels
    MODEL_HAIKU = LLMModels.CLAUDE_3_HAIKU
    MODEL_SONNET = LLMModels.CLAUDE_3_SONNET
    MODEL_GEMINI = LLMModels.GEMINI_1_5_PRO
except ImportError:
    _common_is_mock_mode = None
    MODEL_HAIKU = os.getenv("HAIKU_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
    MODEL_SONNET = os.getenv("SONNET_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0")
    MODEL_GEMINI = os.getenv("GEMINI_MODEL_ID", "gemini-1.5-pro-latest")

# Thinking Level Control import
try:
    from src.common.model_router import (
        calculate_thinking_budget,
        ThinkingLevel,
        THINKING_BUDGET_MAP,
    )
    _THINKING_AVAILABLE = True
except ImportError:
    _THINKING_AVAILABLE = False
    calculate_thinking_budget = None
    ThinkingLevel = None
    THINKING_BUDGET_MAP = {}

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# JSONL 스트리밍 특수 구분자 (LLM 프롬프트에서 사용)
# ═══════════════════════════════════════════════════════════════════════════════
JSONL_DELIMITER = "<<<JSONL_END>>>"  # JSON 객체 끝을 명시하는 구분자

# Focus Window 설정
FOCUS_WINDOW_MAX_NODES = 15  # 컨텍스트에 포함할 최대 노드 수
FOCUS_WINDOW_ADJACENT_DEPTH = 2  # 변경 대상 노드의 인접 깊이


# ═══════════════════════════════════════════════════════════════════════════════
# ① Partial JSON Parser (스트리밍 안정성 강화)
# ═══════════════════════════════════════════════════════════════════════════════

class PartialJSONParser:
    """
    스트리밍 JSONL 파싱을 위한 Robust Parser
    
    LLM이 생성하는 JSON 내부에 개행 문자가 포함되어도
    안전하게 파싱할 수 있도록 Bracket Counting 기법을 사용합니다.
    
    문제 상황:
    - {"type": "node", "data": {"label": "Hello\\nWorld"}}  ← 내부 개행
    - LLM이 여러 줄에 걸쳐 JSON을 생성하는 경우
    - Gemini 3가 JSON 전에 서술형 텍스트를 출력 ("Here is the updated workflow...")
    
    해결:
    - { } 브라켓 카운팅으로 완전한 JSON 객체 감지
    - 특수 구분자(<<<JSONL_END>>>) 우선 인식
    - 파싱 실패 시 버퍼 유지 후 재시도
    - [Critical Fix ②] 첫 번째 { 이전의 preamble 텍스트 자동 제거
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ [② Critical Fix] 이중 방어 구조 (Defense in Depth)                    │
    │                                                                       │
    │ 1차 방어: JSONL_DELIMITER (<<<JSONL_END>>>)                            │
    │   - 프롬프트에서 LLM에게 명시적으로 구분자 사용 요청                      │
    │   - 정상 상황에서 100% 파싱 성공                                        │
    │                                                                       │
    │ 2차 방어: Bracket Counting (depth == 0)                               │
    │   - LLM이 복잡한 추론에 집중하다 Delimiter를 누락할 경우 백업            │
    │   - { } 브라켓 깊이가 0이 되는 지점에서 강제 파싱 시도                │
    │   - 에지 케이스에서도 안정적으로 동작                                    │
    │                                                                       │
    │ 3차 방어: Preamble Stripping                                           │
    │   - JSON 전 서술형 텍스트 자동 제거                                      │
    │   - "Here is the..." 같은 텍스트가 있어도 파싱 성공                     │
    │                                                                       │
    │ 이 이중 방어 구조로 Delimiter 없이도 Bracket이 0이 되는 지점에서     │
    │ 안전하게 JSON을 파싱할 수 있습니다.                                      │
    └─────────────────────────────────────────────────────────────────────┘
    """
    
    def __init__(self, delimiter: str = JSONL_DELIMITER):
        self.buffer = ""
        self.delimiter = delimiter
        self._pending_objects: List[Dict[str, Any]] = []
        self._preamble_stripped = False  # 프리앰블 제거 완료 플래그
    
    def _strip_preamble(self) -> str:
        """
        [Critical Fix ②] 첫 번째 { 이전의 모든 텍스트를 제거
        
        Gemini 3가 가끔 JSON 객체 전에 서술형 텍스트를 출력합니다:
        - "Here is the updated workflow:"
        - "I'll create the following nodes:"
        - "Based on your request, I've designed..."
        
        이 텍스트를 무시하지 않으면 파싱이 실패합니다.
        
        Returns:
            제거된 preamble 텍스트 (로깅용)
        """
        if self._preamble_stripped:
            return ""
        
        # 첫 번째 { 위치 찾기
        first_brace = self.buffer.find("{")
        
        if first_brace == -1:
            # 아직 JSON 시작이 없음 - 너무 긴 preamble은 truncate
            if len(self.buffer) > 500:
                preamble = self.buffer[:500]
                self.buffer = ""
                logger.warning(f"Long preamble truncated (no JSON found): {preamble[:100]}...")
                return preamble
            return ""
        
        if first_brace == 0:
            # 프리앰블 없음
            self._preamble_stripped = True
            return ""
        
        # 프리앰블 제거
        preamble = self.buffer[:first_brace]
        self.buffer = self.buffer[first_brace:]
        self._preamble_stripped = True
        
        # 프리앰블 로깅 (디버깅용)
        if preamble.strip():
            logger.info(f"Stripped preamble before JSON: {preamble.strip()[:100]}...")
        
        return preamble
    
    def feed(self, chunk: str) -> List[Dict[str, Any]]:
        """
        새 청크를 버퍼에 추가하고 완전한 JSON 객체들을 반환
        
        Returns:
            파싱된 JSON 객체 리스트 (비어있을 수 있음)
        """
        self.buffer += chunk
        results = []
        
        # [Critical Fix ②] 첫 번째 JSON 객체 이전의 preamble 제거
        self._strip_preamble()
        
        # 1. 특수 구분자로 분할 시도 (가장 안전)
        if self.delimiter in self.buffer:
            parts = self.buffer.split(self.delimiter)
            # 마지막 부분은 아직 완료되지 않은 것
            for part in parts[:-1]:
                parsed = self._try_parse_json(part.strip())
                if parsed is not None:
                    results.append(parsed)
            self.buffer = parts[-1]
            return results
        
        # 2. Bracket Counting으로 완전한 JSON 감지
        while True:
            obj, remaining = self._extract_complete_json()
            if obj is None:
                break
            results.append(obj)
            self.buffer = remaining
        
        return results
    
    def _extract_complete_json(self) -> Tuple[Optional[Dict], str]:
        """
        버퍼에서 완전한 JSON 객체 하나를 추출
        
        Returns:
            (parsed_object, remaining_buffer) 또는 (None, original_buffer)
        """
        buffer = self.buffer.lstrip()
        if not buffer or not buffer.startswith("{"):
            # 개행으로 구분된 단순 JSONL 시도
            if "\n" in self.buffer:
                line, remaining = self.buffer.split("\n", 1)
                parsed = self._try_parse_json(line.strip())
                if parsed is not None:
                    return parsed, remaining
            return None, self.buffer
        
        # Bracket Counting
        depth = 0
        in_string = False
        escape_next = False
        
        for i, char in enumerate(buffer):
            if escape_next:
                escape_next = False
                continue
            
            if char == "\\":
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    # 완전한 JSON 객체 발견
                    json_str = buffer[:i + 1]
                    remaining = buffer[i + 1:].lstrip()
                    parsed = self._try_parse_json(json_str)
                    if parsed is not None:
                        return parsed, remaining
                    # 파싱 실패 시 계속 진행
                    break
        
        return None, self.buffer
    
    def _try_parse_json(self, text: str) -> Optional[Dict[str, Any]]:
        """JSON 파싱 시도"""
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None
    
    def flush(self) -> List[Dict[str, Any]]:
        """
        스트림 종료 시 남은 버퍼 처리
        """
        results = []
        if self.buffer.strip():
            parsed = self._try_parse_json(self.buffer.strip())
            if parsed is not None:
                results.append(parsed)
        self.buffer = ""
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# ③ Focus Window (컨텍스트 최적화)
# ═══════════════════════════════════════════════════════════════════════════════

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ [① Critical Fix] Union-Find 캐싱 전략                                   │
# │                                                                           │
# │ _group_omitted_nodes에서 Union-Find 알고리즘은 O(nα(n)) 복잡도로          │
# │ 대부분 상황에서 효율적이지만, 노드가 수백 개일 경우 매 스트리밍          │
# │ 요청마다 계산하면 Lambda 실행 시간에 미세한 지연이 발생할 수 있습니다.    │
# │                                                                           │
# │ 프로덕션 최적화 전략:                                                        │
# │   1. 워크플로우 저장 시 클러스터 정보 사전 계산                              │
# │      - save_workflow() 시 compute_workflow_clusters() 호출               │
# │      - 결과를 workflow.metadata._cluster_cache에 저장                      │
# │                                                                           │
# │   2. extract_focus_window 시 캐시 참조                                   │
# │      - 캐시가 있으면 Union-Find 스킵                                        │
# │      - 캐시가 없으면 실시간 계산 (현재 동작)                                  │
# │                                                                           │
# │   3. 캐시 무효화                                                             │
# │      - 노드/엣지 추가/삭제/수정 시 자동 무효화                               │
# │      - _cluster_cache = None 설정                                         │
# │                                                                           │
# │ 현재는 실시간 계산 방식을 사용하며, 대부분의 워크플로우(<100노드)에서    │
# │ 충분히 빠릅니다. 프로덕션 환경에서 성능 이슈 발생 시 위 전략을             │
# │ 적용하시면 됩니다.                                                          │
# └───────────────────────────────────────────────────────────────────────────┘

def compute_workflow_clusters(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    [① Critical Fix] 워크플로우 클러스터 정보 사전 계산
    
    워크플로우 저장 시 호출하여 클러스터 정보를 메타데이터에 캐싱합니다.
    이후 extract_focus_window에서는 이 캐시를 참조하여 응답 속도를 높입니다.
    
    Args:
        workflow: 워크플로우 {"nodes": [...], "edges": [...]}
    
    Returns:
        {
            "clusters": [
                {"root": "node_1", "members": ["node_1", "node_2", "node_3"], "types": ["aiModel", "llm"]},
                ...
            ],
            "node_to_cluster": {"node_1": 0, "node_2": 0, "node_3": 0, ...},
            "centrality": {"node_1": 5, "node_2": 3, ...},  # 연결 수 기반 중요도
            "computed_at": "2026-01-16T12:00:00Z"
        }
    """
    from datetime import datetime
    
    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])
    
    if not nodes:
        return {
            "clusters": [],
            "node_to_cluster": {},
            "centrality": {},
            "computed_at": datetime.utcnow().isoformat() + "Z"
        }
    
    # Union-Find 초기화
    node_ids = [n.get("id") for n in nodes]
    parent = {nid: nid for nid in node_ids}
    rank = {nid: 0 for nid in node_ids}
    
    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])  # Path compression
        return parent[x]
    
    def union(x, y):
        px, py = find(x), find(y)
        if px == py:
            return
        # Union by rank
        if rank[px] < rank[py]:
            parent[px] = py
        elif rank[px] > rank[py]:
            parent[py] = px
        else:
            parent[py] = px
            rank[px] += 1
    
    # 엣지 기반 Union
    for edge in edges:
        src, tgt = edge.get("source", ""), edge.get("target", "")
        if src in parent and tgt in parent:
            union(src, tgt)
    
    # 클러스터 구성
    clusters_map: Dict[str, List[str]] = {}
    for nid in node_ids:
        root = find(nid)
        if root not in clusters_map:
            clusters_map[root] = []
        clusters_map[root].append(nid)
    
    # 클러스터 정보 구성
    node_to_type = {n.get("id"): n.get("type", "unknown") for n in nodes}
    clusters = []
    node_to_cluster = {}
    
    for idx, (root, members) in enumerate(clusters_map.items()):
        types_in_cluster = list(set(node_to_type.get(m, "unknown") for m in members))
        clusters.append({
            "root": root,
            "members": members,
            "types": types_in_cluster,
            "size": len(members)
        })
        for m in members:
            node_to_cluster[m] = idx
    
    # Centrality (연결 수) 계산
    centrality = {nid: 0 for nid in node_ids}
    for edge in edges:
        src, tgt = edge.get("source", ""), edge.get("target", "")
        if src in centrality:
            centrality[src] += 1
        if tgt in centrality:
            centrality[tgt] += 1
    
    return {
        "clusters": clusters,
        "node_to_cluster": node_to_cluster,
        "centrality": centrality,
        "computed_at": datetime.utcnow().isoformat() + "Z"
    }


def get_cached_clusters(workflow: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    워크플로우 메타데이터에서 캐시된 클러스터 정보 조회
    
    Returns:
        캐시된 클러스터 정보 또는 None
    """
    metadata = workflow.get("metadata", {})
    return metadata.get("_cluster_cache")


# 노드 타입별 의미 있는 그룹명 매핑
NODE_TYPE_GROUP_NAMES = {
    "aiModel": "AI 모델 호출",
    "llm": "LLM 처리",
    "dataProcessor": "데이터 전처리",
    "dataTransform": "데이터 변환",
    "condition": "조건 분기",
    "loop": "반복 처리",
    "parallel": "병렬 처리",
    "httpRequest": "API 호출",
    "database": "데이터베이스 연동",
    "storage": "스토리지 연동",
    "notification": "알림 전송",
    "validation": "유효성 검사",
    "operator": "제어 흐름",
    "start": "시작점",
    "end": "종료점",
}


def _group_omitted_nodes(
    omitted_nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    adjacency: Dict[str, set]
) -> List[Dict[str, Any]]:
    """
    [Critical Fix ③] 생략된 노드들을 의미 있는 그룹으로 묶어 반환
    
    단순 ID 목록 대신 "데이터 전처리 그룹 (노드 5개)"와 같이
    추상화된 그룹명으로 요약하여 Gemini가 생략된 부분의 '의미'까지
    파악할 수 있도록 합니다.
    
    그룹화 전략:
    1. 타입별 그룹화: 동일 타입 노드들은 하나의 그룹으로
    2. 연결 기반 그룹화: 서로 연결된 노드들은 하나의 파이프라인으로
    3. 위치 기반 그룹화: 인접한 위치의 노드들은 하나의 영역으로
    
    Args:
        omitted_nodes: 생략된 노드 리스트
        edges: 전체 엣지 리스트
        adjacency: 인접 노드 맵
    
    Returns:
        [
            {"group_name": "데이터 전처리 그룹", "node_count": 5, "node_types": ["dataProcessor", "dataTransform"]},
            {"group_name": "AI 추론 파이프라인", "node_count": 3, "node_types": ["aiModel", "llm"]},
            ...
        ]
    """
    if not omitted_nodes:
        return []
    
    # 1. 타입별 그룹화 (기본 전략)
    type_groups: Dict[str, List[Dict]] = {}
    for node in omitted_nodes:
        node_type = node.get("type", "unknown")
        if node_type not in type_groups:
            type_groups[node_type] = []
        type_groups[node_type].append(node)
    
    # 2. 연결 기반 파이프라인 감지 (Union-Find)
    omitted_ids = {n.get("id") for n in omitted_nodes}
    parent = {nid: nid for nid in omitted_ids}
    
    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]
    
    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py
    
    # 생략된 노드끼리 연결된 경우 같은 그룹으로
    for edge in edges:
        src, tgt = edge.get("source", ""), edge.get("target", "")
        if src in omitted_ids and tgt in omitted_ids:
            union(src, tgt)
    
    # 파이프라인(연결된 노드 그룹) 구성
    pipeline_groups: Dict[str, List[Dict]] = {}
    for node in omitted_nodes:
        node_id = node.get("id", "")
        root = find(node_id)
        if root not in pipeline_groups:
            pipeline_groups[root] = []
        pipeline_groups[root].append(node)
    
    # 3. 그룹 요약 생성
    result = []
    processed_nodes = set()
    
    # 연결 기반 파이프라인 우선 (3개 이상 연결된 경우)
    for root, nodes in sorted(pipeline_groups.items(), key=lambda x: -len(x[1])):
        if len(nodes) >= 3:
            node_ids = {n.get("id") for n in nodes}
            if node_ids & processed_nodes:
                continue  # 이미 처리된 노드 건너뛰기
            
            types_in_pipeline = list(set(n.get("type", "unknown") for n in nodes))
            
            # 파이프라인 이름 추론
            pipeline_name = _infer_pipeline_name(types_in_pipeline, nodes)
            
            result.append({
                "group_name": pipeline_name,
                "node_count": len(nodes),
                "node_types": types_in_pipeline,
                "sample_labels": [n.get("data", {}).get("label", n.get("id", ""))[:20] for n in nodes[:3]]
            })
            processed_nodes.update(node_ids)
    
    # 남은 노드들은 타입별로 그룹화
    remaining_by_type: Dict[str, List[Dict]] = {}
    for node in omitted_nodes:
        if node.get("id") in processed_nodes:
            continue
        node_type = node.get("type", "unknown")
        if node_type not in remaining_by_type:
            remaining_by_type[node_type] = []
        remaining_by_type[node_type].append(node)
    
    for node_type, nodes in remaining_by_type.items():
        if not nodes:
            continue
        
        group_name = NODE_TYPE_GROUP_NAMES.get(node_type, f"{node_type} 노드")
        if len(nodes) > 1:
            group_name += f" 그룹"
        
        result.append({
            "group_name": group_name,
            "node_count": len(nodes),
            "node_types": [node_type],
            "sample_labels": [n.get("data", {}).get("label", n.get("id", ""))[:20] for n in nodes[:2]]
        })
    
    return result


def _infer_pipeline_name(types: List[str], nodes: List[Dict]) -> str:
    """
    노드 타입들로부터 의미 있는 파이프라인 이름 추론
    """
    # 우선순위: AI/LLM 관련 → 데이터 처리 → 일반
    if any(t in ("aiModel", "llm") for t in types):
        if any(t in ("dataProcessor", "dataTransform") for t in types):
            return "AI 데이터 처리 파이프라인"
        return "AI 추론 파이프라인"
    
    if any(t in ("dataProcessor", "dataTransform", "validation") for t in types):
        return "데이터 전처리 파이프라인"
    
    if any(t in ("httpRequest", "database", "storage") for t in types):
        return "외부 연동 파이프라인"
    
    if any(t in ("condition", "loop", "parallel") for t in types):
        return "제어 흐름 블록"
    
    # 라벨에서 힌트 추출
    labels = [n.get("data", {}).get("label", "") for n in nodes]
    common_words = ["처리", "변환", "검증", "호출", "저장"]
    for word in common_words:
        if any(word in label for label in labels):
            return f"{word} 파이프라인"
    
    return f"연결된 노드 그룹 ({len(nodes)}개)"


def _filter_by_centrality(
    focus_node_ids: set,
    adjacency: Dict[str, set],
    mentioned_node_ids: set,
    max_nodes: int
) -> set:
    """
    [③ Critical Fix] Centrality 기반 필터링
    
    focus_node_ids가 max_nodes를 초과할 경우,
    '중요도(Centrality)'가 높은 노드 위주로 필터링합니다.
    
    중요도 계산 기준:
    1. 사용자가 명시적으로 언급한 노드: 최우선 유지
    2. 연결 수(degree centrality)가 많은 노드: 허브 역할
    3. Start/End 노드: 구조적으로 중요
    
    Args:
        focus_node_ids: 현재 포커스 노드 ID 집합
        adjacency: 인접 노드 맵 {node_id -> set of neighbors}
        mentioned_node_ids: 사용자 요청에서 언급된 노드 ID
        max_nodes: 최대 허용 노드 수
    
    Returns:
        필터링된 노드 ID 집합 (크기 <= max_nodes)
    """
    if len(focus_node_ids) <= max_nodes:
        return focus_node_ids
    
    # 각 노드의 중요도 점수 계산
    scores: Dict[str, float] = {}
    
    for node_id in focus_node_ids:
        score = 0.0
        
        # 1. 사용자 언급 노드: 최우선 (+1000점)
        if node_id in mentioned_node_ids:
            score += 1000.0
        
        # 2. 연결 수 (Degree Centrality): 연결당 +10점
        degree = len(adjacency.get(node_id, []))
        score += degree * 10.0
        
        # 3. 포커스 내 연결 비율: 포커스 노드와 많이 연결될수록 중요
        focus_neighbors = len(adjacency.get(node_id, set()) & focus_node_ids)
        score += focus_neighbors * 5.0
        
        scores[node_id] = score
    
    # 점수 순으로 정렬하여 상위 max_nodes개 선택
    sorted_nodes = sorted(focus_node_ids, key=lambda x: scores.get(x, 0), reverse=True)
    filtered = set(sorted_nodes[:max_nodes])
    
    # 언급된 노드는 반드시 포함 (점수와 무관하게)
    for node_id in mentioned_node_ids:
        if node_id in focus_node_ids:
            filtered.add(node_id)
            # max_nodes 초과 시 가장 낮은 점수 노드 제거
            if len(filtered) > max_nodes:
                non_mentioned = [n for n in filtered if n not in mentioned_node_ids]
                if non_mentioned:
                    lowest = min(non_mentioned, key=lambda x: scores.get(x, 0))
                    filtered.discard(lowest)
    
    logger.debug(f"Centrality filtering: {len(focus_node_ids)} -> {len(filtered)} nodes")
    return filtered


def extract_focus_window(
    workflow: Dict[str, Any],
    user_request: str,
    max_nodes: int = FOCUS_WINDOW_MAX_NODES,
    adjacent_depth: int = FOCUS_WINDOW_ADJACENT_DEPTH
) -> Dict[str, Any]:
    """
    대규모 워크플로우에서 '포커스 윈도우'만 추출
    
    전체 워크플로우를 프롬프트에 주입하면:
    1. 토큰 제한에 걸릴 수 있음
    2. 모델이 관련 없는 노드에 혼란스러워함
    3. 비용 증가
    
    해결책:
    - 사용자 요청에서 언급된 노드 식별
    - 해당 노드와 인접 노드만 추출
    - 나머지는 요약 정보로 제공
    
    Args:
        workflow: 전체 워크플로우 {"nodes": [...], "edges": [...]}
        user_request: 사용자 요청 텍스트
        max_nodes: 최대 포함 노드 수
        adjacent_depth: 인접 노드 탐색 깊이
    
    Returns:
        {
            "focus_nodes": [...],  # 상세 정보 포함
            "focus_edges": [...],  # 관련 엣지
            "context_summary": {   # 전체 요약
                "total_nodes": int,
                "total_edges": int,
                "node_types": {...},
                "omitted_node_ids": [...]
            }
        }
    """
    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])
    
    # 워크플로우가 충분히 작으면 전체 반환
    if len(nodes) <= max_nodes:
        return {
            "focus_nodes": nodes,
            "focus_edges": edges,
            "context_summary": None,
            "is_complete": True
        }
    
    # 1. 사용자 요청에서 언급된 노드 ID/라벨 탐색
    mentioned_node_ids = set()
    request_lower = user_request.lower()
    
    for node in nodes:
        node_id = node.get("id", "")
        node_label = node.get("data", {}).get("label", "")
        node_type = node.get("type", "")
        
        # ID, 라벨, 타입이 요청에 언급되었는지 확인
        if (node_id.lower() in request_lower or 
            node_label.lower() in request_lower or
            node_type.lower() in request_lower):
            mentioned_node_ids.add(node_id)
    
    # 언급된 노드가 없으면 휴리스틱 적용
    if not mentioned_node_ids:
        # 수정/추가 관련 키워드로 대상 추론
        modify_keywords = ["수정", "변경", "edit", "modify", "change", "update"]
        add_keywords = ["추가", "연결", "add", "connect", "insert"]
        
        is_modify = any(kw in request_lower for kw in modify_keywords)
        is_add = any(kw in request_lower for kw in add_keywords)
        
        if is_modify or is_add:
            # 마지막 몇 개 노드를 포커스 (최근 작업 대상일 가능성)
            recent_nodes = nodes[-5:] if len(nodes) >= 5 else nodes
            mentioned_node_ids = {n.get("id") for n in recent_nodes}
        else:
            # 전체 생성 요청: Start/End 노드와 중간 일부
            for node in nodes:
                if node.get("type") in ("operator", "start", "end"):
                    mentioned_node_ids.add(node.get("id"))
            # 중간 노드 일부 추가
            if len(mentioned_node_ids) < max_nodes // 2:
                for node in nodes[:max_nodes // 2]:
                    mentioned_node_ids.add(node.get("id"))
    
    # 2. 인접 노드 그래프 구축
    adjacency = {}  # node_id -> set of connected node_ids
    for edge in edges:
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        if src not in adjacency:
            adjacency[src] = set()
        if tgt not in adjacency:
            adjacency[tgt] = set()
        adjacency[src].add(tgt)
        adjacency[tgt].add(src)
    
    # 3. BFS로 인접 노드 탐색
    focus_node_ids = set(mentioned_node_ids)
    frontier = list(mentioned_node_ids)
    
    for _ in range(adjacent_depth):
        next_frontier = []
        for node_id in frontier:
            for neighbor in adjacency.get(node_id, []):
                if neighbor not in focus_node_ids:
                    focus_node_ids.add(neighbor)
                    next_frontier.append(neighbor)
                    if len(focus_node_ids) >= max_nodes:
                        break
            if len(focus_node_ids) >= max_nodes:
                break
        frontier = next_frontier
        if len(focus_node_ids) >= max_nodes:
            break
    
    # ════════════════════════════════════════════════════════════════════
    # [③ Critical Fix] Edge Case 처리: Centrality 기반 필터링
    # - "전체 워크플로우의 모든 노드 타입을 바꿰줘"와 같은 광범위 요청 시
    # - focus_node_ids가 max_nodes를 초과할 경우 발생
    # - Centrality(연결 수)가 높은 노드 위주로 재필터링
    # ════════════════════════════════════════════════════════════════════
    if len(focus_node_ids) > max_nodes:
        focus_node_ids = _filter_by_centrality(
            focus_node_ids=focus_node_ids,
            adjacency=adjacency,
            mentioned_node_ids=mentioned_node_ids,
            max_nodes=max_nodes
        )
        logger.info(f"Centrality filtering applied: {len(focus_node_ids)} nodes after filtering")
    
    # 4. 포커스 노드/엣지 추출
    focus_nodes = [n for n in nodes if n.get("id") in focus_node_ids]
    focus_edges = [
        e for e in edges 
        if e.get("source") in focus_node_ids and e.get("target") in focus_node_ids
    ]
    
    # 5. 컨텍스트 요약 생성
    omitted_nodes = [n for n in nodes if n.get("id") not in focus_node_ids]
    node_types = {}
    for node in nodes:
        ntype = node.get("type", "unknown")
        node_types[ntype] = node_types.get(ntype, 0) + 1
    
    # ════════════════════════════════════════════════════════════════════
    # [Critical Fix ③] 지능형 그룹 요약 생성
    # - 단순 ID 목록 대신 의미 있는 그룹명으로 추상화
    # - Gemini가 생략된 부분의 '의미'까지 파악 가능
    # ════════════════════════════════════════════════════════════════════
    omitted_groups = _group_omitted_nodes(omitted_nodes, edges, adjacency)
    
    return {
        "focus_nodes": focus_nodes,
        "focus_edges": focus_edges,
        "context_summary": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "focus_node_count": len(focus_nodes),
            "node_types": node_types,
            # [Fix ③] 그룹화된 요약 (기존 ID 목록 대체)
            "omitted_groups": omitted_groups,
            "omitted_node_ids": [n.get("id") for n in omitted_nodes][:10],  # 하위 호환용
            "has_more_omitted": len(omitted_nodes) > 10
        },
        "is_complete": False
    }


# Prompts
ANALYSIS_PROMPT = """사용자 요청을 분석하여 두 가지를 판단해.
1. intent: 요청이 구조화된 워크플로우 JSON 생성을 요구하는 'workflow'인지, 단순 정보 요청인 'text'인지 판단.
2. complexity: 요청의 복잡도를 '단순', '보통', '복잡' 중 하나로 분류.

반드시 아래와 같은 JSON 형식으로만 답변해야 해. 다른 설명은 절대 추가하지 마.
예시 응답: {{"intent": "workflow", "complexity": "보통"}}

사용자 요청:
{user_request}
"""


class DesignerService:
    """
    Service for AI-powered workflow design.
    
    Responsibilities:
    - Analyze user requests to determine intent (workflow vs text)
    - Generate workflow components via LLM streaming
    - Provide mock responses for testing
    """
    
    def __init__(self):
        self._bedrock_client = None
        
    @property
    def bedrock_client(self):
        """Lazy Bedrock client initialization."""
        if self._bedrock_client is None:
            import boto3
            self._bedrock_client = boto3.client(
                'bedrock-runtime',
                region_name=os.getenv('AWS_REGION', 'us-east-1')
            )
        return self._bedrock_client
    
    def is_mock_mode(self) -> bool:
        """Check if mock mode is enabled."""
        if _common_is_mock_mode is not None:
            return _common_is_mock_mode()
        return os.getenv("MOCK_MODE", "true").strip().lower() in {"true", "1", "yes", "on"}

    def analyze_request(self, user_request: str) -> Dict[str, str]:
        """
        Analyze user request to determine intent and complexity.
        
        Returns:
            {"intent": "workflow" | "text", "complexity": "단순" | "보통" | "복잡"}
        """
        if self.is_mock_mode():
            logger.info("MOCK_MODE: Forcing intent=workflow")
            return {"intent": "workflow", "complexity": "단순"}
        
        try:
            prompt = ANALYSIS_PROMPT.format(user_request=user_request)
            response = self.invoke_model(MODEL_HAIKU, prompt, "")
            
            # Extract text from response
            text = self._extract_text(response)
            if text:
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass
            
            return {"intent": "text", "complexity": "단순"}
            
        except Exception as e:
            logger.exception(f"analyze_request failed: {e}")
            return {"intent": "text", "complexity": "단순"}

    def invoke_model(
        self,
        model_id: str,
        user_prompt: str,
        system_prompt: str = "",
        max_tokens: int = 1024
    ) -> Dict[str, Any]:
        """
        Invoke a Bedrock model (Claude or Gemini).
        """
        if self.is_mock_mode():
            return self._mock_text_response()
        
        is_gemini = "gemini" in model_id.lower()
        
        if is_gemini:
            payload = {
                "text_generation_config": {"max_output_tokens": max_tokens},
                "contents": [{"role": "user", "parts": [{"text": user_prompt or " "}]}]
            }
            if system_prompt:
                payload["system_instruction"] = {"parts": [{"text": system_prompt}]}
        else:
            payload = {
                "max_tokens": max_tokens,
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": user_prompt or " "}]
            }
            if system_prompt:
                payload["system"] = system_prompt
        
        response = self.bedrock_client.invoke_model(
            body=json.dumps(payload),
            modelId=model_id
        )
        return json.loads(response.get("body").read())

    def stream_workflow_generation(
        self,
        system_prompt: str,
        user_request: str,
        broadcast_fn: Optional[callable] = None,
        thinking_budget_tokens: int = 0,
        enable_thinking: bool = False,
        model_id: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Stream workflow generation from LLM.
        
        [Critical Fix ①] Partial JSON Parser 적용:
        - JSON 내부 개행 문자 안전 처리
        - Bracket Counting으로 완전한 객체 감지
        - 파싱 실패 시 버퍼 유지 후 재시도
        
        [Critical Fix ②] Thinking Budget 적용:
        - Gemini 3 모델 사용 시 thinking_config 주입
        
        [Critical Fix ③] Model ID Mismatch 해결:
        - Gemini/Claude 모델을 동적으로 선택
        - is_gemini 체크와 실제 모델 호출이 일치하도록 보장
        
        Args:
            system_prompt: System prompt for the LLM
            user_request: User's workflow request
            broadcast_fn: Optional function to broadcast chunks (for WebSocket)
            thinking_budget_tokens: Thinking Mode 토큰 예산 (0=비활성화)
            enable_thinking: Thinking Mode 활성화 여부
            model_id: 사용할 모델 ID (None이면 자동 선택)
            
        Yields:
            Workflow components as dictionaries
        """
        if self.is_mock_mode():
            yield from self._mock_workflow_stream(broadcast_fn)
            return
        
        try:
            # ════════════════════════════════════════════════════════════════
            # [Critical Fix ①] Model ID 동적 선택 (Mismatch 해결)
            # ════════════════════════════════════════════════════════════════
            # Thinking Mode가 활성화되면 Gemini를 사용, 아니면 Sonnet 사용
            if model_id is None:
                if enable_thinking and thinking_budget_tokens > 0:
                    selected_model = MODEL_GEMINI
                else:
                    selected_model = MODEL_SONNET
            else:
                selected_model = model_id
            
            is_gemini = "gemini" in selected_model.lower()
            
            logger.info(f"Model selection: {selected_model} (is_gemini={is_gemini}, thinking={enable_thinking})")
            
            # ════════════════════════════════════════════════════════════════
            # [Fix ②] Thinking Budget을 payload에 주입
            # ════════════════════════════════════════════════════════════════
            if is_gemini and enable_thinking and thinking_budget_tokens > 0:
                # Gemini 3 Native API with Thinking Mode
                payload = {
                    "contents": [{"role": "user", "parts": [{"text": user_request}]}],
                    "generation_config": {
                        "max_output_tokens": int(os.getenv("STREAM_MAX_TOKENS", "4096")),
                        "temperature": 0.7,
                        # Thinking Mode 설정
                        "thinking_config": {
                            "thinking_budget": thinking_budget_tokens,
                            "include_thoughts": True  # UI에 사고 과정 노출
                        }
                    }
                }
                if system_prompt:
                    payload["system_instruction"] = {"parts": [{"text": system_prompt}]}
                
                logger.info(f"Gemini streaming with thinking_budget={thinking_budget_tokens}")
            else:
                # 기존 Claude/Bedrock 방식
                payload = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": int(os.getenv("STREAM_MAX_TOKENS", "4096")),
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_request}]
                }
            
            response = self.bedrock_client.invoke_model_with_response_stream(
                modelId=selected_model,  # [Fix ①] 동적으로 선택된 모델 사용
                body=json.dumps(payload)
            )
            
            stream = response.get("body")
            if not stream:
                return
            
            # ════════════════════════════════════════════════════════════════
            # [Fix ①] Partial JSON Parser 사용
            # ════════════════════════════════════════════════════════════════
            decoder = codecs.getincrementaldecoder("utf-8")()
            parser = PartialJSONParser()
            ui_delay = float(os.environ.get("STREAMING_UI_DELAY", "0.1"))
            
            for event in stream:
                chunk = event.get("chunk")
                if not chunk:
                    continue
                chunk_bytes = chunk.get("bytes")
                if not chunk_bytes:
                    continue
                
                try:
                    text = decoder.decode(chunk_bytes)
                except Exception:
                    continue
                
                # Partial JSON Parser로 안전하게 파싱
                parsed_objects = parser.feed(text)
                
                for json_obj in parsed_objects:
                    if broadcast_fn:
                        broadcast_fn(json_obj)
                    yield json_obj
                    
                    if ui_delay > 0:
                        time.sleep(ui_delay)
            
            # 스트림 종료 시 남은 버퍼 처리
            for json_obj in parser.flush():
                if broadcast_fn:
                    broadcast_fn(json_obj)
                yield json_obj
            
            # Emit done status
            done_obj = {"type": "status", "data": "done"}
            if broadcast_fn:
                broadcast_fn(done_obj)
            yield done_obj
            
        except Exception as e:
            logger.exception(f"stream_workflow_generation failed: {e}")
            error_obj = {"type": "error", "data": str(e)}
            if broadcast_fn:
                broadcast_fn(error_obj)
            yield error_obj

    # =========================================================================
    # Mock Helpers
    # =========================================================================
    
    def _mock_workflow_json(self) -> Dict[str, Any]:
        """Generate a mock workflow for testing."""
        return {
            "name": "Mock Workflow",
            "nodes": [
                {"id": "start", "type": "operator", "position": {"x": 150, "y": 50}, "data": {"label": "Start"}},
                {"id": "mock_llm", "type": "aiModel", "prompt_content": "목업 응답", "position": {"x": 150, "y": 150}},
                {"id": "end", "type": "operator", "position": {"x": 150, "y": 250}, "data": {"label": "End"}}
            ],
            "edges": [
                {"id": "e-start-mock_llm", "source": "start", "target": "mock_llm"},
                {"id": "e-mock_llm-end", "source": "mock_llm", "target": "end"}
            ]
        }
    
    def _mock_text_response(self) -> Dict[str, Any]:
        return {"content": [{"text": "목업 응답입니다."}]}
    
    def _mock_workflow_stream(self, broadcast_fn: Optional[callable]) -> Iterator[Dict[str, Any]]:
        """Stream mock workflow components."""
        mock_wf = self._mock_workflow_json()
        ui_delay = float(os.environ.get("STREAMING_UI_DELAY", "0.1"))
        
        for node in mock_wf.get("nodes", []):
            obj = {"type": "node", "data": node}
            if broadcast_fn:
                broadcast_fn(obj)
            yield obj
            if ui_delay > 0:
                time.sleep(ui_delay)
        
        for edge in mock_wf.get("edges", []):
            obj = {"type": "edge", "data": edge}
            if broadcast_fn:
                broadcast_fn(obj)
            yield obj
            if ui_delay > 0:
                time.sleep(ui_delay)
        
        done_obj = {"type": "status", "data": "done"}
        if broadcast_fn:
            broadcast_fn(done_obj)
        yield done_obj
    
    def _extract_text(self, response: Dict[str, Any]) -> Optional[str]:
        """Extract text from Bedrock response."""
        if isinstance(response, dict) and "content" in response:
            blocks = response.get("content", [])
            if blocks and isinstance(blocks[0], dict):
                return blocks[0].get("text")
        return None


# Singleton
_designer_service_instance = None

def get_designer_service() -> DesignerService:
    global _designer_service_instance
    if _designer_service_instance is None:
        _designer_service_instance = DesignerService()
    return _designer_service_instance


# =========================================================================
# 고수준 API 함수들 (핸들러에서 사용)
# =========================================================================

def analyze_request(user_request: str) -> Dict[str, str]:
    """
    사용자 요청의 intent와 complexity 분석
    
    핸들러에서 직접 호출 가능한 편의 함수
    """
    return get_designer_service().analyze_request(user_request)


def stream_workflow_jsonl(
    user_request: str,
    current_workflow: Optional[Dict[str, Any]] = None,
    canvas_mode: str = "agentic-designer",
    enable_thinking: bool = True
) -> Iterator[str]:
    """
    워크플로우 생성 JSONL 스트리밍
    
    [Critical Fix] 세 가지 핵심 개선:
    ① Partial JSON Parser: 스트리밍 안정성 강화
    ② Thinking Budget 실제 적용: API 호출에 주입
    ③ Focus Window: 대규모 워크플로우 컨텍스트 최적화
    
    Args:
        user_request: 사용자 요청
        current_workflow: 현재 워크플로우 상태
        canvas_mode: Canvas 모드
        enable_thinking: Thinking Mode 활성화 여부
        
    Yields:
        JSONL 형식의 응답 문자열
    """
    from services.design.prompts import SYSTEM_PROMPT, get_gemini_system_prompt
    
    service = get_designer_service()
    
    # ════════════════════════════════════════════════════════════════════
    # [Fix ②] Thinking Level Control - 동적 사고 예산 계산
    # ════════════════════════════════════════════════════════════════════
    thinking_budget = 0
    thinking_level = None
    
    if _THINKING_AVAILABLE and enable_thinking:
        thinking_budget, thinking_level, thinking_reason = calculate_thinking_budget(
            canvas_mode=canvas_mode,
            current_workflow=current_workflow or {},
            user_request=user_request
        )
        logger.info(f"Thinking budget calculated: {thinking_level.value}={thinking_budget} tokens ({thinking_reason})")
    elif enable_thinking:
        # Fallback: 모드에 따른 기본값
        thinking_budget = 8192 if canvas_mode == "agentic-designer" else 2048
        logger.info(f"Thinking budget fallback: {thinking_budget} tokens (model_router unavailable)")
    
    # ════════════════════════════════════════════════════════════════════
    # [Fix ③] Focus Window - 대규모 워크플로우 컨텍스트 최적화
    # ════════════════════════════════════════════════════════════════════
    workflow_context = current_workflow or {"nodes": [], "edges": []}
    
    focus_data = extract_focus_window(
        workflow=workflow_context,
        user_request=user_request,
        max_nodes=FOCUS_WINDOW_MAX_NODES,
        adjacent_depth=FOCUS_WINDOW_ADJACENT_DEPTH
    )
    
    # 프롬프트에 포함할 워크플로우 정보 구성
    if focus_data.get("is_complete"):
        # 워크플로우가 작으면 전체 포함
        workflow_json = json.dumps(workflow_context, ensure_ascii=False, indent=2)
        context_note = ""
    else:
        # 포커스 윈도우만 포함
        focus_workflow = {
            "nodes": focus_data["focus_nodes"],
            "edges": focus_data["focus_edges"]
        }
        workflow_json = json.dumps(focus_workflow, ensure_ascii=False, indent=2)
        
        summary = focus_data["context_summary"]
        
        # ════════════════════════════════════════════════════════════════
        # [Critical Fix ③] 지능형 그룹 요약으로 컨텍스트 노트 생성
        # - 단순 ID 목록 대신 의미 있는 그룹명으로 생략된 영역 표현
        # - Gemini가 생략된 부분의 '의미'까지 파악 가능
        # ════════════════════════════════════════════════════════════════
        omitted_groups = summary.get("omitted_groups", [])
        
        if omitted_groups:
            # 그룹별 요약 문자열 생성
            group_descriptions = []
            for group in omitted_groups:
                group_name = group.get("group_name", "기타 노드")
                node_count = group.get("node_count", 0)
                sample_labels = group.get("sample_labels", [])
                
                if sample_labels:
                    samples_str = ", ".join(f'"{l}"' for l in sample_labels[:2])
                    group_descriptions.append(f"  • {group_name} ({node_count}개): 예) {samples_str}")
                else:
                    group_descriptions.append(f"  • {group_name} ({node_count}개)")
            
            omitted_section = "\n".join(group_descriptions)
        else:
            # 폴백: 기존 ID 목록 방식
            omitted_ids = summary.get("omitted_node_ids", [])
            omitted_section = f"  • 생략된 노드 ID: {', '.join(omitted_ids[:10])}{'...' if summary.get('has_more_omitted') else ''}"
        
        context_note = f"""
[참고: 워크플로우 요약]
- 전체 노드 수: {summary['total_nodes']}개 (현재 표시: {summary['focus_node_count']}개)
- 전체 엣지 수: {summary['total_edges']}개
- 노드 유형 분포: {json.dumps(summary['node_types'], ensure_ascii=False)}

[생략된 영역 - 아래 그룹들은 현재 요청과 직접 관련이 없어 상세 정보를 생략합니다]
{omitted_section}

⚠️ 위는 요청과 관련된 부분만 추출한 것입니다. 
   새로운 노드를 추가하거나 연결할 때 생략된 영역과의 연결도 고려해주세요.
"""
        logger.info(f"Focus window applied: {summary['focus_node_count']}/{summary['total_nodes']} nodes, {len(omitted_groups)} omitted groups")
    
    # 프롬프트 구성 (JSONL 구분자 안내 포함)
    enhanced_prompt = f"""사용자 요청: {user_request}

[현재 워크플로우]
{workflow_json}
{context_note}

## 출력 규칙
1. 각 JSON 객체를 별도의 줄에 출력하세요
2. JSON 객체 내부에 개행 문자가 필요하면 \\n으로 이스케이프하세요
3. 노드/엣지 변경, 제안, 설명을 JSONL 형식으로 출력하세요
4. 완료 시 {{"type": "status", "data": "done"}}을 출력하세요

위 요청을 분석하여 워크플로우를 생성/수정해주세요."""
    
    try:
        # [Fix ②] thinking_budget을 실제 API 호출에 전달
        for obj in service.stream_workflow_generation(
            system_prompt=SYSTEM_PROMPT,
            user_request=enhanced_prompt,
            broadcast_fn=None,
            thinking_budget_tokens=thinking_budget,
            enable_thinking=enable_thinking and thinking_budget > 0
        ):
            yield json.dumps(obj, ensure_ascii=False) + "\n"
    except Exception as e:
        logger.error(f"Workflow streaming failed: {e}")
        yield json.dumps({"type": "error", "data": str(e)}) + "\n"


def build_text_response(user_request: str) -> Dict[str, Any]:
    """
    텍스트 응답 생성 (워크플로우가 아닌 일반 질문)
    
    Returns:
        Lambda 응답 형식의 딕셔너리
    """
    service = get_designer_service()
    
    try:
        response = service.invoke_model(
            model_id=MODEL_HAIKU,
            user_prompt=user_request,
            system_prompt="",
            max_tokens=1024
        )
        
        text = service._extract_text(response)
        if not text:
            text = "모델로부터 응답을 받지 못했습니다."
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "complexity": "단순",
                "response": {"text": text}
            })
        }
    except Exception as e:
        logger.exception("텍스트 응답 생성 중 오류")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
