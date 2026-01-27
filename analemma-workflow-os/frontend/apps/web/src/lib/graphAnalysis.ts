/**
 * Graph Analysis Module
 * 
 * 그래프 위상 분석을 통해 병렬 실행과 반복 실행을 자동으로 감지합니다.
 * 이 분석 결과는 workflowConverter에서 백엔드 스키마로 변환할 때 사용됩니다.
 * 
 * v2.0 개선사항:
 * - Nested Logic 지원 (depth, parentGroupId)
 * - Parallel vs Conditional 분기 구분
 * - 위상 정렬 캐싱으로 성능 최적화
 */

import { getOutgoers, getIncomers } from '@xyflow/react';
import type { Node, Edge } from '@xyflow/react';

// ============================================================================
// Types
// ============================================================================

export interface CycleInfo {
  /** 고유 ID (자동 생성) */
  id: string;
  /** Cycle 경로: ["A", "B", "C", "A"] */
  path: string[];
  /** 역방향 엣지 (C → A) */
  backEdge: Edge;
  /** Cycle 내의 노드들 (마지막 중복 제외) */
  loopNodes: string[];
  /** Cycle 내의 엣지들 */
  loopEdges: Edge[];
  /** 중첩 깊이 (0 = 최상위) */
  depth: number;
  /** 부모 그룹 ID (ParallelGroup 또는 CycleInfo) */
  parentGroupId: string | null;
}

export interface ParallelGroup {
  /** 고유 ID (자동 생성) */
  id: string;
  /** 분기 시작 노드 */
  sourceNodeId: string;
  /** 각 브랜치의 노드 ID 배열 */
  branches: string[][];
  /** 합류 지점 노드 ID (없으면 null) */
  convergenceNodeId: string | null;
  /** 분기되는 엣지들 */
  branchEdges: Edge[];
  /** 중첩 깊이 (0 = 최상위) */
  depth: number;
  /** 부모 그룹 ID (ParallelGroup 또는 CycleInfo) */
  parentGroupId: string | null;
  /** 분기 유형: 'parallel' (모두 실행) 또는 'conditional' (조건 실행) */
  branchType: 'parallel' | 'conditional';
}

export interface GraphAnalysisWarning {
  /** 경고 유형 */
  type: 'orphan_node' | 'unreachable_node' | 'accidental_cycle' | 'many_branches';
  /** 경고 메시지 */
  message: string;
  /** 관련 노드 ID들 */
  nodeIds: string[];
  /** 해결 제안 */
  suggestion: string;
}

export interface GraphAnalysisResult {
  /** 감지된 Cycle (Loop) 정보 */
  cycles: CycleInfo[];
  /** 감지된 병렬 분기 정보 */
  parallelGroups: ParallelGroup[];
  /** 역방향 엣지 ID 목록 */
  backEdgeIds: Set<string>;
  /** 병렬 분기 시작점 노드 ID 목록 */
  parallelSourceIds: Set<string>;
  /** 위상 정렬 결과 (성능 최적화용 캐시) */
  topologicalOrder: string[];
  /** 노드별 위상 순서 인덱스 */
  nodeOrderIndex: Map<string, number>;
  /** 그래프 구조 경고 */
  warnings: GraphAnalysisWarning[];
}

// ============================================================================
// Topological Sort (위상 정렬 - 캐싱용)
// ============================================================================

/**
 * Kahn's Algorithm 기반 위상 정렬
 * DAG가 아닌 경우 (사이클 존재) 부분적인 순서만 반환
 */
export function topologicalSort(
  nodes: Node[],
  edges: Edge[],
  backEdgeIds: Set<string> = new Set()
): { order: string[]; nodeIndex: Map<string, number> } {
  const nodeIds = new Set(nodes.map(n => n.id));
  const inDegree = new Map<string, number>();
  const adjacency = new Map<string, string[]>();
  
  // 초기화
  for (const nodeId of nodeIds) {
    inDegree.set(nodeId, 0);
    adjacency.set(nodeId, []);
  }
  
  // 그래프 구성 (back-edge 제외)
  for (const edge of edges) {
    if (backEdgeIds.has(edge.id)) continue;
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue;
    
    adjacency.get(edge.source)?.push(edge.target);
    inDegree.set(edge.target, (inDegree.get(edge.target) || 0) + 1);
  }
  
  // in-degree가 0인 노드들로 시작
  const queue: string[] = [];
  for (const [nodeId, degree] of inDegree) {
    if (degree === 0) queue.push(nodeId);
  }
  
  const order: string[] = [];
  const nodeIndex = new Map<string, number>();
  
  while (queue.length > 0) {
    const current = queue.shift()!;
    nodeIndex.set(current, order.length);
    order.push(current);
    
    for (const neighbor of adjacency.get(current) || []) {
      const newDegree = (inDegree.get(neighbor) || 1) - 1;
      inDegree.set(neighbor, newDegree);
      if (newDegree === 0) {
        queue.push(neighbor);
      }
    }
  }
  
  return { order, nodeIndex };
}

// ============================================================================
// Cycle Detection (반복 실행 감지)
// ============================================================================

/**
 * DFS 기반 Cycle 감지
 * 
 * Back-edge를 찾아 반복 실행 구간을 식별합니다.
 * Back-edge: 현재 노드에서 이미 방문 스택에 있는 조상 노드로 가는 엣지
 */
export function detectCycles(nodes: Node[], edges: Edge[]): CycleInfo[] {
  // 인접 리스트 생성
  const graph: Map<string, Edge[]> = new Map();
  const nodeIds = new Set(nodes.map(n => n.id));
  
  for (const nodeId of nodeIds) {
    graph.set(nodeId, []);
  }
  
  for (const edge of edges) {
    if (nodeIds.has(edge.source) && nodeIds.has(edge.target)) {
      graph.get(edge.source)?.push(edge);
    }
  }
  
  const cycles: CycleInfo[] = [];
  const visited = new Set<string>();
  const recStack = new Set<string>();
  const path: string[] = [];
  const pathEdges: Edge[] = [];
  let cycleCounter = 0;
  
  function dfs(nodeId: string): void {
    visited.add(nodeId);
    recStack.add(nodeId);
    path.push(nodeId);
    
    const outEdges = graph.get(nodeId) || [];
    
    for (const edge of outEdges) {
      const target = edge.target;
      
      if (!visited.has(target)) {
        pathEdges.push(edge);
        dfs(target);
        pathEdges.pop();
      } else if (recStack.has(target)) {
        // Cycle 발견! target은 현재 재귀 스택에 있는 조상 노드
        const cycleStart = path.indexOf(target);
        const cyclePath = [...path.slice(cycleStart), target];
        const loopNodes = path.slice(cycleStart);
        
        // Cycle 내의 엣지들 수집
        const loopEdges: Edge[] = [];
        for (let i = cycleStart; i < path.length - 1; i++) {
          const e = edges.find(e => e.source === path[i] && e.target === path[i + 1]);
          if (e) loopEdges.push(e);
        }
        loopEdges.push(edge); // back-edge 추가
        
        cycles.push({
          id: `cycle_${cycleCounter++}`,
          path: cyclePath,
          backEdge: edge,
          loopNodes,
          loopEdges,
          depth: 0, // 초기값, 나중에 계산
          parentGroupId: null
        });
      }
    }
    
    path.pop();
    recStack.delete(nodeId);
  }
  
  for (const node of nodes) {
    if (!visited.has(node.id)) {
      dfs(node.id);
    }
  }
  
  return cycles;
}

// ============================================================================
// Parallel Detection (병렬 실행 감지)
// ============================================================================

/**
 * 병렬/조건부 분기 감지
 * 
 * 하나의 노드에서 2개 이상의 서로 다른 노드로 나가는 엣지가 있으면
 * 병렬 또는 조건부 실행으로 판단합니다.
 * 
 * 분기 유형 결정:
 * - 모든 나가는 엣지에 condition이 있으면 → 'conditional'
 * - 그 외 → 'parallel'
 */
export function detectParallelBranches(
  nodes: Node[], 
  edges: Edge[],
  backEdgeIds: Set<string>,
  nodeOrderIndex: Map<string, number>
): ParallelGroup[] {
  const parallelGroups: ParallelGroup[] = [];
  let groupCounter = 0;
  
  for (const node of nodes) {
    // 현재 노드에서 나가는 엣지 (back-edge 제외)
    const outEdges = edges.filter(
      e => e.source === node.id && !backEdgeIds.has(e.id)
    );
    
    // 병렬 조건: 2개 이상의 서로 다른 타겟
    const uniqueTargets = [...new Set(outEdges.map(e => e.target))];
    
    if (uniqueTargets.length >= 2) {
      // 분기 유형 결정: 모든 엣지에 condition이 있으면 conditional
      const allHaveCondition = outEdges.every(
        e => e.data?.condition !== undefined && e.data?.condition !== null && e.data?.condition !== ''
      );
      const branchType: 'parallel' | 'conditional' = allHaveCondition ? 'conditional' : 'parallel';
      
      // 각 브랜치 추적
      const branches = uniqueTargets.map(targetId => 
        traceBranchPath(targetId, nodes, edges, backEdgeIds)
      );
      
      // 합류 지점 찾기 (위상 순서 활용)
      const convergence = findConvergenceNodeOptimized(branches, nodes, edges, nodeOrderIndex);
      
      parallelGroups.push({
        id: `parallel_${groupCounter++}`,
        sourceNodeId: node.id,
        branches,
        convergenceNodeId: convergence,
        branchEdges: outEdges,
        depth: 0, // 초기값, 나중에 계산
        parentGroupId: null,
        branchType
      });
    }
  }
  
  return parallelGroups;
}

/**
 * 브랜치 경로 추적
 * 
 * 시작 노드부터 합류 지점(in-degree > 1) 또는 리프 노드까지 추적합니다.
 */
function traceBranchPath(
  startNodeId: string,
  nodes: Node[],
  edges: Edge[],
  backEdgeIds: Set<string>
): string[] {
  const branch: string[] = [startNodeId];
  let current = startNodeId;
  const visited = new Set<string>([startNodeId]);
  
  while (true) {
    // 현재 노드에서 나가는 엣지 (back-edge 제외)
    const outEdges = edges.filter(
      e => e.source === current && !backEdgeIds.has(e.id) && !visited.has(e.target)
    );
    
    // 종료 조건: 나가는 엣지가 없거나 2개 이상 (중첩 병렬)
    if (outEdges.length === 0 || outEdges.length > 1) {
      break;
    }
    
    const next = outEdges[0].target;
    
    // 합류 지점 체크 (in-degree > 1)
    const inEdges = edges.filter(
      e => e.target === next && !backEdgeIds.has(e.id)
    );
    
    if (inEdges.length > 1) {
      // 합류 지점 도달 - 이 노드는 브랜치에 포함하지 않음
      break;
    }
    
    branch.push(next);
    visited.add(next);
    current = next;
  }
  
  return branch;
}

/**
 * 합류 지점(Convergence Node) 찾기 - 최적화 버전
 * 
 * 위상 정렬 인덱스를 활용하여 모든 브랜치가 공통으로 도달하는
 * 가장 가까운(위상적으로 가장 상위) 노드를 찾습니다.
 */
function findConvergenceNodeOptimized(
  branches: string[][],
  nodes: Node[],
  edges: Edge[],
  nodeOrderIndex: Map<string, number>
): string | null {
  if (branches.length < 2) return null;

  // 각 브랜치의 마지막 노드에서 도달 가능한 노드들 수집
  const reachableSets = branches.map(branch => 
    getReachableNodes(branch[branch.length - 1], nodes, edges)
  );
  
  // 모든 브랜치에서 공통으로 도달 가능한 노드 찾기
  const commonNodes = [...reachableSets[0]].filter(
    nodeId => reachableSets.every(set => set.has(nodeId))
  );

  if (commonNodes.length === 0) return null;

  // 위상적으로 가장 상위(가장 빠른 순서)에 있는 노드 선택
  // 모든 브랜치로부터의 위상적 거리 합이 최소인 노드 찾기
  const getTopologicalDistance = (targetId: string): number => {
    const targetOrder = nodeOrderIndex.get(targetId) ?? Infinity;
    return branches.reduce((sum, branch) => {
      const lastNode = branch[branch.length - 1];
      const lastOrder = nodeOrderIndex.get(lastNode) ?? 0;
      return sum + Math.abs(targetOrder - lastOrder);
    }, 0);
  };

  return commonNodes.sort((a, b) => {
    // 위상 순서가 가장 빠른 노드 우선
    const orderA = nodeOrderIndex.get(a) ?? Infinity;
    const orderB = nodeOrderIndex.get(b) ?? Infinity;
    if (orderA !== orderB) return orderA - orderB;
    
    // 같다면 거리 합이 최소인 노드
    return getTopologicalDistance(a) - getTopologicalDistance(b);
  })[0];
}

/**
 * 특정 노드에서 도달 가능한 모든 노드 반환 (BFS)
 */
function getReachableNodes(
  startNodeId: string,
  nodes: Node[],
  edges: Edge[]
): Set<string> {
  const reachable = new Set<string>();
  const queue = [startNodeId];
  const visited = new Set<string>([startNodeId]);
  
  while (queue.length > 0) {
    const current = queue.shift()!;
    
    const outEdges = edges.filter(e => e.source === current);
    for (const edge of outEdges) {
      if (!visited.has(edge.target)) {
        visited.add(edge.target);
        reachable.add(edge.target);
        queue.push(edge.target);
      }
    }
  }
  
  return reachable;
}

// ============================================================================
// Nested Structure Analysis (중첩 구조 분석)
// ============================================================================

/**
 * 중첩 구조 계산
 * 
 * 각 CycleInfo와 ParallelGroup의 depth와 parentGroupId를 설정합니다.
 */
function calculateNesting(
  cycles: CycleInfo[],
  parallelGroups: ParallelGroup[]
): void {
  // 모든 그룹을 노드 집합으로 매핑
  const allGroups: Array<{ id: string; nodes: Set<string>; obj: CycleInfo | ParallelGroup }> = [
    ...cycles.map(c => ({ 
      id: c.id, 
      nodes: new Set(c.loopNodes), 
      obj: c as CycleInfo | ParallelGroup 
    })),
    ...parallelGroups.map(p => ({ 
      id: p.id, 
      nodes: new Set(p.branches.flat()), 
      obj: p as CycleInfo | ParallelGroup 
    }))
  ];
  
  // 포함 관계 그래프 생성
  const containedBy = new Map<string, string[]>();
  
  for (const group of allGroups) {
    containedBy.set(group.id, []);
  }
  
  for (const outer of allGroups) {
    for (const inner of allGroups) {
      if (outer.id === inner.id) continue;
      
      // inner의 모든 노드가 outer에 포함되는지 확인
      const isContained = [...inner.nodes].every(nodeId => outer.nodes.has(nodeId));
      if (isContained) {
        containedBy.get(inner.id)?.push(outer.id);
      }
    }
  }
  
  // 각 그룹의 depth와 직접 부모 계산
  for (const group of allGroups) {
    const containers = containedBy.get(group.id) || [];
    
    if (containers.length === 0) {
      // 최상위 그룹
      group.obj.depth = 0;
      group.obj.parentGroupId = null;
    } else {
      // 가장 작은 포함 그룹 찾기 (직접 부모)
      let directParent: { id: string; size: number } | null = null;
      
      for (const containerId of containers) {
        const containerGroup = allGroups.find(g => g.id === containerId);
        if (!containerGroup) continue;
        
        const size = containerGroup.nodes.size;
        if (!directParent || size < directParent.size) {
          directParent = { id: containerId, size };
        }
      }
      
      group.obj.parentGroupId = directParent?.id || null;
      
      // depth 계산: 부모 체인 따라가기
      let depth = 0;
      let currentParent = directParent?.id;
      while (currentParent) {
        depth++;
        const parentGroup = allGroups.find(g => g.id === currentParent);
        currentParent = parentGroup ? containedBy.get(currentParent)?.[0] : undefined;
        // 무한 루프 방지
        if (depth > 100) break;
      }
      group.obj.depth = depth;
    }
  }
}

// ============================================================================
// Main Analysis Function
// ============================================================================

/**
 * 워크플로우 그래프 분석
 * 
 * Cycle (Loop)과 Parallel 분기를 감지하여 분석 결과를 반환합니다.
 * 중첩 구조도 자동으로 분석합니다.
 */
export function analyzeWorkflowGraph(
  nodes: Node[],
  edges: Edge[]
): GraphAnalysisResult {
  const warnings: GraphAnalysisWarning[] = [];
  
  // 1. Cycle 감지
  const cycles = detectCycles(nodes, edges);
  
  // Back-edge ID 수집
  const backEdgeIds = new Set(cycles.map(c => c.backEdge.id));
  
  // 2. 위상 정렬 (성능 최적화를 위한 캐싱)
  const { order: topologicalOrder, nodeIndex: nodeOrderIndex } = topologicalSort(nodes, edges, backEdgeIds);
  
  // 3. Parallel 분기 감지 (back-edge 제외, 위상 순서 활용)
  const parallelGroups = detectParallelBranches(nodes, edges, backEdgeIds, nodeOrderIndex);
  
  // 4. 중첩 구조 계산
  calculateNesting(cycles, parallelGroups);
  
  // Parallel source 노드 ID 수집
  const parallelSourceIds = new Set(parallelGroups.map(g => g.sourceNodeId));
  
  // 5. 고아 노드 감지 (orphan nodes)
  const orphanNodes = findOrphanNodes(nodes, edges);
  if (orphanNodes.length > 0) {
    warnings.push({
      type: 'orphan_node',
      message: `${orphanNodes.length}개의 노드가 다른 노드와 연결되어 있지 않습니다.`,
      nodeIds: orphanNodes,
      suggestion: '이 노드들을 워크플로우에 연결하거나 제거하세요.'
    });
  }
  
  // 6. 도달 불가능한 노드 감지 (unreachable nodes)
  const unreachableNodes = findUnreachableNodes(nodes, edges);
  if (unreachableNodes.length > 0) {
    warnings.push({
      type: 'unreachable_node',
      message: `${unreachableNodes.length}개의 노드가 시작점에서 도달할 수 없습니다.`,
      nodeIds: unreachableNodes,
      suggestion: '이 노드들을 시작점과 연결하거나 제거하세요.'
    });
  }
  
  // 7. 과도한 병렬 분기 경고 (많은 out-degree)
  parallelGroups.forEach(pg => {
    if (pg.branches.length > 10) {
      warnings.push({
        type: 'many_branches',
        message: `노드에서 ${pg.branches.length}개의 분기가 있습니다. 실행 시간이 길어질 수 있습니다.`,
        nodeIds: [pg.sourceNodeId],
        suggestion: '병렬 분기를 여러 단계로 분할하는 것을 고려하세요.'
      });
    }
  });
  
  // 8. 데드락 경고 (상호 순환 참조)
  if (cycles.length > 1) {
    // 여러 사이클이 서로 얽혀 있는 경우 데드락 가능성
    const overlappingCycles = cycles.filter((c1, i) => 
      cycles.some((c2, j) => i !== j && c1.loopNodes.some(n => c2.loopNodes.includes(n)))
    );
    if (overlappingCycles.length > 0) {
      warnings.push({
        type: 'accidental_cycle',
        message: '여러 사이클이 겹쳐 있어 데드락이 발생할 수 있습니다.',
        nodeIds: overlappingCycles.flatMap(c => c.loopNodes),
        suggestion: '사이클 구조를 단순화하고 명확한 제어 흐름을 설계하세요.'
      });
    }
  }
  
  return {
    cycles,
    parallelGroups,
    backEdgeIds,
    parallelSourceIds,
    topologicalOrder,
    nodeOrderIndex,
    warnings
  };
}

// ============================================================================
// Validation Functions (고아 노드, 도달 불가능 노드 감지)
// ============================================================================

/**
 * 고아 노드 감지: 어떤 엣지와도 연결되지 않은 노드
 * 
 * 노드가 1개만 있으면 고아가 아님 (정상)
 */
export function findOrphanNodes(nodes: Node[], edges: Edge[]): string[] {
  const nodeIds = new Set(nodes.map(n => n.id));
  const connectedNodes = new Set<string>();
  
  // 엣지에 연결된 모든 노드 수집
  for (const edge of edges) {
    if (nodeIds.has(edge.source)) connectedNodes.add(edge.source);
    if (nodeIds.has(edge.target)) connectedNodes.add(edge.target);
  }
  
  // 노드가 1개만 있으면 고아 아님
  if (nodeIds.size <= 1) {
    return [];
  }
  
  // 연결되지 않은 노드들
  const orphans = [...nodeIds].filter(id => !connectedNodes.has(id));
  return orphans;
}

/**
 * 도달 불가능한 노드 감지: 시작점(in-degree=0)에서 도달할 수 없는 노드
 * 
 * BFS로 시작점들로부터 도달 가능한 모든 노드를 찾고,
 * 도달하지 못한 노드들을 반환
 */
export function findUnreachableNodes(nodes: Node[], edges: Edge[]): string[] {
  const nodeIds = new Set(nodes.map(n => n.id));
  
  // In-degree 계산
  const inDegree = new Map<string, number>();
  const adjacency = new Map<string, string[]>();
  
  for (const nodeId of nodeIds) {
    inDegree.set(nodeId, 0);
    adjacency.set(nodeId, []);
  }
  
  for (const edge of edges) {
    if (nodeIds.has(edge.source) && nodeIds.has(edge.target)) {
      adjacency.get(edge.source)?.push(edge.target);
      inDegree.set(edge.target, (inDegree.get(edge.target) || 0) + 1);
    }
  }
  
  // 시작점들 (in-degree = 0)
  const startNodes = [...nodeIds].filter(id => inDegree.get(id) === 0);
  
  // 시작점이 없으면 모든 노드가 사이클에 있거나 고립됨
  if (startNodes.length === 0) {
    return [];
  }
  
  // BFS로 도달 가능한 노드 찾기
  const reachable = new Set<string>();
  const queue = [...startNodes];
  
  while (queue.length > 0) {
    const current = queue.shift()!;
    reachable.add(current);
    
    const neighbors = adjacency.get(current) || [];
    for (const neighbor of neighbors) {
      if (!reachable.has(neighbor)) {
        queue.push(neighbor);
      }
    }
  }
  
  // 도달 불가능한 노드들
  const unreachable = [...nodeIds].filter(id => !reachable.has(id));
  return unreachable;
}

// ============================================================================
// Utility Functions
// ============================================================================

export function isEdgeInCycle(edgeId: string, cycles: CycleInfo[]): boolean {
  return cycles.some(cycle => 
    cycle.loopEdges.some(e => e.id === edgeId)
  );
}

export function isBackEdge(edgeId: string, cycles: CycleInfo[]): boolean {
  return cycles.some(cycle => cycle.backEdge.id === edgeId);
}

/**
 * 특정 노드가 어느 그룹에 속하는지 찾기
 */
export function findContainingGroups(
  nodeId: string,
  cycles: CycleInfo[],
  parallelGroups: ParallelGroup[]
): { cycles: CycleInfo[]; parallelGroups: ParallelGroup[] } {
  return {
    cycles: cycles.filter(c => c.loopNodes.includes(nodeId)),
    parallelGroups: parallelGroups.filter(p => p.branches.some(b => b.includes(nodeId)))
  };
}

/**
 * 그룹을 depth 순으로 정렬 (깊은 것부터)
 */
export function sortGroupsByDepth<T extends { depth: number }>(groups: T[]): T[] {
  return [...groups].sort((a, b) => b.depth - a.depth);
}
