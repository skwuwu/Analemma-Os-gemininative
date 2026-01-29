/**
 * Control Block Auto-Generation Utility
 * 
 * 엣지 연결 시 분기 패턴을 감지하여 자동으로 Control Block 노드를 생성합니다.
 * While 루프의 back-edge도 시각적 노드로 표현합니다.
 */

import type { Node, Edge } from '@xyflow/react';
import { generateNodeId } from './nodeFactory';
import type { ControlBlockType, ControlBlockData, BranchConfig } from '@/components/nodes/ControlBlockNode';

export interface ControlBlockSuggestion {
  /** 생성할 Control Block 노드 */
  controlBlockNode: Node<ControlBlockData>;
  /** 원본 엣지들 (삭제 대상) */
  originalEdges: Edge[];
  /** 새로운 엣지들 (Control Block 연결) */
  newEdges: Edge[];
  /** 제안 메시지 */
  message: string;
}

/**
 * 분기 패턴 감지 및 Control Block 제안
 * 
 * @param sourceNodeId - 분기 시작 노드 ID
 * @param nodes - 전체 노드 목록
 * @param edges - 현재 엣지 목록 (새 엣지는 포함되지 않음)
 * @param newTargetNodeId - 새로 추가하려는 target 노드 ID (optional)
 * @returns null if no pattern detected, ControlBlockSuggestion if pattern found
 */
export function detectAndSuggestControlBlock(
  sourceNodeId: string,
  nodes: Node[],
  edges: Edge[],
  newTargetNodeId?: string
): ControlBlockSuggestion | null {
  const outgoingEdges = edges.filter(e => e.source === sourceNodeId);
  
  // 1. 다중 분기 감지: 기존 엣지가 1개 이상 있고, 새 엣지를 추가하려는 경우
  if (outgoingEdges.length >= 1 && newTargetNodeId) {
    return createConditionalBranchSuggestion(sourceNodeId, outgoingEdges, nodes, newTargetNodeId);
  }
  
  // 2. Back-edge 감지 (While Loop)
  const backEdge = detectBackEdge(sourceNodeId, edges, nodes);
  if (backEdge) {
    return createWhileLoopSuggestion(backEdge, nodes, edges);
  }
  
  return null;
}

/**
 * Conditional Branch Control Block 제안 생성
 * 
 * [Fix] 분기 타입 자동 감지:
 * - 모든 엣지에 condition이 있으면 → conditional
 * - 그 외 → parallel (사용자가 나중에 for_each로 변경 가능)
 * 
 * @param sourceNodeId - 분기 시작 노드 ID
 * @param outgoingEdges - 기존 outgoing edges (새 엣지는 포함되지 않음)
 * @param nodes - 전체 노드 목록
 * @param newTargetNodeId - 새로 추가하려는 target 노드 ID
 */
function createConditionalBranchSuggestion(
  sourceNodeId: string,
  outgoingEdges: Edge[],
  nodes: Node[],
  newTargetNodeId: string
): ControlBlockSuggestion {
  const sourceNode = nodes.find(n => n.id === sourceNodeId);
  if (!sourceNode) throw new Error(`Source node ${sourceNodeId} not found`);
  
  // 분기 타입 자동 감지 (기존 엣지들만 확인)
  const allHaveCondition = outgoingEdges.every(
    e => e.data?.condition !== undefined && e.data?.condition !== null && e.data?.condition !== ''
  );
  const detectedBranchType: ControlBlockType = allHaveCondition ? 'conditional' : 'parallel';
  
  // Control Block 노드 위치 계산 (source 노드 오른쪽)
  const controlBlockPosition = {
    x: sourceNode.position.x + 300,
    y: sourceNode.position.y
  };
  
  // Branches 생성: 기존 엣지들 + 새로 추가하려는 엣지
  const branches: BranchConfig[] = [
    ...outgoingEdges.map((edge, idx) => ({
      id: `branch_${idx}`,
      label: (edge.data?.condition as string) || `Branch ${idx + 1}`,
      targetNodeId: edge.target,
      natural_condition: edge.data?.condition as string | undefined
    })),
    // 새로운 branch 추가
    {
      id: `branch_${outgoingEdges.length}`,
      label: `Branch ${outgoingEdges.length + 1}`,
      targetNodeId: newTargetNodeId,
      natural_condition: ''
    }
  ];
  
  // Control Block 노드 생성
  const controlBlockNode: Node<ControlBlockData> = {
    id: generateNodeId(),
    type: 'control_block',
    position: controlBlockPosition,
    data: {
      label: detectedBranchType === 'conditional' ? 'Conditional Branch' : 'Parallel Execution',
      blockType: detectedBranchType,
      branches
    }
  };
  
  // 새 엣지 생성
  // 1. Source → Control Block
  const sourceToBlock: Edge = {
    id: `${sourceNodeId}-${controlBlockNode.id}`,
    source: sourceNodeId,
    target: controlBlockNode.id,
    type: 'smart'
  };
  
  // 2. Control Block → Target 노드들 (기존 + 새로운)
  const blockToTargets: Edge[] = branches.map(branch => ({
    id: `${controlBlockNode.id}-${branch.targetNodeId}`,
    source: controlBlockNode.id,
    sourceHandle: branch.id, // branch ID를 handle ID로 사용
    target: branch.targetNodeId!,
    type: 'smart'
  }));
  
  return {
    controlBlockNode,
    originalEdges: outgoingEdges, // 기존 엣지들만 제거 대상
    newEdges: [sourceToBlock, ...blockToTargets],
    message: `Detected ${branches.length} branches from this node. Creating Control Block for better organization.`
  };
}

/**
 * While Loop Control Block 제안 생성
 */
function createWhileLoopSuggestion(
  backEdge: Edge,
  nodes: Node[],
  edges: Edge[]
): ControlBlockSuggestion {
  const sourceNode = nodes.find(n => n.id === backEdge.source);
  const targetNode = nodes.find(n => n.id === backEdge.target);
  
  if (!sourceNode || !targetNode) {
    throw new Error('Source or target node not found for back-edge');
  }
  
  // Control Block 위치 (back-edge 중간 지점)
  const controlBlockPosition = {
    x: (sourceNode.position.x + targetNode.position.x) / 2,
    y: sourceNode.position.y + 80 // 약간 아래쪽
  };
  
  // While Loop Control Block 생성
  const controlBlockNode: Node<ControlBlockData> = {
    id: generateNodeId(),
    type: 'control_block',
    position: controlBlockPosition,
    data: {
      label: 'Loop Control',
      blockType: 'while',
      branches: [], // while은 branches 없음
      max_iterations: 10,
      natural_condition: '',
      back_edge_source: targetNode.id
    }
  };
  
  // 🔍 Exit edge 찾기: back-edge source에서 나가는 다른 엣지
  // (루프 종료 후 다음 노드로 진행하는 엣지)
  const exitEdges = edges.filter(e => 
    e.source === backEdge.source && 
    e.target !== backEdge.target && // back-edge 제외
    !e.data?.isBackEdge
  );

  // 새 엣지: Source → Control Block → Target (back-edge 시각화)
  const sourceToBlock: Edge = {
    id: `${backEdge.source}-${controlBlockNode.id}`,
    source: backEdge.source,
    target: controlBlockNode.id,
    type: 'smart'
  };
  
  const blockToTarget: Edge = {
    id: `${controlBlockNode.id}-${backEdge.target}`,
    source: controlBlockNode.id,
    target: backEdge.target,
    type: 'smart',
    data: {
      ...backEdge.data,
      loopType: 'while', // back-edge 표시
      isBackEdge: true
    }
  };

  // 🚪 Exit edge 생성: control block → next node (루프 종료 시)
  const exitEdgesFromBlock: Edge[] = exitEdges.map(exitEdge => ({
    id: `${controlBlockNode.id}-exit-${exitEdge.target}`,
    source: controlBlockNode.id,
    target: exitEdge.target,
    type: 'smart',
    data: {
      ...exitEdge.data,
      isLoopExit: true // 루프 종료 엣지 표시
    }
  }));
  
  return {
    controlBlockNode,
    originalEdges: [backEdge, ...exitEdges],
    newEdges: [sourceToBlock, blockToTarget, ...exitEdgesFromBlock],
    message: `Detected a loop pattern with ${exitEdges.length} exit path(s). Would you like to create a While Loop Control Block?`
  };
}

/**
 * Back-edge 감지 (사이클의 역방향 엣지)
 * 
 * 간단한 휴리스틱: target 노드가 source 노드보다 위에 있거나 왼쪽에 있으면 back-edge로 간주
 */
function detectBackEdge(
  sourceNodeId: string,
  edges: Edge[],
  nodes: Node[]
): Edge | null {
  const outgoingEdges = edges.filter(e => e.source === sourceNodeId);
  const sourceNode = nodes.find(n => n.id === sourceNodeId);
  if (!sourceNode) return null;
  
  for (const edge of outgoingEdges) {
    const targetNode = nodes.find(n => n.id === edge.target);
    if (!targetNode) continue;
    
    // Back-edge 휴리스틱: 타겟이 소스보다 위쪽이거나 왼쪽에 있음
    const isBackEdge = 
      targetNode.position.y < sourceNode.position.y - 50 || // 위쪽
      (Math.abs(targetNode.position.y - sourceNode.position.y) < 100 && 
       targetNode.position.x < sourceNode.position.x); // 왼쪽
    
    // 또는 edge data에 명시적으로 표시된 경우
    const isMarkedAsLoop = edge.data?.loopType === 'while' || edge.data?.loopType === 'for_each';
    
    if (isBackEdge || isMarkedAsLoop) {
      return edge;
    }
  }
  
  return null;
}

/**
 * 기존 Control Block 업데이트 (분기 추가 시)
 */
export function updateControlBlockBranches(
  controlBlock: Node<ControlBlockData>,
  newTargetNodeId: string
): Node<ControlBlockData> {
  const currentBranches = controlBlock.data.branches || [];
  const newBranch: BranchConfig = {
    id: `branch_${currentBranches.length}`,
    label: `Branch ${currentBranches.length + 1}`,
    targetNodeId: newTargetNodeId
  };
  
  return {
    ...controlBlock,
    data: {
      ...controlBlock.data,
      branches: [...currentBranches, newBranch]
    }
  };
}

/**
 * Control Block에서 나가는 모든 엣지가 있는지 확인
 */
export function validateControlBlockEdges(
  controlBlock: Node<ControlBlockData>,
  edges: Edge[]
): { valid: boolean; missingBranches: BranchConfig[] } {
  if (controlBlock.data.blockType === 'while') {
    // While은 단일 출력만 필요
    const hasOutput = edges.some(e => e.source === controlBlock.id);
    return {
      valid: hasOutput,
      missingBranches: []
    };
  }
  
  const missingBranches: BranchConfig[] = [];
  
  for (const branch of controlBlock.data.branches) {
    const hasEdge = edges.some(
      e => e.source === controlBlock.id && e.sourceHandle === branch.id
    );
    if (!hasEdge) {
      missingBranches.push(branch);
    }
  }
  
  return {
    valid: missingBranches.length === 0,
    missingBranches
  };
}
