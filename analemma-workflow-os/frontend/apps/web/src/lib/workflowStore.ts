import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import {
  Node,
  Edge,
  NodeChange,
  EdgeChange,
  Connection,
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
} from '@xyflow/react';
import { toast } from 'sonner';
import { detectAndSuggestControlBlock } from './controlBlockGenerator';

// 서브그래프 정의
interface SubgraphDefinition {
  id: string;
  nodes: Node[];
  edges: Edge[];
  metadata: {
    name: string;
    description?: string;
    createdAt: string;
  };
}

interface WorkflowState {
  nodes: Node[];
  edges: Edge[];
  currentWorkflowId?: string;
  currentWorkflowName?: string;
  currentWorkflowInputs?: Record<string, any>;

  // 서브그래프 관련 상태
  subgraphs: Record<string, SubgraphDefinition>;
  navigationPath: string[]; // 현재 탐색 경로 (서브그래프 ID 스택)

  selectedNodeId: string | null;
  setSelectedNodeId: (id: string | null) => void;

  setNodes: (nodes: Node[]) => void;
  setEdges: (edges: Edge[]) => void;
  setCurrentWorkflow: (id?: string, name?: string, inputs?: Record<string, any>) => void;
  loadWorkflow: (workflow: { nodes: Node[]; edges: Edge[] }) => void;
  clearWorkflow: () => void;
  addNode: (node: Node) => void;
  updateNode: (id: string, changes: Partial<Node>) => void;
  removeNode: (id: string) => void;
  addEdge: (edge: Edge) => void;
  updateEdge: (id: string, changes: Partial<Edge>) => void;
  removeEdge: (id: string) => void;
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;

  // 서브그래프 관련 액션
  groupNodes: (nodeIds: string[], groupName: string) => void;
  ungroupNode: (groupNodeId: string) => void;
  navigateToSubgraph: (subgraphId: string) => void;
  navigateUp: (levels?: number) => void;
  navigateToRoot: () => void;
}

export const useWorkflowStore = create<WorkflowState>()(
  persist(
    (set, get) => ({
      nodes: [],
      edges: [],
      currentWorkflowId: undefined,
      currentWorkflowName: undefined,
      currentWorkflowInputs: undefined,
      subgraphs: {},
      navigationPath: [],
      selectedNodeId: null,

      setSelectedNodeId: (id) => set({ selectedNodeId: id }),

      setNodes: (nodes) => set({ nodes }),
      setEdges: (edges) => set({ edges }),
      setCurrentWorkflow: (id, name, inputs) => set({ currentWorkflowId: id, currentWorkflowName: name, currentWorkflowInputs: inputs }),

      loadWorkflow: (workflow) =>
        set({
          nodes: workflow.nodes || [],
          edges: workflow.edges || [],
        }),

      clearWorkflow: () => set({
        nodes: [],
        edges: [],
        subgraphs: {},
        navigationPath: [],
        currentWorkflowId: undefined,
        currentWorkflowName: undefined,
        currentWorkflowInputs: undefined
      }),

      addNode: (node) => set((state) => ({ nodes: [...state.nodes, node] })),

      updateNode: (id, changes) =>
        set((state) => ({
          nodes: state.nodes.map((n) => {
            if (n.id !== id) return n;

            // data 필드가 변경 사항에 있는 경우 병합 로직
            const updates = (changes.data as any) || {};

            // [특수 로직] nodeTypeChanged 플래그가 있으면 노드 타입 자체를 변경
            if (updates.nodeTypeChanged) {
              const newType = updates.nodeTypeChanged;
              const cleanUpdates = { ...updates };
              // 플래그 제거
              delete cleanUpdates.nodeTypeChanged;

              return {
                ...n,
                ...changes,
                type: newType as string,
                data: { ...n.data, ...cleanUpdates },
              };
            }

            // 일반적인 업데이트
            return {
              ...n,
              ...changes,
              data: { ...n.data, ...updates },
            };
          }),
        })),

      removeNode: (id) =>
        set((state) => ({
          nodes: state.nodes.filter((n) => n.id !== id),
          // 노드가 삭제되면 연결된 엣지도 함께 삭제
          edges: state.edges.filter((e) => e.source !== id && e.target !== id),
        })),

      addEdge: (edge) => set((state) => ({ edges: [...state.edges, edge] })),

      updateEdge: (id, changes) =>
        set((state) => ({
          edges: state.edges.map((e) => (e.id === id ? { ...e, ...changes } : e)),
        })),

      removeEdge: (id) => set((state) => ({ edges: state.edges.filter((e) => e.id !== id) })),

      onNodesChange: (changes) => set((state) => ({ nodes: applyNodeChanges(changes, state.nodes) })),

      onEdgesChange: (changes) => set((state) => ({ edges: applyEdgeChanges(changes, state.edges) })),

      onConnect: (connection) => {
        const state = get();
        
        // 1. Block self-loop: Prevent nodes from connecting to themselves
        if (connection.source === connection.target) {
          toast.error('Cannot connect a node to itself.');
          return;
        }
        
        // 2. Block duplicate edges: Check if source → target connection already exists
        const isDuplicate = state.edges.some(
          (edge) => edge.source === connection.source && edge.target === connection.target
        );
        
        if (isDuplicate) {
          toast.error('This connection already exists.');
          return;
        }

        // 3. Check if this connection creates a cycle (back-edge detection)
        const wouldCreateCycle = () => {
          // Simulate adding the new edge
          const tempEdges = [...state.edges, {
            id: 'temp',
            source: connection.source!,
            target: connection.target!
          }];

          // DFS cycle detection
          const visited = new Set<string>();
          const recStack = new Set<string>();

          const hasCycle = (nodeId: string): boolean => {
            visited.add(nodeId);
            recStack.add(nodeId);

            const outgoing = tempEdges.filter(e => e.source === nodeId);
            for (const edge of outgoing) {
              if (!visited.has(edge.target)) {
                if (hasCycle(edge.target)) return true;
              } else if (recStack.has(edge.target)) {
                return true; // Cycle detected
              }
            }

            recStack.delete(nodeId);
            return false;
          };

          return hasCycle(connection.source!);
        };

        const createsBackEdge = wouldCreateCycle();
        
        // 검증 통과: 새 엣지 추가
        const newEdge = {
          ...connection,
          animated: true,
          style: { stroke: 'hsl(263 70% 60%)', strokeWidth: 2 },
          data: {
            ...connection.data,
            isBackEdge: createsBackEdge,
            edgeType: createsBackEdge ? 'while' : 'edge' // Default to 'while' for loop back-edges
          }
        };

        set((state) => ({
          edges: addEdge(newEdge, state.edges),
        }));

        // 🔄 Back-edge (Loop) 감지 시 While Control Block 제안
        if (createsBackEdge) {
          const sourceNode = state.nodes.find(n => n.id === connection.source);
          const targetNode = state.nodes.find(n => n.id === connection.target);
          
          if (sourceNode && targetNode) {
            toast.info('🔄 Loop detected! Would you like to create a While Control Block for better visualization?', {
              duration: 10000,
              action: {
                label: 'Create Loop Block',
                onClick: () => {
                  // While Loop Control Block 생성
                  const controlBlockPosition = {
                    x: (sourceNode.position.x + targetNode.position.x) / 2,
                    y: sourceNode.position.y + 80
                  };

                  const controlBlockNode = {
                    id: `loop_block_${Date.now()}`,
                    type: 'control_block',
                    position: controlBlockPosition,
                    data: {
                      label: 'Loop Control',
                      blockType: 'while',
                      branches: [],
                      max_iterations: 10,
                      natural_condition: '',
                      back_edge_source: targetNode.id
                    }
                  };

                  // 원래 back-edge 제거하고 Control Block을 통한 엣지로 교체
                  const newSourceToBlock = {
                    id: `${connection.source}-${controlBlockNode.id}`,
                    source: connection.source!,
                    target: controlBlockNode.id,
                    type: 'smart',
                    animated: true
                  };

                  const newBlockToTarget = {
                    id: `${controlBlockNode.id}-${connection.target}`,
                    source: controlBlockNode.id,
                    target: connection.target!,
                    type: 'smart',
                    animated: true,
                    data: {
                      loopType: 'while',
                      isBackEdge: true
                    }
                  };

                  set((state) => ({
                    nodes: [...state.nodes, controlBlockNode],
                    edges: [
                      ...state.edges.filter(e => 
                        !(e.source === connection.source && e.target === connection.target)
                      ),
                      newSourceToBlock,
                      newBlockToTarget
                    ]
                  }));

                  toast.success('While Loop Control Block created!');
                }
              },
              cancel: {
                label: 'Keep as is',
                onClick: () => {
                  toast.info('Keeping as back-edge. You can convert to Control Block later.');
                }
              }
            });
          }
        }

        // 🔀 분기 패턴 감지 및 Conditional Control Block 자동 생성
        const suggestion = detectAndSuggestControlBlock(
          connection.source!,
          state.nodes,
          [...state.edges, newEdge]
        );

        if (suggestion && !createsBackEdge) {  // Loop 제안과 중복 방지
          // Control Block 자동 생성 (사용자 확인 없이)
          set((currentState) => ({
            nodes: [...currentState.nodes, suggestion.controlBlockNode],
            edges: [
              ...currentState.edges.filter(e => !suggestion.originalEdges.includes(e)),
              ...suggestion.newEdges
            ]
          }));
          
          toast.success(`Control Block created for branching at ${sourceNode?.data?.label || connection.source}`);
          
          // 원래 엣지 연결 취소 (Control Block을 통해서만 연결되도록)
          return;
        }
      },

      // 선택된 노드들을 그룹(서브그래프)으로 묶기
      groupNodes: (nodeIds: string[], groupName: string) => {
        const state = get();
        const nodesToGroup = state.nodes.filter((n) => nodeIds.includes(n.id));

        if (nodesToGroup.length < 2) {
          toast.error('Failed to create subgraph: At least 2 nodes are required');
          return;
        }

        // 그룹화할 노드들 간의 내부 엣지 찾기
        const internalEdges = state.edges.filter(
          (e) => nodeIds.includes(e.source) && nodeIds.includes(e.target)
        );

        // 외부에서 들어오는/나가는 엣지 찾기
        const externalEdges = state.edges.filter(
          (e) => (nodeIds.includes(e.source) && !nodeIds.includes(e.target)) ||
            (!nodeIds.includes(e.source) && nodeIds.includes(e.target))
        );

        // ═══════════════════════════════════════════════════════════════
        // 서브그래프 검증 로직
        // ═══════════════════════════════════════════════════════════════
        
        // 1. 진입 엣지 검증 (외부 → 서브그래프)
        const entryEdges = externalEdges.filter(
          (e) => !nodeIds.includes(e.source) && nodeIds.includes(e.target)
        );
        
        // 2. 탈출 엣지 검증 (서브그래프 → 외부)
        const exitEdges = externalEdges.filter(
          (e) => nodeIds.includes(e.source) && !nodeIds.includes(e.target)
        );

        // 3. 진입점이 정확히 1개인지 검증
        if (entryEdges.length === 0) {
          toast.error('Failed to create subgraph: No entry edge from outside');
          return;
        }
        if (entryEdges.length > 1) {
          toast.error(`Failed to create subgraph: Entry edge must be 1, but found ${entryEdges.length}`);
          return;
        }

        // 4. 탈출점이 정확히 1개인지 검증
        if (exitEdges.length === 0) {
          toast.error('Failed to create subgraph: No exit edge to outside');
          return;
        }
        if (exitEdges.length > 1) {
          toast.error(`Failed to create subgraph: Exit edge must be 1, but found ${exitEdges.length}`);
          return;
        }

        // 5. 진입 노드와 탈출 노드 식별
        const entryNodeId = entryEdges[0].target;
        const exitNodeId = exitEdges[0].source;

        // 6. 진입 노드와 탈출 노드가 다른지 검증 (단일 노드 서브그래프 방지)
        if (entryNodeId === exitNodeId && nodesToGroup.length === 1) {
          toast.error('Failed to create subgraph: Single node cannot have same entry/exit node');
          return;
        }

        // 7. 내부 연결성 검증 (모든 노드가 연결되어 있는지)
        const connectedNodes = new Set<string>();
        const queue = [entryNodeId];
        connectedNodes.add(entryNodeId);

        while (queue.length > 0) {
          const current = queue.shift()!;
          const outgoingEdges = internalEdges.filter(e => e.source === current);
          
          for (const edge of outgoingEdges) {
            if (!connectedNodes.has(edge.target) && nodeIds.includes(edge.target)) {
              connectedNodes.add(edge.target);
              queue.push(edge.target);
            }
          }
        }

        const disconnectedNodes = nodesToGroup.filter(n => !connectedNodes.has(n.id));
        if (disconnectedNodes.length > 0) {
          toast.error(`Failed to create subgraph: ${disconnectedNodes.length} node(s) unreachable from entry point`);
          return;
        }

        // 8. 순환 참조 검증 (내부에 사이클이 있는지)
        const hasCycle = () => {
          const visited = new Set<string>();
          const recStack = new Set<string>();

          const detectCycle = (nodeId: string): boolean => {
            visited.add(nodeId);
            recStack.add(nodeId);

            const outgoingEdges = internalEdges.filter(e => e.source === nodeId);
            for (const edge of outgoingEdges) {
              if (!visited.has(edge.target)) {
                if (detectCycle(edge.target)) return true;
              } else if (recStack.has(edge.target)) {
                return true;
              }
            }

            recStack.delete(nodeId);
            return false;
          };

          return detectCycle(entryNodeId);
        };

        if (hasCycle()) {
          toast.warning('Warning: Circular reference detected in subgraph. May cause infinite loop during execution');
          // Show warning and continue (some workflows may intentionally use loops)
        }

        // 9. 그룹 노드 타입 검증 (이미 그룹 노드는 중첩 불가)
        const hasGroupNode = nodesToGroup.some(n => n.type === 'group');
        if (hasGroupNode) {
          toast.error('Failed to create subgraph: Cannot nest subgraphs');
          return;
        }

        // 그룹 노드의 위치 계산 (묶인 노드들의 중심점)
        const avgX = nodesToGroup.reduce((sum, n) => sum + n.position.x, 0) / nodesToGroup.length;
        const avgY = nodesToGroup.reduce((sum, n) => sum + n.position.y, 0) / nodesToGroup.length;

        // 서브그래프 ID 생성
        const subgraphId = `subgraph-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

        // 서브그래프 정의 생성
        const subgraph: SubgraphDefinition = {
          id: subgraphId,
          nodes: nodesToGroup.map((n) => ({
            ...n,
            // 상대 위치로 변환
            position: {
              x: n.position.x - avgX + 200,
              y: n.position.y - avgY + 200,
            },
          })),
          edges: internalEdges,
          metadata: {
            name: groupName,
            createdAt: new Date().toISOString(),
          },
        };

        // 그룹 노드 생성
        const groupNode: Node = {
          id: subgraphId,
          type: 'group',
          position: { x: avgX, y: avgY },
          data: {
            label: groupName,
            subgraphId,
            nodeCount: nodesToGroup.length,
          },
        };

        // 외부 엣지를 그룹 노드에 연결하도록 업데이트
        const updatedExternalEdges = externalEdges.map((edge) => ({
          ...edge,
          source: nodeIds.includes(edge.source) ? subgraphId : edge.source,
          target: nodeIds.includes(edge.target) ? subgraphId : edge.target,
        }));

        // 기존 노드/엣지 제거 및 그룹 노드 추가
        const remainingNodes = state.nodes.filter((n) => !nodeIds.includes(n.id));
        const remainingEdges = state.edges.filter(
          (e) => !nodeIds.includes(e.source) && !nodeIds.includes(e.target)
        );

        set({
          nodes: [...remainingNodes, groupNode],
          edges: [...remainingEdges, ...updatedExternalEdges],
          subgraphs: {
            ...state.subgraphs,
            [subgraphId]: subgraph,
          },
        });

        // 성공 메시지 (진입/탈출 노드 정보 포함)
        const entryNode = nodesToGroup.find(n => n.id === entryNodeId);
        const exitNode = nodesToGroup.find(n => n.id === exitNodeId);
        toast.success(
          `Subgraph "${groupName}" created successfully\n` +
          `Entry: ${entryNode?.data?.label || entryNodeId} → Exit: ${exitNode?.data?.label || exitNodeId}`
        );
      },

      // 그룹 노드를 해제하여 개별 노드로 복원
      ungroupNode: (groupNodeId: string) => {
        const state = get();
        const groupNode = state.nodes.find((n) => n.id === groupNodeId);
        const subgraph = state.subgraphs[groupNodeId];

        if (!groupNode || !subgraph) return;

        // 그룹 노드의 위치를 기준으로 내부 노드 위치 복원
        const restoredNodes = subgraph.nodes.map((n) => ({
          ...n,
          position: {
            x: n.position.x + groupNode.position.x - 200,
            y: n.position.y + groupNode.position.y - 200,
          },
        }));

        // 그룹 노드에 연결된 외부 엣지 찾기
        const edgesToGroup = state.edges.filter((e) => e.target === groupNodeId);
        const edgesFromGroup = state.edges.filter((e) => e.source === groupNodeId);

        // 첫 번째/마지막 노드로 연결 (간단한 휴리스틱)
        const firstNodeId = subgraph.nodes[0]?.id;
        const lastNodeId = subgraph.nodes[subgraph.nodes.length - 1]?.id;

        const reconnectedEdges = [
          ...edgesToGroup.map((e) => ({ ...e, target: firstNodeId })),
          ...edgesFromGroup.map((e) => ({ ...e, source: lastNodeId })),
        ];

        // 그룹 노드 제거, 내부 노드/엣지 복원
        const remainingNodes = state.nodes.filter((n) => n.id !== groupNodeId);
        const remainingEdges = state.edges.filter(
          (e) => e.source !== groupNodeId && e.target !== groupNodeId
        );

        const { [groupNodeId]: removed, ...remainingSubgraphs } = state.subgraphs;

        set({
          nodes: [...remainingNodes, ...restoredNodes],
          edges: [...remainingEdges, ...subgraph.edges, ...reconnectedEdges],
          subgraphs: remainingSubgraphs,
        });
      },

      // 서브그래프 내부로 진입 (네비게이션)
      navigateToSubgraph: (subgraphId: string) => {
        const state = get();
        const subgraph = state.subgraphs[subgraphId];

        if (!subgraph) return;

        // 현재 상태를 임시 저장 (루트 또는 현재 서브그래프)
        const currentPath = state.navigationPath;
        const currentSubgraphId = currentPath[currentPath.length - 1];

        if (currentSubgraphId) {
          // 현재 서브그래프 내용 업데이트
          set((s) => ({
            subgraphs: {
              ...s.subgraphs,
              [currentSubgraphId]: {
                ...s.subgraphs[currentSubgraphId],
                nodes: s.nodes,
                edges: s.edges,
              },
            },
          }));
        } else {
          // 루트 상태 보존을 위해 _root에 저장
          set((s) => ({
            subgraphs: {
              ...s.subgraphs,
              _root: {
                id: '_root',
                nodes: s.nodes,
                edges: s.edges,
                metadata: { name: 'Root', createdAt: '' },
              },
            },
          }));
        }

        // 서브그래프 내용으로 전환
        set({
          nodes: subgraph.nodes,
          edges: subgraph.edges,
          navigationPath: [...currentPath, subgraphId],
        });
      },

      // 상위 레벨로 이동
      navigateUp: (levels = 1) => {
        const state = get();
        const currentPath = state.navigationPath;

        if (currentPath.length === 0) return;

        // 현재 서브그래프 내용 저장
        const currentSubgraphId = currentPath[currentPath.length - 1];
        if (currentSubgraphId && state.subgraphs[currentSubgraphId]) {
          set((s) => ({
            subgraphs: {
              ...s.subgraphs,
              [currentSubgraphId]: {
                ...s.subgraphs[currentSubgraphId],
                nodes: s.nodes,
                edges: s.edges,
              },
            },
          }));
        }

        // 새 경로 계산
        const newPath = currentPath.slice(0, Math.max(0, currentPath.length - levels));
        const targetSubgraphId = newPath[newPath.length - 1];

        if (targetSubgraphId) {
          // 다른 서브그래프로 이동
          const targetSubgraph = state.subgraphs[targetSubgraphId];
          set({
            nodes: targetSubgraph?.nodes || [],
            edges: targetSubgraph?.edges || [],
            navigationPath: newPath,
          });
        } else {
          // 루트로 이동
          const rootSubgraph = state.subgraphs._root;
          set({
            nodes: rootSubgraph?.nodes || [],
            edges: rootSubgraph?.edges || [],
            navigationPath: [],
          });
        }
      },

      // 루트로 바로 이동
      navigateToRoot: () => {
        const state = get();

        if (state.navigationPath.length === 0) return;

        // 현재 서브그래프 내용 저장
        const currentSubgraphId = state.navigationPath[state.navigationPath.length - 1];
        if (currentSubgraphId && state.subgraphs[currentSubgraphId]) {
          set((s) => ({
            subgraphs: {
              ...s.subgraphs,
              [currentSubgraphId]: {
                ...s.subgraphs[currentSubgraphId],
                nodes: s.nodes,
                edges: s.edges,
              },
            },
          }));
        }

        // 루트 상태 복원
        const rootSubgraph = state.subgraphs._root;
        set({
          nodes: rootSubgraph?.nodes || [],
          edges: rootSubgraph?.edges || [],
          navigationPath: [],
        });
      },
    }),
    {
      name: 'workflow-storage', // LocalStorage Key 이름
      storage: createJSONStorage(() => localStorage), // 저장소 지정 (sessionStorage로 변경 가능)
      // 필요 시 특정 필드만 저장하도록 partialize 옵션 사용 가능
      // partialize: (state) => ({ nodes: state.nodes, edges: state.edges }), 
    }
  )
);