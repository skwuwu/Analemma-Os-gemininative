import { useCallback, useState, useMemo, useEffect, Fragment, lazy, Suspense } from 'react';
import {
  ReactFlow,
  Background,
  Edge,
  Node,
  NodeTypes,
  ReactFlowProvider,
  ReactFlowInstance,
  BackgroundVariant,
  useReactFlow,
  useOnSelectionChange,
  SelectionMode,
  OnSelectionChangeParams,
  NodeChange,
  EdgeChange,
} from '@xyflow/react';
import { useShallow } from 'zustand/react/shallow';
import '@xyflow/react/dist/style.css';

// Node components - lazy loaded to break circular dependencies and control execution order
const AIModelNode = lazy(() => import('./nodes/AIModelNode').then(m => ({ default: m.AIModelNode })));
const OperatorNode = lazy(() => import('./nodes/OperatorNode').then(m => ({ default: m.OperatorNode })));
const TriggerNode = lazy(() => import('./nodes/TriggerNode').then(m => ({ default: m.TriggerNode })));
const ControlNode = lazy(() => import('./nodes/ControlNode').then(m => ({ default: m.ControlNode })));
const GroupNode = lazy(() => import('./nodes/GroupNode').then(m => ({ default: m.GroupNode })));
import { SmartEdge } from './edges/SmartEdge';

// Dialog/Modal/Panel components - static imports to avoid runtime initialization issues
import { NodeEditorDialog } from './NodeEditorDialog';
import { GroupNameDialog } from './GroupNameDialog';
import { RollbackDialog } from './RollbackDialog';
import { SuggestionOverlay } from './SuggestionOverlay';
import { EmptyCanvasGuide } from './EmptyCanvasGuide';

import { Button } from './ui/button';
import {
  Keyboard,
  Layers,
  ChevronRight,
  Play,
  History,
  PanelRightClose,
} from 'lucide-react';
import { TooltipProvider } from './ui/tooltip';
import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip';
import { useWorkflowStore } from '@/lib/workflowStore';
import { useCodesignStore } from '@/lib/codesignStore';
import { useCanvasMode } from '@/hooks/useCanvasMode';
import { useAutoValidation } from '@/hooks/useAutoValidation';
import { WorkflowStatusIndicator } from './WorkflowStatusIndicator';
import { useTimeMachine } from '@/hooks/useBriefingAndCheckpoints';
import { toast } from 'sonner';
import type { TimelineItem, RollbackRequest } from '@/lib/types';
import { createWorkflowNode, generateNodeId } from '@/lib/nodeFactory';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

const WorkflowCanvasInner = () => {
  // ==========================================
  // 1. ALL STATE DECLARATIONS FIRST (CRITICAL: Must be before any useEffect)
  // ==========================================
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [selectedNodes, setSelectedNodes] = useState<Node[]>([]);
  const [editorOpen, setEditorOpen] = useState(false);
  const [groupDialogOpen, setGroupDialogOpen] = useState(false);
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance<any, any> | null>(null);
  
  // Time Machine state (for rollback)
  const [rollbackDialogOpen, setRollbackDialogOpen] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<TimelineItem | null>(null);
  const [currentExecutionId, setCurrentExecutionId] = useState<string | null>(null);
  
  // Empty Canvas Guide visibility
  const [emptyGuideVisible, setEmptyGuideVisible] = useState(true);

  // ==========================================
  // 2. NODE/EDGE TYPES (useMemo)
  // ==========================================
  // Wrap lazy-loaded nodes with Suspense to control execution order
  // This prevents "Cannot access X before initialization" errors
  const nodeTypes: NodeTypes = useMemo(() => ({
    aiModel: (props: any) => (
      <Suspense fallback={<div className="p-4 bg-muted/50 animate-pulse rounded-lg border border-border" />}>
        <AIModelNode {...props} />
      </Suspense>
    ),
    operator: (props: any) => (
      <Suspense fallback={<div className="p-4 bg-muted/50 animate-pulse rounded-lg border border-border" />}>
        <OperatorNode {...props} />
      </Suspense>
    ),
    trigger: (props: any) => (
      <Suspense fallback={<div className="p-4 bg-muted/50 animate-pulse rounded-lg border border-border" />}>
        <TriggerNode {...props} />
      </Suspense>
    ),
    control: (props: any) => (
      <Suspense fallback={<div className="p-4 bg-muted/50 animate-pulse rounded-lg border border-border" />}>
        <ControlNode {...props} />
      </Suspense>
    ),
    group: (props: any) => (
      <Suspense fallback={<div className="p-4 bg-muted/50 animate-pulse rounded-lg border border-border" />}>
        <GroupNode {...props} />
      </Suspense>
    ),
  }), []);

  const edgeTypes = useMemo(() => ({
    smart: SmartEdge,
  }), []);

  // ==========================================
  // 3. STORE SUBSCRIPTIONS
  // ==========================================
  // 1. Store optimization: Subscribe to nodes/edges with shallow comparison
  const { nodes, edges, subgraphs, navigationPath } = useWorkflowStore(
    useShallow((state) => ({
      nodes: state.nodes,
      edges: state.edges,
      subgraphs: state.subgraphs || {},
      navigationPath: state.navigationPath || ['root'],
    }))
  );
  // Actions (separate subscription to avoid re-renders on node/edge changes)
  const {
    addNode,
    updateNode,
    removeNode,
    removeEdge,
    addEdge,
    clearWorkflow,
    loadWorkflow,
    onNodesChange,
    onEdgesChange,
    onConnect,
    groupNodes,
    ungroupNode,
    navigateToSubgraph,
    navigateUp,
    setSelectedNodeId,
  } = useWorkflowStore();

  useOnSelectionChange({
    onChange: ({ nodes }) => {
      if (nodes.length > 0) {
        setSelectedNodeId(nodes[0].id);
      } else {
        setSelectedNodeId(null);
      }
    },
  });

  // Co-design store
  const {
    recordChange,
    pendingSuggestions,
    activeSuggestionId,
    setActiveSuggestion,
    acceptSuggestion,
    rejectSuggestion,
    auditIssues,
    requestSuggestions,
    requestAudit,
    recentChanges,
    addMessage,
  } = useCodesignStore();

  // Canvas mode detection
  const canvasMode = useCanvasMode();

  // Auto-validation (background linter)
  // Pass store values directly to avoid module initialization order issues
  const validation = useAutoValidation({
    enabled: canvasMode.mode !== 'agentic-designer',
    debounceMs: 1500,
    nodes,
    edges,
    auditIssues,
    requestAudit,
  });

  // Wrap workflow actions to record changes for Co-design
  const addNodeWithTracking = useCallback((node: Node) => {
    addNode(node);
    recordChange('add_node', {
      id: node.id,
      type: node.type,
      position: node.position,
      data: node.data,
    });
  }, [addNode, recordChange]);

  const updateNodeWithTracking = useCallback((id: string, changes: Partial<Node>) => {
    updateNode(id, changes);
    recordChange('update_node', { id, changes });
  }, [updateNode, recordChange]);

  const removeNodeWithTracking = useCallback((id: string) => {
    removeNode(id);
    recordChange('delete_node', { id });
  }, [removeNode, recordChange]);

  const addEdgeWithTracking = useCallback((edge: Edge) => {
    addEdge(edge);
    recordChange('add_edge', {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourceHandle,
      targetHandle: edge.targetHandle,
    });
  }, [addEdge, recordChange]);

  // Wrap change handlers to record position/drag changes
  const onNodesChangeWithTracking = useCallback((changes: NodeChange[]) => {
    onNodesChange(changes);

    // Record position changes (when dragging stops)
    const positionChanges = changes.filter(c => c.type === 'position' && c.dragging === false);
    positionChanges.forEach(change => {
      recordChange('move_node', {
        id: (change as any).id,
        position: (change as any).position,
      });
    });
  }, [onNodesChange, recordChange]);

  const onEdgesChangeWithTracking = useCallback((changes: EdgeChange[]) => {
    onEdgesChange(changes);

    // Record edge removals
    const removeChanges = changes.filter(c => c.type === 'remove');
    removeChanges.forEach(change => {
      recordChange('delete_edge', { id: (change as any).id });
    });
  }, [onEdgesChange, recordChange]);

  // ==========================================
  // 4. HOOKS FOR BRIEFING AND CHECKPOINTS
  // ==========================================
  // 4. HOOKS FOR TIME MACHINE
  // ==========================================
  const timeMachine = useTimeMachine({
    executionId: currentExecutionId || '',
    onRollbackSuccess: (result) => {
      toast.success(`롤백 성공: 새 브랜치 ${result.branched_thread_id} 생성됨`);
      setRollbackDialogOpen(false);
    },
    onRollbackError: (error) => {
      toast.error(`롤백 실패: ${error.message}`);
    },
  });

  // Handle multi-selection changes
  const onSelectionChange = useCallback((params: OnSelectionChangeParams) => {
    setSelectedNodes(params.nodes);
  }, []);

  const handleGroupConfirm = useCallback((groupName: string) => {
    const nodeIds = selectedNodes.map(n => n.id);
    groupNodes(nodeIds, groupName);
    setGroupDialogOpen(false);
    setSelectedNodes([]);
    toast.success(`${nodeIds.length}개 노드가 "${groupName}"으로 그룹화됨`);
  }, [selectedNodes, groupNodes]);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow');
      const label = event.dataTransfer.getData('label');
      const blockId = event.dataTransfer.getData('blockId');
      const dataString = event.dataTransfer.getData('defaultData');

      if (!type || !reactFlowInstance) return;

      const position = reactFlowInstance.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      // 💡 Node Factory 활용
      let data = { label };
      try {
        if (dataString) {
          data = { ...JSON.parse(dataString), label };
        }
      } catch (e) { }

      const newNode = createWorkflowNode({
        type,
        position,
        data,
        blockId
      });

      addNodeWithTracking(newNode);
    },
    [addNodeWithTracking, reactFlowInstance]
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onNodeDoubleClick = useCallback((_event: React.MouseEvent, node: Node) => {
    // 그룹 노드(서브그래프)인 경우 내부로 진입
    if (node.type === 'group' && node.data?.subgraphId) {
      navigateToSubgraph(node.data.subgraphId as string);
      return;
    }
    setSelectedNode(node);
    setEditorOpen(true);
  }, [navigateToSubgraph]);

  const handleNodeUpdate = useCallback((nodeId: string, updates: any) => {
    updateNodeWithTracking(nodeId, { data: updates });
  }, [updateNodeWithTracking]);

  const handleNodeDelete = useCallback((nodeId: string) => {
    removeNodeWithTracking(nodeId);
  }, [removeNodeWithTracking]);

  const clearCanvas = useCallback(() => {
    clearWorkflow();
  }, [clearWorkflow]);

  // Memoize nodes with onDelete handler to prevent unnecessary re-renders
  const nodesWithHandlers = useMemo(
    () =>
      nodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          onDelete: handleNodeDelete,
        },
      })),
    [nodes, handleNodeDelete]
  );

  // 다이얼로그에 전달할 연결 데이터를 미리 계산
  const dialogConnectionData = useMemo(() => {
    if (!selectedNode || !editorOpen) return null;

    const incoming = edges
      .filter(e => e.target === selectedNode.id)
      .map(edge => ({
        id: edge.id,
        sourceLabel: (nodes.find(n => n.id === edge.source)?.data?.label as string) || edge.source
      }));

    const outgoing = edges
      .filter(e => e.source === selectedNode.id)
      .map(edge => ({
        id: edge.id,
        target: edge.target,
        targetLabel: (nodes.find(n => n.id === edge.target)?.data?.label as string) || edge.target
      }));

    const outgoingTargetNodeIds = new Set(outgoing.map(e => e.target));
    const available = nodes
      .filter(n =>
        n.id !== selectedNode.id &&
        n.type !== 'trigger' &&
        !outgoingTargetNodeIds.has(n.id)
      )
      .map(n => ({ id: n.id, label: (n.data?.label as string) || n.id }));

    return { incoming, outgoing, available };
  }, [selectedNode, nodes, edges, editorOpen]);

  const handleEdgeDeleteInDialog = useCallback((edgeId: string) => {
    removeEdge(edgeId);
  }, [removeEdge]);

  const handleEdgeCreateInDialog = useCallback((source: string, target: string) => {
    const newEdge = {
      id: generateNodeId(),
      source,
      target,
      animated: true,
      style: { stroke: 'hsl(263 70% 60%)', strokeWidth: 2 },
    };
    addEdge(newEdge);
  }, [addEdge]);

  // Co-design: 변경사항이 있을 때 AI 제안 요청
  useEffect(() => {
    if (recentChanges.length > 0) {
      const timeoutId = setTimeout(() => {
        const currentNodes = useWorkflowStore.getState().nodes;
        const currentEdges = useWorkflowStore.getState().edges;
        requestSuggestions({ nodes: currentNodes, edges: currentEdges });
        requestAudit({ nodes: currentNodes, edges: currentEdges });
      }, 2000); // 2초 디바운스

      return () => clearTimeout(timeoutId);
    }
  }, [recentChanges.length, requestSuggestions, requestAudit]);

  // 키보드 단축키 핸들러
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable) return;

      if ((event.key === 'Delete' || event.key === 'Backspace') && selectedNode) {
        event.preventDefault();
        handleNodeDelete(selectedNode.id);
        setSelectedNode(null);
        setEditorOpen(false);
      }

      if (event.key === 'Escape') {
        setSelectedNode(null);
        setEditorOpen(false);
      }

      if (event.key === 'Enter' && selectedNode && !editorOpen) {
        event.preventDefault();
        setEditorOpen(true);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedNode, editorOpen, handleNodeDelete]);

  // 노드 선택 핸들러
  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    setSelectedNode(node);
  }, []);

  // 롤백 핸들러
  const handleRollbackClick = useCallback((item: TimelineItem) => {
    setRollbackTarget(item);
    setRollbackDialogOpen(true);
  }, []);

  const handleRollbackPreview = useCallback(async (checkpointId: string) => {
    await timeMachine.loadPreview(checkpointId);
  }, [timeMachine]);

  const handleRollbackExecute = useCallback(async (request: Omit<RollbackRequest, 'preview_only'>) => {
    return await timeMachine.executeRollback(request);
  }, [timeMachine]);

  const handleQuickStart = useCallback(async (prompt: string, persona?: string, systemPrompt?: string) => {
    addMessage('user', prompt);
    if (persona && systemPrompt) {
      addMessage('system', `도메인 전문가 모드 활성화: ${persona.replace('_', ' ')}`);
    }
    toast.success('AI Designer Activated');
  }, [addMessage]);

  return (
    <>
      <div className="h-full w-full relative flex overflow-hidden bg-[#121212]">
        {/* Main Canvas Area */}
        <div className="flex-1 relative" onDrop={onDrop} onDragOver={onDragOver}>
          {canvasMode.isEmpty && emptyGuideVisible && (
            <EmptyCanvasGuide
              onQuickStart={handleQuickStart}
              onClose={() => setEmptyGuideVisible(false)}
              className="absolute inset-0 z-10 bg-background/95 backdrop-blur-sm"
            />
          )}

          {/* Contextual Toolbar */}
          <div className="absolute top-4 right-4 z-10 flex gap-2">
            <AnimatePresence>
              {selectedNodes.length >= 2 && (
                <motion.div initial={{ scale: 0.8, opacity: 0 }} animate={{ scale: 1, opacity: 1 }} exit={{ scale: 0.8, opacity: 0 }}>
                  <Button variant="secondary" size="sm" onClick={() => setGroupDialogOpen(true)} className="gap-2 bg-slate-800 border-slate-700">
                    <Layers className="w-4 h-4 text-blue-400" />
                    Group Selection ({selectedNodes.length})
                  </Button>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Status Indicator (replaces manual Simulate Run) */}
            {nodes.length > 0 && (
              <WorkflowStatusIndicator
                issueCount={validation.issueCount}
                hasErrors={validation.hasErrors}
                hasWarnings={validation.hasWarnings}
              />
            )}
          </div>

          {/* Breadcrumbs for Subgraphs */}
          {navigationPath.length > 0 && (
            <div className="absolute top-4 left-4 z-10 flex items-center gap-1.5 bg-slate-900/60 backdrop-blur-md px-4 py-2 rounded-2xl border border-slate-800 shadow-xl">
              <button onClick={() => navigateUp(navigationPath.length)} className="text-xs font-black uppercase tracking-widest text-slate-500 hover:text-blue-400 transition-colors">ROOT</button>
              {navigationPath.map((subgraphId, index) => {
                const subgraph = subgraphs[subgraphId];
                const isLast = index === navigationPath.length - 1;
                return (
                  <Fragment key={subgraphId}>
                    <ChevronRight className="w-3.5 h-3.5 text-slate-700" />
                    {isLast ? (
                      <span className="text-xs font-black uppercase tracking-widest text-white">{subgraph?.metadata?.name || subgraphId}</span>
                    ) : (
                      <button onClick={() => navigateUp(navigationPath.length - index - 1)} className="text-xs font-black uppercase tracking-widest text-slate-500 hover:text-blue-400 transition-colors">{subgraph?.metadata?.name || subgraphId}</button>
                    )}
                  </Fragment>
                );
              })}
            </div>
          )}

          <ReactFlow
            nodes={nodesWithHandlers}
            edges={edges}
            onNodesChange={onNodesChangeWithTracking}
            onEdgesChange={onEdgesChangeWithTracking}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onNodeDoubleClick={onNodeDoubleClick}
            onInit={setReactFlowInstance}
            onSelectionChange={onSelectionChange}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.1}
            maxZoom={2}
            snapToGrid={true}
            snapGrid={[20, 20]}
            className="bg-[#121212]"
            deleteKeyCode={null}
            panOnDrag={true}
            zoomOnScroll={true}
            panOnScroll={false}
          >
            <Background color="#222" gap={20} size={1} variant={BackgroundVariant.Dots} style={{ opacity: 0.4 }} />
          </ReactFlow>

          {/* Shortcuts Info */}
          <div className="absolute bottom-4 left-4 z-10">
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-slate-500 bg-slate-900/80 backdrop-blur-sm px-3 py-1.5 rounded-xl border border-slate-800 cursor-help hover:text-slate-300 transition-colors">
                  <Keyboard className="w-3.5 h-3.5" />
                  COMMAND_GUIDE
                </div>
              </TooltipTrigger>
              <TooltipContent side="top" className="bg-slate-900 border-slate-800 p-3 rounded-xl shadow-2xl">
                <div className="grid grid-cols-2 gap-x-6 gap-y-2">
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">DEL</kbd> Delete Node</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">ENT</kbd> Edit Params</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">ESC</kbd> Clear Selection</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">DRG</kbd> Multi-Select</div>
                </div>
              </TooltipContent>
            </Tooltip>
          </div>
        </div>

        <SuggestionOverlay />
      </div>

      <NodeEditorDialog
        node={selectedNode as any}
        open={editorOpen}
        onClose={() => setEditorOpen(false)}
        onSave={handleNodeUpdate}
        onDelete={handleNodeDelete}
        incomingConnections={dialogConnectionData?.incoming}
        outgoingConnections={dialogConnectionData?.outgoing}
        availableTargets={dialogConnectionData?.available}
        onEdgeDelete={handleEdgeDeleteInDialog}
        onEdgeCreate={handleEdgeCreateInDialog}
      />

      <GroupNameDialog
        open={groupDialogOpen}
        onClose={() => setGroupDialogOpen(false)}
        onConfirm={handleGroupConfirm}
        nodeCount={selectedNodes.length}
      />

      <RollbackDialog
        open={rollbackDialogOpen}
        onOpenChange={setRollbackDialogOpen}
        targetCheckpoint={rollbackTarget}
        preview={timeMachine.preview}
        loading={timeMachine.isPreviewLoading}
        onPreview={handleRollbackPreview}
        onExecute={handleRollbackExecute}
        onSuccess={() => {
          // 롤백 성공 시 자동으로 onRollbackSuccess 콜백이 호출됨
          setRollbackDialogOpen(false);
        }}
      />
    </>
  );
}

export const WorkflowCanvas = () => (
  <ReactFlowProvider>
    <TooltipProvider>
      <WorkflowCanvasInner />
    </TooltipProvider>
  </ReactFlowProvider>
);
