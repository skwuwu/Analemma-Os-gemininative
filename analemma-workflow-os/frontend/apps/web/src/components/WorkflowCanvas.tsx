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
  NodeChange,
  EdgeChange,
} from '@xyflow/react';
import { fetchAuthSession } from '@aws-amplify/auth';
import { useShallow } from 'zustand/react/shallow';
import '@xyflow/react/dist/style.css';

// Node components - lazy loaded to break circular dependencies and control execution order
const AIModelNode = lazy(() => import('./nodes/AIModelNode').then(m => ({ default: m.AIModelNode })));
const OperatorNode = lazy(() => import('./nodes/OperatorNode').then(m => ({ default: m.OperatorNode })));
const TriggerNode = lazy(() => import('./nodes/TriggerNode').then(m => ({ default: m.TriggerNode })));
const ControlNode = lazy(() => import('./nodes/ControlNode').then(m => ({ default: m.ControlNode })));
const ControlBlockNode = lazy(() => import('./nodes/ControlBlockNode').then(m => ({ default: m.ControlBlockNode })));
const GroupNode = lazy(() => import('./nodes/GroupNode').then(m => ({ default: m.GroupNode })));
import { SmartEdge } from './edges/SmartEdge';

// Dialog/Modal/Panel components - static imports to avoid runtime initialization issues
import { NodeEditorDialog } from './NodeEditorDialog';
import { GroupNameDialog } from './GroupNameDialog';
import { RollbackDialog } from './RollbackDialog';
import { SuggestionOverlay } from './SuggestionOverlay';
import { EmptyCanvasGuide } from './EmptyCanvasGuide';
import { AuditPanel } from './AuditPanel';

import { Button } from './ui/button';
import {
  Keyboard,
  Layers,
  ChevronRight,
  Play,
  History,
  PanelRightClose,
  Trash2,
} from 'lucide-react';
import { analyzeWorkflowGraph } from '@/lib/graphAnalysis';
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

  // Audit Panel state (Local validation only)
  const [auditPanelOpen, setAuditPanelOpen] = useState(false);

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
    control_block: (props: any) => (
      <Suspense fallback={<div className="p-4 bg-muted/50 animate-pulse rounded-lg border border-border" />}>
        <ControlBlockNode {...props} />
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

  // Handle selection changes for both single and multi-node selection
  const handleSelectionChange = useCallback(({ nodes }: { nodes: Node[] }) => {
    // Update selectedNodes state for grouping functionality
    setSelectedNodes(nodes);

    // Update store for single node selection (property panel)
    if (nodes.length === 1) {
      setSelectedNodeId(nodes[0].id);
    } else if (nodes.length === 0) {
      setSelectedNodeId(null);
    }
    // For multi-selection, keep the first node as active in property panel
    else if (nodes.length > 1) {
      setSelectedNodeId(nodes[0].id);
    }
  }, [setSelectedNodeId]);

  useOnSelectionChange({
    onChange: handleSelectionChange,
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
  } = useCodesignStore(
    useShallow((state) => ({
      recordChange: state.recordChange,
      pendingSuggestions: state.pendingSuggestions,
      activeSuggestionId: state.activeSuggestionId,
      setActiveSuggestion: state.setActiveSuggestion,
      acceptSuggestion: state.acceptSuggestion,
      rejectSuggestion: state.rejectSuggestion,
      auditIssues: state.auditIssues,
      requestSuggestions: state.requestSuggestions,
      requestAudit: state.requestAudit,
      recentChanges: state.recentChanges,
      addMessage: state.addMessage,
    }))
  );

  // Calculate issue summary for ContextualSideRail
  const issueSummary = useMemo(() => ({
    errors: auditIssues.filter(i => i.level === 'error').length,
    warnings: auditIssues.filter(i => i.level === 'warning').length,
    info: auditIssues.filter(i => i.level === 'info').length,
    total: auditIssues.length
  }), [auditIssues]);

  // Canvas mode detection
  const canvasMode = useCanvasMode();

  // Auto-validation using graphAnalysis
  const validation = useMemo(() => {
    if (nodes.length === 0) {
      return { issueCount: 0, hasErrors: false, hasWarnings: false, warnings: [] };
    }
    
    const analysisResult = analyzeWorkflowGraph(nodes, edges);
    const warnings = analysisResult.warnings || [];
    
    // 안정적인 참조를 위해 warnings 배열을 정렬된 문자열로 키 생성
    const warningsKey = warnings.map(w => `${w.type}:${w.nodeIds.join(',')}`).sort().join('|');
    
    return {
      issueCount: warnings.length,
      hasErrors: warnings.some(w => w.type === 'unreachable_node'),
      hasWarnings: warnings.length > 0,
      warnings,
      _warningsKey: warningsKey // 내부 비교용 키
    };
  }, [
    nodes.length,
    edges.length,
    // 더 안정적인 비교: 노드 ID들의 정렬된 문자열
    ...nodes.map(n => n.id).sort(),
    // 엣지 연결들의 정렬된 문자열
    ...edges.map(e => `${e.source}-${e.target}`).sort()
  ]);

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

  const handleGroupConfirm = useCallback((groupName: string) => {
    const nodeIds = selectedNodes.map(n => n.id);
    groupNodes(nodeIds, groupName);
    setGroupDialogOpen(false);
    setSelectedNodes([]);
    // toast.success는 이제 groupNodes 내부에서 처리됨
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

  // Make edges non-editable (deletable, reconnectable, and selectable disabled)
  const edgesWithRestrictions = useMemo(
    () =>
      edges.map((edge) => ({
        ...edge,
        deletable: false,
        reconnectable: false,
        selectable: false,
        focusable: false,
      })),
    [edges]
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
      const timeoutId = setTimeout(async () => {
        try {
          const session = await fetchAuthSession();
          const idToken = session.tokens?.idToken?.toString();
          
          const currentNodes = useWorkflowStore.getState().nodes;
          const currentEdges = useWorkflowStore.getState().edges;
          
          requestSuggestions({ nodes: currentNodes, edges: currentEdges }, idToken);
          requestAudit({ nodes: currentNodes, edges: currentEdges }, idToken);
        } catch (error) {
          console.error('Failed to get auth token for codesign:', error);
        }
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
          {/* EmptyCanvasGuide 비활성화됨 */}
          {/* {canvasMode.isEmpty && emptyGuideVisible && (
            <EmptyCanvasGuide
              onQuickStart={handleQuickStart}
              onClose={() => setEmptyGuideVisible(false)}
              className="absolute inset-0 z-10 bg-background/95 backdrop-blur-sm"
            />
          )} */}

          {/* Contextual Toolbar */}
          <div className="absolute top-4 right-4 z-10 flex gap-2">
            {/* Clear Canvas Button */}
            {nodes.length > 0 && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        if (confirm('Are you sure you want to clear the entire canvas? This action cannot be undone.')) {
                          clearCanvas();
                        }
                      }}
                      className="gap-2 h-8 px-3 bg-slate-800/80 border-slate-700 hover:bg-red-500/20 hover:border-red-500/50 transition-colors"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                      Clear
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent side="bottom" className="text-xs">
                    Clear entire canvas
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
            
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

            {/* Status Indicator - Opens Audit Panel instead of Popover */}
            {nodes.length > 0 && (
              <WorkflowStatusIndicator
                issueCount={validation.issueCount}
                hasErrors={validation.hasErrors}
                hasWarnings={validation.hasWarnings}
                warnings={validation.warnings}
                onNodeClick={(nodeId) => {
                  const node = nodes.find(n => n.id === nodeId);
                  if (node && reactFlowInstance) {
                    reactFlowInstance.fitView({
                      nodes: [node],
                      duration: 400,
                      padding: 0.5
                    });
                    setSelectedNode(node);
                    setEditorOpen(true);
                  }
                }}
                onClick={() => setAuditPanelOpen(true)}
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
            edges={edgesWithRestrictions}
            onNodesChange={onNodesChangeWithTracking}
            onEdgesChange={onEdgesChangeWithTracking}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onNodeDoubleClick={onNodeDoubleClick}
            onInit={setReactFlowInstance}
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
            selectionOnDrag={true}
            selectionKeyCode="Shift"
            zoomOnScroll={true}
            panOnScroll={false}
          >
            <Background color="#222" gap={20} size={1} variant={BackgroundVariant.Dots} style={{ opacity: 0.4 }} />
          </ReactFlow>

          {/* Shortcuts Info */}
          <div className="absolute bottom-4 left-4 z-10 flex items-center gap-3">
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-slate-500 bg-slate-900/80 backdrop-blur-sm px-3 py-1.5 rounded-xl border border-slate-800 cursor-help hover:text-slate-300 transition-colors">
                  <Keyboard className="w-3.5 h-3.5" />
                  SHORTCUTS
                </div>
              </TooltipTrigger>
              <TooltipContent side="top" className="bg-slate-900 border-slate-800 p-3 rounded-xl shadow-2xl">
                <div className="grid grid-cols-2 gap-x-6 gap-y-2">
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">DEL</kbd> Delete Node</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">ENT</kbd> Edit Params</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">ESC</kbd> Clear Selection</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">⇧+DRAG</kbd> Multi-Select</div>
                  <div className="flex items-center gap-2 text-xs font-medium text-slate-400"><kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700 text-[10px]">DBL-CLICK</kbd> Enter Group</div>
                </div>
              </TooltipContent>
            </Tooltip>
            <span className="text-[10px] text-slate-600">
              <kbd className="px-1 py-0.5 bg-slate-800/50 rounded text-slate-500">⇧</kbd>+Drag to select · Double-click group to enter
            </span>
          </div>
        </div>

        <SuggestionOverlay />
      </div>

      {/* Audit Panel Sidebar - Triggered by Status Indicator */}
      <AnimatePresence>
        {auditPanelOpen && (
          <motion.div
            initial={{ x: 400 }}
            animate={{ x: 0 }}
            exit={{ x: 400 }}
            transition={{ type: 'spring', damping: 25, stiffness: 200 }}
            className="absolute right-0 top-0 bottom-0 w-96 border-l border-slate-800 bg-slate-950/50 backdrop-blur-xl z-30 flex flex-col"
          >
            <AuditPanel 
              standalone 
              className="flex-1 overflow-hidden"
              onClose={() => setAuditPanelOpen(false)}
              key="audit-panel"
            />
          </motion.div>
        )}
      </AnimatePresence>

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

      {/* Local Validation Panel */}
      <AnimatePresence>
        {auditPanelOpen && (
          <motion.div
            initial={{ x: 400 }}
            animate={{ x: 0 }}
            exit={{ x: 400 }}
            transition={{ type: 'spring', damping: 25, stiffness: 200 }}
            className="absolute right-0 top-0 bottom-0 w-96 border-l border-slate-800 bg-slate-950/50 backdrop-blur-xl z-30 flex flex-col"
          >
            <AuditPanel
              issues={validation.warnings}
              onNodeClick={(nodeId) => {
                const node = nodes.find(n => n.id === nodeId);
                if (node && reactFlowInstance) {
                  reactFlowInstance.fitView({
                    nodes: [node],
                    duration: 400,
                    padding: 0.5
                  });
                  setSelectedNode(node);
                  setEditorOpen(true);
                }
              }}
              onClose={() => setAuditPanelOpen(false)}
              standalone
              key="validation-panel"
            />
          </motion.div>
        )}
      </AnimatePresence>
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
