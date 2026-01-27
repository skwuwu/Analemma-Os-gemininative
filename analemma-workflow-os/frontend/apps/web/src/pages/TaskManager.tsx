/**
 * Task Manager Page
 * 
 * Rebranded from WorkflowMonitor to Task Manager.
 * 2-Pane layout with left list and right detail panel (Bento Grid).
 */

import React, { useState, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Home,
  Bell,
  Search,
  Filter,
  RefreshCw,
  CheckCircle2,
  Clock,
  AlertCircle,
  Loader2,
  LayoutGrid,
  List,
  Bot,
  XCircle,
  Slash,
  History,
  GitBranch,
  Box,
  Play,
  Square
} from 'lucide-react';
import { toast } from 'sonner';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

// Components
import { TaskBentoGrid } from '@/components/TaskBentoGrid';
import { OutcomeManagerModal } from '@/components/OutcomeManagerModal';
import { CheckpointTimeline } from '@/components/CheckpointTimeline';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { Progress } from '@/components/ui/progress';
import { WorkflowGraphViewer } from '@/components/WorkflowGraphViewer';
import { NodeDetailPanel } from '@/components/NodeDetailPanel';

// Hooks
import { useTaskManager } from '@/hooks/useTaskManager';
import { useNotifications } from '@/hooks/useNotifications';
import { useWorkflowApi } from '@/hooks/useWorkflowApi';
import { useCheckpoints, useTimeMachine } from '@/hooks/useBriefingAndCheckpoints';
import { useNodeExecutionStatus } from '@/hooks/useNodeExecutionStatus';
import { useExecutionTimeline } from '@/hooks/useExecutionTimeline';
import type { TaskSummary, TaskStatus, TimelineItem, NotificationItem, HistoryEntry, ResumeWorkflowRequest } from '@/lib/types';

interface TaskManagerProps {
  signOut?: () => void;
}

const StatusIcon: React.FC<{ status: TaskStatus }> = ({ status }) => {
  const iconProps = { className: 'w-4 h-4' };
  
  switch (status) {
    case 'queued':
      return <Clock {...iconProps} className="w-4 h-4 text-slate-500" />;
    case 'in_progress':
      return <Loader2 {...iconProps} className="w-4 h-4 text-blue-500 animate-spin" />;
    case 'pending_approval':
      return <AlertCircle {...iconProps} className="w-4 h-4 text-amber-500" />;
    case 'completed':
      return <CheckCircle2 {...iconProps} className="w-4 h-4 text-green-500" />;
    case 'failed':
      return <XCircle {...iconProps} className="w-4 h-4 text-red-500" />;
    case 'cancelled':
      return <Slash {...iconProps} className="w-4 h-4 text-gray-400" />;
    default:
      return <Clock {...iconProps} className="w-4 h-4 text-gray-500" />;
  }
};

const StatusBadge = ({ status }: { status: string }) => {
    const variants: Record<string, 'default' | 'secondary' | 'destructive' | 'outline'> = {
      in_progress: 'default',
      completed: 'secondary',
      failed: 'destructive',
      queued: 'outline',
      pending_approval: 'default',
    };
    
    const colorClass = status === 'pending_approval' ? 'bg-amber-500 hover:bg-amber-600' : '';
  
    return (
      <Badge variant={variants[status] || 'outline'} className={`uppercase text-[10px] ${colorClass}`}>
        {status.replace('_', ' ')}
      </Badge>
    );
};

export const TaskManager: React.FC<TaskManagerProps> = ({ signOut }) => {
  const navigate = useNavigate();
  const API_BASE = import.meta.env.VITE_API_BASE_URL;
  
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [outcomeModalOpen, setOutcomeModalOpen] = useState(false);
  const [selectedArtifactId, setSelectedArtifactId] = useState<string | undefined>(undefined);
  
  // Rollback state (for Timeline sub-tab)
  const [rollbackDialogOpen, setRollbackDialogOpen] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<TimelineItem | null>(null);
  
  // Detail View Tab State (Business vs Technical)
  const [detailViewTab, setDetailViewTab] = useState<'business' | 'technical'>('business');
  const [technicalSubTab, setTechnicalSubTab] = useState<'graph' | 'timeline' | 'nodes'>('graph');
  const [responseText, setResponseText] = useState('');
  
  // 기존 notifications 훅 (WebSocket 연결 유지)
  const { notifications } = useNotifications();
  
  // Task Manager 훅
  const taskManager = useTaskManager({
    statusFilter: statusFilter === 'all' ? undefined : statusFilter,
    autoRefresh: true,
    showTechnicalLogs: false,
  });
  
  // API 훅
  const { resumeWorkflow, stopExecution, isStopping, isResuming } = useWorkflowApi();
  
  // ExecutionTimeline 훅
  const { executionTimelines, fetchExecutionTimeline } = useExecutionTimeline(notifications, API_BASE);
  
  // Checkpoints and Time Machine hooks
  const checkpoints = useCheckpoints({
    executionId: taskManager.selectedTask?.task_id,
    enabled: !!taskManager.selectedTask?.task_id,
    refetchInterval: 5000,
  });

  const timeMachine = useTimeMachine({
    executionId: taskManager.selectedTask?.task_id || '',
    onRollbackSuccess: (result) => {
      toast.success(`Rollback successful: New branch ${result.branched_thread_id} created`);
      setRollbackDialogOpen(false);
      checkpoints.refetch();
    },
    onRollbackError: (error) => {
      toast.error(`Rollback failed: ${error.message}`);
    },
  });
  
  // Workflow Graph Data (기술 탭용)
  const workflowGraphData = useMemo(() => {
    if (!taskManager.selectedTask) return null;
    
    // workflow_config 추출
    const config = (taskManager.selectedTask as any)?.workflow_config;
    if (!config || !config.nodes) return null;

    const nodes = (config.nodes || []).map((node: any, idx: number) => ({
      id: node.id,
      type: node.type || 'operator',
      position: node.position || { x: 150 * (idx % 4), y: 150 * Math.floor(idx / 4) },
      data: { label: node.config?.label || node.id, ...node.config },
    }));

    const edges = (config.edges || []).map((edge: any, idx: number) => ({
      id: edge.id || `edge-${idx}`,
      source: edge.source,
      target: edge.target,
      animated: true,
      style: { stroke: 'hsl(263 70% 60%)', strokeWidth: 2 },
    }));

    return { nodes, edges };
  }, [taskManager.selectedTask]);

  // History entries for node status
  const historyEntriesForGraph = useMemo((): HistoryEntry[] => {
    if (!taskManager.selectedTask) return [];
    const stateHistory = (taskManager.selectedTask as any)?.state_history || [];
    return Array.isArray(stateHistory) ? stateHistory : [];
  }, [taskManager.selectedTask]);

  const nodeStatus = useNodeExecutionStatus(historyEntriesForGraph);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  
  // Selected timeline
  const selectedWorkflowTimeline = useMemo(() => {
    if (!taskManager.selectedTaskId) return [] as NotificationItem[];
    return executionTimelines[taskManager.selectedTaskId] || [];
  }, [executionTimelines, taskManager.selectedTaskId]);

  const latestStatus = selectedWorkflowTimeline.length > 0 
    ? selectedWorkflowTimeline[selectedWorkflowTimeline.length - 1] 
    : null;
  
  // 검색 필터링
  const filteredTasks = useMemo(() => {
    if (!searchQuery.trim()) return taskManager.tasks;
    
    const query = searchQuery.toLowerCase();
    return taskManager.tasks.filter(task =>
      task.task_summary?.toLowerCase().includes(query) ||
      task.agent_name?.toLowerCase().includes(query) ||
      task.workflow_name?.toLowerCase().includes(query)
    );
  }, [taskManager.tasks, searchQuery]);
  
  // Task 선택 핸들러
  const handleTaskClick = useCallback((task: TaskSummary) => {
    taskManager.selectTask(task.task_id);
  }, [taskManager]);

  const handleArtifactClick = useCallback((artifactId: string) => {
    setSelectedArtifactId(artifactId);
    setOutcomeModalOpen(true);
  }, []);
  
  const handleRollbackClick = useCallback((item: TimelineItem) => {
    setRollbackTarget(item);
    setRollbackDialogOpen(true);
  }, []);
  
  const handleResumeWorkflow = useCallback(async () => {
    if (!taskManager.selectedTask || !responseText.trim()) {
      toast.error('응답을 입력해주세요');
      return;
    }
    
    try {
      const payload: ResumeWorkflowRequest = {
        conversation_id: taskManager.selectedTask.task_id,
        execution_id: taskManager.selectedTask.task_id,
        user_input: {
          response: responseText.trim(),
        },
      };
      
      await resumeWorkflow(payload);
      setResponseText('');
      toast.success('워크플로우를 재개했습니다');
      taskManager.refreshList();
    } catch (error) {
      toast.error('워크플로우 재개에 실패했습니다');
    }
  }, [taskManager, responseText, resumeWorkflow]);
  
  // 통계
  const stats = useMemo(() => ({
    total: taskManager.tasks.length,
    inProgress: taskManager.inProgressTasks.length,
    pendingApproval: taskManager.pendingApprovalTasks.length,
    completed: taskManager.tasks.filter(t => t.status === 'completed').length,
  }), [taskManager.tasks, taskManager.inProgressTasks, taskManager.pendingApprovalTasks]);

  return (
    <div className="flex flex-col h-screen bg-slate-950 text-slate-100 overflow-hidden">
      {/* 헤더 */}
      <header className="flex items-center justify-between px-6 py-3 border-b border-slate-800 bg-slate-900/50 backdrop-blur-sm shrink-0 h-14">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate('/')} className="text-slate-400 hover:text-slate-100 hover:bg-slate-800">
            <Home className="w-5 h-5" />
          </Button>
          <div className="flex items-center gap-2">
            <h1 className="text-lg font-semibold text-slate-100">Task Manager</h1>
            <Badge variant="outline" className="text-slate-400 border-slate-700">Beta</Badge>
          </div>
        </div>
        
        <div className="flex items-center gap-3">
          {/* 알림 배지 */}
          {stats.pendingApproval > 0 && (
            <Badge variant="destructive" className="animate-pulse bg-red-600 text-white border-red-500">
              <Bell className="w-3 h-3 mr-1" />
              {stats.pendingApproval} Pending Approval
            </Badge>
          )}
          
          <Button
            variant="outline"
            size="sm"
            onClick={() => taskManager.refreshList()}
            disabled={taskManager.isLoading}
            className="border-slate-700 bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-slate-100 h-8"
          >
            <RefreshCw className={`w-3 h-3 mr-2 ${taskManager.isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
          
          {signOut && (
            <Button variant="ghost" size="sm" onClick={signOut} className="text-slate-400 hover:text-slate-100 hover:bg-slate-800 h-8">
              Sign Out
            </Button>
          )}
        </div>
      </header>
      
      <ResizablePanelGroup direction="horizontal" className="flex-1">
        {/* 좌측 패널: 작업 목록 */}
        <ResizablePanel defaultSize={25} minSize={20} maxSize={40} className="bg-slate-900/30 border-r border-slate-800 flex flex-col">
            <div className="p-4 border-b border-slate-800 space-y-3">
                <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                    <Input
                        placeholder="Search tasks..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="pl-9 bg-slate-800 border-slate-700 text-slate-100 placeholder:text-slate-500 focus:border-slate-600 h-9 text-sm"
                    />
                </div>
                <div className="flex gap-2">
                    <Select value={statusFilter} onValueChange={setStatusFilter}>
                        <SelectTrigger className="w-full bg-slate-800 border-slate-700 text-slate-100 h-8 text-xs">
                            <Filter className="w-3 h-3 mr-2" />
                            <SelectValue placeholder="상태" />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-800 border-slate-700">
                            <SelectItem value="all">All Status</SelectItem>
                            <SelectItem value="in_progress">In Progress</SelectItem>
                            <SelectItem value="pending_approval">Pending Approval</SelectItem>
                            <SelectItem value="completed">Completed</SelectItem>
                            <SelectItem value="failed">Failed</SelectItem>
                        </SelectContent>
                    </Select>
                </div>
            </div>

            <ScrollArea className="flex-1">
                <div className="p-3 space-y-2">
                    {taskManager.isLoading ? (
                        [1, 2, 3].map((i) => (
                            <Skeleton key={i} className="h-24 w-full bg-slate-800 rounded-lg" />
                        ))
                    ) : filteredTasks.length === 0 ? (
                        <div className="text-center py-10 text-slate-500 text-sm">
                            No tasks available.
                        </div>
                    ) : (
                        filteredTasks.map((task) => (
                            <div
                                key={task.task_id}
                                onClick={() => handleTaskClick(task)}
                                className={`
                                    p-3 rounded-lg border cursor-pointer transition-all duration-200
                                    ${taskManager.selectedTaskId === task.task_id 
                                        ? 'bg-slate-800 border-sky-500/50 shadow-md' 
                                        : 'bg-slate-800/40 border-slate-700/50 hover:bg-slate-800 hover:border-slate-600'}
                                `}
                            >
                                <div className="flex justify-between items-start mb-2">
                                    <div className="flex items-center gap-2">
                                        <div className="w-6 h-6 rounded-full bg-slate-700 flex items-center justify-center shrink-0">
                                            <Bot className="w-3 h-3 text-slate-300" />
                                        </div>
                                        <span className="text-xs font-medium text-slate-200 truncate max-w-[120px]">
                                            {task.agent_name}
                                        </span>
                                    </div>
                                    <StatusIcon status={task.status} />
                                </div>
                                <h4 className="text-sm font-medium text-slate-100 mb-1 line-clamp-1">
                                    {task.task_summary || 'Task in progress'}
                                </h4>
                                <div className="flex items-center justify-between text-[10px] text-slate-400 mt-2">
                                    <span>{task.current_step_name || 'Waiting'}</span>
                                    <span>{task.progress_percentage}%</span>
                                </div>
                                <Progress value={task.progress_percentage} className="h-1 mt-1 bg-slate-700" />
                            </div>
                        ))
                    )}
                </div>
            </ScrollArea>
        </ResizablePanel>
        
        <ResizableHandle className="bg-slate-800" />
        
        {/* 우측 패널: 상세 정보 (탭 시스템) */}
        <ResizablePanel defaultSize={75} className="bg-slate-950">
            {taskManager.selectedTask ? (
                <div className="h-full flex flex-col">
                    {/* 헤더 with Action Buttons */}
                    <div className="px-6 py-4 border-b border-slate-800 flex justify-between items-center bg-slate-900/30">
                        <div>
                            <h2 className="text-xl font-bold text-slate-100 flex items-center gap-2">
                                {taskManager.selectedTask.task_summary}
                                <StatusBadge status={taskManager.selectedTask.status} />
                                {taskManager.selectedTask.is_interruption && (
                                  <Badge variant="destructive" className="animate-pulse">
                                    <AlertCircle className="w-3 h-3 mr-1" />
                                    응답 대기 중
                                  </Badge>
                                )}
                            </h2>
                            <p className="text-sm text-slate-400 mt-1">
                                ID: {taskManager.selectedTask.task_id.substring(0, 16)}... • Workflow: {taskManager.selectedTask.workflow_name}
                            </p>
                        </div>
                        <div className="flex gap-2">
                            {/* Resume Button (HITP 상태) */}
                            {taskManager.selectedTask.is_interruption && (
                              <Button 
                                size="sm" 
                                onClick={() => {
                                  setDetailViewTab('technical');
                                  toast.info('응답을 입력하려면 기술 탭에서 입력하세요');
                                }}
                                className="bg-amber-600 hover:bg-amber-700"
                              >
                                <Play className="w-4 h-4 mr-2" />
                                Resume
                              </Button>
                            )}
                            {/* Stop Button (진행 중) */}
                            {taskManager.selectedTask.status === 'in_progress' && (
                              <Button 
                                size="sm" 
                                variant="destructive"
                                onClick={() => {
                                  if (taskManager.selectedTask) {
                                    stopExecution(taskManager.selectedTask.task_id);
                                  }
                                }}
                                disabled={isStopping}
                              >
                                <Square className="w-4 h-4 mr-2" />
                                Stop
                              </Button>
                            )}
                        </div>
                    </div>
                    
                    {/* 탭 시스템 */}
                    <Tabs value={detailViewTab} onValueChange={(v) => setDetailViewTab(v as 'business' | 'technical')} className="flex-1 flex flex-col">
                      <div className="px-6 pt-3 border-b border-slate-800">
                        <TabsList className="bg-slate-900">
                          <TabsTrigger value="business" className="data-[state=active]:bg-slate-800">
                            <Box className="w-4 h-4 mr-2" />
                            비즈니스 뷰
                          </TabsTrigger>
                          <TabsTrigger value="technical" className="data-[state=active]:bg-slate-800">
                            <GitBranch className="w-4 h-4 mr-2" />
                            기술 뷰
                          </TabsTrigger>
                        </TabsList>
                      </div>
                      
                      {/* 비즈니스 탭 */}
                      <TabsContent value="business" className="flex-1 overflow-hidden mt-0">
                        <ScrollArea className="h-full">
                          <TaskBentoGrid task={taskManager.selectedTask} onArtifactClick={handleArtifactClick} />
                        </ScrollArea>
                      </TabsContent>
                      
                      {/* 기술 탭 - 서브탭 시스템 */}
                      <TabsContent value="technical" className="flex-1 overflow-hidden mt-0">
                        <Tabs value={technicalSubTab} onValueChange={(v) => setTechnicalSubTab(v as 'graph' | 'timeline' | 'nodes')} className="h-full flex flex-col">
                          {/* Technical 서브탭 헤더 */}
                          <div className="px-4 pt-2 border-b border-slate-800">
                            <TabsList className="bg-slate-900">
                              <TabsTrigger value="graph" className="data-[state=active]:bg-slate-800 text-xs">
                                <GitBranch className="w-3 h-3 mr-1.5" />
                                Graph
                              </TabsTrigger>
                              <TabsTrigger value="timeline" className="data-[state=active]:bg-slate-800 text-xs">
                                <History className="w-3 h-3 mr-1.5" />
                                Timeline
                              </TabsTrigger>
                              <TabsTrigger value="nodes" className="data-[state=active]:bg-slate-800 text-xs">
                                <Box className="w-3 h-3 mr-1.5" />
                                Nodes
                              </TabsTrigger>
                            </TabsList>
                          </div>

                          {/* Graph 서브탭 */}
                          <TabsContent value="graph" className="flex-1 overflow-hidden mt-0">
                            <div className="h-full">
                              {workflowGraphData ? (
                                <WorkflowGraphViewer
                                  nodes={workflowGraphData.nodes}
                                  edges={workflowGraphData.edges}
                                  activeNodeId={nodeStatus.activeNodeIds[0] || undefined}
                                  completedNodeIds={nodeStatus.completedNodeIds}
                                  failedNodeIds={nodeStatus.failedNodeIds}
                                  onNodeClick={setSelectedNodeId}
                                />
                              ) : (
                                <div className="flex items-center justify-center h-full text-slate-500">
                                  <div className="text-center">
                                    <p className="text-lg mb-2">워크플로우 그래프를 로드할 수 없습니다</p>
                                    <p className="text-sm">workflow_config 데이터가 없습니다.</p>
                                  </div>
                                </div>
                              )}
                            </div>
                          </TabsContent>

                          {/* Timeline 서브탭 (기존 CheckpointTimeline) */}
                          <TabsContent value="timeline" className="flex-1 overflow-hidden mt-0">
                            <ScrollArea className="h-full">
                              <div className="p-4">
                                {taskManager.selectedTask ? (
                                  <CheckpointTimeline
                                    items={checkpoints.timeline}
                                    loading={checkpoints.isLoading}
                                    selectedId={timeMachine.selectedCheckpointId}
                                    compareId={timeMachine.compareCheckpointId}
                                    onRollback={handleRollbackClick}
                                    onCompare={(item) => {
                                      if (timeMachine.selectedCheckpointId && timeMachine.selectedCheckpointId !== item.checkpoint_id) {
                                        timeMachine.compare(timeMachine.selectedCheckpointId, item.checkpoint_id);
                                      }
                                    }}
                                    onPreview={(item) => checkpoints.getDetail(item.checkpoint_id)}
                                    compact
                                  />
                                ) : (
                                  <div className="flex flex-col items-center justify-center py-20 opacity-20 text-center">
                                    <History className="w-12 h-12 mb-4" />
                                    <p className="text-sm font-medium uppercase">타임라인 데이터 없음</p>
                                  </div>
                                )}
                              </div>
                            </ScrollArea>
                          </TabsContent>

                          {/* Nodes 서브탭 (NodeDetailPanel + HITP Input) */}
                          <TabsContent value="nodes" className="flex-1 overflow-hidden mt-0">
                            <div className="h-full flex flex-col">
                              {/* HITP Response Input (응답 대기 중일 때만 표시) */}
                              {taskManager.selectedTask?.is_interruption && (
                                <div className="border-b border-slate-800 p-4 bg-amber-900/10">
                                  <div className="flex items-center gap-2 mb-3">
                                    <AlertCircle className="w-5 h-5 text-amber-500" />
                                    <h3 className="font-semibold text-slate-100">사용자 입력 대기 중</h3>
                                  </div>
                                  <div className="space-y-3">
                                    <Input
                                      placeholder="응답을 입력하세요..."
                                      value={responseText}
                                      onChange={(e) => setResponseText(e.target.value)}
                                      className="bg-slate-800 border-slate-700 text-slate-100"
                                      onKeyDown={(e) => {
                                        if (e.key === 'Enter' && !e.shiftKey) {
                                          e.preventDefault();
                                          handleResumeWorkflow();
                                        }
                                      }}
                                    />
                                    <div className="flex gap-2">
                                      <Button
                                        onClick={handleResumeWorkflow}
                                        disabled={!responseText.trim() || isResuming}
                                        className="flex-1 bg-amber-600 hover:bg-amber-700"
                                      >
                                        {isResuming ? (
                                          <>
                                            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                            재개 중...
                                          </>
                                        ) : (
                                          <>
                                            <Play className="w-4 h-4 mr-2" />
                                            워크플로우 재개
                                          </>
                                        )}
                                      </Button>
                                    </div>
                                  </div>
                                </div>
                              )}
                              
                              {/* Node Detail Panel */}
                              <div className="flex-1 overflow-y-auto bg-slate-900/50">
                                <NodeDetailPanel
                                  nodeId={selectedNodeId}
                                  nodeDetails={nodeStatus.nodeDetails}
                                  historyEntries={historyEntriesForGraph}
                                />
                              </div>
                            </div>
                          </TabsContent>
                        </Tabs>
                      </TabsContent>
                    </Tabs>
                </div>
            ) : (
                <div className="h-full flex flex-col items-center justify-center text-slate-500">
                    <LayoutGrid className="w-16 h-16 mb-4 opacity-20" />
                    <p className="text-lg font-medium">Select a task to view details</p>
                    <p className="text-sm opacity-60">Click a task from the left panel to display the dashboard.</p>
                </div>
            )}
        </ResizablePanel>
      </ResizablePanelGroup>

      {taskManager.selectedTask && (
        <OutcomeManagerModal 
            isOpen={outcomeModalOpen} 
            onClose={() => setOutcomeModalOpen(false)} 
            task={taskManager.selectedTask}
            initialArtifactId={selectedArtifactId}
        />
      )}
    </div>
  );
};

export default TaskManager;
