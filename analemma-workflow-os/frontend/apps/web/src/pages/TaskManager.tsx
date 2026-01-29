/**
 * Task Manager Page
 * 
 * Rebranded from WorkflowMonitor to Task Manager.
 * 2-Pane layout with left list and right detail panel (Bento Grid).
 */

import React, { useState, useCallback, useMemo, useEffect } from 'react';
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
  Square,
  ChevronDown,
  ChevronRight
} from 'lucide-react';
import { toast } from 'sonner';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

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
import type { TaskSummary, TaskStatus, TimelineItem, NotificationItem, HistoryEntry, ResumeWorkflowRequest, ExecutionSummary } from '@/lib/types';

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
  
  // Accordion state management (collapsed by default)
  const [expandedGroups, setExpandedGroups] = useState<string[]>([]);
  
  // Rollback state (for Timeline sub-tab)
  const [rollbackDialogOpen, setRollbackDialogOpen] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<TimelineItem | null>(null);
  
  // Detail View Tab State (Business vs Technical)
  const [detailViewTab, setDetailViewTab] = useState<'business' | 'technical'>('business');
  const [technicalSubTab, setTechnicalSubTab] = useState<'graph' | 'timeline' | 'nodes' | 'summary'>('graph');
  const [responseText, setResponseText] = useState('');
  
  // Summary state
  const [summary, setSummary] = useState<any>(null);
  const [summaryLoading, setSummaryLoading] = useState(false);
  
  // 기존 notifications 훅 (WebSocket 연결 유지)
  const { notifications } = useNotifications();
  
  // Task Manager hook
  const taskManager = useTaskManager({
    statusFilter: statusFilter === 'all' ? undefined : statusFilter,
    autoRefresh: true,
    showTechnicalLogs: false,
  });
  
  // API hook
  const { resumeWorkflow, stopExecution, isStopping, isResuming, dismissNotification, isDismissing, fetchExecutions } = useWorkflowApi();
  
  // Execution list state (for completed executions)
  const [completedExecutions, setCompletedExecutions] = useState<ExecutionSummary[]>([]);
  const [isLoadingExecutions, setIsLoadingExecutions] = useState(false);
  const [executionsNextToken, setExecutionsNextToken] = useState<string | undefined>();

  // Load completed executions function
  const loadCompletedExecutions = useCallback(async (token?: string) => {
    try {
      setIsLoadingExecutions(true);
      const response = await fetchExecutions(token);
      
      // Filter only completed status (codesign no longer saved to execution table)
      const completedItems = response.executions.filter((exec: ExecutionSummary) => 
        ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'].includes(exec.status || '')
      );
      
      if (token) {
        setCompletedExecutions(prev => [...prev, ...completedItems]);
      } else {
        setCompletedExecutions(completedItems);
      }
      
      setExecutionsNextToken(response.nextToken);
    } catch (error) {
      console.error('Failed to load completed executions:', error);
      toast.error('Failed to load completed executions list');
    } finally {
      setIsLoadingExecutions(false);
    }
  }, [fetchExecutions]);

  // Load completed executions on component mount (once)
  useEffect(() => {
    loadCompletedExecutions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // ✅ 마운트 시 한 번만 실행
  
  // ExecutionTimeline hook
  const { executionTimelines, fetchExecutionTimeline } = useExecutionTimeline(notifications, API_BASE);
  
  // Checkpoints and Time Machine hooks
  const checkpoints = useCheckpoints({
    executionId: taskManager.selectedTask?.task_id,
    enabled: !!taskManager.selectedTask?.task_id,
    refetchInterval: 5000,
  });

  // Rollback 콜백을 useCallback으로 메모이제이션
  const handleRollbackSuccess = useCallback((result: any) => {
    toast.success(`Rollback successful: New branch ${result.branched_thread_id} created`);
    setRollbackDialogOpen(false);
    checkpoints.refetch();
  }, [checkpoints.refetch]);

  const handleRollbackError = useCallback((error: Error) => {
    toast.error(`Rollback failed: ${error.message}`);
  }, []);

  const timeMachine = useTimeMachine({
    executionId: taskManager.selectedTask?.task_id || '',
    onRollbackSuccess: handleRollbackSuccess,
    onRollbackError: handleRollbackError,
  });
  
  // Workflow Graph Data (for technical tab)
  const workflowGraphData = useMemo(() => {
    if (!taskManager.selectedTask) return null;
    
    // Extract workflow_config
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
  
  // Search filtering
  const filteredTasks = useMemo(() => {
    if (!searchQuery.trim()) return taskManager.tasks;
    
    const query = searchQuery.toLowerCase();
    return taskManager.tasks.filter(task =>
      task.task_summary?.toLowerCase().includes(query) ||
      task.agent_name?.toLowerCase().includes(query) ||
      task.workflow_name?.toLowerCase().includes(query)
    );
  }, [taskManager.tasks, searchQuery]);
  
  // Task selection handler
  const handleTaskClick = useCallback((task: TaskSummary) => {
    console.log('[TaskManager] Task clicked:', task.task_id, task.task_summary);
    taskManager.selectTask(task.task_id);
    console.log('[TaskManager] Selected task ID:', taskManager.selectedTaskId);
    // Fetch execution timeline when task is selected
    fetchExecutionTimeline(task.task_id);
  }, [taskManager.selectTask, taskManager.selectedTaskId, fetchExecutionTimeline]); // ✅ 필요한 메서드만 의존성으로 지정

  const handleArtifactClick = useCallback((artifactId: string) => {
    setSelectedArtifactId(artifactId);
    setOutcomeModalOpen(true);
  }, []);
  
  const handleRollbackClick = useCallback((item: TimelineItem) => {
    setRollbackTarget(item);
    setRollbackDialogOpen(true);
  }, []);
  
  // Gemini summary generation function
  const selectedTaskId = taskManager.selectedTask?.task_id;
  const fetchSummary = useCallback(async (type: 'business' | 'technical' | 'full') => {
    if (!selectedTaskId) return;
    
    setSummaryLoading(true);
    try {
      const response = await fetch(
        `${API_BASE}/tasks/${selectedTaskId}?action=summarize&type=${type}`,
        { 
          headers: { 
            'Content-Type': 'application/json',
          },
          credentials: 'include'
        }
      );
      
      if (!response.ok) {
        throw new Error(`Failed to fetch summary: ${response.statusText}`);
      }
      
      const data = await response.json();
      setSummary(data);
      
      if (data.cached) {
        toast.success('Loaded cached summary');
      } else {
        toast.success('Gemini generated summary');
      }
    } catch (error) {
      console.error('Failed to fetch summary:', error);
      toast.error('Failed to generate summary');
      setSummary(null);
    } finally {
      setSummaryLoading(false);
    }
  }, [selectedTaskId, API_BASE]); // ✅ task_id만 의존성으로 지정
  
  const handleResumeWorkflow = useCallback(async () => {
    if (!taskManager.selectedTask || !responseText.trim()) {
      toast.error('Please enter a response');
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
      toast.success('Workflow resumed successfully');
      taskManager.refreshList();
    } catch (error) {
      toast.error('Failed to resume workflow');
    }
  }, [taskManager.selectedTask, taskManager.refreshList, responseText, resumeWorkflow]); // ✅ 필요한 메서드만 의존성으로 지정
  
  // Statistics
  const stats = useMemo(() => ({
    total: taskManager.tasks.length,
    inProgress: taskManager.inProgressTasks.length,
    pendingApproval: taskManager.pendingApprovalTasks.length,
    completed: taskManager.tasks.filter(t => t.status === 'completed').length,
  }), [taskManager.tasks, taskManager.inProgressTasks, taskManager.pendingApprovalTasks]);

  // Task grouping (in-progress vs completed)
  const taskGroups = useMemo(() => {
    const filteredTasks = taskManager.tasks.filter(task => {
      // Search query filtering
      if (searchQuery && !task.task_summary?.toLowerCase().includes(searchQuery.toLowerCase()) &&
          !task.agent_name?.toLowerCase().includes(searchQuery.toLowerCase())) {
        return false;
      }
      
      // Status filtering
      if (statusFilter !== 'all' && task.status !== statusFilter) {
        return false;
      }
      
      return true;
    });

    const inProgressTasks = filteredTasks.filter(task => 
      ['in_progress', 'pending_approval', 'queued'].includes(task.status)
    );
    
    // Convert completed executions to TaskSummary format with proper status mapping
    const completedTasks = completedExecutions
      .map(exec => {
        // Map AWS Step Functions status to internal status format
        let mappedStatus = 'completed';
        const awsStatus = exec.status?.toUpperCase() || 'SUCCEEDED';
        
        if (awsStatus === 'FAILED') {
          mappedStatus = 'failed';
        } else if (awsStatus === 'ABORTED' || awsStatus === 'TIMED_OUT') {
          mappedStatus = 'cancelled';
        } else if (awsStatus === 'SUCCEEDED') {
          mappedStatus = 'completed';
        } else if (awsStatus === 'RUNNING') {
          mappedStatus = 'in_progress';
        }

        return {
          task_id: exec.executionArn || exec.execution_id || '',
          task_summary: exec.name || 'Completed Execution',
          agent_name: 'System',
          status: mappedStatus,
          current_step_name: exec.status || 'Completed',
          progress_percentage: mappedStatus === 'failed' ? 0 : 100,
          workflow_name: 'Legacy Workflow',
          created_at: exec.startDate ? new Date(exec.startDate).getTime() : Date.now(),
          is_interruption: false,
          artifacts_count: 0,
        };
      })
      .filter(task => {
        // Apply status filter for completed executions
        if (statusFilter !== 'all' && task.status !== statusFilter) {
          return false;
        }
        
        // Apply search filter
        if (searchQuery && !task.task_summary?.toLowerCase().includes(searchQuery.toLowerCase()) &&
            !task.agent_name?.toLowerCase().includes(searchQuery.toLowerCase())) {
          return false;
        }
        
        return true;
      });

    return {
      inProgress: inProgressTasks,
      completed: completedTasks
    };
  }, [taskManager.tasks, searchQuery, statusFilter, completedExecutions]);

  return (
    <div className="flex flex-col h-screen bg-slate-950 text-slate-100 overflow-hidden">
      {/* Header */}
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
          {/* Notification badge */}
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
        {/* Left panel: Task list */}
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
                            <SelectValue placeholder="Status" />
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
                <div className="p-3">
                    {taskManager.isLoading ? (
                        <div className="space-y-2">
                            {[1, 2, 3].map((i) => (
                                <Skeleton key={i} className="h-24 w-full bg-slate-800 rounded-lg" />
                            ))}
                        </div>
                    ) : (
                        <Accordion 
                            type="multiple" 
                            value={expandedGroups} 
                            onValueChange={setExpandedGroups}
                            className="space-y-2"
                        >
                            {/* In-progress tasks group */}
                            <AccordionItem value="in-progress" className="border-slate-700">
                                <AccordionTrigger className="px-3 py-2 text-sm font-medium text-slate-200 hover:bg-slate-800/50 rounded-md">
                                    <div className="flex items-center gap-2">
                                        <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />
                                        <span>In Progress</span>
                                        <Badge variant="outline" className="text-xs text-blue-400 border-blue-700">
                                            {taskGroups.inProgress.length}
                                        </Badge>
                                    </div>
                                </AccordionTrigger>
                                <AccordionContent className="pb-2">
                                    <div className="space-y-2">
                                        {taskGroups.inProgress.length === 0 ? (
                                            <div className="text-center py-4 text-slate-500 text-xs">
                                                No tasks in progress.
                                            </div>
                                        ) : (
                                            taskGroups.inProgress.map((task) => (
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
                                </AccordionContent>
                            </AccordionItem>

                            {/* Completed tasks group */}
                            <AccordionItem value="completed" className="border-slate-700">
                                <AccordionTrigger className="px-3 py-2 text-sm font-medium text-slate-200 hover:bg-slate-800/50 rounded-md">
                                    <div className="flex items-center gap-2">
                                        <CheckCircle2 className="w-4 h-4 text-green-500" />
                                        <span>Completed</span>
                                        <Badge variant="outline" className="text-xs text-green-400 border-green-700">
                                            {taskGroups.completed.length}
                                        </Badge>
                                    </div>
                                </AccordionTrigger>
                                <AccordionContent className="pb-2">
                                    <div className="space-y-2">
                                        {taskGroups.completed.length === 0 ? (
                                            <div className="text-center py-4 text-slate-500 text-xs">
                                                No completed tasks.
                                            </div>
                                        ) : (
                                            taskGroups.completed.map((task) => (
                                                <div
                                                    key={task.task_id}
                                                    onClick={() => {
                                                      // Load completed execution details with history
                                                      taskManager.selectTask(task.task_id);
                                                    }}
                                                    className={`
                                                        p-3 rounded-lg border cursor-pointer transition-all duration-200
                                                        ${taskManager.selectedTask?.task_id === task.task_id 
                                                            ? 'bg-slate-800 border-purple-500/50' 
                                                            : 'bg-slate-800/40 border-slate-700/50 hover:bg-slate-800 hover:border-slate-600'}
                                                    `}
                                                >
                                                    <div className="flex justify-between items-start mb-2">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-6 h-6 rounded-full bg-slate-700 flex items-center justify-center shrink-0">
                                                                <CheckCircle2 className="w-3 h-3 text-green-500" />
                                                            </div>
                                                            <span className="text-xs font-medium text-slate-200 truncate max-w-[120px]">
                                                                {task.agent_name}
                                                            </span>
                                                        </div>
                                                        <StatusBadge status={task.status} />
                                                    </div>
                                                    <h4 className="text-sm font-medium text-slate-100 mb-1 line-clamp-1">
                                                        {task.task_summary}
                                                    </h4>
                                                    <div className="flex items-center justify-between text-[10px] text-slate-400 mt-2">
                                                        <span>{task.current_step_name}</span>
                                                        <span>{task.progress_percentage}%</span>
                                                    </div>
                                                    <Progress value={task.progress_percentage} className="h-1 mt-1 bg-slate-700" />
                                                </div>
                                            ))
                                        )}
                                    </div>
                                </AccordionContent>
                            </AccordionItem>
                        </Accordion>
                    )}
                </div>
            </ScrollArea>
        </ResizablePanel>
        
        <ResizableHandle className="bg-slate-800" />
        
        {/* Right panel: Details (tab system) */}
        <ResizablePanel defaultSize={75} className="bg-slate-950">
            {(() => {
              console.log('[TaskManager] Rendering detail panel - selectedTask:', taskManager.selectedTask ? taskManager.selectedTask.task_id : 'null');
              console.log('[TaskManager] selectedTaskId:', taskManager.selectedTaskId);
              console.log('[TaskManager] isLoadingDetail:', taskManager.isLoadingDetail);
              return taskManager.selectedTask;
            })() ? (
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
                                    Awaiting Response
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
                                  toast.info('Go to Technical tab to enter a response');
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
                            {/* Dismiss Button (실패/취소) */}
                            {(taskManager.selectedTask.status === 'failed' || taskManager.selectedTask.status === 'cancelled') && (
                              <Button 
                                size="sm" 
                                variant="outline"
                                onClick={async () => {
                                  if (taskManager.selectedTask) {
                                    try {
                                      await dismissNotification(taskManager.selectedTask.task_id);
                                      toast.success('Notification dismissed');
                                      // Refresh task list to reflect the change
                                      taskManager.refetch();
                                    } catch (error) {
                                      // Error handled by mutation
                                    }
                                  }
                                }}
                                disabled={isDismissing}
                              >
                                <Bell className="w-4 h-4 mr-2" />
                                Dismiss
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
                            Business View
                          </TabsTrigger>
                          <TabsTrigger value="technical" className="data-[state=active]:bg-slate-800">
                            <GitBranch className="w-4 h-4 mr-2" />
                            Technical View
                          </TabsTrigger>
                        </TabsList>
                      </div>
                      
                      {/* Business tab */}
                      <TabsContent value="business" className="flex-1 overflow-hidden mt-0">
                        <ScrollArea className="h-full">
                          <TaskBentoGrid task={taskManager.selectedTask} onArtifactClick={handleArtifactClick} />
                        </ScrollArea>
                      </TabsContent>
                      
                      {/* Technical tab - Sub-tab system */}
                      <TabsContent value="technical" className="flex-1 overflow-hidden mt-0">
                        <Tabs value={technicalSubTab} onValueChange={(v) => setTechnicalSubTab(v as 'graph' | 'timeline' | 'nodes' | 'summary')} className="h-full flex flex-col">
                          {/* Technical sub-tab header */}
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
                              <TabsTrigger value="summary" className="data-[state=active]:bg-slate-800 text-xs">
                                <Bot className="w-3 h-3 mr-1.5" />
                                Summary
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
                                    <p className="text-lg mb-2">Cannot load workflow graph</p>
                                    <p className="text-sm">workflow_config data is not available.</p>
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
                                    <p className="text-sm font-medium uppercase">No Timeline Data</p>
                                  </div>
                                )}
                              </div>
                            </ScrollArea>
                          </TabsContent>

                          {/* Nodes sub-tab (NodeDetailPanel + HITP Input) */}
                          <TabsContent value="nodes" className="flex-1 overflow-hidden mt-0">
                            <div className="h-full flex flex-col">
                              {/* HITP Response Input (응답 대기 중일 때만 표시) */}
                              {taskManager.selectedTask?.is_interruption && (
                                <div className="border-b border-slate-800 p-4 bg-amber-900/10">
                                  <div className="flex items-center gap-2 mb-3">
                                    <AlertCircle className="w-5 h-5 text-amber-500" />
                                    <h3 className="font-semibold text-slate-100">Awaiting User Input</h3>
                                  </div>
                                  <div className="space-y-3">
                                    <Input
                                      placeholder="Enter your response..."
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
                                            Resuming...
                                          </>
                                        ) : (
                                          <>
                                            <Play className="w-4 h-4 mr-2" />
                                            Resume Workflow
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
                          
                          {/* Summary sub-tab (Gemini summary) */}
                          <TabsContent value="summary" className="flex-1 overflow-hidden mt-0">
                            <ScrollArea className="h-full">
                              <div className="p-6 space-y-6">
                                {/* Summary type selection */}
                                <div className="flex gap-2">
                                  <Button 
                                    size="sm" 
                                    onClick={() => fetchSummary('business')} 
                                    disabled={summaryLoading}
                                    className="bg-blue-600 hover:bg-blue-700"
                                  >
                                    {summaryLoading ? (
                                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                    ) : (
                                      <Bot className="w-4 h-4 mr-2" />
                                    )}
                                    Business Summary
                                  </Button>
                                  <Button 
                                    size="sm" 
                                    variant="outline" 
                                    onClick={() => fetchSummary('technical')} 
                                    disabled={summaryLoading}
                                  >
                                    {summaryLoading ? (
                                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                    ) : (
                                      <GitBranch className="w-4 h-4 mr-2" />
                                    )}
                                    Technical Summary
                                  </Button>
                                  <Button 
                                    size="sm" 
                                    variant="outline" 
                                    onClick={() => fetchSummary('full')} 
                                    disabled={summaryLoading}
                                  >
                                    {summaryLoading ? (
                                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                    ) : (
                                      <LayoutGrid className="w-4 h-4 mr-2" />
                                    )}
                                    Full Summary
                                  </Button>
                                </div>
                                
                                {/* 로딩 */}
                                {summaryLoading && (
                                  <div className="flex items-center gap-2 text-slate-400 bg-slate-800/30 p-4 rounded-lg border border-slate-700">
                                    <Loader2 className="w-5 h-5 animate-spin" />
                                    <span>Gemini 2.0 Flash is analyzing execution logs...</span>
                                  </div>
                                )}
                                
                                {/* 요약 결과 */}
                                {summary && !summaryLoading && (
                                  <div className="space-y-4">
                                    {/* 캐시 뱃지 */}
                                    {summary.cached && (
                                      <Badge variant="outline" className="text-green-400 border-green-700">
                                        <CheckCircle2 className="w-3 h-3 mr-1" />
                                        Cached (Instant)
                                      </Badge>
                                    )}
                                    
                                    {/* 요약 텍스트 */}
                                    <div className="bg-slate-800/50 p-4 rounded-lg border border-slate-700">
                                      <h3 className="font-semibold mb-3 text-slate-100 flex items-center gap-2">
                                        <Bot className="w-4 h-4" />
                                        Summary
                                      </h3>
                                      <p className="text-slate-300 text-sm whitespace-pre-wrap leading-relaxed">
                                        {summary.summary}
                                      </p>
                                    </div>
                                    
                                    {/* Insights */}
                                    {summary.key_insights && summary.key_insights.length > 0 && (
                                      <div className="bg-blue-900/20 p-4 rounded-lg border border-blue-700/50">
                                        <h3 className="font-semibold mb-3 text-blue-300 flex items-center gap-2">
                                          <GitBranch className="w-4 h-4" />
                                          Key Insights
                                        </h3>
                                        <ul className="space-y-2">
                                          {summary.key_insights.map((insight: string, i: number) => (
                                            <li key={i} className="text-slate-300 text-sm flex gap-2">
                                              <span className="text-blue-400 shrink-0">•</span>
                                              <span>{insight}</span>
                                            </li>
                                          ))}
                                        </ul>
                                      </div>
                                    )}
                                    
                                    {/* Recommendations */}
                                    {summary.recommendations && summary.recommendations.length > 0 && (
                                      <div className="bg-amber-900/20 p-4 rounded-lg border border-amber-700/50">
                                        <h3 className="font-semibold mb-3 text-amber-300 flex items-center gap-2">
                                          <AlertCircle className="w-4 h-4" />
                                          Recommendations
                                        </h3>
                                        <ul className="space-y-2">
                                          {summary.recommendations.map((rec: string, i: number) => (
                                            <li key={i} className="text-slate-300 text-sm flex gap-2">
                                              <span className="text-amber-400 shrink-0">•</span>
                                              <span>{rec}</span>
                                            </li>
                                          ))}
                                        </ul>
                                      </div>
                                    )}
                                    
                                    {/* Metadata */}
                                    <div className="text-xs text-slate-500 flex flex-wrap gap-4 bg-slate-900/50 p-3 rounded border border-slate-800">
                                      <span className="flex items-center gap-1">
                                        <Bot className="w-3 h-3" />
                                        Model: {summary.model_used}
                                      </span>
                                      <span className="flex items-center gap-1">
                                        <Box className="w-3 h-3" />
                                        Tokens: {summary.token_usage?.total_tokens?.toLocaleString() || 'N/A'}
                                      </span>
                                      <span className="flex items-center gap-1">
                                        <Clock className="w-3 h-3" />
                                        Generation Time: {summary.generation_time_ms}ms
                                      </span>
                                      {summary.token_usage?.estimated_cost_usd && (
                                        <span className="flex items-center gap-1 text-green-400">
                                          💰 Cost: ${summary.token_usage.estimated_cost_usd.toFixed(6)}
                                        </span>
                                      )}
                                    </div>
                                  </div>
                                )}
                                
                                {/* Initial state */}
                                {!summary && !summaryLoading && (
                                  <div className="text-center py-12 text-slate-500">
                                    <Bot className="w-16 h-16 mx-auto mb-4 opacity-20" />
                                    <p className="text-lg mb-2">Summarize Execution Logs with Gemini 2.0 Flash</p>
                                    <p className="text-sm">Click the buttons above to view AI-analyzed summary</p>
                                  </div>
                                )}
                              </div>
                            </ScrollArea>
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
