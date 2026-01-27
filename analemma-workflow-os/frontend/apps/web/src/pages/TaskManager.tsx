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
  PanelRightClose,
  History
} from 'lucide-react';
import { toast } from 'sonner';
import { motion, AnimatePresence } from 'framer-motion';

// Components
import { TaskBentoGrid } from '@/components/TaskBentoGrid';
import { OutcomeManagerModal } from '@/components/OutcomeManagerModal';
import { ContextualSideRail } from '@/components/ContextualSideRail';
import type { RailTab } from '@/components/ContextualSideRail';
import { CheckpointTimeline } from '@/components/CheckpointTimeline';
import { AuditPanel } from '@/components/AuditPanel';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { Progress } from '@/components/ui/progress';

// Hooks
import { useTaskManager } from '@/hooks/useTaskManager';
import { useNotifications } from '@/hooks/useNotifications';
import { useWorkflowApi } from '@/hooks/useWorkflowApi';
import { useCheckpoints, useTimeMachine } from '@/hooks/useBriefingAndCheckpoints';
import type { TaskSummary, TaskStatus, TimelineItem } from '@/lib/types';

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
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [outcomeModalOpen, setOutcomeModalOpen] = useState(false);
  const [selectedArtifactId, setSelectedArtifactId] = useState<string | undefined>(undefined);
  
  // Right Panel System (from WorkflowCanvas)
  const [rightPanelOpen, setRightPanelOpen] = useState(false);
  const [activePanelTab, setActivePanelTab] = useState<RailTab>('timeline');
  const [rollbackDialogOpen, setRollbackDialogOpen] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<TimelineItem | null>(null);
  const [currentExecutionId, setCurrentExecutionId] = useState<string | null>(null);
  
  // 기존 notifications 훅 (WebSocket 연결 유지)
  const { notifications } = useNotifications();
  
  // Task Manager 훅
  const taskManager = useTaskManager({
    statusFilter: statusFilter === 'all' ? undefined : statusFilter,
    autoRefresh: true,
    showTechnicalLogs: false,
  });
  
  // API 훅
  const { resumeWorkflow } = useWorkflowApi();
  
  // Checkpoints and Time Machine hooks
  const checkpoints = useCheckpoints({
    executionId: currentExecutionId || undefined,
    enabled: !!currentExecutionId && rightPanelOpen && activePanelTab === 'timeline',
    refetchInterval: 5000,
  });

  const timeMachine = useTimeMachine({
    executionId: currentExecutionId || '',
    onRollbackSuccess: (result) => {
      toast.success(`Rollback successful: New branch ${result.branched_thread_id} created`);
      setRollbackDialogOpen(false);
      checkpoints.refetch();
    },
    onRollbackError: (error) => {
      toast.error(`Rollback failed: ${error.message}`);
    },
  });
  
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
        
        {/* 우측 패널: 상세 정보 (Bento Grid) */}
        <ResizablePanel defaultSize={75} className="bg-slate-950">
            {taskManager.selectedTask ? (
                <div className="h-full flex flex-col">
                    <div className="px-6 py-4 border-b border-slate-800 flex justify-between items-center bg-slate-900/30">
                        <div>
                            <h2 className="text-xl font-bold text-slate-100 flex items-center gap-2">
                                {taskManager.selectedTask.task_summary}
                                <StatusBadge status={taskManager.selectedTask.status} />
                            </h2>
                            <p className="text-sm text-slate-400 mt-1">
                                ID: {taskManager.selectedTask.task_id} • Workflow: {taskManager.selectedTask.workflow_name}
                            </p>
                        </div>
                        <div className="flex gap-2">
                            {/* Action Buttons can go here */}
                        </div>
                    </div>
                    <ScrollArea className="flex-1 bg-slate-950/50">
                        <TaskBentoGrid task={taskManager.selectedTask} onArtifactClick={handleArtifactClick} />
                    </ScrollArea>
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

      {/* Contextual Side Rail */}
      <ContextualSideRail
        activeTab={activePanelTab}
        onTabChange={setActivePanelTab}
        issueCount={0}
        hasErrors={false}
        isExecuting={!!currentExecutionId}
        panelOpen={rightPanelOpen}
        onTogglePanel={() => setRightPanelOpen(!rightPanelOpen)}
      />

      {/* Unified Sidebar Panel */}
      <AnimatePresence>
        {rightPanelOpen && (
          <motion.div
            initial={{ x: 400 }}
            animate={{ x: 0 }}
            exit={{ x: 400 }}
            transition={{ type: 'spring', damping: 25, stiffness: 200 }}
            className="absolute right-0 top-14 bottom-0 w-96 border-l border-slate-800 bg-slate-950/50 backdrop-blur-xl z-30 flex flex-col"
          >
            <div className="p-6 pb-2">
              <div className="flex items-center justify-between mb-6">
                <h3 className="text-sm font-black uppercase tracking-widest text-slate-100">
                  {activePanelTab === 'timeline' && 'Execution Timeline'}
                  {activePanelTab === 'audit' && 'Validation Results'}
                </h3>
                <Button variant="ghost" size="icon" onClick={() => setRightPanelOpen(false)} className="h-8 w-8 text-slate-500 hover:text-white">
                  <PanelRightClose className="w-4 h-4" />
                </Button>
              </div>

              <div className="flex-1 overflow-hidden">
                {activePanelTab === 'timeline' && (
                  <div className="h-[calc(100vh-180px)] overflow-y-auto custom-scrollbar">
                    {currentExecutionId ? (
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
                        <p className="text-[10px] font-black uppercase">No active operations</p>
                      </div>
                    )}
                  </div>
                )}

                {activePanelTab === 'audit' && (
                  <div className="h-[calc(100vh-180px)] overflow-y-auto">
                    <AuditPanel standalone />
                  </div>
                )}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

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
