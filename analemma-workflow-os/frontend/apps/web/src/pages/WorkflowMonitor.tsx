// Import separated components
import { WorkflowMonitorHeader } from "./components/WorkflowMonitorHeader";
import { TabNavigation } from "./components/TabNavigation";
import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useWorkflowApi } from '@/hooks/useWorkflowApi';
import { makeAuthenticatedRequest } from '@/lib/api';
import { toast } from 'sonner';
import { AlertCircle, Home, Activity, Bell } from 'lucide-react';
import WorkflowDetailPane from '@/components/WorkflowDetailPane';
import { useNotifications } from '@/hooks/useNotifications';
import type { NotificationItem, HistoryEntry, ExecutionSummary, ResumeWorkflowRequest } from '@/lib/types';
import useWorkflowTimer from '@/hooks/useWorkflowTimer';
import ActiveWorkflowList from '@/components/ActiveWorkflowList';
import ExecutionList from '@/components/ExecutionList';
import NotificationsList from '@/components/NotificationsList';
import { sanitizeResumePayload as utilSanitizeResumePayload } from '@/lib/utils';
import { useExecutionTimeline } from '@/hooks/useExecutionTimeline';
import { GlobalStatusBar } from '@/components/GlobalStatusBar';
import { WorkflowGraphViewer } from '@/components/WorkflowGraphViewer';
import { useNodeExecutionStatus } from '@/hooks/useNodeExecutionStatus';
import { useSmoothTaskUpdates } from '@/hooks/useSmoothTaskUpdates';
import { ConfidenceGauge } from '@/components/ui/confidence-gauge';
import { NodeDetailPanel } from '@/components/NodeDetailPanel';

interface WorkflowMonitorProps {
  signOut?: () => void;
}

export const WorkflowMonitor: React.FC<WorkflowMonitorProps> = ({ signOut }) => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { notifications, isLoadingSaved: isLoadingNotifications, remove, markRead } = useNotifications();
  const { deleteExecution, isDeleting, resumeWorkflow, isResuming, stopExecution, isStopping, dismissNotification, isDismissing, getWorkflowByName } = useWorkflowApi();
  const API_BASE = import.meta.env.VITE_API_BASE_URL;

  // Custom Hook for Timeline Management
  const { executionTimelines, fetchExecutionTimeline } = useExecutionTimeline(notifications, API_BASE);

  const [selectedExecutionId, setSelectedExecutionId] = useState<string | null>(null);
  const notificationsRef = useRef<HTMLDivElement | null>(null);

  // History 자동 갱신을 위한 완료 워크플로우 수 추적
  const completedWorkflowsCountRef = useRef(0);

  // 선택된 탭 상태 추가
  const [selectedTab, setSelectedTab] = useState<'active' | 'history' | 'notifications'>('active');

  // Selected history entry (an individual state transition) and its key for highlighting
  const [selectedHistoryEntry, setSelectedHistoryEntry] = useState<HistoryEntry | null>(null);

  // Execution summary for history tab right pane
  const [executionSummary, setExecutionSummary] = useState<ExecutionSummary | null>(null);

  // 실행 목록 상태 추가
  const [executions, setExecutions] = useState<ExecutionSummary[]>([]);
  const [isLoadingExecutions, setIsLoadingExecutions] = useState(false);
  const [nextToken, setNextToken] = useState<string | undefined>();

  // Selection helpers
  const isNotificationSelection = selectedExecutionId?.startsWith('notification:') ?? false;
  const selectedNotificationId = isNotificationSelection ? selectedExecutionId!.split(':', 2)[1] : null;
  const selectedNotification = selectedNotificationId ? (notifications.find(n => n.id === selectedNotificationId) || null) : null;

  // [NEW] Graph visualization state
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [showGraphView, setShowGraphView] = useState(true);

  // [변경 1] Active Workflows 그룹화 (execution_id로 dedupe 하고 최신 항목만 유지)
  // Note: Map already maintains insertion order, notifications come pre-sorted from backend
  const groupedActiveWorkflows = useMemo(() => {
    const map = new Map<string, NotificationItem>();
    (Array.isArray(notifications) ? notifications : [])
      .filter(n => n.action === 'execution_progress' || n.action === 'hitp_pause')
      .forEach(n => {
        const execId = n.payload?.execution_id || n.execution_id;
        if (!execId) return;
        const existing = map.get(execId);
        const currentTs = n.payload?.timestamp || n.receivedAt || 0;
        const existingTs = existing?.payload?.timestamp || existing?.receivedAt || 0;
        if (!existing || currentTs > existingTs) {
          map.set(execId, { ...n, id: execId });
        }
      });

    // Backend notifications already sorted, trust the order
    return Array.from(map.values());
  }, [notifications]);

  // Normalize event timestamps (returns milliseconds)
  const normalizeEventTs = (e: NotificationItem): number => {
    const candidate = e?.payload?.timestamp ?? e?.receivedAt ?? e?.start_time;
    if (!candidate && candidate !== 0) return 0;
    const num = Number(candidate) || 0;
    // If > 1 trillion (approx year 2001 in ms), assume it's already ms
    return num < 1000000000000 ? num * 1000 : num;
  };

  // Selected timeline: use `executionTimelines` as the single source of truth.
  // Note: Backend already sorts timeline with ScanIndexForward=True, no need to re-sort
  const selectedWorkflowTimeline = useMemo(() => {
    if (!selectedExecutionId) return [] as NotificationItem[];

    if (isNotificationSelection && selectedNotificationId) {
      const key = `notification:${selectedNotificationId}`;
      return executionTimelines[key] || (selectedNotification ? [selectedNotification] : []);
    }

    // Backend CheckpointService already returns sorted timeline, trust it
    return executionTimelines[selectedExecutionId] || [];
  }, [executionTimelines, selectedExecutionId, isNotificationSelection, selectedNotificationId, selectedNotification]);

  const latestStatus = selectedWorkflowTimeline.length > 0 ? selectedWorkflowTimeline[selectedWorkflowTimeline.length - 1] : null;
  // Apply smoothing to the latest status for the detail view
  const smoothedLatestStatus = useSmoothTaskUpdates(latestStatus);



  // [NEW] Extract workflow config for graph visualization
  const workflowGraphData = useMemo(() => {
    // 1. Try to get from latestStatus (websocket update)
    let config = latestStatus?.payload?.workflow_config ||
      (latestStatus as any)?.workflow_config;

    // 2. Fallback to notification payload
    if (!config && selectedNotification) {
      config = selectedNotification.payload?.workflow_config ||
        (selectedNotification as any)?.workflow_config;
    }

    if (!config || !config.nodes) return null;

    // Transform to ReactFlow format if needed
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
  }, [latestStatus, selectedNotification]);

  // [NEW] Extract history entries for node status tracking
  const historyEntriesForGraph = useMemo((): HistoryEntry[] => {
    // [FIX] state_history is now directly in inner_payload, not nested in step_function_state
    // Check multiple locations for backwards compatibility
    const stateHistory =
      latestStatus?.state_history ||
      latestStatus?.payload?.state_history ||
      selectedNotification?.state_history ||
      (latestStatus?.step_function_state as any)?.state_history ||
      (latestStatus?.payload?.step_function_state as any)?.state_history ||
      (selectedNotification?.step_function_state as any)?.state_history;

    if (stateHistory && Array.isArray(stateHistory)) {
      return stateHistory;
    }

    return [];
  }, [latestStatus, selectedNotification]);

  // [NEW] Use node status hook
  const nodeStatus = useNodeExecutionStatus(historyEntriesForGraph);

  // Timer hook
  const timerStartSec = selectedNotification?.start_time as number | undefined
    ?? (latestStatus?.payload?.start_time ?? latestStatus?.start_time);
  const timerStatusRaw = selectedNotification?.status ?? latestStatus?.payload?.status ?? latestStatus?.status;
  const timerStatus = typeof timerStatusRaw === 'string' ? timerStatusRaw.toUpperCase() : timerStatusRaw;

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { elapsed: _elapsedTime, isRunning: _isTimerActive } = useWorkflowTimer(timerStartSec as number | undefined, timerStatus as string | undefined);

  // 디버깅: 선택된 항목 변경 시 로그 출력
  useEffect(() => {
    if (selectedNotification) {
      console.log('🔍 Selected notification:', {
        action: selectedNotification.action,
        status: selectedNotification.status,
        showResume: selectedNotification.status === 'PAUSED_FOR_HITP',
        execution_id: selectedNotification.execution_id,
        conversation_id: selectedNotification.conversation_id,
      });
    } else if (latestStatus) {
      console.log('🔍 Selected execution latest status:', {
        action: latestStatus.action,
        status: latestStatus.payload?.status || latestStatus.status,
        execution_id: latestStatus.execution_id || latestStatus.payload?.execution_id,
      });
    }
  }, [selectedNotification, latestStatus]);

  // 실행 목록 조회 함수
  const fetchExecutions = useCallback(async (token?: string) => {
    try {
      setIsLoadingExecutions(true);
      const params = new URLSearchParams();
      params.set('limit', '20');
      if (token) params.set('nextToken', token);

      const response = await makeAuthenticatedRequest(`${API_BASE}/executions?${params.toString()}`);

      if (!response.ok) {
        throw new Error(`Failed to fetch executions: ${response.status}`);
      }

      const rawText = await response.text();
      let parsed;
      try {
        parsed = JSON.parse(rawText);
      } catch {
        throw new Error('Invalid response format');
      }

      const data = parsed.body ? JSON.parse(parsed.body) : parsed;
      const items = data.executions || [];

      if (token) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        setExecutions(prev => [...prev, ...items.map((item: any) => ({
          execution_id: item.executionArn || item.execution_id,
          name: item.name,
          status: item.status,
          startDate: item.startDate,
          stopDate: item.stopDate,
          error: item.error,
          workflowId: item.workflowId,
          executionArn: item.executionArn,
          created_at: item.startDate ? new Date(item.startDate).getTime() : null,
          step_function_state: item.step_function_state,
          final_result: item.step_function_state?.final_state || item.step_function_state?.output || null,
        }))]);
      } else {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        setExecutions(items.map((item: any) => ({
          execution_id: item.executionArn || item.execution_id,
          name: item.name,
          status: item.status,
          startDate: item.startDate,
          stopDate: item.stopDate,
          error: item.error,
          workflowId: item.workflowId,
          executionArn: item.executionArn,
          created_at: item.startDate ? new Date(item.startDate).getTime() : null,
          step_function_state: item.step_function_state,
          final_result: item.step_function_state?.final_state || item.step_function_state?.output || null,
        })));
      }

      setNextToken(data.nextToken);
    } catch (error) {
      console.error('Failed to fetch executions:', error);
      toast.error('Failed to load execution history');
      setExecutions([]);
    } finally {
      setIsLoadingExecutions(false);
    }
  }, [API_BASE]);

  // 컴포넌트 마운트 시 실행 목록 로드
  useEffect(() => {
    fetchExecutions();
  }, [fetchExecutions]);

  // Stabilize handlers passed to child components
  const handleStopExecution = useCallback((id: string) => {
    return stopExecution(id);
  }, [stopExecution]);

  const handleResumeWorkflow = useCallback((payload: ResumeWorkflowRequest) => {
    return resumeWorkflow(payload);
  }, [resumeWorkflow]);

  const handleDeleteExecution = useCallback(async (executionArn: string) => {
    try {
      await deleteExecution(executionArn);
      setExecutions(prev => (prev || []).filter(e => e.executionArn !== executionArn));
      if (selectedExecutionId === executionArn) setSelectedExecutionId(null);
      if (executionSummary?.executionArn === executionArn) {
        setExecutionSummary(null);
      }
    } catch (e) {
      console.error('Failed to delete execution', e);
      toast.error('Failed to delete execution');
    }
  }, [deleteExecution, selectedExecutionId, executionSummary]);

  const handleFetchExecutionTimeline = useCallback((id: string, force = false) => {
    return fetchExecutionTimeline(id, force);
  }, [fetchExecutionTimeline]);

  // URL 파라미터에서 execution_id 또는 executionId 가져오기
  const executionIdFromUrl = searchParams.get('executionId') || searchParams.get('execution_id');

  useEffect(() => {
    if (!executionIdFromUrl) return;

    // 1. 활성 워크플로우에서 찾기
    const targetWorkflow = groupedActiveWorkflows.find(
      w => (w.payload?.execution_id || w.execution_id)?.includes(executionIdFromUrl) ||
        w.conversation_id?.includes(executionIdFromUrl)
    );

    if (targetWorkflow) {
      const execId = targetWorkflow.payload?.execution_id || targetWorkflow.execution_id;
      if (execId) {
        setSelectedExecutionId(execId);
        setSelectedTab('active');
        setShowGraphView(false); // 상세 뷰를 우선 보여줌
      }
    } else {
      // 2. 히스토리에서 찾기 (이미 로드된 데이터가 있다면)
      const targetHistory = executions.find(
        e => (e.executionArn || e.execution_id)?.includes(executionIdFromUrl)
      );
      if (targetHistory) {
        const execId = targetHistory.executionArn || targetHistory.execution_id!;
        setSelectedExecutionId(execId);
        setSelectedTab('history');
        setExecutionSummary(targetHistory);
      }
    }
  }, [groupedActiveWorkflows, executions, executionIdFromUrl]);

  // Helper for safe date conversion
  const safeToISOString = (val: any): string | null => {
    if (!val) return null;
    try {
      // If number, assume seconds if small, ms if large
      if (typeof val === 'number') {
        const ms = val < 1000000000000 ? val * 1000 : val;
        const d = new Date(ms);
        return isNaN(d.getTime()) ? null : d.toISOString();
      }
      // If string, try parsing
      const d = new Date(val);
      return isNaN(d.getTime()) ? null : d.toISOString();
    } catch {
      return null;
    }
  };

  // Sync WebSocket notifications to executions state
  useEffect(() => {
    if (!notifications || notifications.length === 0) return;

    setExecutions(prev => {
      const newExecutions = [...prev];
      let hasChanges = false;

      notifications.forEach(n => {
        const execId = n.payload?.execution_id || n.execution_id;
        if (!execId) return;

        const existingIndex = newExecutions.findIndex(e => e.executionArn === execId || e.execution_id === execId);

        // Map notification to ExecutionSummary format
        const updatedExec: ExecutionSummary = {
          executionArn: execId,
          execution_id: execId,
          status: n.payload?.status || n.status || 'UNKNOWN',
          name: n.payload?.workflowId || n.workflow_name, // fallback
          workflowId: n.payload?.workflowId,
          startDate: safeToISOString(n.payload?.start_time),
          stopDate: safeToISOString(n.payload?.stop_time),
          error: null,
          final_result: n.payload?.final_result || n.payload?.output || null,
          created_at: n.payload?.start_time ? (n.payload.start_time < 1000000000000 ? n.payload.start_time * 1000 : n.payload.start_time) : (n.start_time ? (n.start_time < 1000000000000 ? n.start_time * 1000 : n.start_time) : Date.now()),
          updated_at: n.payload?.last_update_time ? (n.payload.last_update_time < 1000000000000 ? n.payload.last_update_time * 1000 : n.payload.last_update_time) : Date.now(),
          step_function_state: {
            current_segment: n.payload?.current_segment,
            total_segments: n.payload?.total_segments,
            estimated_remaining_seconds: n.payload?.estimated_remaining_seconds,
          }
        };

        if (existingIndex >= 0) {
          // Update existing if timestamp is newer
          const existing = newExecutions[existingIndex];
          const existingTs = existing.updated_at || 0;
          const newTs = updatedExec.updated_at || 0;

          if (newTs >= existingTs) {
            newExecutions[existingIndex] = { ...existing, ...updatedExec };
            hasChanges = true;
          }
        } else {
          // Add new execution if it's active
          if (['RUNNING', 'STARTED', 'PAUSED_FOR_HITP'].includes(updatedExec.status)) {
            newExecutions.unshift(updatedExec);
            hasChanges = true;
          }
        }
      });

      return hasChanges ? newExecutions : prev;
    });
  }, [notifications]);

  // History 탭 자동 갱신 (Legacy logic kept for safety, but main sync is above)
  useEffect(() => {
    const completedWorkflows = (Array.isArray(notifications) ? notifications : []).filter(n => n.status === 'COMPLETE');
    const currentCompletedCount = completedWorkflows.length;

    if (currentCompletedCount > completedWorkflowsCountRef.current) {
      console.log(`🔄 History 자동 갱신: ${currentCompletedCount - completedWorkflowsCountRef.current}개 워크플로우 완료됨`);
      fetchExecutions(); // Re-fetch to get full history details
      completedWorkflowsCountRef.current = currentCompletedCount;
    }
  }, [(notifications || []).length, fetchExecutions, notifications]);

  const formatTimestamp = (timestamp: number) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / (1000 * 60));
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    return date.toLocaleDateString();
  };

  const formatFullTimestamp = (timestamp: number) => {
    const date = new Date(timestamp);
    return date.toLocaleString('ko-KR', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatTimeRemaining = useCallback((seconds?: number): string => {
    if (!seconds || seconds <= 0) return '계산 중...';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    if (hours > 0) return `${hours}시간 ${minutes}분 남음`;
    if (minutes > 0) return `${minutes}분 남음`;
    return '곧 완료';
  }, []);

  const formatElapsedTime = useCallback((seconds: number): string => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) {
      return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    } else {
      return `${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
  }, []);

  const calculateProgress = useCallback((workflow: NotificationItem | null): number => {
    if (!workflow || !workflow.current_segment || !workflow.total_segments) return 0;
    return Math.round((workflow.current_segment / workflow.total_segments) * 100);
  }, []);

  const wsUrl = import.meta.env.VITE_WS_URL;
  const unreadCount = (Array.isArray(notifications) ? notifications : []).filter(n => !n.read).length;

  const [responseText, setResponseText] = useState('');

  return (
    <div className="flex flex-col h-screen w-full bg-background">
      {/* 헤더 */}
      <WorkflowMonitorHeader
        activeWorkflowsCount={groupedActiveWorkflows.length}
        unreadCount={unreadCount}
        onNotificationsClick={() => notificationsRef.current?.scrollIntoView({ behavior: "smooth" })}
        signOut={signOut}
      />
      {!wsUrl && (
        <div className="p-4 bg-orange-50 border-b border-orange-200">
          <div className="flex items-center gap-2 text-orange-700">
            <AlertCircle className="w-5 h-5" />
            <div>
              <div className="font-medium">WebSocket Not Connected</div>
              <div className="text-sm">Real-time workflow progress is unavailable. WebSocket URL not configured.</div>
            </div>
          </div>
        </div>
      )}

      {/* 메인 컨텐츠 */}
      <div className="flex flex-1 overflow-hidden">
        {/* 왼쪽: 탭 패널 */}
        <div className="w-96 border-r flex flex-col">
          {/* 탭 헤더 */}
          <TabNavigation
            selectedTab={selectedTab}
            onTabChange={(tab) => {
              setSelectedTab(tab);
              setSelectedExecutionId(null);
              setExecutionSummary(null);
            }}
            activeCount={groupedActiveWorkflows.length}
            historyCount={(Array.isArray(executions) ? executions : []).filter(e => !["RUNNING", "STARTED", "PAUSED_FOR_HITP"].includes(e.status)).length}
            notificationsCount={unreadCount}
          />

          {selectedTab === 'history' && (
            <div
              role="tabpanel"
              id="tabpanel-history"
              aria-labelledby="tab-history"
            >
              <ScrollArea className="h-full">
                <ExecutionList
                  executions={(Array.isArray(executions) ? executions : [])
                    .filter(e => !['RUNNING', 'STARTED', 'PAUSED_FOR_HITP'].includes(e.status))
                    .map((e, index) => ({
                      ...e,
                      id: e.executionArn || e.name || `execution-${index}`,
                      started_at: e.created_at,
                      finished_at: e.stopDate ? Math.floor(new Date(e.stopDate).getTime() / 1000) : null,
                      summary: e.name || undefined,
                    }))}
                  selectedExecutionId={selectedExecutionId}
                  onSelect={(id) => {
                    if (id) {
                      setSelectedExecutionId(id);
                      if (id.startsWith('arn:')) {
                        fetchExecutionTimeline(id, true);
                      }
                      const execution = executions.find((e, index) => e.executionArn === id || (e.name || `execution-${index}`) === id);
                      if (execution) {
                        setExecutionSummary(execution);
                        setSelectedHistoryEntry(null);
                      }
                    } else {
                      setSelectedExecutionId(null);
                      setExecutionSummary(null);
                    }
                  }}
                  onLoadMore={() => fetchExecutions(nextToken)}
                  isLoading={isLoadingExecutions}
                />
              </ScrollArea>
            </div>
          )}

          {selectedTab === 'notifications' && (
            <div
              role="tabpanel"
              id="tabpanel-notifications"
              aria-labelledby="tab-notifications"
              aria-live="polite"
              ref={notificationsRef}
            >
              <ScrollArea className="h-full">
                <NotificationsList
                  notifications={notifications}
                  onSelect={(id) => {
                    if (!id) return setSelectedExecutionId(null);
                    const n = notifications.find(x => x.id === id);
                    if (!n) return setSelectedExecutionId(null);
                    const exec = n.payload?.execution_id || n.execution_id;
                    if (exec) setSelectedExecutionId(exec);
                    else setSelectedExecutionId(`notification:${id}`);
                  }}
                  onRemove={(id) => { remove(id); if (selectedExecutionId === `notification:${id}`) setSelectedExecutionId(null); }}
                  onMarkRead={(id) => { markRead(id); }}
                />
              </ScrollArea>
            </div>
          )}
        </div>
        {/* 오른쪽: 선택된 워크플로우 상세 정보 */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Graph/Detail Toggle - always show in active tab */}
          {selectedTab === 'active' && selectedExecutionId && (
            <div className="flex items-center gap-2 p-2 border-b bg-muted/30">
              <Button
                variant={showGraphView ? "default" : "outline"}
                size="sm"
                onClick={() => setShowGraphView(true)}
              >
                그래프 뷰
              </Button>
              <Button
                variant={!showGraphView ? "default" : "outline"}
                size="sm"
                onClick={() => setShowGraphView(false)}
              >
                상세 뷰
              </Button>
              {!workflowGraphData && showGraphView && (
                <span className="text-xs text-muted-foreground">
                  (Loading workflow data...)
                </span>
              )}
            </div>
          )}

          {/* Conditional Rendering: Graph View or Detail Pane */}
          {selectedTab === 'active' && showGraphView && selectedExecutionId ? (
            <div className="flex-1 flex flex-col overflow-hidden">
              {/* Graph Viewer */}
              <div className="flex-1 min-h-[300px]">
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
                  <div className="flex items-center justify-center h-full text-muted-foreground">
                    <div className="text-center">
                      <p className="text-lg mb-2">워크플로우 그래프를 로드할 수 없습니다</p>
                      <p className="text-sm">workflow_config 데이터가 없거나 아직 수신되지 않았습니다.</p>
                    </div>
                  </div>
                )}
              </div>
              {/* Node Detail Panel */}
              <div className="h-[250px] border-t overflow-y-auto">
                <NodeDetailPanel
                  nodeId={selectedNodeId}
                  nodeDetails={nodeStatus.nodeDetails}
                  historyEntries={historyEntriesForGraph}
                />
              </div>
            </div>
          ) : (
            <WorkflowDetailPane
              selectedExecutionId={selectedExecutionId}
              isNotificationSelection={isNotificationSelection}
              selectedNotification={selectedNotification}
              selectedWorkflowTimeline={selectedWorkflowTimeline}
              latestStatus={smoothedLatestStatus}
              responseText={responseText}
              setResponseText={setResponseText}
              sanitizeResumePayload={(workflow: NotificationItem, response: string) => utilSanitizeResumePayload({ conversation_id: workflow.conversation_id, execution_id: workflow.execution_id }, response)}
              resumeWorkflow={handleResumeWorkflow}
              isResuming={isResuming}
              stopExecution={handleStopExecution}
              isStopping={isStopping}
              fetchExecutionTimeline={handleFetchExecutionTimeline}
              formatFullTimestamp={formatFullTimestamp}
              formatTimestamp={formatTimestamp}
              formatElapsedTime={formatElapsedTime}
              formatTimeRemaining={formatTimeRemaining}
              calculateProgress={calculateProgress}
              setSelectedExecutionId={setSelectedExecutionId}
              selectedHistoryEntry={selectedHistoryEntry}
              deleteExecution={handleDeleteExecution}
              isDeleting={isDeleting}
              isHistoryContext={selectedTab === 'history'}
              executionSummary={selectedTab === 'history' ? executionSummary : null}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default WorkflowMonitor;
