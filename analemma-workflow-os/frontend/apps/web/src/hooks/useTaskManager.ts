/**
 * Task Manager Hooks (v2.0)
 * ===========================
 * 
 * Task Manager UI를 위한 React Query 훅입니다.
 * 
 * v2.0 Changes:
 * - Ref-based Map 관리로 고주파 WebSocket 성능 개선
 * - React Query 캐시 업데이트 최적화 (개별 Task 타게팅)
 * - optionsRef 패턴으로 콜백 안정성 개선
 * - 부분 실패 시 사용자 피드백 추가
 */

import { useState, useCallback, useEffect, useRef, useReducer } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  listTasks,
  getTaskDetail,
  getTaskOutcomes,
  getTaskMetrics,
  type TaskListOptions,
  type TaskDetailOptions,
} from '@/lib/taskApi';
import type { TaskSummary, TaskDetail, TaskStreamPayload } from '@/lib/types';

// ============ Task List Hook ============

interface UseTaskListOptions {
  status?: string;
  limit?: number;
  includeCompleted?: boolean;
  enabled?: boolean;
  refetchInterval?: number | false;
}

export function useTaskList(options: UseTaskListOptions = {}) {
  const { status, limit, includeCompleted, enabled = true, refetchInterval = false } = options;

  const tasksQuery = useQuery({
    queryKey: ['tasks', { status, limit, includeCompleted }],
    queryFn: async () => {
      const result = await listTasks({ status, limit, includeCompleted });
      return result.tasks;
    },
    enabled,
    refetchInterval,
  });

  return {
    tasks: tasksQuery.data || [],
    isLoading: tasksQuery.isLoading,
    isError: tasksQuery.isError,
    error: tasksQuery.error,
    refetch: tasksQuery.refetch,
  };
}

// ============ Task Detail Hook ============

interface UseTaskDetailOptions {
  taskId?: string;
  includeTechnicalLogs?: boolean;
  enabled?: boolean;
  refetchInterval?: number | false;
}

export function useTaskDetail(options: UseTaskDetailOptions = {}) {
  const { taskId, includeTechnicalLogs, enabled = true, refetchInterval = false } = options;
  const queryClient = useQueryClient();
  const [partialFailures, setPartialFailures] = useState<string[]>([]);

  const detailQuery = useQuery({
    queryKey: ['task', taskId, { includeTechnicalLogs }],
    queryFn: async () => {
      console.log('[useTaskDetail] queryFn executing for taskId:', taskId);
      if (!taskId) throw new Error('Task ID is required');

      const failures: string[] = [];
      const API_BASE = import.meta.env.VITE_API_BASE_URL;
      console.log('[useTaskDetail] API_BASE:', API_BASE);
ole.log('[useTaskDetail] Fetching detail, outcomes, and metrics for:', taskId);
      const [detail, outcomesResponse, metricsResponse] = await Promise.all([
        getTaskDetail(taskId, { includeTechnicalLogs }),
        getTaskOutcomes(taskId).catch(err => {
          console.error('Failed to fetch outcomes:', err);
          failures.push('outcomes');
          return { outcomes: [] };
        }),
        getTaskMetrics(taskId).catch(err => {
          console.error('Failed to fetch metrics:', err);
          failures.push('metrics');
          return null;
        }),
      ]);
      console.log('[useTaskDetail] Received detail:', detail?.task_id, 'status:', detail?.status   return null;
        }),
      ]);

      // [FIX] 완료된 작업의 경우 execution history를 추가로 가져옴
      let executionHistory = null;
      if (detail.status === 'completed' || detail.status === 'failed' || detail.status === 'cancelled') {
        try {
          const historyUrl = `${API_BASE}/executions/${encodeURIComponent(taskId)}/history`;
          const historyResp = await fetch(historyUrl, { credentials: 'include' });
          if (historyResp.ok) {
            const historyData = await historyResp.json();
            executionHistory = historyData;
            
            // state_history를 payload에 병합
            if (historyData?.step_function_state?.state_history) {
              if (!detail.payload) detail.payload = {};
              if (!detail.payload.state_history) {
                detail.payload.state_history = historyData.step_function_state.state_history;
              }
            }
            
            // workflow_config도 병합
            if (historyData?.step_function_state?.workflow_config) {
              if (!detail.workflow_config) {
                detail.workflow_config = historyData.step_function_state.workflow_config;
              }
            }
          }
        } catch (err) {
          console.warn('Failed to fetch execution history:', err);
          failures.push('execution_history');
        }
      }

      // Track partial failures for UI feedback
      setPartialFailures(failures);

      return {
        ...detail,
        // 결과물 데이터 동기화
        artifacts: outcomesResponse.outcomes || detail.artifacts,
        // 비즈니스 지표 데이터 추가
        business_metrics: metricsResponse,
        collapsed_history: outcomesResponse.collapsed_history,
        execution_history: executionHistory,
        _partialFailures: failures, // Expose to UI
      };
    },
    enabled: enabled && !!taskId,
    refetchInterval,
  });

  // 캐시 무효화
  const invalidate = useCallback(() => {
    if (taskId) {
      queryClient.invalidateQueries({ queryKey: ['task', taskId] });
    }
    queryClient.invalidateQueries({ queryKey: ['tasks'] });
    setPartialFailures([]); // Clear failure state on manual invalidate
  }, [queryClient, taskId]);

  return {
    task: detailQuery.data,
    isLoading: detailQuery.isLoading,
    isError: detailQuery.isError,
    error: detailQuery.error,
    partialFailures, // Expose partial failures to UI
    refetch: detailQuery.refetch,
    invalidate,
  };
}

// ============ Task Stream Hook ============

interface UseTaskStreamOptions {
  onTaskUpdate?: (payload: TaskStreamPayload) => void;
  onTechnicalLog?: (log: any) => void;
}

/**
 * Task 스트림 훅
 * 
 * WebSocket 메시지에서 task_context 정보를 추출하여 UI 상태를 업데이트합니다.
 * 기술적 로그는 별도로 저장합니다.
 */

// Map 상태 최대 크기 상수
const MAX_ACTIVE_TASKS = 50;
const MAX_TECHNICAL_LOGS_ENTRIES = 20;

export function useTaskStream(options: UseTaskStreamOptions = {}) {
  const optionsRef = useRef(options);
  const queryClient = useQueryClient();
  
  // Keep callbacks stable
  useEffect(() => {
    optionsRef.current = options;
  });

  // Use ref for high-frequency updates (avoid state updates on every WebSocket message)
  const activeTasksRef = useRef<Map<string, TaskStreamPayload>>(new Map());
  const technicalLogsRef = useRef<Map<string, any[]>>(new Map());
  
  // Force re-render when needed (for UI subscription)
  const [, forceUpdate] = useReducer((x) => x + 1, 0);

  // WebSocket 메시지 처리 (optimized for high-frequency updates)
  const processMessage = useCallback((message: any) => {
    // Task 업데이트 메시지 처리
    if (message.type === 'task_update' && message.task_id) {
      const payload: TaskStreamPayload = {
        type: 'task_update',
        task_id: message.task_id,
        thought: message.thought || '',
        thought_type: message.thought_type,
        progress: message.progress,
        current_step: message.current_step,
        updated_at: message.updated_at || new Date().toISOString(),
        is_interruption: message.is_interruption,
        artifacts_count: message.artifacts_count,
        agent_name: message.agent_name,
      };

      // Update ref directly (no React state update = no re-render)
      activeTasksRef.current.set(payload.task_id, payload);
      
      // Map 크기 제한: 가장 오래된 항목 제거
      if (activeTasksRef.current.size > MAX_ACTIVE_TASKS) {
        const oldestKey = activeTasksRef.current.keys().next().value;
        if (oldestKey) activeTasksRef.current.delete(oldestKey);
      }

      // Trigger re-render only if subscribed components need it
      forceUpdate();

      optionsRef.current.onTaskUpdate?.(payload);

      // Optimize: Update individual Task detail cache instead of full list
      // This prevents unnecessary re-renders of the entire task list
      queryClient.setQueryData(['task', payload.task_id], (old: any) => {
        if (!old) return old;
        return {
          ...old,
          current_thought: payload.thought,
          progress_percentage: payload.progress ?? old.progress_percentage,
          current_step_name: payload.current_step || old.current_step_name,
          is_interruption: payload.is_interruption ?? old.is_interruption,
        };
      });
      
      // Optionally update list cache (less frequent, throttled approach)
      // For now, let refetchInterval handle list updates
    }

    // 기술 로그 메시지 처리 (디버그 탭용)
    if (message.type === 'node_start' || message.type === 'node_end' || message.type === 'node_error') {
      const executionId = message.execution_id || message.task_id;
      if (executionId) {
        const logs = technicalLogsRef.current.get(executionId) || [];
        logs.push({
          type: message.type,
          timestamp: message.timestamp || new Date().toISOString(),
          node_id: message.node_id,
          data: message.data || message,
        });
        
        technicalLogsRef.current.set(executionId, logs.slice(-100)); // 각 실행당 최대 100개 유지
        
        // Map 크기 제한: 가장 오래된 실행 로그 제거
        if (technicalLogsRef.current.size > MAX_TECHNICAL_LOGS_ENTRIES) {
          const oldestKey = technicalLogsRef.current.keys().next().value;
          if (oldestKey) technicalLogsRef.current.delete(oldestKey);
        }

        forceUpdate(); // Re-render for debug tab
        optionsRef.current.onTechnicalLog?.(message);
      }
    }
  }, [queryClient]);

  // 특정 Task의 실시간 상태 가져오기
  const getTaskState = useCallback((taskId: string): TaskStreamPayload | undefined => {
    return activeTasksRef.current.get(taskId);
  }, []);

  // 특정 Task의 기술 로그 가져오기
  const getTechnicalLogs = useCallback((taskId: string): any[] => {
    return technicalLogsRef.current.get(taskId) || [];
  }, []);

  // 상태 초기화
  const clearTask = useCallback((taskId: string) => {
    activeTasksRef.current.delete(taskId);
    technicalLogsRef.current.delete(taskId);
    forceUpdate();
  }, []);

  return {
    activeTasks: Array.from(activeTasksRef.current.values()),
    processMessage,
    getTaskState,
    getTechnicalLogs,
    clearTask,
  };
}

// ============ Combined Task Manager Hook ============

interface UseTaskManagerOptions {
  selectedTaskId?: string | null;
  statusFilter?: string;
  autoRefresh?: boolean;
  showTechnicalLogs?: boolean;
}

/**
 * Task Manager 통합 훅
 * 
 * 리스트, 상세, 스트림을 통합하여 관리합니다.
 */
export function useTaskManager(options: UseTaskManagerOptions = {}) {
  const optionsRef = useRef(options);
  const [selectedId, setSelectedId] = useState<string | null>(options.selectedTaskId || null);

  // Keep options ref up to date
  useEffect(() => {
    optionsRef.current = options;
  });

  // prop 변경 시 내부 상태 동기화
  useEffect(() => {
    if (options.selectedTaskId !== undefined && options.selectedTaskId !== selectedId) {
      setSelectedId(options.selectedTaskId);
    }
  }, [options.selectedTaskId, selectedId]);

  // 리스트 조회
  const taskList = useTaskList({
    status: optionsRef.current.statusFilter,
    refetchInterval: optionsRef.current.autoRefresh ? 10000 : false,
  });

  // 선택된 Task 상세 조회
  console.log('[useTaskManager] Initializing taskDetail with selectedId:', selectedId);
  const taskDetail = useTaskDetail({
    taskId: selectedId || undefined,
    includeTechnicalLogs: optionsRef.current.showTechnicalLogs,
    refetchInterval: optionsRef.current.autoRefresh && selectedId ? 5000 : false,
  });
  console.log('[useTaskManager] taskDetail state:', {
    task: taskDetail.task?.task_id,
    isLoading: taskDetail.isLoading,
    isError: taskDetail.isError,
    error: taskDetail.error?.message
  });

  // 실시간 스트림
  const taskStream = useTaskStream({
    onTaskUpdate: (payload) => {
      // 선택된 Task가 업데이트되면 상세 정보 새로고침
      if (payload.task_id === selectedId) {
        taskDetail.invalidate();
      }
    },
  });

  // Task 선택
  const selectTask = useCallback((taskId: string | null) => {
    console.log('[useTaskManager] selectTask called with:', taskId);
    console.log('[useTaskManager] Previous selectedId:', selectedId);
    setSelectedId(taskId);
    console.log('[useTaskManager] setSelectedId called');
  }, [selectedId]);

  // 승인 대기 Task 필터
  const pendingApprovalTasks = taskList.tasks.filter(t => t.status === 'pending_approval');

  // 진행 중 Task 필터
  const inProgressTasks = taskList.tasks.filter(t => t.status === 'in_progress');

  return {
    // 리스트
    tasks: taskList.tasks,
    isLoading: taskList.isLoading,
    pendingApprovalTasks,
    inProgressTasks,

    // 선택된 Task
    selectedTaskId: selectedId,
    selectedTask: taskDetail.task,
    isLoadingDetail: taskDetail.isLoading,

    // 액션
    selectTask,
    refreshList: taskList.refetch,
    refreshDetail: taskDetail.refetch,
    refetch: () => {
      taskList.refetch();
      if (selectedId) {
        taskDetail.refetch();
      }
    },

    // 스트림
    processStreamMessage: taskStream.processMessage,
    getTaskState: taskStream.getTaskState,
    getTechnicalLogs: taskStream.getTechnicalLogs,
  };
}
