/**
 * Task Manager Hooks
 * 
 * Task Manager UI를 위한 React Query 훅입니다.
 */

import { useState, useCallback, useEffect, useRef } from 'react';
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

  const detailQuery = useQuery({
    queryKey: ['task', taskId, { includeTechnicalLogs }],
    queryFn: async () => {
      if (!taskId) throw new Error('Task ID is required');

      // 상세 정보, 결과물, 지표를 병렬로 조회
      const [detail, outcomesResponse, metricsResponse] = await Promise.all([
        getTaskDetail(taskId, { includeTechnicalLogs }),
        getTaskOutcomes(taskId).catch(err => {
          console.error('Failed to fetch outcomes:', err);
          return { outcomes: [] };
        }),
        getTaskMetrics(taskId).catch(err => {
          console.error('Failed to fetch metrics:', err);
          return null;
        }),
      ]);

      return {
        ...detail,
        // 결과물 데이터 동기화
        artifacts: outcomesResponse.outcomes || detail.artifacts,
        // 비즈니스 지표 데이터 추가
        business_metrics: metricsResponse,
        collapsed_history: outcomesResponse.collapsed_history,
      };
    },
    enabled: enabled && !!taskId,
    refetchInterval,
  });

  // 캐시 무효화
  const invalidate = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ['task', taskId] });
    queryClient.invalidateQueries({ queryKey: ['tasks'] });
  }, [queryClient, taskId]);

  return {
    task: detailQuery.data,
    isLoading: detailQuery.isLoading,
    isError: detailQuery.isError,
    error: detailQuery.error,
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
  const { onTaskUpdate, onTechnicalLog } = options;
  const [activeTasks, setActiveTasks] = useState<Map<string, TaskStreamPayload>>(new Map());
  const [technicalLogs, setTechnicalLogs] = useState<Map<string, any[]>>(new Map());
  const queryClient = useQueryClient();

  // WebSocket 메시지 처리
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

      setActiveTasks(prev => {
        const next = new Map(prev);
        next.set(payload.task_id, payload);
        // Map 크기 제한: 가장 오래된 항목 제거
        if (next.size > MAX_ACTIVE_TASKS) {
          const oldestKey = next.keys().next().value;
          if (oldestKey) next.delete(oldestKey);
        }
        return next;
      });

      onTaskUpdate?.(payload);

      // React Query 캐시 업데이트
      queryClient.setQueryData<TaskSummary[]>(['tasks'], (old) => {
        if (!old) return old;
        return old.map(task =>
          task.task_id === payload.task_id
            ? {
              ...task,
              current_thought: payload.thought,
              progress_percentage: payload.progress ?? task.progress_percentage,
              current_step_name: payload.current_step || task.current_step_name,
              is_interruption: payload.is_interruption ?? task.is_interruption,
            }
            : task
        );
      });
    }

    // 기술 로그 메시지 처리 (디버그 탭용)
    if (message.type === 'node_start' || message.type === 'node_end' || message.type === 'node_error') {
      const executionId = message.execution_id || message.task_id;
      if (executionId) {
        setTechnicalLogs(prev => {
          const next = new Map(prev);
          const logs = (next.get(executionId) || []) as any[];
          logs.push({
            type: message.type,
            timestamp: message.timestamp || new Date().toISOString(),
            node_id: message.node_id,
            data: message.data || message,
          });
          next.set(executionId, logs.slice(-100)); // 각 실행당 최대 100개 유지
          // Map 크기 제한: 가장 오래된 실행 로그 제거
          if (next.size > MAX_TECHNICAL_LOGS_ENTRIES) {
            const oldestKey = next.keys().next().value;
            if (oldestKey) next.delete(oldestKey);
          }
          return next;
        });

        onTechnicalLog?.(message);
      }
    }
  }, [onTaskUpdate, onTechnicalLog, queryClient]);

  // 특정 Task의 실시간 상태 가져오기
  const getTaskState = useCallback((taskId: string): TaskStreamPayload | undefined => {
    return activeTasks.get(taskId);
  }, [activeTasks]);

  // 특정 Task의 기술 로그 가져오기
  const getTechnicalLogs = useCallback((taskId: string): any[] => {
    return technicalLogs.get(taskId) || [];
  }, [technicalLogs]);

  // 상태 초기화
  const clearTask = useCallback((taskId: string) => {
    setActiveTasks(prev => {
      const next = new Map(prev);
      next.delete(taskId);
      return next;
    });
    setTechnicalLogs(prev => {
      const next = new Map(prev);
      next.delete(taskId);
      return next;
    });
  }, []);

  return {
    activeTasks: Array.from(activeTasks.values()),
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
  const {
    selectedTaskId,
    statusFilter,
    autoRefresh = true,
    showTechnicalLogs = false
  } = options;

  const [selectedId, setSelectedId] = useState<string | null>(selectedTaskId || null);

  // prop 변경 시 내부 상태 동기화
  useEffect(() => {
    if (selectedTaskId !== undefined && selectedTaskId !== selectedId) {
      setSelectedId(selectedTaskId);
    }
  }, [selectedTaskId, selectedId]);

  // 리스트 조회
  const taskList = useTaskList({
    status: statusFilter,
    refetchInterval: autoRefresh ? 10000 : false,
  });

  // 선택된 Task 상세 조회
  const taskDetail = useTaskDetail({
    taskId: selectedId || undefined,
    includeTechnicalLogs: showTechnicalLogs,
    refetchInterval: autoRefresh && selectedId ? 5000 : false,
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
    setSelectedId(taskId);
  }, []);

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
