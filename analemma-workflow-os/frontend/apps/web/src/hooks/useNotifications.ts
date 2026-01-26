import { useEffect, useRef, useCallback } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { fetchAuthSession } from '@aws-amplify/auth';
import { makeAuthenticatedRequest } from '@/lib/api';
import type { NotificationItem, PendingNotification } from '@/lib/types';

// WebSocket URL must be supplied explicitly via VITE_WS_URL (no fallback to API base)
const WS_URL = import.meta.env.VITE_WS_URL || '';
const API_BASE = import.meta.env.VITE_API_BASE_URL;

export interface UseNotificationsOptions {
  onWorkflowComponentStream?: (componentData: any) => void;
  onWorkflowStatusUpdate?: (notification: NotificationItem) => void;
}

const MAX_NOTIFICATIONS = 200;

export const useNotifications = (options: UseNotificationsOptions = {}) => {
  const queryClient = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);

  // 최근에 토스트를 띄운 알림 ID 기록 (중복 방지)
  const toastHistoryMap = useRef<Map<string, number>>(new Map());
  const TOAST_DEDUP_WINDOW = 1000 * 60 * 5; // 5분

  // WebSocket 재연결 관련 상태
  const reconnectAttempts = useRef(0);
  const reconnectTimeoutId = useRef<NodeJS.Timeout | null>(null);
  const isReconnecting = useRef(false);
  const maxReconnectAttempts = 10; // 최대 재연결 시도 횟수

  // Store callbacks in refs to avoid recreating connect function
  // Update refs every render (this is safe as it doesn't trigger re-renders)
  const onWorkflowComponentStreamRef = useRef(options.onWorkflowComponentStream);
  const onWorkflowStatusUpdateRef = useRef(options.onWorkflowStatusUpdate);
  onWorkflowComponentStreamRef.current = options.onWorkflowComponentStream;
  onWorkflowStatusUpdateRef.current = options.onWorkflowStatusUpdate;

  // 1. React Query for fetching saved notifications
  const {
    data: notifications = [],
    isLoading: isLoadingSaved,
    refetch
  } = useQuery({
    queryKey: ['notifications'],
    queryFn: async () => {
      if (!API_BASE) {
        console.warn('useNotifications: VITE_API_BASE_URL not configured');
        return [];
      }

      console.log('📥 Fetching saved notifications from API...');
      const response = await makeAuthenticatedRequest(`${API_BASE}/notifications?status=pending&limit=50`);

      if (!response.ok) {
        throw new Error(`Failed to fetch notifications: ${response.status}`);
      }

      const rawText = await response.text();
      const parsed = JSON.parse(rawText);
      const data = parsed.body ? JSON.parse(parsed.body) : parsed;

      if (data.notifications && Array.isArray(data.notifications)) {
        console.log(`📥 Found ${data.notifications.length} saved notifications`);
        return data.notifications.map((item: PendingNotification) => convertToNotificationItem(item));
      }
      return [];
    },
    staleTime: 1000 * 60, // 1분간 캐시 유지
  });

  // Helper to convert API response to UI model
  const convertToNotificationItem = (item: PendingNotification): NotificationItem => {
    const action = item.notification?.payload?.action || item.type || 'workflow_status';
    let status = item.notification?.payload?.status;

    if (action === 'hitp_pause' && !status) status = 'PAUSED_FOR_HITP';
    if (action === 'execution_progress' && !status) status = 'RUNNING';

    return {
      id: item.notificationId || crypto.randomUUID(),
      type: 'workflow_status',
      action: action,
      conversation_id: item.notification?.payload?.conversation_id,
      execution_id: item.notification?.payload?.execution_id,
      workflowId: item.notification?.payload?.workflowId,
      workflow_name: item.notification?.payload?.workflow_name || item.notification?.payload?.workflow_config?.name,
      message: item.notification?.payload?.message || 'Saved workflow notification',
      segment_to_run: item.notification?.payload?.segment_to_run,
      total_segments: item.notification?.payload?.total_segments,
      current_segment: item.notification?.payload?.current_segment,
      current_state: item.notification?.payload?.current_state,
      pre_hitp_output: item.notification?.payload?.current_state || item.notification?.payload?.pre_hitp_output,
      receivedAt: item.timestamp,
      read: item.status === 'sent',
      payload: item.notification?.payload,
      raw: item,
      status: status,
      workflow_config: item.notification?.payload?.workflow_config,
      step_function_state: item.notification?.payload?.step_function_state,
      estimated_completion_time: item.notification?.payload?.estimated_completion_time,
      estimated_remaining_seconds: item.notification?.payload?.estimated_remaining_seconds,
      current_step_label: item.notification?.payload?.current_step_label,
      average_segment_duration: item.notification?.payload?.average_segment_duration,
      state_durations: item.notification?.payload?.state_durations,
      start_time: item.notification?.payload?.start_time,
      last_update_time: item.notification?.payload?.last_update_time,
    };
  };

  // 순서 검증 함수
  const isNotificationOutOfOrder = useCallback((existing: NotificationItem, newNotification: NotificationItem): boolean => {
    // 1. Sequence number 비교 (가장 정확)
    if (existing.payload?.sequence_number && newNotification.payload?.sequence_number) {
      return newNotification.payload.sequence_number <= existing.payload.sequence_number;
    }
    
    // 2. Server timestamp 비교
    if (existing.payload?.server_timestamp && newNotification.payload?.server_timestamp) {
      return newNotification.payload.server_timestamp <= existing.payload.server_timestamp;
    }
    
    // 3. Segment 진행률 비교 (fallback)
    if (existing.current_segment !== undefined && newNotification.current_segment !== undefined) {
      // 세그먼트가 역행하는 경우만 out-of-order로 판단
      return newNotification.current_segment < existing.current_segment;
    }
    
    // 4. 판단할 수 없는 경우 허용
    return false;
  }, []);

  // Helper to update query cache with new notification
  const updateNotificationsCache = useCallback((newNotification: NotificationItem) => {
    queryClient.setQueryData<NotificationItem[]>(['notifications'], (oldData = []) => {
      // execution_progress는 업데이트
      // [Fix] Update existing notification if execution_id matches, regardless of action
      // This ensures that a 'workflow_status' (e.g. SUCCEEDED) replaces 'execution_progress' (RUNNING)
      // preventing "Active(1)" issues when workflow completes.
      const existingIndex = oldData.findIndex(n =>
        (newNotification.execution_id && n.execution_id === newNotification.execution_id) ||
        (newNotification.conversation_id && n.conversation_id === newNotification.conversation_id)
      );

      if (existingIndex !== -1) {
        const existing = oldData[existingIndex];
        
        // 순서 검증: 새 알림이 기존 알림보다 이전 데이터인지 확인
        const isOutOfOrder = isNotificationOutOfOrder(existing, newNotification);
        
        if (isOutOfOrder) {
          console.warn('Out-of-order notification ignored:', {
            existing: {
              sequence: existing.payload?.sequence_number,
              segment: existing.current_segment,
              timestamp: existing.payload?.server_timestamp
            },
            new: {
              sequence: newNotification.payload?.sequence_number,
              segment: newNotification.current_segment,
              timestamp: newNotification.payload?.server_timestamp
            }
          });
          return oldData; // 순서가 뒤바뀐 알림은 무시
        }
        
        // 정상적인 업데이트
        const updated = [...oldData];
        updated[existingIndex] = {
          ...existing,
          ...newNotification,
          receivedAt: Date.now(), // Bump to top
        };
        return updated.sort((a, b) => b.receivedAt - a.receivedAt).slice(0, MAX_NOTIFICATIONS);
      }

      // 중복 체크 (Check by ID just in case findIndex miss handled something or for extra safety)
      const isDuplicate = oldData.some(n => n.id === newNotification.id);

      if (isDuplicate) return oldData;

      // 새 알림 추가
      return [newNotification, ...oldData]
        .sort((a, b) => b.receivedAt - a.receivedAt)
        .slice(0, MAX_NOTIFICATIONS);
    });
  }, [queryClient]);

  // WebSocket 재연결 함수 - connectRef를 사용하여 최신 connect 함수 참조
  const connectRef = useRef<() => Promise<void>>();

  const scheduleReconnect = useCallback(() => {
    if (reconnectAttempts.current >= maxReconnectAttempts) {
      console.error(`❌ WebSocket 재연결 최대 시도 횟수(${maxReconnectAttempts}) 초과. 수동 재연결 필요`);
      isReconnecting.current = false;
      return;
    }

    if (isReconnecting.current) {
      console.log('🔄 WebSocket 재연결 이미 진행 중...');
      return;
    }

    isReconnecting.current = true;
    reconnectAttempts.current += 1;

    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts.current - 1), 30000);
    console.log(`🔄 WebSocket 재연결 시도 ${reconnectAttempts.current}/${maxReconnectAttempts} - ${delay}ms 후`);

    reconnectTimeoutId.current = setTimeout(async () => {
      try {
        // Use ref to get the latest connect function
        if (connectRef.current) {
          await connectRef.current();
        }
      } catch (error) {
        console.error('❌ WebSocket 재연결 실패:', error);
        isReconnecting.current = false;
        scheduleReconnect();
      }
    }, delay);
  }, [maxReconnectAttempts]);

  const cancelReconnect = useCallback(() => {
    if (reconnectTimeoutId.current) {
      clearTimeout(reconnectTimeoutId.current);
      reconnectTimeoutId.current = null;
    }
    isReconnecting.current = false;
  }, []);

  const connect = useCallback(async () => {
    try {
      let url = WS_URL;
      if (wsRef.current && (wsRef.current.readyState === WebSocket.OPEN || wsRef.current.readyState === WebSocket.CONNECTING)) {
        return;
      }
      if (wsRef.current) {
        try { wsRef.current.close(); } catch (e) { /* ignore */ }
        wsRef.current = null;
      }
      if (!url) {
        console.error('❌ WebSocket URL not configured');
        return;
      }

      let token: string | null = null;
      try {
        // Force refresh token on reconnect to ensure valid credentials
        const session = await fetchAuthSession({ forceRefresh: reconnectAttempts.current > 0 });
        // Use idToken for WebSocket (same as HTTP API)
        token = session.tokens?.idToken?.toString() || null;
      } catch (e) {
        console.warn('Failed to get auth token for WS connection', e);
      }

      if (token) {
        const sep = url.includes('?') ? '&' : '?';
        url = `${url}${sep}token=${encodeURIComponent(token)}`;
      }

      wsRef.current = new WebSocket(url);

      wsRef.current.onopen = () => {
        console.log('✅ Notifications WS connected');
        reconnectAttempts.current = 0;
        isReconnecting.current = false;
        cancelReconnect();
        // 재연결 시 최신 데이터 갱신
        refetch();
      };

      wsRef.current.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);

          if (!data || typeof data !== 'object') return;

          if (data.type === 'workflow_status') {
            const payload = data.payload || {};
            const action = payload.action || 'workflow_status';
            let status = payload.status;

            if (action === 'hitp_pause' && !status) status = 'PAUSED_FOR_HITP';
            if (action === 'execution_progress' && !status) status = 'RUNNING';

            const notification: NotificationItem = {
              id: payload.conversation_id || payload.execution_id || crypto.randomUUID(),
              type: data.type,
              action: action,
              conversation_id: payload.conversation_id,
              execution_id: payload.execution_id,
              workflowId: payload.workflowId,
              workflow_name: payload.workflow_config?.name,
              message: payload.message || 'Workflow status update',
              segment_to_run: payload.segment_to_run,
              total_segments: payload.total_segments,
              current_segment: payload.current_segment,
              current_state: payload.current_state,
              pre_hitp_output: payload.current_state || payload.pre_hitp_output || null,
              receivedAt: Date.now(),
              read: false,
              payload: payload,
              raw: data,
              status: status,
              workflow_config: payload.workflow_config,
              step_function_state: payload.step_function_state,
              estimated_completion_time: payload.estimated_completion_time,
              estimated_remaining_seconds: payload.estimated_remaining_seconds,
              current_step_label: payload.current_step_label,
              average_segment_duration: payload.average_segment_duration,
              state_durations: payload.state_durations,
              start_time: payload.start_time,
              last_update_time: payload.last_update_time,
              // 순서 보장 필드들
              sequence_number: payload.sequence_number,
              server_timestamp: payload.server_timestamp,
              segment_sequence: payload.segment_sequence,
            };

            // Cache Update
            updateNotificationsCache(notification);

            // Callback
            onWorkflowStatusUpdateRef.current?.(notification);

            // Toast Logic
            if (notification.action === 'hitp_pause') {
              const now = Date.now();
              const lastShown = toastHistoryMap.current.get(notification.id);
              // 1분 이내 중복 토스트 방지
              if (!lastShown || (now - lastShown > TOAST_DEDUP_WINDOW)) {
                toastHistoryMap.current.set(notification.id, now);
                setTimeout(() => {
                  import('sonner').then(({ toast }) => {
                    toast.warning('⏸️ Workflow Paused', {
                      description: notification.workflow_name ? `"${notification.workflow_name}" requires your input` : 'Your approval is required to continue',
                      duration: 10000,
                    });
                  });
                }, 100);
              }
            }
          } else if (data.type === 'workflow_component_stream') {
            if (onWorkflowComponentStreamRef.current) {
              try {
                // Backend may send payload as object or JSON string
                const componentData = typeof data.payload === 'string' 
                  ? JSON.parse(data.payload) 
                  : data.payload;
                onWorkflowComponentStreamRef.current(componentData);
              } catch (e) {
                console.error("Invalid component JSON:", data.payload, e);
              }
            }
          }
        } catch (e) {
          console.error('Failed to parse WS message:', e);
        }
      };

      wsRef.current.onclose = (ev) => {
        console.log(`Notifications WS closed (code: ${ev.code})`);
        wsRef.current = null;
        if (ev.code !== 1000) {
          scheduleReconnect();
        }
      };

      wsRef.current.onerror = (e) => {
        console.error('Notifications WS error', e);
      };
    } catch (e) {
      console.error('Failed to connect notifications WS', e);
    }
  }, [cancelReconnect, queryClient, refetch, scheduleReconnect, updateNotificationsCache]);

  // Keep connectRef updated with the latest connect function
  useEffect(() => {
    connectRef.current = connect;
  }, [connect]);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
    cancelReconnect();
  }, [cancelReconnect]);

  useEffect(() => {
    connect().catch(() => undefined);
    return () => disconnect();
  }, [connect, disconnect]);

  const markRead = (id: string) => {
    queryClient.setQueryData<NotificationItem[]>(['notifications'], (oldData = []) => {
      return oldData.map((n) => (n.id === id ? { ...n, read: true } : n));
    });
  };

  const remove = async (id: string) => {
    // 1. Optimistic Update on Cache
    const previousNotifications = queryClient.getQueryData<NotificationItem[]>(['notifications']);
    const itemToRemove = previousNotifications?.find(n => n.id === id);

    queryClient.setQueryData<NotificationItem[]>(['notifications'], (oldData = []) => {
      return oldData.filter((n) => n.id !== id);
    });

    if (itemToRemove) {
      console.log('🗑️ Removing notification:', { id, action: itemToRemove.action });

      const executionId = itemToRemove.execution_id || itemToRemove.id;
      if (executionId && executionId.startsWith('arn:aws:states:')) {
        try {
          if (!API_BASE) return;
          await makeAuthenticatedRequest(`${API_BASE}/notifications/dismiss`, {
            method: 'POST',
            body: JSON.stringify({ executionId })
          });
          console.log('✅ Notification dismissed on backend');
        } catch (e) {
          console.error('Failed to dismiss notification on backend:', e);
          // Rollback on failure? Usually not needed for dismissal (UX priority)
          // queryClient.setQueryData(['notifications'], previousNotifications);
        }
      }
    }
  };

  return {
    notifications,
    isLoadingSaved,
    connect,
    disconnect,
    markRead,
    remove,
    fetchSavedNotifications: refetch, // Alias refetch to match old interface
  };
};

export default useNotifications;
