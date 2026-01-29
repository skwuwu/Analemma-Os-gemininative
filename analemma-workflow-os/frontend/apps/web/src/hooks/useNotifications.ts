import { useEffect, useRef, useCallback } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { makeAuthenticatedRequest } from '@/lib/api';
import { WebSocketManager } from '@/lib/WebSocketManager';
import type { NotificationItem, PendingNotification } from '@/lib/types';

// WebSocket URL must be supplied explicitly via VITE_WS_URL (no fallback to API base)
const WS_URL = import.meta.env.VITE_WS_URL || '';
const API_BASE = import.meta.env.VITE_API_BASE_URL;

export interface UseNotificationsOptions {
  onWorkflowComponentStream?: (componentData: any) => void;
  onWorkflowStatusUpdate?: (notification: NotificationItem) => void;
}

const MAX_NOTIFICATIONS = 200;

// Singleton WebSocketManager to prevent multiple connections
let globalWsManager: WebSocketManager | null = null;
let subscriberCount = 0;

export const useNotifications = (options: UseNotificationsOptions = {}) => {
  const queryClient = useQueryClient();
  const optionsRef = useRef(options);
  const toastHistoryMap = useRef<Map<string, number>>(new Map());
  const TOAST_DEDUP_WINDOW = 1000 * 60 * 5; // 5분

  // Keep options ref up to date
  useEffect(() => {
    optionsRef.current = options;
  });

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

  // 순서 검증 함수 (v2.0: 백엔드 GSI 정렬 신뢰, 네트워크 지연만 체크)
  // Backend CheckpointService uses ScanIndexForward=True, guarantees ordering
  // Only validate severe network delays (>5s past messages)
  const isNotificationOutOfOrder = useCallback((existing: NotificationItem, newNotification: NotificationItem): boolean => {
    const NETWORK_DELAY_THRESHOLD_MS = 5000; // 5초 이상 과거면 무시
    
    // receivedAt 기반 간단 체크 (네트워크 지연만 방어)
    const existingTime = existing.receivedAt || 0;
    const newTime = newNotification.receivedAt || Date.now();
    
    // 새 메시지가 기존 메시지보다 5초 이상 과거면 out-of-order
    return newTime < (existingTime - NETWORK_DELAY_THRESHOLD_MS);
  }, []);

  // Helper to update query cache with new notification (optimized to avoid sorting on every update)
  const updateNotificationsCache = useCallback((newNotification: NotificationItem) => {
    queryClient.setQueryData<NotificationItem[]>(['notifications'], (oldData = []) => {
      const existingIndex = oldData.findIndex(n =>
        (newNotification.execution_id && n.execution_id === newNotification.execution_id) ||
        (newNotification.conversation_id && n.conversation_id === newNotification.conversation_id)
      );

      if (existingIndex !== -1) {
        const existing = oldData[existingIndex];
        
        const isOutOfOrder = isNotificationOutOfOrder(existing, newNotification);
        
        if (isOutOfOrder) {
          console.warn('Out-of-order notification ignored');
          return oldData;
        }
        
        const updated = [...oldData];
        updated[existingIndex] = {
          ...existing,
          ...newNotification,
          receivedAt: Date.now(),
        };
        
        // Backend guarantees order, no need to re-sort on every update
        // Only move to front if significantly newer (avoid micro-reordering)
        if (existingIndex > 0 && updated[existingIndex].receivedAt > updated[0].receivedAt + 1000) {
          const item = updated.splice(existingIndex, 1)[0];
          return [item, ...updated].slice(0, MAX_NOTIFICATIONS);
        }
        return updated.slice(0, MAX_NOTIFICATIONS);
      }

      const isDuplicate = oldData.some(n => n.id === newNotification.id);
      if (isDuplicate) return oldData;

      // New notification: insert at front (already sorted position)
      return [newNotification, ...oldData].slice(0, MAX_NOTIFICATIONS);
    });
  }, [queryClient, isNotificationOutOfOrder]);

  // Handle notification messages from WebSocket (stable callback using optionsRef)
  const handleNotificationMessage = useCallback((notification: NotificationItem) => {
    updateNotificationsCache(notification);
    optionsRef.current.onWorkflowStatusUpdate?.(notification);

    // Toast Logic
    if (notification.action === 'hitp_pause') {
      const now = Date.now();
      const lastShown = toastHistoryMap.current.get(notification.id);
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
  }, [updateNotificationsCache]);

  // Initialize or reuse singleton WebSocketManager
  useEffect(() => {
    subscriberCount++;
    
    if (!globalWsManager) {
      globalWsManager = new WebSocketManager({
        url: WS_URL,
        maxReconnectAttempts: 10,
        onMessage: handleNotificationMessage,
        onComponentStream: optionsRef.current.onWorkflowComponentStream,
        onConnected: () => {
          refetch();
        },
      });
      globalWsManager.connect();
    } else {
      // Update callbacks for existing manager
      globalWsManager.updateCallbacks({
        onMessage: handleNotificationMessage,
        onComponentStream: optionsRef.current.onWorkflowComponentStream,
      });
      // Ensure connection is active
      if (!globalWsManager.isConnected()) {
        globalWsManager.connect();
      }
    }

    return () => {
      subscriberCount--;
      if (subscriberCount === 0 && globalWsManager) {
        globalWsManager.disconnect();
        globalWsManager = null;
      }
    };
  }, [handleNotificationMessage, refetch]);

  // Update callbacks when options change (without reconnecting)
  useEffect(() => {
    if (globalWsManager && handleNotificationMessage) {
      globalWsManager.updateCallbacks({
        onMessage: handleNotificationMessage,
        onComponentStream: optionsRef.current.onWorkflowComponentStream,
      });
    }
  }, [handleNotificationMessage]);

  const connect = useCallback(async () => {
    await globalWsManager?.connect();
  }, []);

  const disconnect = useCallback(() => {
    globalWsManager?.disconnect();
  }, []);

  const markRead = (id: string) => {
    queryClient.setQueryData<NotificationItem[]>(['notifications'], (oldData = []) => {
      return oldData.map((n) => (n.id === id ? { ...n, read: true } : n));
    });
  };

  const remove = async (id: string) => {
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
    fetchSavedNotifications: refetch,
  };
};

export default useNotifications;
