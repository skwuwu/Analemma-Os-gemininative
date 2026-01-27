/**
 * Co-design Store: AI 협업 설계 상태 관리
 * 
 * 사용자의 워크플로우 변경 추적, AI 제안 관리, 동기화 상태를 관리합니다.
 */
import { create } from 'zustand';
import { Node, Edge } from '@xyflow/react';
import { streamCoDesignAssistant, callCoDesignAssistantSync } from './streamingFetch';

// ──────────────────────────────────────────────────────────────
// 타입 정의
// ──────────────────────────────────────────────────────────────

export type ChangeType =
  | 'add_node'
  | 'move_node'
  | 'delete_node'
  | 'update_node'
  | 'add_edge'
  | 'delete_edge'
  | 'group_nodes'
  | 'ungroup_nodes';

export type SuggestionAction =
  | 'group'
  | 'add_node'
  | 'modify'
  | 'delete'
  | 'reorder'
  | 'connect'
  | 'optimize';

export type SuggestionStatus = 'pending' | 'accepted' | 'rejected';

export type AuditLevel = 'error' | 'warning' | 'info';

export type SyncStatus = 'idle' | 'syncing' | 'error';
export type CanvasMode = 'agentic-designer' | 'co-design' | 'generating';

export interface CodesignChange {
  timestamp: number;
  type: ChangeType;
  data: Record<string, unknown>;
}

export interface SuggestionPreview {
  id: string;
  action: SuggestionAction;
  reason: string;
  affectedNodes: string[];
  proposedChange: Record<string, unknown>;
  confidence: number;
  status: SuggestionStatus;
  createdAt?: number;
}

export interface AuditIssue {
  level: AuditLevel;
  type: string;
  message: string;
  affectedNodes: string[];
  suggestion?: string;
}

export interface CodesignMessage {
  id: string;
  type: 'user' | 'assistant' | 'system' | 'thought';
  content: string;
  timestamp: number;
  metadata?: Record<string, any>;
}

// ──────────────────────────────────────────────────────────────
// 스토어 상태 인터페이스
// ──────────────────────────────────────────────────────────────

interface CodesignState {
  // 변경 추적
  recentChanges: CodesignChange[];

  // AI 제안
  pendingSuggestions: SuggestionPreview[];
  activeSuggestionId: string | null;

  // 검증 이슈
  auditIssues: AuditIssue[];

  // 채팅 기록
  messages: CodesignMessage[];

  // 동기화 상태
  syncStatus: SyncStatus;
  lastSyncTime: number | null;

  // 패널 상태
  isPanelOpen: boolean;
  activeTab: 'chat' | 'suggestions' | 'audit';

  // Actions - 변경 추적
  recordChange: (type: ChangeType, data: Record<string, unknown>) => void;
  clearChanges: () => void;

  // Actions - 제안 관리
  addSuggestion: (suggestion: Omit<SuggestionPreview, 'status'>) => void;
  acceptSuggestion: (id: string) => void;
  rejectSuggestion: (id: string) => void;
  setActiveSuggestion: (id: string | null) => void;
  clearSuggestions: () => void;

  // Actions - 검증 이슈
  setAuditIssues: (issues: AuditIssue[]) => void;
  clearAuditIssues: () => void;

  // Actions - 채팅
  addMessage: (type: CodesignMessage['type'], content: string, metadata?: CodesignMessage['metadata']) => void;
  clearMessages: () => void;

  // Actions - 동기화
  setSyncStatus: (status: SyncStatus) => void;
  setLastSyncTime: (time: number) => void;

  // Actions - 패널
  togglePanel: () => void;
  setActiveTab: (tab: CodesignState['activeTab']) => void;

  // Actions - Remote Driven State
  remoteMode: CanvasMode | null;
  setRemoteMode: (mode: CanvasMode | null) => void;

  // Actions - AI Assistant (API calls)
  requestSuggestions: (workflowData: { nodes: Node[], edges: Edge[] }, authToken?: string) => Promise<void>;
  requestAudit: (workflowData: { nodes: Node[], edges: Edge[] }, authToken?: string) => Promise<void>;
  requestSimulation: (workflowData: { nodes: Node[], edges: Edge[] }, authToken?: string) => Promise<void>;
}

// ──────────────────────────────────────────────────────────────
// 스토어 생성
// ──────────────────────────────────────────────────────────────

export const useCodesignStore = create<CodesignState>((set, get) => ({
  // 초기 상태
  recentChanges: [],
  pendingSuggestions: [],
  activeSuggestionId: null,
  auditIssues: [],
  messages: [],
  syncStatus: 'idle',
  lastSyncTime: null,
  isPanelOpen: false,
  activeTab: 'chat',
  remoteMode: null,

  setRemoteMode: (mode) => set({ remoteMode: mode }),

  // ─────────────────────────────────────────────
  // 변경 추적 Actions
  // ─────────────────────────────────────────────

  recordChange: (type, data) => {
    set((state) => ({
      recentChanges: [
        ...state.recentChanges.slice(-19),  // 최근 20개 유지
        {
          timestamp: Date.now(),
          type,
          data
        }
      ]
    }));
  },

  clearChanges: () => set({ recentChanges: [] }),

  // ─────────────────────────────────────────────
  // 제안 관리 Actions
  // ─────────────────────────────────────────────

  addSuggestion: (suggestion) => {
    set((state) => {
      // 중복 제안 방지
      const exists = state.pendingSuggestions.some(s => s.id === suggestion.id);
      if (exists) return state;

      return {
        pendingSuggestions: [
          ...state.pendingSuggestions,
          {
            ...suggestion,
            status: 'pending' as SuggestionStatus,
            createdAt: Date.now()
          }
        ]
      };
    });
  },

  acceptSuggestion: (id) => {
    set((state) => ({
      pendingSuggestions: state.pendingSuggestions.map(s =>
        s.id === id ? { ...s, status: 'accepted' as SuggestionStatus } : s
      ),
      activeSuggestionId: state.activeSuggestionId === id ? null : state.activeSuggestionId
    }));

    // 시스템 메시지 추가
    const suggestion = get().pendingSuggestions.find(s => s.id === id);
    if (suggestion) {
      get().addMessage('system', `제안 "${suggestion.action}"이(가) 적용되었습니다.`);
    }
  },

  rejectSuggestion: (id) => {
    set((state) => ({
      pendingSuggestions: state.pendingSuggestions.map(s =>
        s.id === id ? { ...s, status: 'rejected' as SuggestionStatus } : s
      ),
      activeSuggestionId: state.activeSuggestionId === id ? null : state.activeSuggestionId
    }));
  },

  setActiveSuggestion: (id) => set({ activeSuggestionId: id }),

  clearSuggestions: () => set({
    pendingSuggestions: [],
    activeSuggestionId: null
  }),

  // ─────────────────────────────────────────────
  // 검증 이슈 Actions
  // ─────────────────────────────────────────────

  setAuditIssues: (issues) => set({ auditIssues: issues }),

  clearAuditIssues: () => set({ auditIssues: [] }),

  // ─────────────────────────────────────────────
  // 채팅 Actions
  // ─────────────────────────────────────────────

  addMessage: (type, content, metadata) => {
    set((state) => ({
      messages: [
        ...state.messages,
        {
          id: `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          type,
          content,
          timestamp: Date.now(),
          metadata
        }
      ]
    }));
  },

  clearMessages: () => set({ messages: [] }),

  // ─────────────────────────────────────────────
  // 동기화 Actions
  // ─────────────────────────────────────────────

  setSyncStatus: (status) => set({ syncStatus: status }),

  setLastSyncTime: (time) => set({ lastSyncTime: time }),

  // ─────────────────────────────────────────────
  // 패널 Actions
  // ─────────────────────────────────────────────

  togglePanel: () => set((state) => ({ isPanelOpen: !state.isPanelOpen })),

  setActiveTab: (tab) => set({ activeTab: tab }),

  // ─────────────────────────────────────────────
  // API Actions - Co-design Assistant
  // ─────────────────────────────────────────────

  requestSuggestions: async (workflowData: { nodes: Node[], edges: Edge[] }, authToken?: string) => {
    set({ syncStatus: 'syncing' });
    try {
      const body = {
        request: 'Analyze this workflow and provide suggestions',
        current_workflow: workflowData,
        recent_changes: get().recentChanges,
        mode: 'suggest'
      };

      await streamCoDesignAssistant('codesign', body, {
        authToken,
        onMessage: (data: any) => {
          if (data.type === 'suggestion') {
            get().addSuggestion({
              id: data.id || `suggestion-${Date.now()}`,
              action: data.action,
              reason: data.reason,
              affectedNodes: data.affectedNodes || [],
              proposedChange: data.proposedChange || {},
              confidence: data.confidence || 0.8
            });
          }
        },
        onDone: () => {
          set({ syncStatus: 'idle', lastSyncTime: Date.now() });
        },
        onError: (error) => {
          console.error('Co-design suggestion error:', error);
          set({ syncStatus: 'error' });
        }
      });
    } catch (error) {
      console.error('Failed to request suggestions:', error);
      set({ syncStatus: 'error' });
    }
  },

  requestAudit: async (workflowData: { nodes: Node[], edges: Edge[] }, authToken?: string) => {
    set({ syncStatus: 'syncing' });
    try {
      const body = {
        workflow: {
          nodes: workflowData.nodes,
          edges: workflowData.edges
        }
      };

      const result = await callCoDesignAssistantSync('audit', body, authToken);
      console.log('Audit API response:', result);
      
      if (result && result.issues && Array.isArray(result.issues)) {
        get().setAuditIssues(result.issues);
      } else if (result && Array.isArray(result)) {
        // 응답이 배열 자체인 경우
        get().setAuditIssues(result);
      } else {
        console.error('Unexpected audit response format:', result);
        get().setAuditIssues([]);
      }
      set({ syncStatus: 'idle', lastSyncTime: Date.now() });
    } catch (error) {
      console.error('Audit request failed:', error);
      set({ syncStatus: 'error' });
      get().setAuditIssues([]);
    }
  },

  requestSimulation: async (workflowData: { nodes: Node[], edges: Edge[] }, authToken?: string) => {
    set({ syncStatus: 'syncing' });
    try {
      const body = {
        workflow: {
          nodes: workflowData.nodes,
          edges: workflowData.edges
        },
        mock_inputs: {}
      };

      const result = await callCoDesignAssistantSync('simulate', body, authToken);
      // 시뮬레이션 결과를 메시지로 추가
      if (result.simulation_result) {
        get().addMessage('system', `Simulation completed: ${result.simulation_result}`, {
          type: 'simulation',
          data: result
        });
      }
      set({ syncStatus: 'idle', lastSyncTime: Date.now() });
    } catch (error) {
      console.error('Simulation request failed:', error);
      set({ syncStatus: 'error' });
    }
  }
}));

// ──────────────────────────────────────────────────────────────
// 선택자 (Selectors)
// ──────────────────────────────────────────────────────────────

export const selectPendingSuggestions = (state: CodesignState) =>
  state.pendingSuggestions.filter(s => s.status === 'pending');

export const selectActiveSuggestion = (state: CodesignState) =>
  state.pendingSuggestions.find(s => s.id === state.activeSuggestionId);

export const selectErrorIssues = (state: CodesignState) =>
  state.auditIssues.filter(i => i.level === 'error');

export const selectWarningIssues = (state: CodesignState) =>
  state.auditIssues.filter(i => i.level === 'warning');

export const selectIssueSummary = (state: CodesignState) => ({
  errors: state.auditIssues.filter(i => i.level === 'error').length,
  warnings: state.auditIssues.filter(i => i.level === 'warning').length,
  info: state.auditIssues.filter(i => i.level === 'info').length,
  total: state.auditIssues.length
});

export const selectUnreadSuggestionsCount = (state: CodesignState) =>
  state.pendingSuggestions.filter(s => s.status === 'pending').length;
