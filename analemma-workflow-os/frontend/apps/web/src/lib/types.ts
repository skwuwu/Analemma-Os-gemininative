// Shared UI types
export interface HistoryEntry {
  id?: string;
  state_name: string;
  entered_at: number | string;
  exited_at: number | string | null;
  input: object | string | null;
  output: object | string | null;
  duration: number;
  error?: { message: string; type: string } | object | string | null;
  // Node execution tracking fields (from StateHistoryCallback)
  node_id?: string;
  name?: string;
  type?: string; // 'ai_thought', 'tool_usage', etc.
  status?: string; // 'RUNNING', 'COMPLETED', 'FAILED'
  timestamp?: number;
  content?: string;
  usage?: { total_tokens?: number; model?: string;[key: string]: any };
  [key: string]: any;
}

export type MinimalWorkflowRef = {
  conversation_id?: string | null;
  execution_id?: string | null;
};

export interface WorkflowConfig {
  name?: string;
  nodes?: unknown[];
  edges?: unknown[];
  [key: string]: unknown;
}

// API Response Type for Notifications
export interface PendingNotification {
  notificationId: string;
  ownerId: string;
  timestamp: number;
  status: 'pending' | 'sent';
  type: 'hitp_pause' | 'execution_progress' | 'workflow_completed';
  notification: {
    type: 'workflow_status';
    payload: {
      action: 'hitp_pause' | 'execution_progress' | 'workflow_completed';
      conversation_id: string;
      execution_id: string;
      workflowId: string;
      workflow_name?: string;
      message: string;
      status: 'STARTED' | 'PAUSED_FOR_HITP' | 'RUNNING' | 'COMPLETE' | 'FAILED' | 'TIMED_OUT';
      segment_to_run?: number;
      total_segments?: number;
      current_state?: unknown;
      workflow_config?: WorkflowConfig;
      step_function_state?: any;
      // Additional fields
      current_segment?: number;
      estimated_completion_time?: number;
      estimated_remaining_seconds?: number;
      current_step_label?: string;
      average_segment_duration?: number;
      state_durations?: { [status: string]: number };
      start_time?: number;
      last_update_time?: number;
      pre_hitp_output?: any;
    };
  };
  ttl: number;
}

// UI Representation of a Notification (Flattened)
export interface NotificationItem {
  id: string;
  action: string; // e.g. 'hitp_pause', 'execution_progress', 'workflow_completed'
  conversation_id?: string;
  execution_id?: string;
  workflowId?: string;
  workflow_name?: string;
  message?: string;
  segment_to_run?: number;
  total_segments?: number;
  current_segment?: number; // 현재 실행 중인 세그먼트 (0-based)
  current_state?: any;
  pre_hitp_output?: any; // legacy compatibility
  receivedAt: number;
  read: boolean;
  type?: string;
  payload?: any;
  raw?: any;
  // execution_progress 전용 필드들
  status?: 'STARTED' | 'RUNNING' | 'PAUSED_FOR_HITP' | 'COMPLETE' | 'FAILED' | 'TIMED_OUT' | string;
  workflow_config?: any;
  step_function_state?: any;
  // 추가 진행 상황 필드들 (API 규약에 따라)
  estimated_completion_time?: number; // 예상 완료 시간 (Unix timestamp)
  estimated_remaining_seconds?: number; // 남은 예상 시간 (초)
  current_step_label?: string; // 현재 단계 설명
  average_segment_duration?: number; // 평균 세그먼트 처리 시간 (초)
  state_durations?: { [status: string]: number }; // 상태별 실행 시간 (초 단위)
  start_time?: number; // 실행 시작 시간 (Unix timestamp)
  last_update_time?: number; // 마지막 업데이트 시간 (Unix timestamp)
  timestamp?: number;
  // 순서 보장을 위한 필드들
  sequence_number?: number; // 메시지 순서 보장용 시퀀스 번호
  server_timestamp?: number; // 서버 타임스탬프
  segment_sequence?: number; // 세그먼트 기반 순서
}

export interface ExecutionSummary {
  status: string;
  startDate: string | null;
  stopDate: string | null;
  error: string | null;
  name: string | null;
  final_result: object | string | null;
  created_at: number | null;
  updated_at: number | null;
  step_function_state: object | null;
  executionArn?: string;
  workflowId?: string;
  execution_id?: string;
}

export interface ResumeWorkflowRequest {
  execution_id?: string;
  conversation_id: string;  // Required by backend
  user_input: Record<string, unknown>;  // Backend expects object, not string 'response'
}

export interface WorkflowSummary {
  name: string;
  workflowId?: string;
  ownerId?: string;
}

export interface WorkflowListResult {
  workflows: WorkflowSummary[];
  nextToken: string | null;
}

export interface WorkflowDetailResponse {
  workflowId?: string;
  ownerId?: string;
  createdAt?: number;
  updatedAt?: number;
  is_scheduled?: boolean;
  next_run_time?: number | null;
  config?: WorkflowConfig | null;
  [key: string]: unknown;
}

export interface ExecutionHistoryResponse {
  step_function_state?: {
    state_history?: HistoryEntry[];
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

// ===== Plan Briefing Types =====

export type RiskLevel = 'low' | 'medium' | 'high';

export interface PlanStep {
  step_number: number;
  node_id: string;
  node_name: string;
  node_type?: string;
  action_description: string;
  estimated_duration_seconds: number;
  risk_level: RiskLevel;
  risk_description?: string | null;
  expected_input_summary?: string | null;
  expected_output_summary?: string | null;
  has_external_side_effect: boolean;
  external_systems: string[];
  is_conditional?: boolean;
  condition_description?: string | null;
}

export interface DraftResult {
  result_id?: string;
  result_type: string;
  title: string;
  content_preview: string;
  recipients?: string[] | null;
  metadata?: Record<string, unknown>;
  warnings: string[];
  requires_review: boolean;
}

export interface PlanBriefing {
  briefing_id: string;
  workflow_id: string;
  workflow_name: string;
  summary: string;
  total_steps: number;
  estimated_total_duration_seconds: number;
  steps: PlanStep[];
  draft_results: DraftResult[];
  overall_risk_level: RiskLevel;
  warnings: string[];
  requires_confirmation: boolean;
  confirmation_message?: string | null;
  generated_at: string;
  confidence_score: number;
  confirmation_token?: string | null;
  token_expires_at?: string | null;
}

// ===== Checkpoint & Time Machine Types =====

export type CheckpointStatus = 'active' | 'branched' | 'archived';

export interface Checkpoint {
  checkpoint_id: string;
  node_id: string;
  node_name: string;
  step_number: number;
  created_at: string;
  state_hash: string;
  branch_depth: number;
  status: CheckpointStatus;
}

export interface CheckpointDetail extends Checkpoint {
  state: Record<string, unknown>;
  state_preview: Record<string, unknown>;
}

export interface TimelineItem {
  checkpoint_id: string;
  execution_id: string;
  step: number;
  node_id: string;
  node_name: string;
  timestamp: string;
  can_rollback: boolean;
  is_reversible?: boolean;
  has_external_effect?: boolean;
  description?: string | null;
  branch_id?: string;
  branch_depth: number;
  status: string;
  state_preview?: Record<string, unknown>;
}

export interface RollbackRequest {
  thread_id: string;
  target_checkpoint_id: string;
  state_modifications: Record<string, unknown>;
  reason?: string | null;
  preview_only?: boolean;
}

export interface RollbackPreview {
  original_state_preview: Record<string, unknown>;
  modified_state_preview: Record<string, unknown>;
  diff: StateDiff;
  resume_from_node: string | null;
  branch_depth: number;
  estimated_impact: string;
}

export interface StateDiff {
  added: Record<string, unknown>;
  removed: Record<string, unknown>;
  modified: Record<string, { old: unknown; new: unknown }>;
  unchanged_count: number;
  total_changes: number;
}

export interface BranchInfo {
  success: boolean;
  original_thread_id: string;
  branched_thread_id: string;
  branch_point_checkpoint_id: string;
  state_modifications: Record<string, unknown>;
  branch_depth: number;
  resume_from_node: string | null;
  ready_to_resume: boolean;
}

export interface RollbackSuggestion {
  checkpoint_id: string;
  node_id: string;
  node_name?: string;
  step: number;
  reason: string;
  priority: number;
  timestamp: string;
}

export interface DetailedDraft {
  type: string;
  draft: Record<string, unknown>;
  warnings: string[];
  can_edit: boolean;
  node_id?: string;
}

export interface CheckpointCompareResult extends StateDiff {
  checkpoint_a: string;
  checkpoint_b: string;
  source_node_name?: string;
  target_node_name?: string;
}

export interface ExecutionHistoryResponse {
  history: HistoryEntry[];
  nextToken: string | null;
}

// ===== Task Manager Types =====

export type TaskStatus = 'queued' | 'in_progress' | 'pending_approval' | 'completed' | 'failed' | 'cancelled';

export interface ArtifactPreview {
  artifact_id: string;
  artifact_type: 'text' | 'file' | 'image' | 'data' | 'link';
  title: string;
  preview_content?: string | null;
  download_url?: string | null;
  thumbnail_url?: string | null;
  created_at: string;
  metadata?: Record<string, unknown>;
}

export interface AgentThought {
  thought_id: string;
  timestamp: string;
  thought_type: 'progress' | 'decision' | 'question' | 'warning' | 'success' | 'error';
  message: string;
  technical_detail?: string | null;
  node_id?: string | null;
  is_important?: boolean;
}

export interface DecisionOption {
  label: string;
  value: string;
  description?: string;
}

export interface PendingDecision {
  decision_id: string;
  question: string;
  context: string;
  options: DecisionOption[];
  default_option?: string | null;
  timeout_seconds?: number | null;
  created_at: string;
}

export interface TaskSummary {
  task_id: string;
  execution_id?: string | null;  // 체크포인트 조회용
  conversation_id?: string | null;
  task_summary: string;
  agent_name: string;
  agent_avatar?: string | null;
  status: TaskStatus;
  progress_percentage: number;
  current_step_name: string;
  current_thought: string;
  is_interruption: boolean;
  started_at?: string | null;
  updated_at: string;
  workflow_name?: string;
  workflow_id?: string;
  error_message?: string | null;
  // 비즈니스 가치 지표 (개선된 계산 로직)
  execution_alias?: string | null;               // LLM 기반 컨텍스트 레이블
  estimated_completion_time?: string | null;     // TPS 기반 하이브리드 ETA
  estimated_completion_seconds?: number | null;  // 남은 초 (원시값)
  confidence_score?: number | null;              // 다차원 신뢰도 점수 (0-100)
  confidence_components?: {                      // 신뢰도 세부 구성
    self_reflection?: number;
    schema_match?: number;
    instruction_alignment?: number;
  } | null;
  autonomy_rate?: number | null;                 // 자율도 (0-100%)
  autonomy_display?: string | null;              // 자율도 표시 문자열
  throughput?: number | null;                    // 시간당 처리량
  throughput_display?: string | null;            // 처리량 표시 문자열
  intervention_history?: InterventionHistory | null;  // 정성적 개입 이력
}

// 개입 이력 타입 (정성적 분류)
export interface InterventionHistory {
  total_count: number;
  positive_count: number;   // 필수 승인 단계
  negative_count: number;   // 오류 수정
  neutral_count: number;    // 단순 피드백
  summary: string;          // 요약 문자열
  history?: Array<{
    timestamp?: string;
    type: 'positive' | 'negative' | 'neutral';
    reason?: string;
    node_id?: string;
  }>;
}

// =============================================================================
// Quick Fix Types (장애 유형별 동적 액션)
// =============================================================================

export type QuickFixType = 'RETRY' | 'REDIRECT' | 'SELF_HEALING' | 'INPUT' | 'ESCALATE';

export interface QuickFix {
  fix_type: QuickFixType;
  label: string;                    // 버튼 라벨 (예: '재시도하기')
  action_id: string;                // 실행할 액션 ID
  context: {
    execution_id?: string;
    node_id?: string;
    error_code?: string;
    redirect_url?: string;
    delay_seconds?: number;
    missing_fields?: string[];
    original_error?: string;
    [key: string]: unknown;
  };
  secondary_action?: {
    label: string;
    url: string;
  } | null;
}

// =============================================================================
// Outcome Manager Types (결과물 중심 UI)
// =============================================================================

export interface OutcomeItem {
  artifact_id: string;
  artifact_type: 'text' | 'file' | 'image' | 'data' | 'link';
  title: string;
  preview_text?: string | null;
  content_ref?: string | null;
  download_url?: string | null;
  is_final: boolean;
  version: number;
  created_at: string;
  logic_trace_id?: string | null;   // 사고 과정 링크
  word_count?: number | null;
  file_size_bytes?: number | null;
}

export interface CollapsedHistory {
  summary: string;                  // 간략 요약
  node_count: number;
  llm_call_count: number;
  total_duration_seconds?: number | null;
  key_decisions: string[];          // 핵심 의사결정 (최대 3개)
  full_trace_available: boolean;
}

export interface OutcomesResponse {
  task_id: string;
  task_title: string;
  status: string;
  outcomes: OutcomeItem[];
  collapsed_history: CollapsedHistory;
  correction_applied: boolean;      // HITL 수정 적용 여부
  last_updated: string;
}

export interface ReasoningStep {
  step_id: string;
  timestamp: string;
  step_type: 'decision' | 'observation' | 'action' | 'reasoning';
  content: string;
  node_id?: string | null;
  confidence?: number | null;
}

export interface ReasoningPathResponse {
  artifact_id: string;
  artifact_title: string;
  reasoning_steps: ReasoningStep[];
  total_steps: number;
  total_duration_seconds?: number | null;
}

// =============================================================================
// HITL Correction Types (지침 증류용)
// =============================================================================

export interface CorrectionDelta {
  original_output_ref: string;
  corrected_output_ref: string;
  diff_summary?: string | null;
  distilled_instructions?: string[] | null;
  correction_timestamp: string;
  node_id?: string | null;
}

export interface TaskDetail extends TaskSummary {
  pending_decision?: PendingDecision | null;
  artifacts: ArtifactPreview[];
  thought_history: AgentThought[];
  error_suggestion?: string | null;
  token_usage?: { input?: number; output?: number };
  technical_logs?: Array<{
    timestamp: string;
    node_id: string;
    input?: unknown;
    output?: unknown;
    duration: number;
    error?: unknown;
  }>;
  // 새로운 필드들
  quick_fix?: QuickFix | null;               // 동적 복구 액션
  correction_delta?: CorrectionDelta | null; // HITL 수정 데이터
  collapsed_history?: CollapsedHistory | null; // 축약된 히스토리
}

export interface TaskListResponse {
  tasks: TaskSummary[];
  total: number;
  filters_applied: {
    status?: string | null;
    include_completed: boolean;
  };
}

export interface TaskStreamPayload {
  type: 'task_update';
  task_id: string;
  thought: string;
  thought_type?: string;
  progress?: number;
  current_step?: string;
  updated_at: string;
  is_interruption?: boolean;
  artifacts_count?: number;
  agent_name?: string;
}
