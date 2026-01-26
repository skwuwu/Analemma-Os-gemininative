/**
 * Task Card Component
 * 
 * Task Manager의 리스트에 표시되는 카드 형태 UI입니다.
 * 프로그레스 바, 에이전트 아바타, 한 줄 요약을 표시합니다.
 */

import React from 'react';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import {
  Clock,
  Loader2,
  AlertCircle,
  CheckCircle2,
  XCircle,
  Slash,
  Bot,
  TrendingUp,
  Shield
} from 'lucide-react';
import { cn } from '@/lib/utils';
import type { TaskSummary, TaskStatus, InterventionHistory } from '@/lib/types';

interface TaskCardProps {
  task: TaskSummary;
  isSelected?: boolean;
  onClick?: () => void;
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

// 상수를 컴포넌트 외부에 정의하여 매 렌더링마다 재생성 방지
const STATUS_CONFIG: Record<TaskStatus, { label: string; variant: 'default' | 'secondary' | 'destructive' | 'outline' }> = {
  queued: { label: '대기 중', variant: 'secondary' },
  in_progress: { label: '진행 중', variant: 'default' },
  pending_approval: { label: '승인 대기', variant: 'outline' },
  completed: { label: '완료', variant: 'secondary' },
  failed: { label: '실패', variant: 'destructive' },
  cancelled: { label: '취소됨', variant: 'secondary' },
};

const StatusBadge: React.FC<{ status: TaskStatus }> = ({ status }) => {
  const config = STATUS_CONFIG[status] || { label: status, variant: 'secondary' as const };

  return (
    <Badge
      variant={config.variant}
      className={cn(
        'text-xs',
        status === 'pending_approval' && 'border-amber-500 text-amber-600 bg-amber-50',
        status === 'in_progress' && 'bg-blue-500',
        status === 'completed' && 'bg-green-100 text-green-700',
      )}
    >
      {config.label}
    </Badge>
  );
};

export const TaskCard = React.memo<TaskCardProps>(({ task, isSelected, onClick }) => {
  const isRunning = task.status === 'in_progress';
  const needsAttention = task.status === 'pending_approval';

  // 시간 포맷팅
  const formatTime = (isoString?: string | null) => {
    if (!isoString) return '';
    try {
      const date = new Date(isoString);
      return date.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });
    } catch {
      return '';
    }
  };

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if ((event.key === 'Enter' || event.key === ' ') && onClick) {
      event.preventDefault();
      onClick();
    }
  };

  return (
    <Card
      className={cn(
        'cursor-pointer transition-all hover:shadow-md',
        isSelected && 'ring-2 ring-primary shadow-md',
        needsAttention && 'border-amber-300 bg-amber-50/50',
        task.status === 'failed' && 'border-red-200 bg-red-50/30',
      )}
      onClick={onClick}
      onKeyDown={handleKeyDown}
      tabIndex={onClick ? 0 : -1}
      role={onClick ? 'button' : undefined}
      aria-label={`${task.agent_name} 작업: ${task.current_step_name || '진행 중'}`}
    >
      <CardContent className="p-4">
        {/* 상단: 에이전트 정보 및 상태 */}
        <div className="flex items-start justify-between mb-3">
          <div className="flex items-center gap-2">
            <Avatar className="w-8 h-8">
              {task.agent_avatar ? (
                <AvatarImage src={task.agent_avatar} alt={task.agent_name} />
              ) : null}
              <AvatarFallback className="bg-primary/10">
                <Bot className="w-4 h-4 text-primary" />
              </AvatarFallback>
            </Avatar>
            <div>
              <p className="text-sm font-medium text-foreground">{task.agent_name}</p>
              {task.workflow_name && (
                <p className="text-xs text-muted-foreground">{task.workflow_name}</p>
              )}
            </div>
          </div>
          <div className="flex items-center gap-2">
            <StatusIcon status={task.status} />
            <StatusBadge status={task.status} />
          </div>
        </div>

        {/* 업무 요약 */}
        <h3 className="text-sm font-semibold mb-2 line-clamp-1">
          {task.execution_alias || task.task_summary || '작업 진행 중'}
        </h3>

        {/* 현재 상태/생각 */}
        <p className="text-xs text-muted-foreground mb-3 line-clamp-2 min-h-[2rem]">
          {task.current_thought || task.current_step_name || '준비 중...'}
        </p>

        {/* 비즈니스 가치 지표 - 벤토 그리드 레이아웃 */}
        {/* 
          Grid 매핑:
          - execution_alias: center-top (col-span-2, 업무 요약 섹션에서 표시)
          - ETA: right-top
          - confidence: left-bottom  
          - autonomy: center-bottom
          - intervention: right-bottom
        */}
        {(task.estimated_completion_time || task.confidence_score || task.autonomy_rate || task.intervention_history) && (
          <div className="grid grid-cols-3 gap-2 mb-3 text-xs">
            {/* Row 1: 전체 너비 ETA (가장 중요한 정보) */}
            {task.estimated_completion_time && (
              <div className="col-span-3 flex items-center justify-end gap-1 text-blue-600 bg-blue-50/50 rounded px-2 py-1">
                <Clock className="w-3 h-3" />
                <span className="font-medium">{task.estimated_completion_time}</span>
              </div>
            )}

            {/* Row 2: 3열 그리드 (신뢰도 | 자율도 | 개입이력) */}
            {/* 신뢰도 점수 - Left */}
            <div className="flex items-center justify-center gap-1 rounded bg-slate-50 px-2 py-1.5">
              {task.confidence_score !== null && task.confidence_score !== undefined ? (
                <>
                  <Shield className={cn(
                    'w-3 h-3',
                    task.confidence_score >= 80 ? 'text-green-500' :
                      task.confidence_score >= 60 ? 'text-amber-500' : 'text-red-500'
                  )} />
                  <span className={cn(
                    'font-semibold',
                    task.confidence_score >= 80 ? 'text-green-600' :
                      task.confidence_score >= 60 ? 'text-amber-600' : 'text-red-600'
                  )}>
                    {task.confidence_score.toFixed(0)}%
                  </span>
                  <span className="text-muted-foreground text-[10px]">신뢰</span>
                </>
              ) : (
                <span className="text-muted-foreground">-</span>
              )}
            </div>

            {/* 자율도 - Center */}
            <div className="flex items-center justify-center gap-1 rounded bg-emerald-50/50 px-2 py-1.5">
              {task.autonomy_rate !== null && task.autonomy_rate !== undefined ? (
                <>
                  <TrendingUp className="w-3 h-3 text-emerald-500" />
                  <span className="font-semibold text-emerald-600">
                    {task.autonomy_rate.toFixed(0)}%
                  </span>
                  <span className="text-muted-foreground text-[10px]">자율</span>
                </>
              ) : (
                <span className="text-muted-foreground">-</span>
              )}
            </div>

            {/* 개입 이력 - Right */}
            <div className="flex items-center justify-center gap-1 rounded px-2 py-1.5"
              style={{
                backgroundColor: task.intervention_history?.negative_count && task.intervention_history.negative_count > 0
                  ? 'rgba(239, 68, 68, 0.05)'
                  : 'rgba(245, 158, 11, 0.05)'
              }}
            >
              {task.intervention_history && typeof task.intervention_history === 'object' && task.intervention_history.total_count > 0 ? (
                <>
                  <AlertCircle className={cn(
                    'w-3 h-3',
                    task.intervention_history.negative_count > 0 ? 'text-red-500' : 'text-amber-500'
                  )} />
                  <span className={cn(
                    'font-semibold',
                    task.intervention_history.negative_count > 0 ? 'text-red-600' : 'text-amber-600'
                  )}>
                    {task.intervention_history.total_count}
                  </span>
                  <span className="text-muted-foreground text-[10px]">개입</span>
                </>
              ) : (
                <>
                  <CheckCircle2 className="w-3 h-3 text-green-500" />
                  <span className="text-green-600 font-medium text-[10px]">무개입</span>
                </>
              )}
            </div>
          </div>
        )}

        {/* 진행률 바 (진행 중일 때만) */}
        {(isRunning || task.progress_percentage > 0) && (
          <div className="space-y-1">
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>{task.current_step_name || '진행 중'}</span>
              <span>{task.progress_percentage}%</span>
            </div>
            <Progress
              value={task.progress_percentage}
              className={cn(
                'h-1.5',
                needsAttention && '[&>div]:bg-amber-500'
              )}
            />
          </div>
        )}

        {/* 에러 메시지 */}
        {task.error_message && (
          <div className="mt-2 p-2 bg-red-50 rounded text-xs text-red-600">
            {task.error_message}
          </div>
        )}

        {/* 하단: 시간 정보 */}
        <div className="flex justify-between items-center mt-3 pt-2 border-t text-xs text-muted-foreground">
          <span>
            {task.started_at && `시작: ${formatTime(task.started_at)}`}
          </span>
          <span>
            {task.updated_at && `업데이트: ${formatTime(task.updated_at)}`}
          </span>
        </div>
      </CardContent>
    </Card>
  );
});

export default TaskCard;
