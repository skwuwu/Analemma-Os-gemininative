import { Handle, Position } from '@xyflow/react';
import { Clock, Webhook, Zap, X, Play } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { TRIGGER_CONFIG, type TriggerType } from '@/lib/nodeConstants';

// Icon name to component mapping
const ICON_MAP = {
  Clock,
  Webhook,
  Zap
} as const;

interface TriggerNodeProps {
  data: {
    label: string;
    triggerType?: keyof typeof TRIGGER_CONFIG;
    configValue?: string; // 예: "0 9 * * *", "POST /hook", "On User Signup"
    onRun?: () => void;   // 수동 실행 핸들러
  };
  id: string;
  onDelete?: (id: string) => void;
  selected?: boolean;
}

export const TriggerNode = ({ data, id, onDelete, selected }: TriggerNodeProps) => {
  const config = TRIGGER_CONFIG[data.triggerType || 'default'] || TRIGGER_CONFIG.default;
  const Icon = ICON_MAP[config.iconName as keyof typeof ICON_MAP] || Zap;

  return (
    <div
      className={cn(
        "px-3 py-3 rounded-lg border bg-card backdrop-blur-sm min-w-[150px] relative group transition-all duration-200",
        selected && "ring-2 ring-ring ring-offset-1"
      )}
      style={{
        borderColor: `hsl(${config.color})`,
        background: `linear-gradient(to bottom right, hsl(${config.color} / 0.15), hsl(${config.color} / 0.05))`,
        boxShadow: `0 0 20px hsl(${config.color} / 0.15)`
      }}
    >
      {/* Trigger는 시작점이므로 Target Handle이 없습니다. Source만 존재. */}

      {/* 상단 액션 버튼 그룹 */}
      <div className="absolute -top-3 right-2 flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity z-10">
        {/* 수동 실행 버튼 (테스트용) */}
        {data.onRun && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                size="icon"
                variant="outline"
                className="h-6 w-6 rounded-full border-primary/50 bg-background hover:bg-primary hover:text-primary-foreground"
                onClick={(e) => {
                  e.stopPropagation();
                  data.onRun?.();
                }}
              >
                <Play className="w-3 h-3" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="top" className="text-xs">Test Run</TooltipContent>
          </Tooltip>
        )}

        {/* 삭제 버튼 */}
        {onDelete && (
           <Button
             size="icon"
             variant="destructive"
             className="h-6 w-6 rounded-full shadow-sm"
             onClick={(e) => {
               e.stopPropagation();
               onDelete(id);
             }}
           >
             <X className="w-3 h-3" />
           </Button>
        )}
      </div>

      <div className="flex items-start gap-3">
        <div
          className="p-2 rounded-md flex-shrink-0"
          style={{ backgroundColor: `hsl(${config.color} / 0.15)` }}
        >
          <Icon
            className="w-4 h-4"
            style={{ color: `hsl(${config.color})` }}
          />
        </div>

        <div className="flex flex-col min-w-0">
          <span className="text-[10px] text-muted-foreground font-medium uppercase tracking-wider">
            {config.label}
          </span>
          <span className="text-sm font-bold truncate text-foreground leading-tight">
            {data.label}
          </span>

          {/* 상세 설정값 표시 (Cron, Method 등) */}
          {data.configValue && (
            <Badge
              variant="secondary"
              className="mt-1.5 w-fit max-w-[120px] truncate text-[9px] h-4 px-1.5 font-mono border-0"
              style={{
                backgroundColor: `hsl(${config.color} / 0.1)`,
                color: `hsl(${config.color})`
              }}
            >
              {data.configValue}
            </Badge>
          )}
        </div>
      </div>

      <Handle
        type="source"
        position={Position.Right}
        className="w-2.5 h-2.5"
        style={{ backgroundColor: `hsl(${config.color})` }}
      />
    </div>
  );
};
