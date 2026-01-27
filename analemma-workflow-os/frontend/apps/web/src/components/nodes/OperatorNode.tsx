import { Handle, Position } from '@xyflow/react';
import {
  Globe,
  Database,
  X,
  CheckCircle2,
  AlertCircle,
  Loader2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';
import { OPERATOR_CONFIG, type OperatorType } from '@/lib/nodeConstants';

// Icon name to component mapping
const ICON_MAP = {
  Globe,
  Database,
  CheckCircle2,
  AlertCircle,
  Loader2
} as const;

interface OperatorNodeProps {
  data: {
    label: string;
    operatorType?: keyof typeof OPERATOR_CONFIG;
    operatorVariant?: 'custom' | 'official';
    action?: string;
    status?: 'idle' | 'running' | 'failed' | 'completed';
    authStatus?: 'connected' | 'disconnected' | 'unknown';
    authMessage?: string;
  };
  id: string;
  onDelete?: (id: string) => void;
  selected?: boolean;
}

export const OperatorNode = ({ data, id, onDelete, selected }: OperatorNodeProps) => {
  const config = OPERATOR_CONFIG[data.operatorType as keyof typeof OPERATOR_CONFIG] || OPERATOR_CONFIG.default;
  const status = data.status || 'idle';
  const Icon = ICON_MAP[config.iconName as keyof typeof ICON_MAP] || Globe;

  const statusIcons = {
    running: <Loader2 className="w-3 h-3 animate-spin" />,
    failed: <AlertCircle className="w-3 h-3" />,
    completed: <CheckCircle2 className="w-3 h-3" />,
    idle: null
  };

  const getStatusStyle = () => {
    switch (status) {
      case 'running': return { border: 'border-yellow-500 shadow-yellow-500/20', text: 'text-yellow-500' };
      case 'completed': return { border: 'border-green-500 shadow-green-500/20', text: 'text-green-500' };
      case 'failed': return { border: 'border-destructive shadow-destructive/20', text: 'text-destructive' };
      default: return { border: 'border-white/10', text: 'text-muted-foreground' };
    }
  };

  const statusStyle = getStatusStyle();

  return (
    <div
      className={cn(
        "px-3 py-2.5 rounded-xl border bg-card/80 backdrop-blur-md shadow-xl min-w-[150px] relative group transition-all duration-300",
        statusStyle.border,
        selected && "ring-2 ring-primary ring-offset-2"
      )}
      style={{
        background: `linear-gradient(to bottom right, hsl(${config.color} / 0.1), hsl(${config.color} / 0.02))`
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="w-2.5 h-2.5 bg-muted-foreground/50 border-none"
      />

      {/* Status Overlay */}
      <AnimatePresence>
        {status !== 'idle' && (
          <motion.div
            initial={{ opacity: 0, y: -5 }}
            animate={{ opacity: 1, y: 0 }}
            className={cn(
              "absolute top-2 right-2 p-1 rounded-full bg-background/50 backdrop-blur-sm border border-white/5",
              statusStyle.text
            )}
          >
            {statusIcons[status]}
          </motion.div>
        )}
      </AnimatePresence>

      <div className="flex items-center gap-3">
        <div
          className="p-2 rounded-lg"
          style={{ backgroundColor: `hsl(${config.color} / 0.15)` }}
        >
          <Icon className="w-4 h-4" style={{ color: `hsl(${config.color})` }} />
        </div>

        <div className="flex-1 min-w-0">
          <div className="text-[10px] text-muted-foreground/60 font-black uppercase tracking-tighter flex items-center justify-between">
            {config.label}
          </div>
          <div className="text-sm font-black text-foreground truncate leading-tight">
            {data.label}
          </div>

          {data.action && (
            <div className="mt-1">
              <Badge
                variant="outline"
                className="text-[9px] h-4 px-1 border-0"
                style={{ backgroundColor: `hsl(${config.color} / 0.1)`, color: `hsl(${config.color})` }}
              >
                {data.action}
              </Badge>
            </div>
          )}
        </div>
      </div>

      <Handle
        type="source"
        position={Position.Right}
        className="w-2.5 h-2.5 bg-muted-foreground/50 border-none"
      />

      {onDelete && (
        <Button
          size="icon"
          variant="ghost"
          className="absolute -top-2 -right-2 h-6 w-6 rounded-full bg-destructive text-destructive-foreground opacity-0 group-hover:opacity-100 hover:bg-destructive shadow-lg transition-all z-20"
          onClick={(e) => { e.stopPropagation(); onDelete(id); }}
        >
          <X className="w-3 h-3" />
        </Button>
      )}
    </div>
  );
};

export default OperatorNode;
