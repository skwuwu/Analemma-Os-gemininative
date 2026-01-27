import { Handle, Position } from '@xyflow/react';
import {
  Brain,
  X,
  Settings2,
  Hammer,
  Activity,
  Clock,
  DollarSign,
  FileText,
  CheckCircle2,
  AlertCircle,
  Loader2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import { useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

interface AIModelNodeProps {
  data: {
    label: string;
    modelName?: string;
    model?: string; // Added for compatibility with NodeEditorDialog
    temperature?: number;
    toolsCount?: number;
    status?: 'idle' | 'running' | 'failed' | 'completed';
    tokens?: number;
    latency?: number;
    cost?: number;
    systemPrompt?: string;
    streamContent?: string;
  };
  id: string;
  onDelete?: (id: string) => void;
  selected?: boolean;
}

export const AIModelNode = ({ data, id, onDelete, selected }: AIModelNodeProps) => {
  const status = data.status || 'idle';

  const statusConfig = {
    idle: { border: 'border-primary/20', icon: null, text: 'text-muted-foreground' },
    running: { border: 'border-blue-500 shadow-blue-500/20', icon: <Loader2 className="w-3 h-3 animate-spin" />, text: 'text-blue-500' },
    failed: { border: 'border-destructive shadow-destructive/20', icon: <AlertCircle className="w-3 h-3" />, text: 'text-destructive' },
    completed: { border: 'border-green-500 shadow-green-500/20', icon: <CheckCircle2 className="w-3 h-3" />, text: 'text-green-500' },
  }[status];

  const formattedCost = data.cost !== undefined ? `$${data.cost.toFixed(4)}` : null;
  const formattedLatency = data.latency ? (data.latency > 1000 ? `${(data.latency / 1000).toFixed(2)}s` : `${data.latency}ms`) : null;

  const streamRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (streamRef.current && status === 'running') {
      streamRef.current.scrollTop = streamRef.current.scrollHeight;
    }
  }, [data.streamContent, status]);

  return (
    <div className={cn(
      "px-3 py-3 rounded-xl border bg-card/80 backdrop-blur-md min-w-[220px] transition-all duration-300 relative group overflow-hidden",
      statusConfig.border,
      selected && "ring-2 ring-primary ring-offset-2"
    )}>
      {/* Glossy Overlay for Status */}
      <AnimatePresence>
        {status !== 'idle' && (
          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            className={cn(
              "absolute top-2 right-2 p-1 rounded-full bg-background/80 border border-white/5 backdrop-blur-md z-10",
              statusConfig.text
            )}
          >
            {statusConfig.icon}
          </motion.div>
        )}
      </AnimatePresence>

      <Handle type="target" position={Position.Left} className="w-2.5 h-2.5 bg-muted-foreground/50 border-none" />

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

      {/* Header */}
      <div className="flex items-start gap-3 mb-3">
        <div className="p-2 rounded-lg bg-primary/10">
          <Brain className="w-4 h-4 text-primary" />
        </div>
        <div className="flex flex-col min-w-0">
          <span className="text-[10px] text-muted-foreground font-bold uppercase tracking-tighter">AI Agent Step</span>
          <span className="text-sm font-black truncate text-foreground">{data.label}</span>
        </div>
      </div>

      {/* Model Context */}
      <div className="grid grid-cols-2 gap-1.5 mb-3">
        <div className="flex items-center gap-1.5 px-2 py-1 rounded-md bg-muted/40 text-[10px] text-muted-foreground border border-white/5 truncate">
          <Settings2 className="w-3 h-3 opacity-60" />
          {data.modelName || data.model || 'GPT-4o'}
        </div>
        {formattedCost && (
          <div className="flex items-center gap-1 px-2 py-1 rounded-md bg-green-500/5 text-[10px] text-green-500 border border-green-500/10 truncate font-mono">
            <DollarSign className="w-2.5 h-2.5" />
            {formattedCost}
          </div>
        )}
      </div>

      {/* Streaming / Output Area */}
      {status === 'running' && data.streamContent && (
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          className="mb-3"
        >
          <div
            ref={streamRef}
            className="bg-[#0a0a0a] text-blue-400 text-[10px] font-mono p-2.5 rounded-lg border border-blue-500/20 max-h-24 overflow-y-auto leading-relaxed shadow-inner"
          >
            <div className="flex items-center gap-2 mb-1 opacity-50">
              <span className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse" />
              <span className="text-[8px] uppercase tracking-widest font-black">Streaming Response</span>
            </div>
            {data.streamContent}
          </div>
        </motion.div>
      )}

      {/* Analytics Footer */}
      {(data.tokens || data.latency) && (
        <div className="flex items-center justify-between pt-2 border-t border-white/5 mt-auto">
          {formattedLatency && (
            <div className="flex items-center gap-1.5 text-[9px] text-muted-foreground/60 font-medium">
              <Clock className="w-3 h-3" />
              {formattedLatency}
            </div>
          )}

          {data.tokens && (
            <div className="flex items-center gap-1.5 text-[9px] text-muted-foreground/60 font-mono ml-auto">
              <Activity className="w-3 h-3 opacity-40" />
              {data.tokens.toLocaleString()}
            </div>
          )}
        </div>
      )}

      <Handle type="source" position={Position.Right} className="w-2.5 h-2.5 bg-muted-foreground/50 border-none" />
    </div>
  );
};
