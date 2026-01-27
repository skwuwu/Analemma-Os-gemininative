import { useState, useRef, useMemo } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import {
  Loader2,
  Sparkles,
  CheckCircle2,
  Zap,
  ArrowRight,
  BrainCircuit,
  Layout,
  ScrollText
} from 'lucide-react';
import { toast } from 'sonner';
import { convertWorkflowFromBackendFormat } from '@/lib/workflowConverter';
import { streamDesignAssistant, resolveDesignAssistantEndpoint } from '@/lib/streamingFetch';
import { fetchAuthSession } from '@aws-amplify/auth';
import { applyWorkflowPatch } from '@/lib/patchEngine';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';

interface WelcomeDialogProps {
  open: boolean;
  onWorkflowGenerated: (workflow: { nodes: any[]; edges: any[] }) => void;
  onClose: () => void;
}

interface GenerationLog {
  id: string;
  message: string;
  status: 'pending' | 'completed';
  timestamp: number;
}

export const WelcomeDialog = ({ open, onWorkflowGenerated, onClose }: WelcomeDialogProps) => {
  const [input, setInput] = useState('');
  const [isGenerating, setIsGenerating] = useState(false);
  const [logs, setLogs] = useState<GenerationLog[]>([]);
  const abortCtrlRef = useRef<AbortController | null>(null);

  const addLog = (message: string) => {
    setLogs(prev => {
      // 중복 메시지 방지 혹은 마지막Pending 업데이트
      const last = prev[prev.length - 1];
      if (last && last.message === message && last.status === 'pending') return prev;

      return [
        ...prev.map(l => ({ ...l, status: 'completed' as const })),
        { id: Math.random().toString(36).substr(2, 9), message, status: 'pending', timestamp: Date.now() }
      ];
    });
  };

  const handleGenerate = async () => {
    if (!input.trim() || isGenerating) return;

    setIsGenerating(true);
    setLogs([{ id: 'init', message: 'Architectural Analysis Initiated...', status: 'pending', timestamp: Date.now() }]);

    try {
      const resolved = resolveDesignAssistantEndpoint();
      let token: string | null = null;
      if (resolved.requiresAuth) {
        try {
          const session = await fetchAuthSession();
          token = session.tokens?.idToken?.toString() || null;
        } catch (e) {
          console.error('Failed to get auth token for streaming:', e);
          throw new Error('Authentication required for this endpoint');
        }
      }

      const bodyPayload: Record<string, unknown> = { request: `워크플로우를 생성해주세요: ${input}` };
      abortCtrlRef.current = new AbortController();

      let currentNodes: any[] = [];
      let currentEdges: any[] = [];

      // 데이터 정규화 헬퍼 (백엔드 -> 프론트엔드 변환 일관성 보장)
      const normalizeIncomingData = (type: 'node' | 'edge', data: any) => {
        if (!data) return data;
        if (type === 'node') {
          // 이미 프론트엔드 포맷이면 그대로 반환
          if (['aiModel', 'operator', 'trigger', 'control', 'group'].includes(data.type) && data.data) return data;
          // 백엔드 포맷이면 변환
          const converted = convertWorkflowFromBackendFormat({ nodes: [data], edges: [] });
          return converted.nodes[0];
        } else {
          // 엣지 정규화
          if (data.source && data.target && data.id) return data;
          const converted = convertWorkflowFromBackendFormat({ nodes: [], edges: [data] });
          return converted.edges[0];
        }
      };

      onWorkflowGenerated({ nodes: [], edges: [] });

      await streamDesignAssistant(bodyPayload, {
        authToken: token,
        signal: abortCtrlRef.current.signal,
        onMessage: (obj: any) => {
          try {
            if (!obj || typeof obj !== 'object') return;

            // 1. 패치 엔진 가동 (Refactored)
            if (obj.op) {
              const patchedObj = { ...obj };
              if (obj.data) patchedObj.data = normalizeIncomingData(obj.type, obj.data);
              if (obj.changes) patchedObj.changes = normalizeIncomingData(obj.type, obj.changes);

              if (obj.type === 'node') {
                currentNodes = applyWorkflowPatch(currentNodes, patchedObj);
                if (obj.op === 'add' && patchedObj.data?.data?.label) {
                  addLog(`Mapping Node: ${patchedObj.data.data.label}`);
                }
              } else if (obj.type === 'edge') {
                currentEdges = applyWorkflowPatch(currentEdges, patchedObj);
              }
            }
            // 2. 단일 데이터 또는 폴백 처리
            else if (obj.type === 'node' && obj.data) {
              const normalizedNode = normalizeIncomingData('node', obj.data);
              currentNodes = applyWorkflowPatch(currentNodes, { op: 'add', id: normalizedNode.id, data: normalizedNode });
              if (normalizedNode.data?.label) addLog(`Mapping Node: ${normalizedNode.data.label}`);
            } else if (obj.type === 'edge' && obj.data) {
              const normalizedEdge = normalizeIncomingData('edge', obj.data);
              currentEdges = applyWorkflowPatch(currentEdges, { op: 'add', id: normalizedEdge.id, data: normalizedEdge });
            }
            // 3. 비스트리밍 단일 응답 경로 (Full JSON)
            else if (obj.response && obj.response.tool_use) {
              const wf = obj.response.tool_use.input?.workflow_json;
              if (wf && wf.nodes && wf.edges) {
                const frontendWorkflow = convertWorkflowFromBackendFormat(wf);
                currentNodes = frontendWorkflow.nodes;
                currentEdges = frontendWorkflow.edges;
                addLog('Optimizing Vector Graph Relations...');
              }
            }

            // 4. 추론 과정 혹은 상세 피드백 인지
            if (obj.thought) {
              addLog(obj.thought);
            }

            // 부모에게 업데이트 전파 (XYFlow 반영)
            onWorkflowGenerated({ nodes: [...currentNodes], edges: [...currentEdges] });
          } catch (e) {
            console.error('stream onMessage error in WelcomeDialog:', e);
          }
        },
        onDone: () => {
          addLog('Neural Synthesis Complete. Deploying Blueprint.');
          setTimeout(() => {
            setIsGenerating(false);
            toast.success('Strategy Matrix generated successfully!');
            onClose();
          }, 800);
        },
        onError: (e) => {
          setIsGenerating(false);
          const errMsg = e?.message || 'Synthesis error';
          toast.error(`Design Failed: ${errMsg}`);
        }
      });

    } catch (error) {
      console.error('Error generating workflow:', error);
      toast.error('Failed to synthesize workflow.');
      setIsGenerating(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && !isGenerating && onClose()}>
      <DialogContent className="sm:max-w-xl max-w-[calc(100vw-260px-350px-2rem)] left-[calc(50%+130px)] p-0 overflow-hidden bg-slate-950 border-slate-800 shadow-2xl rounded-3xl">
        <div className="relative p-8">
          {/* Background Decorative Sparkle */}
          <div className="absolute top-0 right-0 p-8 opacity-10 pointer-events-none">
            <Zap className="w-32 h-32 text-blue-500 fill-blue-500" />
          </div>

          <DialogHeader className="relative z-10">
            <div className="flex items-center gap-3 mb-2">
              <div className="p-2 bg-blue-500 rounded-xl shadow-lg shadow-blue-500/20">
                <BrainCircuit className="w-5 h-5 text-white" />
              </div>
              <DialogTitle className="text-2xl font-black tracking-tight text-white uppercase italic">
                AI_DESIGN_ASSISTANT
              </DialogTitle>
            </div>
            <DialogDescription className="text-slate-400 font-medium leading-relaxed">
              Describe your operational objective. I will synthesize the underlying infrastructure logic and visualize the neural workflow fiber for you.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-6 pt-8 relative z-10">
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1">
                <span className="text-[10px] font-black tracking-widest text-slate-500 uppercase">Input Objective</span>
              </div>
              <div className="relative group">
                <Input
                  placeholder="e.g., Track premium leads and trigger automated outreach via LinkedIn..."
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && handleGenerate()}
                  disabled={isGenerating}
                  className="h-14 bg-slate-900/50 border-slate-800 text-slate-100 rounded-2xl focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-all pl-5 placeholder:text-slate-600"
                  autoFocus
                />
                {!isGenerating && (
                  <div className="absolute right-3 top-1/2 -translate-y-1/2">
                    <Button
                      size="sm"
                      onClick={handleGenerate}
                      disabled={!input.trim()}
                      className="bg-blue-600 hover:bg-blue-700 text-white font-black rounded-xl h-9"
                    >
                      <Sparkles className="w-4 h-4 mr-2" />
                      Initiate
                    </Button>
                  </div>
                )}
              </div>
            </div>

            {/* Generation Status Visualizer */}
            <AnimatePresence>
              {isGenerating && (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  className="space-y-4"
                >
                  <div className="p-5 bg-black/40 rounded-2xl border border-slate-800 shadow-inner overflow-hidden">
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center gap-2">
                        <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />
                        <span className="text-[11px] font-black tracking-[0.2em] text-blue-400 uppercase">Neural Synthesis in Progress</span>
                      </div>
                      <div className="flex gap-1">
                        {[1, 2, 3].map(i => (
                          <div key={i} className="w-1.5 h-1.5 rounded-full bg-blue-500/20 animate-pulse" style={{ animationDelay: `${i * 0.2}s` }} />
                        ))}
                      </div>
                    </div>

                    <div className="space-y-3 max-h-[160px] overflow-y-auto pr-2 custom-scrollbar">
                      {logs.map((log, idx) => (
                        <motion.div
                          key={log.id}
                          initial={{ opacity: 0, x: -10 }}
                          animate={{ opacity: 1, x: 0 }}
                          className="flex items-start gap-3"
                        >
                          {log.status === 'completed' ? (
                            <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500 mt-0.5 flex-shrink-0" />
                          ) : (
                            <Zap className="w-3.5 h-3.5 text-blue-400 mt-0.5 animate-pulse flex-shrink-0" />
                          )}
                          <span className={cn(
                            "text-xs font-medium transition-colors duration-500",
                            log.status === 'completed' ? "text-slate-500" : "text-slate-200"
                          )}>
                            {log.message}
                          </span>
                        </motion.div>
                      ))}
                      <div id="log-end" />
                    </div>
                  </div>

                  {/* Progress Illustration */}
                  <div className="flex items-center justify-center gap-6 py-2 opacity-50">
                    <div className="flex flex-col items-center gap-2">
                      <ScrollText className="w-5 h-5 text-slate-500" />
                      <span className="text-[8px] font-bold uppercase tracking-tighter">Schema</span>
                    </div>
                    <ArrowRight className="w-4 h-4 text-slate-800" />
                    <div className="flex flex-col items-center gap-2">
                      <Layout className="w-5 h-5 text-blue-500" />
                      <span className="text-[8px] font-bold uppercase tracking-tighter text-blue-500">Topology</span>
                    </div>
                    <ArrowRight className="w-4 h-4 text-slate-800" />
                    <div className="flex flex-col items-center gap-2">
                      <Sparkles className="w-5 h-5 text-slate-500" />
                      <span className="text-[8px] font-bold uppercase tracking-tighter">Polish</span>
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>

            {!isGenerating && (
              <div className="grid grid-cols-2 gap-3 pt-2">
                <div className="p-4 rounded-2xl bg-slate-900 flex items-center gap-3 border border-slate-800 group hover:border-blue-500/30 transition-all cursor-default">
                  <div className="p-2 bg-slate-800 rounded-xl group-hover:bg-blue-500/10 transition-colors">
                    <Zap className="w-4 h-4 text-amber-500" />
                  </div>
                  <div className="flex flex-col">
                    <span className="text-[10px] font-black text-white uppercase tracking-tighter">Infinite Blocks</span>
                    <span className="text-[9px] text-slate-500 font-bold">Smart Tool Chaining</span>
                  </div>
                </div>
                <div className="p-4 rounded-2xl bg-slate-900 flex items-center gap-3 border border-slate-800 group hover:border-blue-500/30 transition-all cursor-default">
                  <div className="p-2 bg-slate-800 rounded-xl group-hover:bg-blue-500/10 transition-colors">
                    <Sparkles className="w-4 h-4 text-blue-500" />
                  </div>
                  <div className="flex flex-col">
                    <span className="text-[10px] font-black text-white uppercase tracking-tighter">Neural Layout</span>
                    <span className="text-[9px] text-slate-500 font-bold">DGP-based Edge Routing</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
};
