/**
 * Plan Briefing Modal
 * 
 * 워크플로우 실행 전 미리보기를 보여주는 모달 컴포넌트입니다.
 * 실행 계획, 예상 결과물, 위험 분석을 표시하여 사용자의 통제권을 보장합니다.
 */

import React, { useState, useMemo } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion';
import { Separator } from '@/components/ui/separator';
import { Skeleton } from '@/components/ui/skeleton';
import {
  AlertTriangle,
  CheckCircle2,
  Clock,
  ExternalLink,
  Mail,
  FileText,
  Bell,
  Play,
  Edit,
  X,
  Info,
  Zap,
  Shield,
  Layers,
  ArrowRight
} from 'lucide-react';
import type { PlanBriefing, PlanStep, DraftResult, RiskLevel } from '@/lib/types';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

interface PlanBriefingModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  briefing: PlanBriefing | null;
  loading?: boolean;
  onConfirm: () => void;
  onEdit?: () => void;
  onCancel: () => void;
}

// --- HELPERS ---

const getRiskTheme = (level: RiskLevel) => {
  const themes = {
    low: {
      color: 'text-emerald-300 dark:text-emerald-300',
      border: 'border-emerald-700/50 dark:border-emerald-700/50',
      bg: 'bg-emerald-950/20 dark:bg-emerald-950/20',
      indicator: 'bg-emerald-500',
      label: 'Low Risk'
    },
    medium: {
      color: 'text-amber-300 dark:text-amber-300',
      border: 'border-amber-700/50 dark:border-amber-700/50',
      bg: 'bg-amber-950/20 dark:bg-amber-950/20',
      indicator: 'bg-amber-500',
      label: 'Medium Risk'
    },
    high: {
      color: 'text-rose-300 dark:text-rose-300',
      border: 'border-rose-700/50 dark:border-rose-700/50',
      bg: 'bg-rose-950/20 dark:bg-rose-950/20',
      indicator: 'bg-rose-500',
      label: 'High Risk'
    },
  };
  return themes[level] || themes.low;
};

// --- SUB-COMPONENTS ---

/**
 * 리스크 등급 표시 배지
 */
const RiskBadge = React.memo(({ level }: { level: RiskLevel }) => {
  const theme = getRiskTheme(level);
  const Icon = level === 'low' ? CheckCircle2 : AlertTriangle;

  return (
    <Badge
      variant="outline"
      className={cn("gap-1.5 font-bold uppercase text-[10px] tracking-widest px-2 py-1", theme.color, theme.border, theme.bg)}
      aria-label={`위험도: ${theme.label}`}
    >
      <Icon className="w-3 h-3" />
      {theme.label}
    </Badge>
  );
});

RiskBadge.displayName = 'RiskBadge';

/**
 * 실행 단계를 보여주는 카드
 */
const StepCard = React.memo(({ step }: { step: PlanStep }) => {
  const theme = getRiskTheme(step.risk_level);

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="mb-3"
    >
      <Card className={cn("overflow-hidden border-slate-700 dark:border-slate-700 bg-slate-800 dark:bg-slate-800 border-l-4 transition-all hover:shadow-md", `border-l-${step.risk_level === 'low' ? 'emerald' : step.risk_level === 'medium' ? 'amber' : 'rose'}-500`)}>
        <CardHeader className="py-4 px-5 bg-slate-900/50 dark:bg-slate-900/50">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span className="flex items-center justify-center w-6 h-6 rounded-lg bg-slate-700 dark:bg-slate-700 text-white text-[10px] font-black font-mono shadow-sm">
                {step.step_number}
              </span>
              <CardTitle className="text-[13px] font-black tracking-tight text-slate-200 dark:text-slate-200">
                {step.node_name}
              </CardTitle>
            </div>
            <div className="flex items-center gap-3">
              <RiskBadge level={step.risk_level} />
              <div className="flex items-center gap-1.5 text-[10px] font-bold text-slate-300 dark:text-slate-300 bg-slate-700 dark:bg-slate-700 px-2 py-1 rounded-md">
                <Clock className="w-3 h-3" />
                ~{step.estimated_duration_seconds}s
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent className="py-4 px-5 space-y-3">
          <p className="text-[13px] text-slate-300 dark:text-slate-300 leading-relaxed">
            {step.action_description}
          </p>

          <div className="flex flex-wrap gap-2">
            {step.has_external_side_effect && (
              <Badge variant="outline" className="text-[10px] font-bold bg-orange-950/20 dark:bg-orange-950/20 text-orange-300 dark:text-orange-300 border-orange-700 dark:border-orange-700 gap-1.5 py-1 px-2 uppercase tracking-tighter">
                <ExternalLink className="w-3 h-3" />
                Irreversible
              </Badge>
            )}

            {step.external_systems.map(sys => (
              <Badge key={sys} variant="secondary" className="text-[10px] font-medium bg-slate-700 dark:bg-slate-700 text-slate-300 dark:text-slate-300 py-1 px-2 border-none">
                @{sys}
              </Badge>
            ))}
          </div>

          {step.risk_description && (
            <div className={cn("p-2.5 rounded-xl border flex gap-2.5 items-start transition-colors", theme.bg, theme.border)}>
              <Info className={cn("w-4 h-4 mt-0.5 shrink-0", theme.color)} />
              <p className={cn("text-[11px] font-medium leading-normal", theme.color)}>
                {step.risk_description}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    </motion.div>
  );
});

StepCard.displayName = 'StepCard';

/**
 * 사전 결과물 미리보기 카드
 */
const DraftResultCard = React.memo(({ draft }: { draft: DraftResult }) => {
  const typeConfig: Record<string, { icon: React.ElementType; label: string; color: string }> = {
    email: { icon: Mail, label: 'Email Draft', color: 'text-blue-500' },
    document: { icon: FileText, label: 'Document', color: 'text-emerald-500' },
    notification: { icon: Bell, label: 'Push Notification', color: 'text-purple-500' },
    api_call: { icon: Zap, label: 'Web Service Request', color: 'text-amber-500' },
    default: { icon: FileText, label: 'System Output', color: 'text-slate-500' },
  };

  const { icon: Icon, label, color } = typeConfig[draft.result_type] || typeConfig.default;

  return (
    <Card className="mb-3 overflow-hidden bg-slate-800 dark:bg-slate-800 border-slate-700 dark:border-slate-700 transition-all hover:border-slate-600 dark:hover:border-slate-600">
      <CardHeader className="py-3 px-5 bg-slate-900/50 dark:bg-slate-900/50 border-b border-slate-700 dark:border-slate-700">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className={cn("p-1.5 rounded-lg bg-slate-700 dark:bg-slate-700 shadow-sm border border-slate-600 dark:border-slate-600", color)}>
              <Icon className="w-4 h-4" />
            </div>
            <CardTitle className="text-xs font-black tracking-tight text-slate-200 dark:text-slate-200">{draft.title}</CardTitle>
          </div>
          <Badge variant="outline" className="text-[9px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-400 border-slate-600 dark:border-slate-600">
            {label}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="p-5 space-y-4">
        <div className="bg-slate-900 dark:bg-slate-900 p-4 rounded-2xl shadow-inner border border-slate-700 dark:border-slate-700">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2.5 h-2.5 rounded-full bg-rose-500/20" />
            <span className="text-[9px] font-bold text-slate-500 uppercase tracking-widest">Preview Content</span>
          </div>
          <p className="text-[12px] font-mono text-slate-300 leading-relaxed whitespace-pre-wrap line-clamp-4 italic">
            {draft.content_preview}
          </p>
        </div>

        {draft.recipients && draft.recipients.length > 0 && (
          <div className="flex flex-wrap gap-1.5 items-center">
            <span className="text-[10px] font-black text-slate-400 dark:text-slate-400 uppercase tracking-tighter mr-1 flex items-center gap-1">
              <ArrowRight className="w-3 h-3" /> Target:
            </span>
            {draft.recipients.map(r => (
              <Badge key={r} variant="secondary" className="px-2 py-0 h-5 text-[10px] bg-blue-950/20 dark:bg-blue-950/20 text-blue-300 dark:text-blue-300 border-blue-700/30 dark:border-blue-700/30">
                {r}
              </Badge>
            ))}
          </div>
        )}

        {draft.warnings.length > 0 && (
          <div className="space-y-1.5">
            {draft.warnings.map((warning, i) => (
              <div key={i} className="p-2 rounded-lg bg-rose-950/20 dark:bg-rose-950/20 text-[10px] font-bold text-rose-300 dark:text-rose-300 flex items-center gap-2 border border-rose-800/30 dark:border-rose-800/30">
                <AlertTriangle className="w-3 h-3" />
                {warning}
              </div>
            ))}
          </div>
        )}

        {draft.requires_review && (
          <div className="flex items-center gap-2 p-3 rounded-xl bg-blue-950/20 dark:bg-blue-950/20 border border-blue-800/30 dark:border-blue-800/30">
            <Shield className="w-4 h-4 text-blue-400 dark:text-blue-400 shrink-0" />
            <span className="text-[11px] text-blue-300 dark:text-blue-300 font-black">
              Agent will pause after execution. User review and final approval required.
            </span>
          </div>
        )}
      </CardContent>
    </Card>
  );
});

DraftResultCard.displayName = 'DraftResultCard';

// --- MAIN PANEL ---

export const PlanBriefingModal: React.FC<PlanBriefingModalProps> = ({
  open,
  onOpenChange,
  briefing,
  loading = false,
  onConfirm,
  onEdit,
  onCancel,
}) => {
  const [confirmLoading, setConfirmLoading] = useState(false);

  const handleConfirm = async () => {
    setConfirmLoading(true);
    try {
      await onConfirm();
    } finally {
      setConfirmLoading(false);
    }
  };

  const formatDuration = (seconds: number): string => {
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const remaining = seconds % 60;
    return remaining > 0 ? `${minutes}m ${remaining}s` : `${minutes}m`;
  };

  const riskTheme = useMemo(() =>
    briefing ? getRiskTheme(briefing.overall_risk_level) : null,
    [briefing]
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl h-[85vh] flex flex-col p-0 bg-slate-950 dark:bg-slate-950 border-slate-800 dark:border-slate-800 rounded-3xl overflow-hidden shadow-2xl">
        {/* Risk Banner Line */}
        {briefing && (
          <div className={cn("absolute top-0 left-0 w-full h-1.5 z-50 transition-colors", riskTheme?.indicator)} />
        )}

        <DialogHeader className={cn(
          "px-8 py-7 border-b transition-colors bg-slate-900 dark:bg-slate-900 border-slate-800 dark:border-slate-800"
        )}>
          <div className="flex items-center justify-between">
            <div className="space-y-1.5">
              <DialogTitle className="text-xl font-black tracking-tight flex items-center gap-3 text-white dark:text-white">
                <span className="flex items-center justify-center w-8 h-8 rounded-xl bg-primary text-white shadow-lg">
                  <Layers className="w-4 h-4" />
                </span>
                Agent Briefing Report
              </DialogTitle>
              <DialogDescription className="font-bold text-slate-400 dark:text-slate-400 flex items-center gap-2">
                {briefing?.workflow_name || 'System Assessment'}
                {briefing && (
                  <Badge variant="outline" className="text-[9px] font-black uppercase tracking-widest border-slate-600 dark:border-slate-600 text-slate-300 dark:text-slate-300">
                    PRE-FLIGHT CHECK
                  </Badge>
                )}
              </DialogDescription>
            </div>
            {briefing && (
              <div className="flex items-center gap-2">
                <span className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-400 mr-2">System Security Level:</span>
                <RiskBadge level={briefing.overall_risk_level} />
              </div>
            )}
          </div>
        </DialogHeader>

        <ScrollArea className="flex-1 custom-scrollbar">
          {loading ? (
            <div className="p-8 space-y-6">
              <div className="flex gap-4">
                <Skeleton className="h-24 flex-1 rounded-3xl" />
                <Skeleton className="h-24 w-48 rounded-3xl" />
              </div>
              <Skeleton className="h-64 w-full rounded-3xl" />
              <div className="flex gap-4">
                <Skeleton className="h-32 flex-1 rounded-3xl" />
                <Skeleton className="h-32 flex-1 rounded-3xl" />
              </div>
            </div>
          ) : briefing ? (
            <div className="p-8 space-y-8">
              {/* Executive Summary */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Card className="md:col-span-2 rounded-3xl border-slate-700 dark:border-slate-700 shadow-sm bg-slate-800 dark:bg-slate-800">
                  <CardHeader className="py-4 px-6 border-b border-slate-700 dark:border-slate-700">
                    <CardTitle className="text-[11px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-400 flex items-center gap-1.5">
                      <Info className="w-3.5 h-3.5" /> Assessment Summary
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="p-6">
                    <p className="text-sm font-medium text-slate-300 dark:text-slate-300 leading-relaxed italic">
                      "{briefing.summary}"
                    </p>
                  </CardContent>
                </Card>

                <Card className="rounded-3xl border-slate-100 dark:border-slate-700 shadow-sm bg-blue-600 dark:bg-blue-700 text-white overflow-hidden relative">
                  <div className="absolute top-0 right-0 w-24 h-24 bg-white/10 rounded-full -mr-12 -mt-12 blur-2xl" />
                  <CardHeader className="py-4 px-6 border-b border-white/10">
                    <CardTitle className="text-[11px] font-black uppercase tracking-widest text-white/60 dark:text-white/60 flex items-center gap-1.5 text-center">
                      Resource Estimates
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="p-6 flex flex-col justify-center items-center gap-4 h-24">
                    <div className="grid grid-cols-2 gap-8 w-full text-center">
                      <div>
                        <div className="text-[10px] uppercase font-black text-white/50 tracking-widest mb-1">Nodes</div>
                        <div className="text-xl font-black font-mono">{briefing.total_steps}</div>
                      </div>
                      <div>
                        <div className="text-[10px] uppercase font-black text-white/50 tracking-widest mb-1">Duration</div>
                        <div className="text-xl font-black font-mono">{formatDuration(briefing.estimated_total_duration_seconds)}</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Critical Warnings */}
              <AnimatePresence>
                {briefing.warnings.length > 0 && (
                  <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                  >
                    <Alert variant="destructive" className="rounded-3xl border-rose-900 dark:border-rose-900 shadow-lg shadow-rose-900/5 bg-rose-950/20 dark:bg-rose-950/20 py-6 px-8 flex flex-col gap-4">
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-rose-500 rounded-xl shadow-lg shadow-rose-500/20">
                          <AlertTriangle className="h-4 w-4 text-white" />
                        </div>
                        <div>
                          <AlertTitle className="text-rose-400 dark:text-rose-400 font-black uppercase tracking-widest text-xs">Security Advisory</AlertTitle>
                          <span className="text-[10px] text-rose-500/60 dark:text-rose-500/60 font-medium">Please review these critical points before deployment.</span>
                        </div>
                      </div>
                      <AlertDescription>
                        <ul className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-3 mt-2">
                          {briefing.warnings.map((warning, i) => (
                            <li key={i} className="text-sm font-bold text-rose-300 dark:text-rose-300 flex items-start gap-3">
                              <span className="w-1.5 h-1.5 rounded-full bg-rose-500 mt-1.5 shrink-0" />
                              {warning}
                            </li>
                          ))}
                        </ul>
                      </AlertDescription>
                    </Alert>
                  </motion.div>
                )}
              </AnimatePresence>

              <Accordion type="single" collapsible className="w-full space-y-4">
                {/* Execution Steps */}
                <AccordionItem value="steps" className="border border-slate-700 dark:border-slate-700 bg-slate-800 dark:bg-slate-800 rounded-2xl overflow-hidden px-4 hover:bg-slate-700 dark:hover:bg-slate-700 transition-colors">
                  <AccordionTrigger className="hover:no-underline py-5">
                    <div className="flex items-center gap-3">
                      <div className="p-2 bg-slate-700 dark:bg-slate-700 rounded-lg">
                        <Play className="w-4 h-4 text-slate-300 dark:text-slate-300" />
                      </div>
                      <div className="text-left">
                        <div className="text-xs font-black uppercase tracking-widest text-slate-400 dark:text-slate-400">Operation Sequence</div>
                        <div className="text-sm font-bold text-slate-200 dark:text-slate-200">{briefing.steps.length} Critical Nodes</div>
                      </div>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent className="pb-6">
                    <div className="grid grid-cols-1 gap-1 pt-4 border-t border-slate-700 dark:border-slate-700">
                      {briefing.steps.map((step) => (
                        <StepCard key={step.step_number} step={step} />
                      ))}
                    </div>
                  </AccordionContent>
                </AccordionItem>

                {/* Predicted Results */}
                {briefing.draft_results.length > 0 && (
                  <AccordionItem value="results" className="border border-slate-700 dark:border-slate-700 bg-slate-800 dark:bg-slate-800 rounded-2xl overflow-hidden px-4 hover:bg-slate-700 dark:hover:bg-slate-700 transition-colors">
                    <AccordionTrigger className="hover:no-underline py-5">
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-slate-700 dark:bg-slate-700 rounded-lg">
                          <FileText className="w-4 h-4 text-slate-300 dark:text-slate-300" />
                        </div>
                        <div className="text-left">
                          <div className="text-xs font-black uppercase tracking-widest text-slate-400 dark:text-slate-400">Predicted Yields</div>
                          <div className="text-sm font-bold text-slate-200 dark:text-slate-200">{briefing.draft_results.length} Artifacts Prepared</div>
                        </div>
                      </div>
                    </AccordionTrigger>
                    <AccordionContent className="pb-6">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-4 border-t border-slate-700 dark:border-slate-700">
                        {briefing.draft_results.map((draft, i) => (
                          <DraftResultCard key={draft.result_id || i} draft={draft} />
                        ))}
                      </div>
                    </AccordionContent>
                  </AccordionItem>
                )}
              </Accordion>

              {/* 승인 필요 메시지 */}
              {briefing.requires_confirmation && briefing.confirmation_message && (
                <div className="p-6 rounded-3xl bg-blue-950/20 dark:bg-blue-950/20 border border-blue-800/30 dark:border-blue-800/30 shadow-inner flex gap-5 items-center">
                  <div className="w-12 h-12 rounded-2xl bg-blue-600 flex items-center justify-center shadow-lg shadow-blue-500/20 shrink-0">
                    <Shield className="w-6 h-6 text-white" />
                  </div>
                  <div className="space-y-1">
                    <h4 className="text-[11px] font-black uppercase tracking-[0.2em] text-blue-400 dark:text-blue-400">Human Confirmation Required</h4>
                    <p className="text-sm font-bold text-slate-200 dark:text-slate-200">
                      {briefing.confirmation_message}
                    </p>
                  </div>
                </div>
              )}

              {/* Footer Confidence Info */}
              <div className="flex items-center justify-between pt-4">
                <div className="flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full bg-emerald-500" />
                  <span className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-400">All Security Checks Passed</span>
                </div>
                <div className="flex items-center gap-3 text-slate-400 dark:text-slate-400">
                  <span className="text-[10px] font-bold uppercase tracking-widest">Model Probability:</span>
                  <Badge variant="outline" className="font-mono font-black text-xs border-emerald-500/30 text-emerald-500">
                    {Math.round(briefing.confidence_score * 100)}%
                  </Badge>
                </div>
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-full text-slate-400 dark:text-slate-400 p-12 text-center">
              <div className="w-16 h-16 rounded-3xl bg-slate-800 dark:bg-slate-800 flex items-center justify-center mb-6">
                <AlertTriangle className="w-8 h-8 opacity-20" />
              </div>
              <h4 className="font-bold text-slate-300 dark:text-slate-300 mb-2">Simulation Failed</h4>
              <p className="text-xs text-slate-500 dark:text-slate-500 leading-relaxed max-w-[200px]">
                An error occurred during workflow state analysis. Please try again in a moment.
              </p>
            </div>
          )}
        </ScrollArea>

        <Separator />

        <DialogFooter className="px-8 py-5 gap-3 bg-slate-900 dark:bg-slate-900 backdrop-blur-sm">
          <Button variant="ghost" onClick={onCancel} className="h-11 px-6 font-bold text-xs text-slate-400 dark:text-slate-400 hover:text-white dark:hover:text-white hover:bg-slate-700 dark:hover:bg-slate-700 rounded-xl transition-all">
            <X className="w-4 h-4 mr-2" />
            Discard Run
          </Button>
          <div className="flex-1" />
          {onEdit && (
            <Button variant="outline" onClick={onEdit} className="h-11 px-6 font-bold text-xs border-slate-600 dark:border-slate-600 text-slate-300 dark:text-slate-300 rounded-xl hover:bg-slate-700 dark:hover:bg-slate-700 transition-all">
              <Edit className="w-4 h-4 mr-2" />
              Adjust Logic
            </Button>
          )}
          <Button
            onClick={handleConfirm}
            disabled={loading || confirmLoading || (briefing?.requires_confirmation && !briefing?.confirmation_token)}
            className={cn(
              "h-11 px-8 font-black text-xs uppercase tracking-widest rounded-xl shadow-xl transition-all active:scale-95",
              briefing?.overall_risk_level === 'high' ? "bg-rose-600 hover:bg-rose-500 shadow-rose-900/20" : "bg-primary hover:bg-primary shadow-primary/20"
            )}
          >
            {confirmLoading ? (
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            ) : (
              <>
                <Play className="w-4 h-4 mr-2" />
                Confirm & Deploy
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

const Loader2 = ({ className }: { className?: string }) => (
  <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
    <path d="M21 12a9 9 0 1 1-6.219-8.56" />
  </svg>
);

export default PlanBriefingModal;
