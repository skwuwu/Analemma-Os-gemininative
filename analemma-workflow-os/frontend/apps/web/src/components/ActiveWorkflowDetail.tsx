import React, { useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Textarea } from '@/components/ui/textarea';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Activity, X, History, RotateCcw, Zap } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { TimelineItem } from '@/components/TimelineItem';
import { CheckpointTimeline } from '@/components/CheckpointTimeline';
import { RollbackDialog } from '@/components/RollbackDialog';
import { StateDiffViewer } from '@/components/StateDiffViewer';
import { useCheckpoints, useTimeMachine } from '@/hooks/useBriefingAndCheckpoints';
import { NotificationItem, TimelineItem as CheckpointTimelineItem, RollbackRequest } from '@/lib/types';
import { toast } from 'sonner';
import { ConfidenceGauge } from '@/components/ui/confidence-gauge';
import NumberTicker from '@/components/ui/number-ticker';

interface ResumeWorkflowRequest {
    execution_id?: string;
    conversation_id: string;
    user_input: Record<string, unknown>;
}

interface ActiveWorkflowDetailProps {
    selectedExecutionId: string;
    latestStatus: NotificationItem | null;
    selectedWorkflowTimeline: NotificationItem[];
    stopExecution: (executionId: string) => void;
    resumeWorkflow: (payload: ResumeWorkflowRequest) => any;
    sanitizeResumePayload: (workflow: NotificationItem, response: string) => ResumeWorkflowRequest;
    responseText: string;
    setResponseText: (s: string) => void;
    onClose: () => void;
}

export const ActiveWorkflowDetail: React.FC<ActiveWorkflowDetailProps> = ({
    selectedExecutionId,
    latestStatus,
    selectedWorkflowTimeline,
    stopExecution,
    resumeWorkflow,
    sanitizeResumePayload,
    responseText,
    setResponseText,
    onClose,
}) => {
    const rawProgress = Math.round((latestStatus?.payload?.current_segment || 0) / (latestStatus?.payload?.total_segments || 1) * 100);
    // Ensure progress is valid number
    const progress = isNaN(rawProgress) ? 0 : rawProgress;

    // Check for confidence score in payload (business metrics)
    // Assuming structure: payload.metrics.confidence_score or payload.confidence_score
    const confidenceScore = latestStatus?.payload?.confidence_score ?? latestStatus?.payload?.business_metrics?.confidence_score ?? 0;
    const autonomyRate = latestStatus?.payload?.autonomy_rate ?? latestStatus?.payload?.business_metrics?.autonomy_rate ?? 0;

    const status = latestStatus?.payload?.status || 'UNKNOWN';
    const isRunning = ['RUNNING', 'STARTED', 'PAUSED_FOR_HITP'].includes(status);

    // 체크포인트 관련 상태
    const [activeTab, setActiveTab] = useState<string>('timeline');
    const [rollbackDialogOpen, setRollbackDialogOpen] = useState(false);
    const [rollbackTarget, setRollbackTarget] = useState<CheckpointTimelineItem | null>(null);

    // 체크포인트 훅
    const checkpoints = useCheckpoints({
        executionId: selectedExecutionId,
        enabled: !!selectedExecutionId,
        refetchInterval: isRunning ? 5000 : false,
    });

    const timeMachine = useTimeMachine({
        executionId: selectedExecutionId,
        onRollbackSuccess: (result) => {
            toast.success(`롤백 성공: 브랜치 ${result.branched_thread_id} 생성됨`);
            setRollbackDialogOpen(false);
            checkpoints.refetch();
        },
        onRollbackError: (error) => {
            toast.error(`롤백 실패: ${error.message}`);
        },
    });

    // 롤백 핸들러
    const handleRollbackClick = useCallback((item: CheckpointTimelineItem) => {
        setRollbackTarget(item);
        setRollbackDialogOpen(true);
    }, []);

    const handleRollbackPreview = useCallback(async (checkpointId: string) => {
        await timeMachine.loadPreview(checkpointId);
    }, [timeMachine]);

    const handleRollbackExecute = useCallback(async (request: Omit<RollbackRequest, 'preview_only'>) => {
        return await timeMachine.executeRollback(request);
    }, [timeMachine]);

    return (
        <>
            <div className="p-6 border-b bg-card/50">
                <div className="flex justify-between items-start mb-4">
                    <div>
                        <h2 className="text-2xl font-semibold">
                            {latestStatus?.payload?.workflowId || 'Workflow Detail'}
                        </h2>
                        <p className="text-sm text-muted-foreground mt-1">
                            Execution ID: {selectedExecutionId.split(':').pop()}
                        </p>
                        <div className="flex gap-4 mt-2 text-sm text-muted-foreground">
                            <div>
                                <span className="font-medium">Status:</span> {status}
                            </div>
                            {latestStatus?.payload?.start_time && (
                                <div>
                                    <span className="font-medium">Started:</span>{' '}
                                    {new Date(latestStatus.payload.start_time * 1000).toLocaleTimeString()}
                                </div>
                            )}
                            {latestStatus?.payload?.estimated_remaining_seconds && (
                                <div>
                                    <span className="font-medium">Est. Remaining:</span>{' '}
                                    {Math.round(latestStatus.payload.estimated_remaining_seconds)}s
                                </div>
                            )}
                        </div>
                    </div>
                    <div className="flex gap-2">
                        {isRunning && (
                            <Button variant="destructive" size="sm" onClick={() => stopExecution(selectedExecutionId)}>
                                Stop Execution
                            </Button>
                        )}
                        <Button variant="ghost" size="sm" onClick={onClose}>
                            <X className="w-4 h-4" />
                        </Button>
                    </div>
                </div>
                <div className="space-y-4">
                    {/* Main Progress */}
                    <div className="space-y-1">
                        <div className="flex justify-between text-sm">
                            <span className="font-medium">Overall Progress</span>
                            <div className="flex items-center gap-0.5 font-mono">
                                <NumberTicker value={progress} />
                                <span>%</span>
                            </div>
                        </div>
                        <Progress value={progress} className="h-2" />
                    </div>

                    {/* Business Metrics Grid (displayed if relevant) */}
                    {(confidenceScore > 0 || autonomyRate > 0) && (
                        <div className="grid grid-cols-2 gap-4 pt-2">
                            <ConfidenceGauge value={confidenceScore} label="Confidence Score" />
                            <ConfidenceGauge value={autonomyRate * 100} label="Autonomy Rate" />
                        </div>
                    )}
                </div>
            </div>

            <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col">
                <div className="px-6 pt-4 border-b bg-card/30">
                    <TabsList className="grid w-full max-w-md grid-cols-4">
                        <TabsTrigger value="timeline" className="gap-2">
                            <Activity className="w-4 h-4" />
                            타임라인
                        </TabsTrigger>
                        <TabsTrigger value="checkpoints" className="gap-2">
                            <History className="w-4 h-4" />
                            체크포인트
                            {checkpoints.timeline.length > 0 && (
                                <span className="ml-1 text-xs bg-muted px-1.5 py-0.5 rounded-full">
                                    {checkpoints.timeline.length}
                                </span>
                            )}
                        </TabsTrigger>
                        <TabsTrigger value="suggestions" className="gap-2">
                            <Zap className="w-4 h-4 text-yellow-500" />
                            AI 제안
                            {timeMachine.suggestions.length > 0 && (
                                <span className="ml-1 text-xs bg-yellow-100 text-yellow-700 px-1.5 py-0.5 rounded-full">
                                    {timeMachine.suggestions.length}
                                </span>
                            )}
                        </TabsTrigger>
                        <TabsTrigger value="compare" className="gap-2">
                            <RotateCcw className="w-4 h-4" />
                            상태 비교
                        </TabsTrigger>
                    </TabsList>
                </div>

                <TabsContent value="timeline" className="flex-1 mt-0">
                    <ScrollArea className="h-full p-6 bg-muted/10">
                        <div className="max-w-3xl mx-auto bg-card rounded-xl border p-6 shadow-sm">
                            <h3 className="font-semibold mb-6 flex items-center gap-2">
                                <Activity className="w-4 h-4" /> Execution Timeline
                            </h3>
                            <div className="ml-2">
                                {selectedWorkflowTimeline.map((event, idx) => (
                                    <TimelineItem
                                        key={event.id || idx}
                                        event={event}
                                        isLast={idx === selectedWorkflowTimeline.length - 1}
                                    />
                                ))}
                            </div>

                            {status === 'PAUSED_FOR_HITP' && (
                                <div className="mt-6 p-4 border rounded-lg bg-orange-50/50 border-orange-100 animate-in fade-in slide-in-from-bottom-4">
                                    <h4 className="font-medium text-orange-800 mb-2">User Input Required</h4>
                                    <Textarea
                                        value={responseText}
                                        onChange={(e) => setResponseText(e.target.value)}
                                        placeholder="Enter value to resume..."
                                        className="mb-3 bg-white"
                                    />
                                    <Button
                                        onClick={() => {
                                            try {
                                                if (!latestStatus) return;
                                                const payload = sanitizeResumePayload({ ...latestStatus, id: selectedExecutionId } as any, responseText);
                                                resumeWorkflow(payload);
                                                setResponseText('');
                                            } catch (e) {
                                                console.error('Resume failed', e);
                                            }
                                        }}
                                        disabled={!responseText.trim()}
                                    >
                                        Resume Workflow
                                    </Button>
                                </div>
                            )}
                        </div>
                    </ScrollArea>
                </TabsContent>

                <TabsContent value="checkpoints" className="flex-1 mt-0">
                    <ScrollArea className="h-full p-6 bg-muted/10">
                        <div className="max-w-3xl mx-auto">
                            <CheckpointTimeline
                                items={checkpoints.timeline}
                                loading={checkpoints.isLoading}
                                selectedId={timeMachine.selectedCheckpointId}
                                compareId={timeMachine.compareCheckpointId}
                                onSelect={(item) => {
                                    // 선택 시 상세 정보 표시
                                    checkpoints.getDetail(item.checkpoint_id);
                                }}
                                onRollback={handleRollbackClick}
                                onCompare={(item) => {
                                    if (timeMachine.selectedCheckpointId &&
                                        timeMachine.selectedCheckpointId !== item.checkpoint_id) {
                                        timeMachine.compare(timeMachine.selectedCheckpointId, item.checkpoint_id);
                                        setActiveTab('compare');
                                    }
                                }}
                                onPreview={(item) => {
                                    checkpoints.getDetail(item.checkpoint_id);
                                }}
                            />
                        </div>
                    </ScrollArea>
                </TabsContent>

                <TabsContent value="compare" className="flex-1 mt-0">
                    <ScrollArea className="h-full p-6 bg-muted/10">
                        <div className="max-w-3xl mx-auto">
                            {timeMachine.compareResult ? (
                                <StateDiffViewer
                                    diff={null}
                                    compareResult={timeMachine.compareResult}
                                    loading={timeMachine.isCompareLoading}
                                />
                            ) : (
                                <div className="text-center py-12 text-muted-foreground">
                                    <RotateCcw className="w-12 h-12 mx-auto mb-4 opacity-30" />
                                    <p className="text-lg font-medium">상태 비교</p>
                                    <p className="text-sm mt-2">
                                        체크포인트 탭에서 두 개의 체크포인트를 선택하여 비교하세요.
                                    </p>
                                    <p className="text-xs mt-4 text-muted-foreground/70">
                                        첫 번째 체크포인트를 클릭하고, 두 번째 체크포인트의 🔄 버튼을 클릭하세요.
                                    </p>
                                </div>
                            )}
                        </div>
                    </ScrollArea>
                </TabsContent>

                <TabsContent value="suggestions" className="flex-1 mt-0">
                    <ScrollArea className="h-full p-6 bg-muted/10">
                        <div className="max-w-3xl mx-auto space-y-4">
                            {timeMachine.suggestions.length > 0 ? (
                                timeMachine.suggestions.map((suggestion, idx) => (
                                    <Card key={idx} className="border-l-4 border-l-yellow-500 transition-all hover:shadow-md">
                                        <CardHeader className="pb-2">
                                            <div className="flex justify-between items-start">
                                                <CardTitle className="text-sm font-bold flex items-center gap-2">
                                                    <RotateCcw className="w-4 h-4 text-primary" />
                                                    {suggestion.node_name || suggestion.node_id} (Step {suggestion.step})
                                                </CardTitle>
                                                <Badge variant="outline" className="bg-yellow-50 text-yellow-700 border-yellow-200">
                                                    Priority {suggestion.priority}
                                                </Badge>
                                            </div>
                                        </CardHeader>
                                        <CardContent>
                                            <p className="text-sm text-foreground mb-4 leading-relaxed">{suggestion.reason}</p>
                                            <div className="flex gap-2">
                                                <Button
                                                    size="sm"
                                                    variant="secondary"
                                                    className="bg-yellow-500 hover:bg-yellow-600 text-white border-none"
                                                    onClick={() => {
                                                        const item = checkpoints.timeline.find(t => t.checkpoint_id === suggestion.checkpoint_id);
                                                        if (item) handleRollbackClick(item as any);
                                                    }}
                                                >
                                                    이 시점으로 롤백
                                                </Button>
                                                <Button
                                                    size="sm"
                                                    variant="ghost"
                                                    onClick={() => {
                                                        timeMachine.selectCheckpoint(suggestion.checkpoint_id);
                                                        setActiveTab('checkpoints');
                                                    }}
                                                >
                                                    체크포인트 보기
                                                </Button>
                                            </div>
                                        </CardContent>
                                    </Card>
                                ))
                            ) : (
                                <div className="text-center py-12 text-muted-foreground bg-card rounded-xl border border-dashed">
                                    <Zap className="w-12 h-12 mx-auto mb-4 opacity-10" />
                                    <p className="text-lg font-medium">AI 제안 없음</p>
                                    <p className="text-sm mt-2">
                                        현재 상황에서 추천되는 롤백 포인트가 없습니다.
                                    </p>
                                </div>
                            )}
                        </div>
                    </ScrollArea>
                </TabsContent>
            </Tabs>

            {/* 롤백 다이얼로그 */}
            <RollbackDialog
                open={rollbackDialogOpen}
                onOpenChange={setRollbackDialogOpen}
                targetCheckpoint={rollbackTarget}
                preview={timeMachine.preview}
                loading={timeMachine.isPreviewLoading}
                onPreview={handleRollbackPreview}
                onExecute={handleRollbackExecute}
                onSuccess={() => {
                    checkpoints.refetch();
                }}
            />
        </>
    );
};

export default ActiveWorkflowDetail;
