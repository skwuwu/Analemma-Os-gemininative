import { useWorkflowStore } from '@/lib/workflowStore';
import { useShallow } from 'zustand/react/shallow';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Slider } from '@/components/ui/slider';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useState, useCallback, useMemo } from 'react';
import { ArrowRight, ArrowLeft, Trash2, Plug, Settings, Cpu, Zap, Activity, HelpCircle, X, Navigation2 } from 'lucide-react';
import { BLOCK_CATEGORIES } from './BlockLibrary';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

const AI_MODELS = BLOCK_CATEGORIES.find(cat => cat.type === 'aiModel')?.items || [];
const OPERATORS = BLOCK_CATEGORIES.find(cat => cat.type === 'operator')?.items || [];
const TRIGGERS = BLOCK_CATEGORIES.find(cat => cat.type === 'trigger')?.items || [];
const CONTROLS = BLOCK_CATEGORIES.find(cat => cat.type === 'control')?.items || [];

// --- SUB-COMPONENTS ---

/**
 * AI 모델 노드 특화 설정
 */
const AIModelSettings = ({ data, updateField }: any) => (
  <div className="space-y-4 border rounded-xl p-4 bg-slate-50/50">
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">LLM Model Selection</Label>
      <Select value={data.model} onValueChange={(val) => updateField('model', val)}>
        <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
        <SelectContent>
          {AI_MODELS.map((m: any) => <SelectItem key={m.id} value={m.id}>{m.label}</SelectItem>)}
        </SelectContent>
      </Select>
    </div>
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">System Instruction</Label>
      <Textarea
        value={data.prompt_content}
        onChange={(e) => updateField('prompt_content', e.target.value)}
        placeholder="이 에이전트의 역할과 작업 지침을 기재하세요..."
        className="min-h-[120px] font-mono text-xs bg-white border-slate-200 leading-relaxed shadow-inner"
      />
    </div>
    <div className="space-y-3">
      <div className="flex justify-between items-center">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Sampling Temp</Label>
        <span className="text-[10px] font-mono font-bold text-primary bg-primary/10 px-1.5 py-0.5 rounded">{data.temperature.toFixed(1)}</span>
      </div>
      <Slider
        value={[data.temperature]}
        onValueChange={(val) => updateField('temperature', val[0])}
        min={0} max={2} step={0.1}
      />
    </div>
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Token Quota</Label>
      <Input
        type="number"
        value={data.max_tokens}
        onChange={(e) => updateField('max_tokens', e.target.value)}
        className="h-9 bg-white"
      />
    </div>
  </div>
);

/**
 * 트리거 노드 특화 설정
 */
const TriggerSettings = ({ data, updateField }: any) => (
  <div className="space-y-4 border rounded-xl p-4 bg-slate-50/50">
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Activation Event</Label>
      <Select value={data.triggerType} onValueChange={(val) => updateField('triggerType', val)}>
        <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
        <SelectContent>
          {TRIGGERS.map((t: any) => <SelectItem key={t.id} value={t.id}>{t.label}</SelectItem>)}
        </SelectContent>
      </Select>
    </div>
    {data.triggerType === 'time' && (
      <div className="grid grid-cols-2 gap-3 pt-1">
        <div className="space-y-1.5">
          <Label className="text-[10px] font-bold text-slate-400">Hour (0-23)</Label>
          <Input type="number" min={0} max={23} value={data.triggerHour} onChange={(e) => updateField('triggerHour', e.target.value)} className="h-8 bg-white" />
        </div>
        <div className="space-y-1.5">
          <Label className="text-[10px] font-bold text-slate-400">Minute (0-59)</Label>
          <Input type="number" min={0} max={59} value={data.triggerMinute} onChange={(e) => updateField('triggerMinute', e.target.value)} className="h-8 bg-white" />
        </div>
      </div>
    )}
  </div>
);

/**
 * 제어 로직 노드 특화 설정
 */
const ControlSettings = ({ data, updateField }: any) => {
  const { controlType } = data;
  return (
    <div className="space-y-4 border rounded-xl p-4 bg-slate-50/50">
      <div className="space-y-2">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Logic Mechanism</Label>
        <Select value={controlType} onValueChange={(val) => updateField('controlType', val)}>
          <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
          <SelectContent>
            {CONTROLS.map((c: any) => <SelectItem key={c.id} value={c.id}>{c.label}</SelectItem>)}
          </SelectContent>
        </Select>
      </div>

      <AnimatePresence mode="wait">
        {controlType === 'for_each' && (
          <motion.div key="for_each" initial={{ opacity: 0, height: 0 }} animate={{ opacity: 1, height: 'auto' }} exit={{ opacity: 0, height: 0 }} className="space-y-4 pt-1">
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400 flex items-center gap-1.5">
                Items Collection Path <HelpCircle className="w-3 h-3 cursor-help text-slate-300" />
              </Label>
              <Input value={data.items_path} onChange={(e) => updateField('items_path', e.target.value)} placeholder="e.g. state.data_lines" className="h-8 text-xs font-mono bg-white" />
              <p className="text-[9px] text-slate-400 italic">Example: $.data.results or state.users</p>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1.5">
                <Label className="text-[10px] font-bold text-slate-400">Iterator Key</Label>
                <Input value={data.item_key} onChange={(e) => updateField('item_key', e.target.value)} placeholder="item" className="h-8 text-xs bg-white" />
              </div>
              <div className="space-y-1.5">
                <Label className="text-[10px] font-bold text-slate-400">Output Target</Label>
                <Input value={data.output_key} onChange={(e) => updateField('output_key', e.target.value)} placeholder="processed_items" className="h-8 text-xs bg-white" />
              </div>
            </div>
          </motion.div>
        )}

        {controlType === 'human' && (
          <motion.div key="human" initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-3 pt-1">
            <div className="p-2.5 rounded-lg bg-amber-50 border border-amber-100/50 text-[10px] text-amber-700 leading-normal flex gap-2">
              <Activity className="w-4 h-4 shrink-0 text-amber-400" />
              <span>이 노드에 도달하면 워크플로우가 대기 상태로 전환되며, 운영자의 수동 승인(Resume)이 있을 때까지 멈춥니다.</span>
            </div>
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400 text-amber-900/40">Operation Message</Label>
              <Textarea value={data.approval_message} onChange={(e) => updateField('approval_message', e.target.value)} placeholder="검토가 필요한 항목이 있습니다. 승인하시겠습니까?" className="min-h-[70px] text-xs bg-white border-amber-100" />
            </div>
          </motion.div>
        )}

        {controlType === 'aggregator' && (
          <motion.div key="aggregator" initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-3 pt-1">
            <div className="p-2.5 rounded-lg bg-blue-50 border border-blue-100/50 text-[10px] text-blue-700 leading-normal flex gap-2">
              <Activity className="w-4 h-4 shrink-0 text-blue-400" />
              <span>병렬 브랜치나 반복 작업의 결과를 병합하고 토큰 사용량을 집계합니다.</span>
            </div>
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400">Aggregation Strategy</Label>
              <Select value={data.strategy || 'auto'} onValueChange={(val) => updateField('strategy', val)}>
                <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="auto">Auto (자동 감지)</SelectItem>
                  <SelectItem value="merge">Merge (병합)</SelectItem>
                  <SelectItem value="concat">Concat (연결)</SelectItem>
                  <SelectItem value="sum">Sum (합산)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400">Output Key</Label>
              <Input value={data.output_key || 'aggregated_result'} onChange={(e) => updateField('output_key', e.target.value)} placeholder="aggregated_result" className="h-8 text-xs bg-white" />
            </div>
          </motion.div>
        )}

        {controlType === 'while' && (
          <motion.div key="while" initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-3 pt-1">
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400">Loop Logic Condition</Label>
              <Input value={data.whileCondition} onChange={(e) => updateField('whileCondition', e.target.value)} placeholder="e.g. data.status != 'complete'" className="h-8 text-xs font-mono bg-white" />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};

/**
 * 연결 내비게이션 및 생성 관리
 */
const ConnectionManager = ({
  nodeId,
  incoming,
  outgoing,
  available,
  onJump,
  onDelete,
  onCreate
}: any) => {
  const [selectedTarget, setSelectedTarget] = useState('');

  return (
    <div className="space-y-5 pt-2">
      <div className="flex items-center gap-2 pb-2 border-b">
        <Plug className="w-4 h-4 text-slate-400" />
        <h4 className="font-bold text-xs uppercase tracking-widest text-slate-500">Infrastructure Links</h4>
      </div>

      <div className="grid gap-4">
        <div className="space-y-2">
          <Label className="text-[10px] font-bold uppercase tracking-widest text-slate-400 flex items-center gap-1">
            <ArrowLeft className="w-3 h-3" /> Source Nodes (Incoming)
          </Label>
          <div className="flex flex-wrap gap-1.5">
            {incoming.map((conn: any) => (
              <Badge key={conn.id} variant="secondary" className="group h-7 px-2.5 bg-slate-100 border-none hover:bg-primary/5 hover:text-primary transition-all cursor-default">
                <span className="flex items-center gap-2">
                  <span className="cursor-pointer font-bold py-1" onClick={() => onJump(conn.sourceId)}>
                    {conn.sourceLabel}
                  </span>
                  <X className="w-3 h-3 hover:text-destructive cursor-pointer opacity-0 group-hover:opacity-100 transition-opacity" onClick={() => onDelete(conn.id)} />
                </span>
              </Badge>
            ))}
            {incoming.length === 0 && <span className="text-[10px] text-slate-300 italic pl-1">Isolated entry</span>}
          </div>
        </div>

        <div className="space-y-2">
          <Label className="text-[10px] font-bold uppercase tracking-widest text-slate-400 flex items-center gap-1">
            <ArrowRight className="w-3 h-3" /> Cascade Nodes (Outgoing)
          </Label>
          <div className="flex flex-wrap gap-1.5">
            {outgoing.map((conn: any) => (
              <Badge key={conn.id} variant="secondary" className="group h-7 px-2.5 bg-slate-100 border-none hover:bg-primary/5 hover:text-primary transition-all cursor-default">
                <span className="flex items-center gap-2">
                  <span className="cursor-pointer font-bold py-1" onClick={() => onJump(conn.targetId)}>
                    {conn.targetLabel}
                  </span>
                  <X className="w-3 h-3 hover:text-destructive cursor-pointer opacity-0 group-hover:opacity-100 transition-opacity" onClick={() => onDelete(conn.id)} />
                </span>
              </Badge>
            ))}
            {outgoing.length === 0 && <span className="text-[10px] text-slate-300 italic pl-1">Process terminal</span>}
          </div>
        </div>

        <div className="flex gap-2 pt-1">
          <Select value={selectedTarget} onValueChange={setSelectedTarget}>
            <SelectTrigger className="h-9 bg-slate-50 border-dashed text-xs">
              <SelectValue placeholder="Propagate to..." />
            </SelectTrigger>
            <SelectContent>
              {available.map((t: any) => <SelectItem key={t.id} value={t.id}>{t.label}</SelectItem>)}
            </SelectContent>
          </Select>
          <Button
            size="sm"
            className="h-9 font-bold px-4 active:scale-95 transition-transform"
            disabled={!selectedTarget}
            onClick={() => { onCreate(nodeId, selectedTarget); setSelectedTarget(''); }}
          >
            Link
          </Button>
        </div>
      </div>
    </div>
  );
};

// --- MAIN PANEL ---

export const NodePropertyPanel = () => {
  const { selectedNodeId, setSelectedNodeId, nodes, edges, updateNode, removeNode, addEdge, removeEdge } = useWorkflowStore(
    useShallow((state) => ({
      selectedNodeId: state.selectedNodeId,
      setSelectedNodeId: state.setSelectedNodeId,
      nodes: state.nodes,
      edges: state.edges,
      updateNode: state.updateNode,
      removeNode: state.removeNode,
      addEdge: state.addEdge,
      removeEdge: state.removeEdge,
    }))
  );

  const selectedNode = useMemo(() => nodes.find((n) => n.id === selectedNodeId), [nodes, selectedNodeId]);

  if (!selectedNode) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-muted-foreground p-8 text-center bg-slate-50/20">
        <div className="w-16 h-16 rounded-3xl bg-white shadow-sm flex items-center justify-center mb-6 opacity-40">
          <Settings className="w-8 h-8 text-slate-300" />
        </div>
        <h3 className="font-bold text-slate-400 text-sm mb-1">No Node Selected</h3>
        <p className="text-[11px] text-slate-300 leading-relaxed">
          Select a node from the canvas<br />to edit its properties and connections.
        </p>
      </div>
    );
  }

  // Calculate connections with correct object structure
  const incomingConnections = edges
    .filter((e) => e.target === selectedNode.id)
    .map((e) => {
      const sourceNode = nodes.find((n) => n.id === e.source);
      return { id: e.id, sourceId: e.source, sourceLabel: sourceNode?.data.label || e.source };
    });

  const outgoingConnections = edges
    .filter((e) => e.source === selectedNode.id)
    .map((e) => {
      const targetNode = nodes.find((n) => n.id === e.target);
      return { id: e.id, targetId: e.target, targetLabel: targetNode?.data.label || e.target };
    });

  const availableTargets = nodes
    .filter((n) => n.id !== selectedNode.id && !outgoingConnections.some(c => c.targetId === n.id))
    .map((n) => ({ id: n.id, label: n.data.label || n.id }));

  return (
    <div className="h-full flex flex-col bg-white border-l shadow-2xl shadow-slate-200/50">
      <div className="p-5 border-b flex items-center justify-between bg-slate-50/50">
        <div className="flex items-center gap-3">
          <div className="p-1.5 bg-primary/10 rounded-lg">
            <Settings className="w-4 h-4 text-primary" />
          </div>
          <h2 className="font-bold text-sm tracking-tight">Runtime Config</h2>
        </div>
        <Badge variant="outline" className="h-5 px-2 text-[9px] font-bold uppercase tracking-widest border-slate-200 text-slate-500">
          {selectedNode.type}
        </Badge>
      </div>
      <ScrollArea className="flex-1 custom-scrollbar">
        <NodeForm
          key={selectedNode.id}
          node={selectedNode}
          onUpdate={(updates: any) => updateNode(selectedNode.id, updates)}
          onDelete={() => { removeNode(selectedNode.id); setSelectedNodeId(null); }}
          incomingConnections={incomingConnections}
          outgoingConnections={outgoingConnections}
          availableTargets={availableTargets}
          onEdgeDelete={removeEdge}
          onEdgeCreate={(source: string, target: string) => addEdge({ id: `e${source}-${target}-${Date.now()}`, source, target, type: 'smart' })}
          onJump={setSelectedNodeId}
        />
      </ScrollArea>
    </div>
  );
}

// --- NODE FORM INTERNAL ---

const NodeForm = ({
  node,
  onUpdate,
  onDelete,
  incomingConnections,
  outgoingConnections,
  availableTargets,
  onEdgeDelete,
  onEdgeCreate,
  onJump
}: any) => {
  const nodeType = node?.type || '';
  const [formData, setFormData] = useState(() => ({
    label: node?.data.label || '',
    prompt_content: node?.data.prompt_content || node?.data.prompt || '',
    temperature: node?.data.temperature ?? 0.7,
    model: node?.data.model || 'gpt-4',
    max_tokens: node?.data.max_tokens || node?.data.maxTokens || 2000,
    operatorType: node?.data.operatorType || 'email',
    operatorVariant: node?.data.operatorVariant || 'official',
    triggerType: node?.data.triggerType || (node?.data.blockId as string) || 'request',
    triggerHour: node?.data.triggerHour ?? 9,
    triggerMinute: node?.data.triggerMinute ?? 0,
    controlType: node?.data.controlType || 'while',
    whileCondition: node?.data.whileCondition || '',
    maxIterations: node?.data.max_iterations || node?.data.maxIterations || 10,
    items_path: node?.data.items_path || 'state.items',
    item_key: node?.data.item_key || 'item',
    output_key: node?.data.output_key || 'results',
    strategy: node?.data.strategy || 'auto',
    approval_message: node?.data.approval_message || '',
  }));

  const updateField = useCallback((key: string, value: any) => {
    setFormData(prev => ({ ...prev, [key]: value }));
  }, []);

  const handleSave = () => {
    const updates: any = { label: formData.label };
    switch (nodeType) {
      case 'aiModel':
        updates.prompt_content = formData.prompt_content;
        updates.temperature = formData.temperature;
        updates.model = formData.model;
        updates.max_tokens = Number(formData.max_tokens);
        break;
      case 'operator':
        updates.operatorType = formData.operatorType;
        updates.operatorVariant = formData.operatorVariant;
        break;
      case 'trigger':
        updates.triggerType = formData.triggerType;
        if (formData.triggerType === 'time') {
          updates.triggerHour = Number(formData.triggerHour);
          updates.triggerMinute = Number(formData.triggerMinute);
        }
        break;
      case 'control':
        updates.controlType = formData.controlType;
        if (formData.controlType === 'while') {
          updates.whileCondition = formData.whileCondition;
          updates.maxIterations = Number(formData.maxIterations);
        } else if (formData.controlType === 'for_each') {
          updates.items_path = formData.items_path;
          updates.item_key = formData.item_key;
          updates.output_key = formData.output_key;
          updates.max_iterations = Number(formData.maxIterations);
        } else if (formData.controlType === 'aggregator') {
          updates.strategy = formData.strategy || 'auto';
          updates.output_key = formData.output_key || 'aggregated_result';
        } else if (formData.controlType === 'human') {
          updates.approval_message = formData.approval_message;
        }
        break;
    }
    onUpdate(updates);
    toast.success("Configuration preserved successfully.");
  };

  return (
    <div className="p-6 space-y-9">
      {/* Basic Metadata */}
      <div className="space-y-3">
        <div className="flex items-center gap-2 mb-1">
          <Navigation2 className="w-4 h-4 text-slate-400 rotate-90" />
          <h3 className="text-xs font-bold text-slate-700">Identity</h3>
        </div>
        <div className="space-y-1.5 pl-6">
          <Label htmlFor="label" className="text-[10px] font-bold text-slate-400 uppercase tracking-widest pl-1">Alias</Label>
          <Input
            id="label"
            value={formData.label}
            onChange={(e) => updateField('label', e.target.value)}
            placeholder="Block Identifier"
            className="h-10 bg-white border-slate-200 font-bold placeholder:text-slate-300"
          />
        </div>
      </div>

      {/* Dynamic Parameters */}
      <div className="space-y-4">
        <div className="flex items-center gap-2 mb-1">
          <Activity className="w-4 h-4 text-slate-400" />
          <h3 className="text-xs font-bold text-slate-700">Operational Logic</h3>
        </div>
        <div className="pl-6">
          {nodeType === 'aiModel' && <AIModelSettings data={formData} updateField={updateField} />}
          {nodeType === 'operator' && (
            <div className="space-y-4 border rounded-xl p-4 bg-slate-50/50">
              <div className="space-y-2">
                <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Action Type</Label>
                <Select value={formData.operatorType} onValueChange={(val) => updateField('operatorType', val)}>
                  <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {OPERATORS.map((op: any) => <SelectItem key={op.id} value={op.id}>{op.label}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}
          {nodeType === 'trigger' && <TriggerSettings data={formData} updateField={updateField} />}
          {nodeType === 'control' && <ControlSettings data={formData} updateField={updateField} />}
        </div>
      </div>

      {/* Connectivity */}
      <div className="pl-6">
        <ConnectionManager
          nodeId={node.id}
          incoming={incomingConnections}
          outgoing={outgoingConnections}
          available={availableTargets}
          onJump={onJump}
          onDelete={onEdgeDelete}
          onCreate={onEdgeCreate}
        />
      </div>

      {/* Persistent Actions */}
      <div className="pt-6 border-t flex items-center justify-between">
        <Button
          variant="ghost"
          size="sm"
          onClick={onDelete}
          className="text-slate-400 hover:text-red-500 hover:bg-red-50 h-9 font-bold text-xs gap-2 px-3 transition-colors"
        >
          <Trash2 className="w-3.5 h-3.5" /> Decommission
        </Button>
        <Button size="sm" onClick={handleSave} className="h-9 px-6 font-bold bg-primary shadow-lg shadow-primary/20 hover:scale-[1.02] active:scale-95 transition-all">
          Save & Deploy
        </Button>
      </div>
    </div>
  );
}
