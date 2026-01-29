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
import { Checkbox } from '@/components/ui/checkbox';
import { useState, useCallback, useMemo, useEffect } from 'react';
import { ArrowRight, ArrowLeft, Trash2, Plug, Settings, Cpu, Zap, Activity, HelpCircle, X, Navigation2, Wrench, Search, Loader2, Brain, FolderOpen } from 'lucide-react';
import { BLOCK_CATEGORIES } from './BlockLibrary';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';
import { listSkills, type Skill, type ToolDefinition } from '@/lib/skillsApi';

const AI_MODELS = BLOCK_CATEGORIES.find(cat => cat.type === 'aiModel')?.items || [];
const OPERATORS = BLOCK_CATEGORIES.find(cat => cat.type === 'operator')?.items || [];
const TRIGGERS = BLOCK_CATEGORIES.find(cat => cat.type === 'trigger')?.items || [];
const CONTROLS = BLOCK_CATEGORIES.find(cat => cat.type === 'control')?.items || [];

// --- SUB-COMPONENTS ---

/**
 * Compact Tools Selector for Property Panel
 */
const ToolsSelectorCompact = ({
  selectedTools,
  onToolsChange
}: {
  selectedTools: ToolDefinition[],
  onToolsChange: (tools: ToolDefinition[]) => void
}) => {
  const [skills, setSkills] = useState<Skill[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    const fetchSkills = async () => {
      setIsLoading(true);
      try {
        const response = await listSkills();
        setSkills(response.items || []);
      } catch (error) {
        console.error('Failed to fetch skills:', error);
      } finally {
        setIsLoading(false);
      }
    };
    fetchSkills();
  }, []);

  const isToolSelected = (skill: Skill) => {
    return selectedTools.some(t => t.skill_id === skill.skill_id);
  };

  const toggleSkill = (skill: Skill) => {
    if (isToolSelected(skill)) {
      onToolsChange(selectedTools.filter(t => t.skill_id !== skill.skill_id));
    } else {
      const toolDef: ToolDefinition = {
        name: skill.name,
        description: skill.description || '',
        skill_id: skill.skill_id,
        skill_version: skill.version,
        parameters: skill.tool_definitions?.[0]?.parameters,
        required_api_keys: skill.required_api_keys,
      };
      onToolsChange([...selectedTools, toolDef]);
    }
  };

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 flex items-center gap-1.5">
          <Wrench className="w-3 h-3" />
          Tools & Skills
        </Label>
        <Badge variant="secondary" className="text-[9px] bg-amber-100 text-amber-700 border-none">
          {selectedTools.length} selected
        </Badge>
      </div>

      {/* Selected Tools */}
      {selectedTools.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {selectedTools.map((tool, i) => (
            <Badge
              key={`${tool.skill_id}-${tool.name}-${i}`}
              variant="secondary"
              className="text-[9px] bg-amber-50 text-amber-700 border border-amber-200 pr-1"
            >
              {tool.name}
              <button
                onClick={() => onToolsChange(selectedTools.filter((_, idx) => idx !== i))}
                className="ml-1 hover:text-red-500"
              >
                ×
              </button>
            </Badge>
          ))}
        </div>
      )}

      {/* Expand/Collapse Button */}
      <Button
        variant="outline"
        size="sm"
        onClick={() => setExpanded(!expanded)}
        className="w-full h-8 text-[10px] border-dashed"
      >
        {expanded ? 'Hide Skills' : 'Add Skills'}
      </Button>

      {/* Skills List */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
          >
            <ScrollArea className="h-[150px] rounded-lg border bg-white">
              {isLoading ? (
                <div className="flex items-center justify-center h-full">
                  <Loader2 className="w-4 h-4 animate-spin text-slate-400" />
                </div>
              ) : skills.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-full text-slate-400 text-[10px] p-3">
                  <Wrench className="w-6 h-6 mb-1 opacity-50" />
                  <p>No skills available</p>
                </div>
              ) : (
                <div className="p-1.5 space-y-0.5">
                  {skills.map(skill => (
                    <div
                      key={skill.skill_id}
                      className={cn(
                        "flex items-center gap-2 p-2 rounded cursor-pointer transition-colors",
                        isToolSelected(skill) ? "bg-amber-50" : "hover:bg-slate-50"
                      )}
                      onClick={() => toggleSkill(skill)}
                    >
                      <Checkbox
                        checked={isToolSelected(skill)}
                        className="h-3.5 w-3.5"
                      />
                      {skill.skill_type === 'subgraph_based' ? (
                        <FolderOpen className="w-3 h-3 text-violet-500" />
                      ) : (
                        <Brain className="w-3 h-3 text-blue-500" />
                      )}
                      <span className="text-[10px] font-medium truncate flex-1">{skill.name}</span>
                    </div>
                  ))}
                </div>
              )}
            </ScrollArea>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};

/**
 * AI Model Node Specialized Settings
 */
const AIModelSettings = ({ data, updateField }: any) => (
  <div className="space-y-4 border rounded-xl p-4 bg-slate-50/50">
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">LLM Model Selection</Label>
      <Select value={data.model} onValueChange={(val) => updateField('model', val)}>
        <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
        <SelectContent>
          {AI_MODELS.map((m: any) => (
            <SelectItem 
              key={m.id} 
              value={m.id} 
              disabled={m.disabled}
              className={cn(m.disabled && "opacity-50 cursor-not-allowed")}
            >
              {m.label}
              {m.disabled && <span className="text-[9px] text-amber-500 ml-2">(Not Available)</span>}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {AI_MODELS.find((m: any) => m.id === data.model)?.disabled && (
        <p className="text-[10px] text-amber-600 bg-amber-50 border border-amber-200 rounded px-2 py-1">
          ⚠️ {AI_MODELS.find((m: any) => m.id === data.model)?.disabledReason}
        </p>
      )}
    </div>
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">System Instruction</Label>
      <Textarea
        value={data.prompt_content}
        onChange={(e) => updateField('prompt_content', e.target.value)}
        placeholder="Describe the agent's role and task instructions..."
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

    {/* Tools/Skills Selection */}
    <div className="pt-3 border-t">
      <ToolsSelectorCompact
        selectedTools={data.tools || []}
        onToolsChange={(tools) => updateField('tools', tools)}
      />
    </div>

    {/* Gemini Thinking Mode Toggle */}
    {(data.model?.includes('gemini') || data.model === 'gemini') && (
      <div className="pt-3 border-t space-y-3">
        <div className="flex items-center justify-between">
          <div className="space-y-0.5">
            <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">🧠 Thinking Mode</Label>
            <p className="text-[9px] text-slate-500">Enable Chain-of-Thought reasoning</p>
          </div>
          <Checkbox
            checked={data.enable_thinking || false}
            onCheckedChange={(checked) => updateField('enable_thinking', checked)}
          />
        </div>
        {data.enable_thinking && (
          <div className="space-y-2 animate-in slide-in-from-top-2">
            <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Token Budget</Label>
            <Input
              type="number"
              value={data.thinking_budget_tokens || 4096}
              onChange={(e) => updateField('thinking_budget_tokens', parseInt(e.target.value))}
              min={1024}
              max={32768}
              className="h-9 bg-white"
            />
            <p className="text-[9px] text-slate-500">Max tokens for reasoning (1024-32768)</p>
          </div>
        )}
      </div>
    )}
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

        {controlType === 'conditional' && (
          <motion.div key="conditional" initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-3 pt-1">
            <div className="p-2.5 rounded-lg bg-purple-50 border border-purple-100/50 text-[10px] text-purple-700 leading-normal flex gap-2">
              <Activity className="w-4 h-4 shrink-0 text-purple-400" />
              <span>Python 표현식을 평가하여 조건에 따라 다음 노드를 결정합니다 (if-elif-else 로직).</span>
            </div>
            <div className="space-y-2">
              <Label className="text-[10px] font-bold text-slate-400">Conditions (순서대로 평가)</Label>
              <div className="space-y-1.5 text-[9px] text-slate-500 italic">
                예시: state.score &gt;= 90, state.status == 'approved'
              </div>
            </div>
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400">Default Target Node</Label>
              <Input 
                value={data.default_node || ''} 
                onChange={(e) => updateField('default_node', e.target.value)} 
                placeholder="manual_review" 
                className="h-8 text-xs bg-white font-mono" 
              />
              <p className="text-[9px] text-slate-400 italic">모든 조건이 거짓일 때 이동할 노드 ID</p>
            </div>
            <div className="space-y-1.5">
              <Label className="text-[10px] font-bold text-slate-400">Evaluation Mode</Label>
              <Select value={data.evaluation_mode || 'first_match'} onValueChange={(val) => updateField('evaluation_mode', val)}>
                <SelectTrigger className="h-9 bg-white"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="first_match">First Match (첫 번째 참인 조건)</SelectItem>
                  <SelectItem value="all_match">All Match (모든 조건 평가)</SelectItem>
                </SelectContent>
              </Select>
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
 * Control Block Settings (for control_block node type)
 */
const ControlBlockSettings = ({ data, updateField }: any) => {
  return (
    <div className="space-y-4 border rounded-xl p-4 bg-slate-50/50">
      {/* Block Type Selection */}
      <div className="space-y-2">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Block Type</Label>
        <Select 
          value={data.blockType || 'parallel'} 
          onValueChange={(val) => updateField('blockType', val)}
        >
          <SelectTrigger className="h-9 bg-white">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="conditional">Conditional Branch</SelectItem>
            <SelectItem value="parallel">Parallel Execution</SelectItem>
            <SelectItem value="for_each">For Each Loop</SelectItem>
            <SelectItem value="while">While Loop</SelectItem>
          </SelectContent>
        </Select>
        <p className="text-[9px] text-slate-500 mt-1">
          {data.blockType === 'conditional' && '조건에 따라 분기를 선택합니다'}
          {data.blockType === 'parallel' && '모든 분기를 동시에 실행합니다'}
          {data.blockType === 'for_each' && '배열의 각 항목에 대해 반복 실행합니다'}
          {data.blockType === 'while' && '조건이 참인 동안 반복 실행합니다'}
        </p>
      </div>

      {/* While specific settings */}
      {data.blockType === 'while' && (
        <div className="space-y-3 pt-1">
          <div className="space-y-1.5">
            <Label className="text-[10px] font-bold text-slate-400">Loop Condition (Natural Language)</Label>
            <Textarea
              value={data.natural_condition || ''}
              onChange={(e) => updateField('natural_condition', e.target.value)}
              placeholder="e.g., content is not detailed enough"
              className="min-h-[60px] text-xs bg-white resize-none"
            />
          </div>
          <div className="space-y-1.5">
            <Label className="text-[10px] font-bold text-slate-400">Max Iterations</Label>
            <Input
              type="number"
              value={data.maxIterations || 10}
              onChange={(e) => updateField('maxIterations', parseInt(e.target.value))}
              min={1}
              max={100}
              className="h-8 text-xs bg-white"
            />
          </div>
        </div>
      )}

      {/* Branches info (read-only for now) */}
      {data.blockType !== 'while' && data.branches && data.branches.length > 0 && (
        <div className="space-y-2 pt-2 border-t">
          <Label className="text-[10px] font-bold text-slate-400">Branches ({data.branches.length})</Label>
          <div className="space-y-1">
            {data.branches.map((branch: any, idx: number) => (
              <div key={branch.id} className="text-[10px] text-slate-600 bg-white p-2 rounded border">
                {idx + 1}. {branch.label}
                {branch.natural_condition && <span className="text-slate-400 ml-2">({branch.natural_condition})</span>}
              </div>
            ))}
          </div>
          <p className="text-[9px] text-slate-500 italic">
            Add/remove branches by connecting/disconnecting edges to the Control Block
          </p>
        </div>
      )}
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
    tools: node?.data.tools || [],  // AI Model tools
    enable_thinking: node?.data.enable_thinking || false,
    thinking_budget_tokens: node?.data.thinking_budget_tokens || 4096,
    operatorType: node?.data.operatorType || 'email',
    operatorVariant: node?.data.operatorVariant || 'official',
    triggerType: node?.data.triggerType || (node?.data.blockId as string) || 'request',
    triggerHour: node?.data.triggerHour ?? 9,
    triggerMinute: node?.data.triggerMinute ?? 0,
    controlType: node?.data.controlType || 'while',
    whileCondition: node?.data.whileCondition || '',
    maxIterations: node?.data.max_iterations || node?.data.maxIterations || 10,
    items_path: node?.data.items_path || 'state.items',
    // Control Block specific
    blockType: node?.data.blockType || 'parallel',
    branches: node?.data.branches || [],
    natural_condition: node?.data.natural_condition || '',
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
        updates.tools = formData.tools || [];
        updates.enable_thinking = formData.enable_thinking || false;
        updates.thinking_budget_tokens = Number(formData.thinking_budget_tokens || 4096);
        
        // Map model ID to provider
        if (formData.model.includes('gemini') || formData.model === 'gemini') {
          updates.provider = 'google';
          updates.modelName = 'gemini-2.0-flash-exp';
        } else if (formData.model.includes('claude') || formData.model === 'claude') {
          updates.provider = 'anthropic';
          updates.modelName = 'claude-3-5-sonnet-20241022';
        } else if (formData.model.includes('gpt') || formData.model === 'gpt4') {
          updates.provider = 'openai';
          updates.modelName = 'gpt-4';
        } else {
          // Default fallback
          updates.provider = 'openai';
          updates.modelName = formData.model;
        }
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
          // Generate configValue for display (e.g., "09:00")
          const hour = String(formData.triggerHour || 0).padStart(2, '0');
          const minute = String(formData.triggerMinute || 0).padStart(2, '0');
          updates.configValue = `${hour}:${minute}`;
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
        } else if (formData.controlType === 'conditional') {
          updates.conditions = formData.conditions || [];
          updates.default_node = formData.default_node;
          updates.defaultNode = formData.default_node;
          updates.evaluation_mode = formData.evaluation_mode || 'first_match';
        } else if (formData.controlType === 'human') {
          updates.approval_message = formData.approval_message;
        }
        break;
      case 'control_block':
        updates.blockType = formData.blockType;
        updates.branches = formData.branches;
        if (formData.blockType === 'while') {
          updates.natural_condition = formData.natural_condition;
          updates.max_iterations = Number(formData.maxIterations);
        }
        break;
    }
    onUpdate(updates);
    toast.success("Configuration preserved successfully.");
  };

  return (
    <div className="p-6 pb-8 space-y-9">
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
          {nodeType === 'control_block' && <ControlBlockSettings data={formData} updateField={updateField} />}
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
      <div className="pt-6 border-t flex items-center justify-between mb-4">
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
