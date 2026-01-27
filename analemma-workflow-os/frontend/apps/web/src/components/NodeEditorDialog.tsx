import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Slider } from '@/components/ui/slider';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Checkbox } from '@/components/ui/checkbox';
import { useState, useMemo, useCallback, useEffect } from 'react';
import { ArrowRight, ArrowLeft, Trash2, Plug, Settings2, Info, AlertCircle, Wrench, Search, Loader2, Brain, FolderOpen } from 'lucide-react';
import { BLOCK_CATEGORIES } from './BlockLibrary';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';
import { listSkills, type Skill, type ToolDefinition } from '@/lib/skillsApi';

// --- TYPES & CONSTANTS ---

interface NodeData {
  label: string;
  prompt_content?: string;
  temperature?: number;
  model?: string;
  max_tokens?: number;
  tools?: ToolDefinition[];  // AI Model tools
  operatorType?: string;
  strategy?: string;
  url?: string;
  method?: string;
  triggerType?: string;
  triggerHour?: number;
  triggerMinute?: number;
  controlType?: string;
  whileCondition?: string;
  maxIterations?: number;
  output_key?: string;
  [key: string]: any;
}

interface NodeEditorDialogProps {
  node: { id: string; type?: string; data: NodeData } | null;
  open: boolean;
  onClose: () => void;
  onSave: (nodeId: string, updates: Partial<NodeData>) => void;
  onDelete?: (nodeId: string) => void;
  incomingConnections?: { id: string; sourceLabel: string }[];
  outgoingConnections?: { id: string; targetLabel: string }[];
  availableTargets?: { id: string; label: string }[];
  onEdgeDelete?: (edgeId: string) => void;
  onEdgeCreate?: (source: string, target: string) => void;
}

const AI_MODELS = BLOCK_CATEGORIES.find(cat => cat.type === 'aiModel')?.items || [];
const OPERATORS = BLOCK_CATEGORIES.find(cat => cat.type === 'operator')?.items || [];
const TRIGGERS = BLOCK_CATEGORIES.find(cat => cat.type === 'trigger')?.items || [];
const CONTROLS = BLOCK_CATEGORIES.find(cat => cat.type === 'control')?.items || [];

// Safe Transform Strategies (from backend OperatorStrategy enum)
const SAFE_TRANSFORM_STRATEGIES = [
  { value: 'list_filter', label: 'List Filter', description: 'Filter list items by condition' },
  { value: 'list_map', label: 'List Map', description: 'Transform each list item' },
  { value: 'json_parse', label: 'JSON Parse', description: 'Parse JSON string to object' },
  { value: 'json_stringify', label: 'JSON Stringify', description: 'Convert object to JSON string' },
  { value: 'deep_get', label: 'Deep Get', description: 'Get nested value from object' },
  { value: 'deep_set', label: 'Deep Set', description: 'Set nested value in object' },
  { value: 'string_template', label: 'String Template', description: 'Render string template' },
  { value: 'regex_extract', label: 'Regex Extract', description: 'Extract text using regex' },
  { value: 'merge_objects', label: 'Merge Objects', description: 'Merge multiple objects' },
  { value: 'pick_fields', label: 'Pick Fields', description: 'Select specific fields from object' },
  { value: 'to_int', label: 'To Integer', description: 'Convert value to integer' },
  { value: 'to_string', label: 'To String', description: 'Convert value to string' },
  { value: 'if_else', label: 'If/Else', description: 'Conditional value selection' },
  { value: 'default_value', label: 'Default Value', description: 'Provide fallback value' },
];

// --- SUB-COMPONENTS ---

/**
 * Tools/Skills Selection Component
 */
const ToolsSelector = ({
  selectedTools,
  onToolsChange
}: {
  selectedTools: ToolDefinition[],
  onToolsChange: (tools: ToolDefinition[]) => void
}) => {
  const [skills, setSkills] = useState<Skill[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');

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

  const filteredSkills = useMemo(() => {
    if (!searchQuery) return skills;
    const query = searchQuery.toLowerCase();
    return skills.filter(s =>
      s.name.toLowerCase().includes(query) ||
      s.description?.toLowerCase().includes(query) ||
      s.category?.toLowerCase().includes(query)
    );
  }, [skills, searchQuery]);

  const isToolSelected = (skill: Skill) => {
    return selectedTools.some(t => t.skill_id === skill.skill_id);
  };

  const toggleSkill = (skill: Skill) => {
    if (isToolSelected(skill)) {
      onToolsChange(selectedTools.filter(t => t.skill_id !== skill.skill_id));
    } else {
      // Convert skill to tool definition
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

  const toggleAllToolsFromSkill = (skill: Skill) => {
    // Add all tool_definitions from this skill
    if (skill.tool_definitions && skill.tool_definitions.length > 0) {
      const existingIds = new Set(selectedTools.map(t => `${t.skill_id}-${t.name}`));
      const newTools = skill.tool_definitions
        .filter(td => !existingIds.has(`${skill.skill_id}-${td.name}`))
        .map(td => ({
          ...td,
          skill_id: skill.skill_id,
          skill_version: skill.version,
        }));
      onToolsChange([...selectedTools, ...newTools]);
    }
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1 flex items-center gap-2">
          <Wrench className="w-3.5 h-3.5" />
          Tools & Skills
        </Label>
        <Badge variant="secondary" className="text-[10px] bg-amber-500/10 text-amber-400 border-amber-500/20">
          {selectedTools.length} selected
        </Badge>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
        <Input
          placeholder="Search skills..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="pl-9 h-9 bg-slate-800 border-slate-700 text-slate-100 text-sm"
        />
      </div>

      {/* Skills List */}
      <ScrollArea className="h-[200px] rounded-lg border border-slate-700 bg-slate-800/50">
        {isLoading ? (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="w-5 h-5 animate-spin text-slate-500" />
          </div>
        ) : filteredSkills.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-slate-500 text-sm p-4">
            <Wrench className="w-8 h-8 mb-2 opacity-50" />
            <p>No skills found</p>
            <p className="text-[10px] text-slate-600">Create skills in the Skills section</p>
          </div>
        ) : (
          <div className="p-2 space-y-1">
            {filteredSkills.map(skill => (
              <div
                key={skill.skill_id}
                className={cn(
                  "flex items-start gap-3 p-2.5 rounded-lg cursor-pointer transition-all",
                  isToolSelected(skill)
                    ? "bg-amber-500/10 border border-amber-500/30"
                    : "hover:bg-slate-700/50 border border-transparent"
                )}
                onClick={() => toggleSkill(skill)}
              >
                <Checkbox
                  checked={isToolSelected(skill)}
                  onCheckedChange={() => toggleSkill(skill)}
                  className="mt-0.5 border-slate-600 data-[state=checked]:bg-amber-500 data-[state=checked]:border-amber-500"
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    {skill.skill_type === 'subgraph_based' ? (
                      <FolderOpen className="w-3.5 h-3.5 text-violet-400" />
                    ) : (
                      <Brain className="w-3.5 h-3.5 text-blue-400" />
                    )}
                    <span className="text-sm font-medium text-slate-200 truncate">{skill.name}</span>
                    {skill.tool_definitions && skill.tool_definitions.length > 1 && (
                      <Badge variant="outline" className="text-[9px] h-4 px-1.5 border-slate-600 text-slate-400">
                        {skill.tool_definitions.length} tools
                      </Badge>
                    )}
                  </div>
                  <p className="text-[10px] text-slate-500 truncate mt-0.5">{skill.description || 'No description'}</p>
                  {skill.category && (
                    <Badge variant="secondary" className="text-[9px] mt-1 bg-slate-700 text-slate-400">
                      {skill.category}
                    </Badge>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </ScrollArea>

      {/* Selected Tools Preview */}
      {selectedTools.length > 0 && (
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-500 pl-1">Selected Tools:</Label>
          <div className="flex flex-wrap gap-1.5">
            {selectedTools.map((tool, i) => (
              <Badge
                key={`${tool.skill_id}-${tool.name}-${i}`}
                variant="secondary"
                className="text-[10px] bg-amber-500/10 text-amber-400 border-amber-500/20 pr-1 group"
              >
                {tool.name}
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onToolsChange(selectedTools.filter((_, idx) => idx !== i));
                  }}
                  className="ml-1 hover:text-red-400 opacity-60 hover:opacity-100"
                >
                  ×
                </button>
              </Badge>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

/**
 * AI Model Configuration Component
 */
const AIModelSettings = ({ data, onChange }: { data: NodeData, onChange: (key: string, val: any) => void }) => (
  <motion.div
    initial={{ opacity: 0, y: 10 }}
    animate={{ opacity: 1, y: 0 }}
    className="space-y-4 border border-slate-700 rounded-2xl p-5 bg-slate-900/50"
  >
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Engine Identity</Label>
      <Select value={data.model} onValueChange={(val) => onChange('model', val)}>
        <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue /></SelectTrigger>
        <SelectContent>
          {AI_MODELS.map(m => (
            <SelectItem 
              key={m.id} 
              value={m.id}
              disabled={m.disabled}
              className={cn(m.disabled && "opacity-40 cursor-not-allowed blur-[0.5px]")}
            >
              {m.label}
              {m.disabled && <span className="text-[9px] text-amber-400 ml-2">(Unavailable)</span>}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {AI_MODELS.find(m => m.id === data.model)?.disabled && (
        <p className="text-[10px] text-amber-400 bg-amber-950/30 border border-amber-800/50 rounded px-2 py-1.5">
          ⚠️ {AI_MODELS.find(m => m.id === data.model)?.disabledReason}
        </p>
      )}
    </div>
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Core Instruction (System Prompt)</Label>
      <Textarea
        value={data.prompt_content}
        onChange={(e) => onChange('prompt_content', e.target.value)}
        placeholder="Define the agent's personality and mission..."
        className="min-h-[120px] font-mono text-xs bg-slate-800 border-slate-700 text-slate-100 leading-relaxed shadow-inner placeholder:text-slate-500"
      />
    </div>
    <div className="space-y-3 pt-2">
      <div className="flex justify-between items-center px-1">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Creativity (Temp)</Label>
        <span className="text-xs font-mono font-bold text-primary bg-primary/10 px-2 py-0.5 rounded-full">{data.temperature?.toFixed(1)}</span>
      </div>
      <Slider
        value={[data.temperature ?? 0.7]}
        onValueChange={(val) => onChange('temperature', val[0])}
        min={0} max={2} step={0.1}
        className="py-2"
      />
    </div>
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Token Limit</Label>
      <Input
        type="number"
        value={data.max_tokens}
        onChange={(e) => onChange('max_tokens', parseInt(e.target.value))}
        className="bg-slate-800 border-slate-700 text-slate-100"
      />
    </div>

    {/* Tools/Skills Selection */}
    <div className="pt-2 border-t border-slate-700">
      <ToolsSelector
        selectedTools={data.tools || []}
        onToolsChange={(tools) => onChange('tools', tools)}
      />
    </div>
  </motion.div>
);

/**
 * 연산 및 액션 설정 컴포넌트
 */
const OperatorSettings = ({ data, onChange }: { data: NodeData, onChange: (key: string, val: any) => void }) => (
  <motion.div
    initial={{ opacity: 0, y: 10 }}
    animate={{ opacity: 1, y: 0 }}
    className="space-y-4 border border-slate-700 rounded-2xl p-5 bg-slate-900/50"
  >
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Operator Type</Label>
      <Select value={data.operatorType} onValueChange={(val) => onChange('operatorType', val)}>
        <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue /></SelectTrigger>
        <SelectContent>
          {OPERATORS.map(op => <SelectItem key={op.id} value={op.id}>{op.label}</SelectItem>)}
        </SelectContent>
      </Select>
      <div className="flex gap-2 p-3 bg-slate-800/50 border border-slate-700 rounded-xl mt-2">
        <Info className="w-4 h-4 text-blue-400 shrink-0" />
        <p className="text-[10px] text-slate-300 leading-snug">
          All operators are production-ready with dedicated backend runners: api_call and safe_operator.
        </p>
      </div>
    </div>

    {/* Safe Transform Strategy Selection */}
    {data.operatorType === 'safe_operator' && (
      <div className="space-y-2 animate-in slide-in-from-top-2">
        <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Transformation Strategy</Label>
        <Select value={data.strategy} onValueChange={(val) => onChange('strategy', val)}>
          <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue placeholder="Select strategy..." /></SelectTrigger>
          <SelectContent>
            {SAFE_TRANSFORM_STRATEGIES.map(s => (
              <SelectItem key={s.value} value={s.value}>
                <div className="flex flex-col">
                  <span>{s.label}</span>
                  <span className="text-[9px] text-slate-400">{s.description}</span>
                </div>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <p className="text-[9px] text-slate-400 pl-1">50+ built-in strategies available. Required field.</p>
      </div>
    )}

    {/* API Call Configuration */}
    {data.operatorType === 'api_call' && (
      <div className="space-y-4 animate-in slide-in-from-top-2">
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-400 pl-1 font-bold">API Endpoint URL</Label>
          <Input value={data.url} onChange={(e) => onChange('url', e.target.value)} placeholder="https://api.example.com/endpoint" className="bg-slate-800 border-slate-700 text-slate-100" />
        </div>
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-400 pl-1 font-bold">HTTP Method</Label>
          <Select value={data.method} onValueChange={(val) => onChange('method', val)}>
            <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="GET">GET</SelectItem>
              <SelectItem value="POST">POST</SelectItem>
              <SelectItem value="PUT">PUT</SelectItem>
              <SelectItem value="DELETE">DELETE</SelectItem>
              <SelectItem value="PATCH">PATCH</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
    )}
  </motion.div>
);

/**
 * 트리거 및 이벤트 설정 컴포넌트
 */
const TriggerSettings = ({ data, onChange }: { data: NodeData, onChange: (key: string, val: any) => void }) => (
  <motion.div
    initial={{ opacity: 0, y: 10 }}
    animate={{ opacity: 1, y: 0 }}
    className="space-y-4 border border-slate-700 rounded-2xl p-5 bg-slate-900/50"
  >
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Trigger Protocol</Label>
      <Select value={data.triggerType} onValueChange={(val) => onChange('triggerType', val)}>
        <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue /></SelectTrigger>
        <SelectContent>
          {TRIGGERS.map(t => <SelectItem key={t.id} value={t.id}>{t.label}</SelectItem>)}
        </SelectContent>
      </Select>
    </div>
    <AnimatePresence>
      {data.triggerType === 'time' && (
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          exit={{ opacity: 0, height: 0 }}
          className="grid grid-cols-2 gap-4 pt-2"
        >
          <div className="space-y-2">
            <Label className="text-[10px] text-slate-400 pl-1 font-bold">Hour (24h)</Label>
            <Input type="number" min={0} max={23} value={data.triggerHour} onChange={(e) => onChange('triggerHour', parseInt(e.target.value))} className="bg-slate-800 border-slate-700 text-slate-100" />
          </div>
          <div className="space-y-2">
            <Label className="text-[10px] text-slate-400 pl-1 font-bold">Minute (mm)</Label>
            <Input type="number" min={0} max={59} value={data.triggerMinute} onChange={(e) => onChange('triggerMinute', parseInt(e.target.value))} className="bg-slate-800 border-slate-700 text-slate-100" />
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  </motion.div>
);

/**
 * 로직 및 흐름 제어 설정 컴포넌트
 */
const ControlSettings = ({ data, onChange }: { data: NodeData, onChange: (key: string, val: any) => void }) => (
  <motion.div
    initial={{ opacity: 0, y: 10 }}
    animate={{ opacity: 1, y: 0 }}
    className="space-y-4 border border-slate-700 rounded-2xl p-5 bg-slate-900/50"
  >
    <div className="space-y-2">
      <Label className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Flow Control Behavior</Label>
      <Select value={data.controlType} onValueChange={(val) => onChange('controlType', val)}>
        <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue /></SelectTrigger>
        <SelectContent>
          {CONTROLS.map(c => <SelectItem key={c.id} value={c.id}>{c.label}</SelectItem>)}
        </SelectContent>
      </Select>
    </div>
    {data.controlType === 'while' && (
      <div className="space-y-4 pt-2 animate-in slide-in-from-top-2">
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-400 pl-1 font-bold italic">Stop Condition Path</Label>
          <Input value={data.whileCondition} onChange={(e) => onChange('whileCondition', e.target.value)} placeholder="e.g. data.status == 'ready'" className="bg-white" />
        </div>
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-400 pl-1 font-bold">Max Iteration Guard</Label>
          <Input type="number" value={data.maxIterations} onChange={(e) => onChange('maxIterations', parseInt(e.target.value))} className="bg-white" />
        </div>
      </div>
    )}
    {data.controlType === 'aggregator' && (
      <div className="space-y-4 pt-2 animate-in slide-in-from-top-2">
        <div className="p-3 rounded-lg bg-blue-500/10 border border-blue-500/20 text-[11px] text-blue-300 leading-relaxed">
          병렬 브랜치나 반복 작업의 결과를 병합하고 토큰 사용량을 집계합니다.
        </div>
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-400 pl-1 font-bold">Aggregation Strategy</Label>
          <Select value={data.strategy || 'auto'} onValueChange={(val) => onChange('strategy', val)}>
            <SelectTrigger className="h-10 bg-slate-800 border-slate-700 text-slate-100"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="auto">Auto (자동 감지)</SelectItem>
              <SelectItem value="merge">Merge (병합)</SelectItem>
              <SelectItem value="concat">Concat (연결)</SelectItem>
              <SelectItem value="sum">Sum (합산)</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <Label className="text-[10px] text-slate-400 pl-1 font-bold">Output Key</Label>
          <Input value={data.output_key || 'aggregated_result'} onChange={(e) => onChange('output_key', e.target.value)} placeholder="aggregated_result" className="bg-white" />
        </div>
      </div>
    )}
  </motion.div>
);

/**
 * 연결 관리 컴포넌트
 */
const ConnectionManager = ({
  incoming,
  outgoing,
  available,
  onDelete,
  onCreate,
  nodeId
}: any) => {
  const [selectedTarget, setSelectedTarget] = useState('');

  return (
    <div className="space-y-6 pt-4">
      <div className="flex items-center gap-3 border-b pb-3">
        <div className="p-1.5 bg-slate-800 rounded-lg">
          <Plug className="w-4 h-4 text-slate-500" />
        </div>
        <h4 className="font-bold text-sm tracking-tight text-slate-300">Link Infrastructure</h4>
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Incoming Section */}
        <div className="space-y-3">
          <div className="flex items-center gap-1.5">
            <ArrowLeft className="w-3 h-3 text-slate-300" />
            <Label className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Incoming</Label>
          </div>
          {incoming.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {incoming.map((conn: any) => (
                <Badge key={conn.id} variant="secondary" className="pl-2 pr-1 h-6 bg-slate-800 border-slate-700 text-slate-300 group/badge">
                  <span className="truncate max-w-[80px] text-[10px] font-medium">{conn.sourceLabel}</span>
                  <Button
                    variant="ghost"
                    className="h-4 w-4 p-0 ml-1 hover:text-destructive transition-colors opacity-0 group-hover/badge:opacity-100"
                    onClick={() => onDelete?.(conn.id)}
                  >
                    <X className="w-2.5 h-2.5" />
                  </Button>
                </Badge>
              ))}
            </div>
          ) : (
            <p className="text-[10px] text-slate-300 italic pl-1">No dependency</p>
          )}
        </div>

        {/* Outgoing Section */}
        <div className="space-y-3">
          <div className="flex items-center gap-1.5">
            <ArrowRight className="w-3 h-3 text-slate-300" />
            <Label className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Outgoing</Label>
          </div>
          {outgoing.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {outgoing.map((conn: any) => (
                <Badge key={conn.id} variant="secondary" className="pl-2 pr-1 h-6 bg-slate-800 border-slate-700 text-slate-300 group/badge">
                  <span className="truncate max-w-[80px] text-[10px] font-medium">{conn.targetLabel}</span>
                  <Button
                    variant="ghost"
                    className="h-4 w-4 p-0 ml-1 hover:text-destructive transition-colors opacity-0 group-hover/badge:opacity-100"
                    onClick={() => onDelete?.(conn.id)}
                  >
                    <X className="w-2.5 h-2.5" />
                  </Button>
                </Badge>
              ))}
            </div>
          ) : (
            <p className="text-[10px] text-slate-300 italic pl-1">Endpoint terminal</p>
          )}
        </div>
      </div>

      {/* Create New Link */}
      {onCreate && available.length > 0 && (
        <div className="flex gap-2 p-3 bg-slate-800/50 rounded-2xl border border-slate-700">
          <Select value={selectedTarget} onValueChange={setSelectedTarget}>
            <SelectTrigger className="h-9 text-xs bg-slate-800 border-slate-700 text-slate-100">
              <SelectValue placeholder="Propagate to..." />
            </SelectTrigger>
            <SelectContent>
              {available.map((t: any) => (
                <SelectItem key={t.id} value={t.id} disabled={t.id === nodeId}>
                  {t.label}
                  {t.id === nodeId && <span className="text-[10px] ml-2 text-red-400 opacity-50">(Self)</span>}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            size="sm"
            className="h-9 px-4 font-bold active:scale-95 transition-transform"
            disabled={!selectedTarget}
            onClick={() => {
              onCreate(nodeId, selectedTarget);
              setSelectedTarget('');
            }}
          >
            Connect
          </Button>
        </div>
      )}
    </div>
  );
};

// --- MAIN COMPONENT ---

export const NodeEditorDialog = ({
  node,
  open,
  onClose,
  onSave,
  onDelete,
  incomingConnections = [],
  outgoingConnections = [],
  availableTargets = [],
  onEdgeDelete,
  onEdgeCreate
}: NodeEditorDialogProps) => {
  const nodeType = node?.type || '';

  // 폼 상태 데이터 (Any 제거 후 정규화된 키 사용)
  const [formData, setFormData] = useState<NodeData>(() => ({
    label: node?.data.label || '',
    prompt_content: node?.data.prompt_content || node?.data.prompt || '',
    temperature: node?.data.temperature ?? 0.7,
    model: node?.data.model || (nodeType === 'aiModel' ? 'gpt-4' : ''),
    max_tokens: node?.data.max_tokens || node?.data.maxTokens || 2000,
    tools: node?.data.tools || [],  // AI Model tools
    operatorType: node?.data.operatorType || (nodeType === 'operator' ? 'api_call' : ''),
    strategy: node?.data.strategy || 'list_filter',
    url: node?.data.url || '',
    method: node?.data.method || 'GET',
    triggerType: node?.data.triggerType || (nodeType === 'trigger' ? (node?.data.blockId as string || 'request') : ''),
    triggerHour: node?.data.triggerHour ?? 9,
    triggerMinute: node?.data.triggerMinute ?? 0,
    controlType: node?.data.controlType || (nodeType === 'control' ? 'while' : ''),
    whileCondition: node?.data.whileCondition || '',
    maxIterations: node?.data.max_iterations || node?.data.maxIterations || 10,
    output_key: node?.data.output_key || 'aggregated_result',
  }));

  const updateField = useCallback((key: string, value: any) => {
    setFormData(prev => ({ ...prev, [key]: value }));
  }, []);

  const handleSave = () => {
    if (!node) return;

    // Payload extraction logic based on type
    const updates: Partial<NodeData> = { label: formData.label };

    if (nodeType === 'aiModel') {
      const { prompt_content, temperature, model, max_tokens, tools } = formData;
      Object.assign(updates, { prompt_content, temperature, model, max_tokens: Number(max_tokens), tools: tools || [] });
    } else if (nodeType === 'operator') {
      const { operatorType, strategy, url, method } = formData;
      Object.assign(updates, { operatorType, strategy, url, method });
    } else if (nodeType === 'trigger') {
      const { triggerType, triggerHour, triggerMinute } = formData;
      Object.assign(updates, { triggerType, triggerHour: Number(triggerHour || 0), triggerMinute: Number(triggerMinute || 0) });
      // Generate configValue for display
      if (triggerType === 'time') {
        const hour = String(triggerHour || 0).padStart(2, '0');
        const minute = String(triggerMinute || 0).padStart(2, '0');
        Object.assign(updates, { configValue: `${hour}:${minute}` });
      }
    } else if (nodeType === 'control') {
      const { controlType, whileCondition, maxIterations, strategy, output_key } = formData;
      Object.assign(updates, { controlType, whileCondition, maxIterations: Number(maxIterations || 0) });
      if (controlType === 'aggregator') {
        Object.assign(updates, { strategy: strategy || 'auto', output_key: output_key || 'aggregated_result' });
      }
    }

    onSave(node.id, updates);
    onClose();
  };

  if (!node) return null;

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[520px] max-h-[90vh] flex flex-col p-0 gap-0 overflow-hidden bg-slate-950 border-slate-800 shadow-2xl rounded-3xl">
        <div className="absolute top-0 left-0 w-full h-1.5 bg-gradient-to-r from-primary/80 to-indigo-500/80" />

        <DialogHeader className="p-7 pb-4">
          <div className="flex items-center justify-between">
            <div className="space-y-1">
              <DialogTitle className="flex items-center gap-3 text-xl font-bold tracking-tight text-slate-100">
                Block Configuration
                <Badge variant="outline" className="h-5 text-[9px] font-bold uppercase tracking-widest border-slate-700 bg-slate-800 text-slate-400">
                  {nodeType}
                </Badge>
              </DialogTitle>
              <DialogDescription className="text-slate-400">
                ID: <span className="font-mono text-[10px] font-bold">{node.id}</span> • Specify block behavior and connectivity.
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <ScrollArea className="flex-1 max-h-[70vh] custom-scrollbar">
          <div className="px-7 py-2 space-y-8">
            {/* Common Fields */}
            <div className="space-y-2.5">
              <Label htmlFor="label" className="text-[11px] font-bold uppercase tracking-wider text-slate-400 pl-1">Block Alias (Display Name)</Label>
              <Input
                id="label"
                value={formData.label}
                onChange={(e) => updateField('label', e.target.value)}
                placeholder="e.g. Master Intelligence Unit"
                className="h-11 bg-slate-800 border-slate-700 text-slate-100 font-medium placeholder:text-slate-500"
              />
            </div>

            <div className="relative">
              <div className="absolute -left-7 top-0 bottom-0 w-1 bg-primary/20 rounded-r-full" />
              <div className="space-y-2 mb-4 flex items-center gap-2">
                <Settings2 className="w-4 h-4 text-primary" />
                <h3 className="text-xs font-bold text-slate-300">Internal Logic Settings</h3>
              </div>

              {/* Switch settings UI based on type */}
              {nodeType === 'aiModel' && <AIModelSettings data={formData} onChange={updateField} />}
              {nodeType === 'operator' && <OperatorSettings data={formData} onChange={updateField} />}
              {nodeType === 'trigger' && <TriggerSettings data={formData} onChange={updateField} />}
              {nodeType === 'control' && <ControlSettings data={formData} onChange={updateField} />}
            </div>

            {/* Connection Infrastructure */}
            <ConnectionManager
              incoming={incomingConnections}
              outgoing={outgoingConnections}
              available={availableTargets}
              onDelete={onEdgeDelete}
              onCreate={onEdgeCreate}
              nodeId={node.id}
            />
          </div>
          <div className="h-8" /> {/* Scroll margin */}
        </ScrollArea>

        <DialogFooter className="p-6 border-t border-slate-800 bg-slate-900/80 flex-row justify-between items-center sm:justify-between">
          <div className="flex items-center">
            {onDelete && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => { onDelete(node.id); onClose(); }}
                className="text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-xl h-10 px-4 font-bold text-xs gap-2 transition-all"
              >
                <Trash2 className="w-4 h-4" /> Discard Block
              </Button>
            )}
          </div>
          <div className="flex gap-2">
            <Button variant="ghost" className="h-10 rounded-xl font-bold text-xs" onClick={onClose}>Dismiss</Button>
            <Button
              onClick={handleSave}
              className="h-10 px-8 rounded-xl font-bold text-xs bg-primary shadow-lg shadow-primary/20 active:scale-95 transition-all"
            >
              Commit Changes
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default NodeEditorDialog;

const X = ({ className, onClick }: { className?: string, onClick?: () => void }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="24"
    height="24"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    onClick={onClick}
  >
    <path d="M18 6 6 18" /><path d="m6 6 12 12" />
  </svg>
);
