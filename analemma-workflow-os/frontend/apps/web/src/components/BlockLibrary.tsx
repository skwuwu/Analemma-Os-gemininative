import React, { useState, useEffect, useMemo } from 'react';
import { Brain, Clock, Webhook, Zap, MessageSquare, Globe, Database, Search, GripVertical, ChevronDown, ChevronRight, Repeat, GitFork, CheckCircle2, FolderOpen, Merge, Split, type LucideIcon } from 'lucide-react';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Input } from '@/components/ui/input';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion';
import { cn } from '@/lib/utils';
import { listSkills, type Skill } from '@/lib/skillsApi';

// 1. 블록 데이터 정의 (확장성을 위해 type 정의 추천)
type BlockType = 'trigger' | 'aiModel' | 'operator' | 'control';

interface BlockDefinition {
  id: string;
  label: string;
  icon: LucideIcon; // 타입 강화
  type: BlockType;
  description?: string; // 툴팁용 설명
  defaultData: Record<string, unknown>; // 타입 강화
}

// 2. 카테고리별 데이터 그룹화
export const BLOCK_CATEGORIES: { title: string; type: BlockType; items: Omit<BlockDefinition, 'type'>[] }[] = [
  {
    title: 'Triggers',
    type: 'trigger',
    items: [
      { id: 'time', label: 'Time-based (Schedule)', icon: Clock, description: 'Schedule workflows to run at specific times using cron expressions', defaultData: { triggerType: 'time', configValue: '0 9 * * *' } },
      { id: 'request', label: 'API Request', icon: Webhook, description: 'Start workflows when receiving HTTP requests via webhooks', defaultData: { triggerType: 'request', configValue: 'POST /api/webhook' } },
      { id: 'event', label: 'External Event', icon: Zap, description: 'Trigger workflows based on external system events', defaultData: { triggerType: 'event', configValue: 'user_signup' } },
    ]
  },
  {
    title: 'AI Models',
    type: 'aiModel',
    items: [
      { id: 'gpt4', label: 'GPT-4', icon: Brain, description: 'OpenAI\'s most advanced language model for complex reasoning tasks', defaultData: { modelType: 'gpt4', temperature: 0.7 } },
      { id: 'claude', label: 'Claude', icon: Brain, description: 'Anthropic\'s helpful and truthful AI assistant', defaultData: { modelType: 'claude', temperature: 0.7 } },
      { id: 'gemini', label: 'Gemini', icon: Brain, description: 'Google\'s multimodal AI model for various tasks', defaultData: { modelType: 'gemini', temperature: 0.7 } },
    ]
  },
  {
    title: 'Operators',
    type: 'operator',
    items: [
      { id: 'api_call', label: 'API Call', icon: Globe, description: 'Execute HTTP API requests (GET, POST, PUT, DELETE) with template rendering support', defaultData: { operatorType: 'api_call', action: 'API Request', url: '', method: 'GET' } },
      { id: 'safe_operator', label: 'Safe Transform', icon: Zap, description: '50+ built-in transformation strategies (list_filter, json_parse, string_template, etc.)', defaultData: { operatorType: 'safe_operator', action: 'Transform Data', strategy: 'list_filter' } },
    ]
  },
  {
    title: 'Controls',
    type: 'control',
    items: [
      { id: 'for_each', label: 'For Each Loop', icon: Repeat, description: 'Iterate over a list of items and execute sub-workflow for each', defaultData: { controlType: 'for_each', items_path: 'state.items', item_key: 'item', output_key: 'results', max_iterations: 20 } },
      { id: 'parallel', label: 'Parallel Branches', icon: GitFork, description: 'Execute multiple branches concurrently', defaultData: { controlType: 'parallel', branches: [] } },
      { id: 'conditional', label: 'Conditional Router', icon: Split, description: 'Route workflow based on Python expressions (if-elif-else logic)', defaultData: { controlType: 'conditional', conditions: [], default_node: null, evaluation_mode: 'first_match' } },
      { id: 'aggregator', label: 'Aggregator', icon: Merge, description: 'Merge and aggregate results from parallel branches or iterations (includes token usage)', defaultData: { controlType: 'aggregator', strategy: 'auto', sources: [], output_key: 'aggregated_result' } },
      { id: 'while', label: 'While Loop', icon: Clock, description: 'Repeat workflow steps while a condition is true', defaultData: { controlType: 'loop', condition: 'count < 5' } },
      { id: 'confirmation', label: 'User Intervention', icon: MessageSquare, description: 'Pause workflow execution for human approval or input', defaultData: { controlType: 'human', condition: 'approval_required' } },
    ]
  }
];

// 3. 스타일 설정 객체 (Switch문 대체)
const STYLE_CONFIG: Record<BlockType, { bg: string; border: string; iconBg: string; iconColor: string }> = {
  trigger: {
    bg: 'from-[hsl(142_76%_36%)]/10 to-[hsl(142_76%_36%)]/5',
    border: 'border-[hsl(142_76%_36%)]/50',
    iconBg: 'bg-[hsl(142_76%_36%)]/20',
    iconColor: 'text-[hsl(142_76%_36%)]'
  },
  aiModel: {
    bg: 'from-primary/10 to-primary/5',
    border: 'border-primary/50',
    iconBg: 'bg-primary/20',
    iconColor: 'text-primary'
  },
  operator: {
    bg: 'from-[hsl(25_95%_60%)]/10 to-[hsl(25_95%_60%)]/5',
    border: 'border-[hsl(25_95%_60%)]/50',
    iconBg: 'bg-[hsl(25_95%_60%)]/20',
    iconColor: 'text-[hsl(25_95%_60%)]'
  },
  control: {
    bg: 'from-[hsl(280_65%_60%)]/10 to-[hsl(280_65%_60%)]/5',
    border: 'border-[hsl(280_65%_60%)]/50',
    iconBg: 'bg-[hsl(280_65%_60%)]/20',
    iconColor: 'text-[hsl(280_65%_60%)]'
  }
};

const BlockItem = ({ id, label, icon: Icon, type, defaultData, description }: BlockDefinition) => {
  const onDragStart = (event: React.DragEvent) => {
    event.dataTransfer.setData('application/reactflow', type);
    event.dataTransfer.setData('label', label);
    event.dataTransfer.setData('blockId', id);
    event.dataTransfer.setData('defaultData', JSON.stringify(defaultData));
    event.dataTransfer.effectAllowed = 'move';
  };

  const style = STYLE_CONFIG[type];

  const blockElement = (
    <div
      draggable
      onDragStart={onDragStart}
      className={cn(
        "group flex items-center gap-3 px-3 py-2.5 rounded-lg border bg-gradient-to-br cursor-grab active:cursor-grabbing hover:shadow-md transition-all duration-200",
        style.bg,
        style.border
      )}
    >
      {/* 드래그 핸들 (시각적 힌트) */}
      <GripVertical className="w-3 h-3 text-muted-foreground/30 opacity-0 group-hover:opacity-100 transition-opacity" />

      <div className={cn("p-1.5 rounded-md", style.iconBg)}>
        <Icon className={cn("w-4 h-4", style.iconColor)} />
      </div>
      <span className="text-xs font-medium text-foreground select-none">{label}</span>
    </div>
  );

  // description이 있으면 Tooltip으로 감싸기
  if (description) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          {blockElement}
        </TooltipTrigger>
        <TooltipContent side="right" className="max-w-xs">
          <p className="text-xs">{description}</p>
        </TooltipContent>
      </Tooltip>
    );
  }

  return blockElement;
};

export const BlockLibrary = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [userSkills, setUserSkills] = useState<Skill[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    const fetchSkills = async () => {
      setIsLoading(true);
      try {
        const response = await listSkills();
        setUserSkills(response.items || []);
      } catch (error) {
        console.error('Failed to fetch user skills:', error);
      } finally {
        setIsLoading(false);
      }
    };
    fetchSkills();
  }, []);

  const allCategories = useMemo(() => {
    if (userSkills.length === 0) return BLOCK_CATEGORIES;

    const userSkillCategory = {
      title: 'User Skills',
      type: 'operator' as BlockType,
      items: userSkills.map(skill => ({
        id: skill.skill_id,
        label: skill.name,
        icon: skill.skill_type === 'subgraph_based' ? FolderOpen : Brain,
        description: skill.description || 'Custom user skill',
        defaultData: {
          operatorType: 'skill',
          skillId: skill.skill_id,
          version: skill.version,
          skillType: skill.skill_type
        }
      }))
    };

    return [...BLOCK_CATEGORIES, userSkillCategory];
  }, [userSkills]);

  // 검색 필터링 로직
  const filteredCategories = allCategories.map(category => ({
    ...category,
    items: category.items.filter(item =>
      item.label.toLowerCase().includes(searchTerm.toLowerCase())
    )
  })).filter(category => category.items.length > 0);

  return (
    <div className="h-full bg-card border-r border-border flex flex-col w-[280px]">
      <div className="p-4 border-b border-border">
        <h2 className="text-sm font-bold mb-3 text-foreground flex items-center gap-2">
          <FolderOpen className="w-4 h-4" />
          Block Library
        </h2>
        <div className="relative">
          <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search blocks..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-8 h-9 text-xs"
          />
        </div>
      </div>

      <ScrollArea className="flex-1">
        <div className="p-4">
          {filteredCategories.length > 0 ? (
            <Accordion type="multiple" defaultValue={["triggers"]} className="space-y-2">
              {filteredCategories.map((category) => (
                <AccordionItem key={category.title} value={category.title.toLowerCase().replace(' ', '-')}>
                  <AccordionTrigger className="text-xs font-semibold text-muted-foreground uppercase tracking-wider hover:no-underline">
                    {category.title}
                  </AccordionTrigger>
                  <AccordionContent className="pb-2">
                    <div className="grid gap-2">
                      {category.items.map((item) => (
                        <BlockItem
                          key={item.id}
                          {...item}
                          type={category.type}
                        />
                      ))}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          ) : (
            <div className="text-center text-xs text-muted-foreground py-8">
              No blocks found
            </div>
          )}
        </div>
      </ScrollArea>
    </div>
  );
};
