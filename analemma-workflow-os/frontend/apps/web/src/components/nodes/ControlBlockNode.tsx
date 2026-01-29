import { Handle, Position, useReactFlow } from '@xyflow/react';
import { GitBranch, Repeat, Plus, Trash2, Settings } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { cn } from '@/lib/utils';
import { useState } from 'react';

// Control Block 타입 정의
export type ControlBlockType = 'conditional' | 'parallel' | 'for_each' | 'while';

export interface BranchConfig {
  id: string;
  label: string;
  natural_condition?: string;  // conditional용 자연어 조건
  items_path?: string;          // for_each용 배열 경로
  targetNodeId?: string;        // 분기 대상 노드 ID
}

export interface ControlBlockData {
  label: string;
  blockType: ControlBlockType;
  branches: BranchConfig[];
  // while 전용 필드
  max_iterations?: number;
  natural_condition?: string;
  back_edge_source?: string;
  [key: string]: any;
}

interface ControlBlockNodeProps {
  data: ControlBlockData;
  id: string;
  selected?: boolean;
}

const BLOCK_TYPE_CONFIG = {
  conditional: {
    icon: GitBranch,
    color: '142 76% 36%', // 초록색
    label: 'Conditional Branch',
    description: 'Route based on conditions',
  },
  parallel: {
    icon: GitBranch,
    color: '217 91% 60%', // 파란색
    label: 'Parallel Execution',
    description: 'Execute branches simultaneously',
  },
  for_each: {
    icon: Repeat,
    color: '280 65% 60%', // 보라색
    label: 'For Each Loop',
    description: 'Iterate over items',
  },
  while: {
    icon: Repeat,
    color: '48 96% 53%', // 노란색
    label: 'While Loop',
    description: 'Loop until condition met',
  },
} as const;

export const ControlBlockNode = ({ data, id, selected }: ControlBlockNodeProps) => {
  const { updateNodeData } = useReactFlow();
  const [expanded, setExpanded] = useState(false);

  const config = BLOCK_TYPE_CONFIG[data.blockType];
  const IconComponent = config.icon;

  // 블록 타입 변경
  const handleTypeChange = (newType: ControlBlockType) => {
    updateNodeData(id, {
      ...data,
      blockType: newType,
      // 타입 변경 시 타입별 초기 branches 설정
      branches: newType === 'while' 
        ? [] // while은 branches 없음
        : data.branches.length > 0 
          ? data.branches 
          : [{ id: 'branch_0', label: 'Branch 1' }],
    });
  };

  // 분기 추가
  const addBranch = () => {
    const newBranch: BranchConfig = {
      id: `branch_${data.branches.length}`,
      label: `Branch ${data.branches.length + 1}`,
    };
    updateNodeData(id, {
      ...data,
      branches: [...data.branches, newBranch],
    });
  };

  // 분기 삭제
  const removeBranch = (branchId: string) => {
    updateNodeData(id, {
      ...data,
      branches: data.branches.filter(b => b.id !== branchId),
    });
  };

  // 분기 조건 업데이트
  const updateBranchCondition = (branchId: string, condition: string) => {
    updateNodeData(id, {
      ...data,
      branches: data.branches.map(b =>
        b.id === branchId ? { ...b, natural_condition: condition } : b
      ),
    });
  };

  // while 조건 업데이트
  const updateWhileCondition = (condition: string) => {
    updateNodeData(id, { ...data, natural_condition: condition });
  };

  // while 최대 반복 횟수 업데이트
  const updateMaxIterations = (max: number) => {
    updateNodeData(id, { ...data, max_iterations: max });
  };

  return (
    <div
      className={cn(
        "px-4 py-3 rounded-lg border-2 bg-card backdrop-blur-sm min-w-[220px] max-w-[400px] transition-all duration-200 relative group",
        selected && "ring-2 ring-ring ring-offset-2"
      )}
      style={{
        borderColor: `hsl(${config.color})`,
        background: `linear-gradient(to bottom right, hsl(${config.color} / 0.15), hsl(${config.color} / 0.05))`,
        boxShadow: `0 4px 20px hsl(${config.color} / 0.2)`,
      }}
    >
      {/* 입력 핸들 */}
      <Handle
        type="target"
        position={Position.Left}
        className="w-3 h-3"
        style={{ backgroundColor: `hsl(${config.color})` }}
      />

      {/* 헤더 */}
      <div className="flex items-start gap-2 mb-3">
        <div
          className="p-2 rounded-md"
          style={{ backgroundColor: `hsl(${config.color} / 0.2)` }}
        >
          <IconComponent className="w-4 h-4" style={{ color: `hsl(${config.color})` }} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <Input
              value={data.label}
              onChange={(e) => updateNodeData(id, { ...data, label: e.target.value })}
              className="text-sm font-semibold h-7 px-2 border-0 bg-transparent focus-visible:ring-1"
              placeholder="Block name"
            />
          </div>
          <Badge variant="outline" className="text-[10px] mt-1 h-5">
            {config.label}
          </Badge>
        </div>
        <Button
          size="icon"
          variant="ghost"
          className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
          onClick={() => setExpanded(!expanded)}
        >
          <Settings className="w-3 h-3" />
        </Button>
      </div>

      {/* 설정 패널 (확장 시) */}
      {expanded && (
        <div className="space-y-3 border-t pt-3 mt-2">
          {/* 블록 타입 선택 */}
          <div className="space-y-1.5">
            <Label className="text-xs">Block Type</Label>
            <Select value={data.blockType} onValueChange={handleTypeChange}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {Object.entries(BLOCK_TYPE_CONFIG).map(([key, cfg]) => (
                  <SelectItem key={key} value={key} className="text-xs">
                    {cfg.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-[10px] text-muted-foreground">{config.description}</p>
          </div>

          {/* Conditional/Parallel/For_each: 분기 설정 */}
          {data.blockType !== 'while' && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label className="text-xs">Branches ({data.branches.length})</Label>
                <Button
                  size="icon"
                  variant="ghost"
                  className="h-6 w-6"
                  onClick={addBranch}
                >
                  <Plus className="w-3 h-3" />
                </Button>
              </div>
              <div className="space-y-2 max-h-[200px] overflow-y-auto">
                {data.branches.map((branch, idx) => (
                  <div key={branch.id} className="p-2 rounded border bg-muted/50 space-y-1.5">
                    <div className="flex items-center gap-2">
                      <Input
                        value={branch.label}
                        onChange={(e) => {
                          updateNodeData(id, {
                            ...data,
                            branches: data.branches.map(b =>
                              b.id === branch.id ? { ...b, label: e.target.value } : b
                            ),
                          });
                        }}
                        className="h-6 text-xs flex-1"
                        placeholder={`Branch ${idx + 1}`}
                      />
                      {data.branches.length > 1 && (
                        <Button
                          size="icon"
                          variant="ghost"
                          className="h-6 w-6"
                          onClick={() => removeBranch(branch.id)}
                        >
                          <Trash2 className="w-3 h-3" />
                        </Button>
                      )}
                    </div>
                    
                    {/* Conditional: 자연어 조건 */}
                    {data.blockType === 'conditional' && (
                      <Input
                        value={branch.natural_condition || ''}
                        onChange={(e) => updateBranchCondition(branch.id, e.target.value)}
                        className="h-7 text-xs"
                        placeholder="Natural language condition (e.g., 'if quality is high')"
                      />
                    )}
                    
                    {/* For_each: items 경로 */}
                    {data.blockType === 'for_each' && (
                      <Input
                        value={branch.items_path || ''}
                        onChange={(e) => {
                          updateNodeData(id, {
                            ...data,
                            branches: data.branches.map(b =>
                              b.id === branch.id ? { ...b, items_path: e.target.value } : b
                            ),
                          });
                        }}
                        className="h-7 text-xs"
                        placeholder="Items path (e.g., 'state.items')"
                      />
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* While: 조건 및 최대 반복 횟수 */}
          {data.blockType === 'while' && (
            <div className="space-y-2">
              <div className="space-y-1.5">
                <Label className="text-xs">Loop Condition (Natural Language)</Label>
                <Input
                  value={data.natural_condition || ''}
                  onChange={(e) => updateWhileCondition(e.target.value)}
                  className="h-8 text-xs"
                  placeholder="e.g., 'content is not detailed enough'"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs">Max Iterations</Label>
                <Input
                  type="number"
                  value={data.max_iterations || 10}
                  onChange={(e) => updateMaxIterations(parseInt(e.target.value))}
                  className="h-8 text-xs"
                  min={1}
                  max={100}
                />
              </div>
              {data.back_edge_source && (
                <p className="text-[10px] text-muted-foreground">
                  ↻ Loops back to: {data.back_edge_source}
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {/* 출력 핸들 (분기 수만큼 생성) */}
      {/* 🎨 시각 효과: 모든 handle을 같은 위치에 배치하여 하나의 점에서 나가는 것처럼 보이게 함 */}
      {data.blockType !== 'while' && data.branches.map((branch, idx) => (
        <Handle
          key={branch.id}
          type="source"
          position={Position.Right}
          id={branch.id}
          className="w-3 h-3"
          style={{
            backgroundColor: `hsl(${config.color})`,
            top: '50%', // 모든 handle을 중앙에 배치
            transform: 'translateY(-50%)', // 정확한 중앙 정렬
          }}
        />
      ))}

      {/* While: 단일 출력 핸들 */}
      {data.blockType === 'while' && (
        <Handle
          type="source"
          position={Position.Right}
          className="w-3 h-3"
          style={{ backgroundColor: `hsl(${config.color})` }}
        />
      )}

      {/* 간단한 미리보기 (접힌 상태) */}
      {!expanded && data.blockType !== 'while' && data.branches.length > 0 && (
        <div className="mt-2 pt-2 border-t">
          <p className="text-[10px] text-muted-foreground">
            {data.branches.length} branch(es)
            {data.blockType === 'conditional' && data.branches.some(b => b.natural_condition) && ' • Natural conditions'}
          </p>
        </div>
      )}
      
      {!expanded && data.blockType === 'while' && data.natural_condition && (
        <div className="mt-2 pt-2 border-t">
          <p className="text-[10px] text-muted-foreground truncate">
            Condition: {data.natural_condition}
          </p>
        </div>
      )}
    </div>
  );
};
