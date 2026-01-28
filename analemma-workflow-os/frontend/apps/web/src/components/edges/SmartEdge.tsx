import { BaseEdge, EdgeLabelRenderer, getBezierPath, useReactFlow, EdgeProps } from '@xyflow/react';
import { Activity, ChevronDown, ArrowRight, RefreshCw, GitBranch, Hand, Pause } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuSeparator, DropdownMenuLabel } from '@/components/ui/dropdown-menu';
import { Input } from '@/components/ui/input';
import { useState, useCallback } from 'react';

// 백엔드 엣지 타입 정의
export type BackendEdgeType = 'edge' | 'if' | 'while' | 'hitp' | 'conditional_edge' | 'pause';

// 엣지 타입별 설정
const EDGE_TYPE_CONFIG: Record<BackendEdgeType, { 
  label: string; 
  icon: React.ComponentType<{ className?: string }>; 
  color: string;
  description: string;
  needsCondition?: boolean;
}> = {
  edge: { 
    label: 'Normal', 
    icon: ArrowRight, 
    color: 'hsl(263 70% 60%)',
    description: '일반 연결 (항상 진행)'
  },
  if: { 
    label: 'Conditional', 
    icon: GitBranch, 
    color: 'hsl(142 76% 36%)',
    description: '조건이 참일 때만 진행',
    needsCondition: true
  },
  while: { 
    label: 'While Loop', 
    icon: RefreshCw, 
    color: 'hsl(38 92% 50%)',
    description: '조건이 참인 동안 반복',
    needsCondition: true
  },
  hitp: { 
    label: 'Human Approval', 
    icon: Hand, 
    color: 'hsl(280 70% 50%)',
    description: '사람의 승인 후 진행'
  },
  conditional_edge: { 
    label: 'Multi-Branch', 
    icon: GitBranch, 
    color: 'hsl(200 70% 50%)',
    description: '라우터 함수로 분기 결정',
    needsCondition: true
  },
  pause: { 
    label: 'Pause', 
    icon: Pause, 
    color: 'hsl(0 70% 50%)',
    description: '일시 정지 후 재개'
  },
};

// Edge 데이터 타입 정의 - Edge 인터페이스 확장
interface SmartEdgeData extends Record<string, unknown> {
  label?: string;           // 조건 라벨 (예: "Yes", "Tool Call")
  active?: boolean;         // 현재 실행 중인지 여부 (애니메이션 트리거)
  stateDelta?: string;      // Summary of state data being passed (JSON string)
  edgeType?: BackendEdgeType; // Backend edge type
  condition?: string;       // Conditional expression (for if, while)
  max_iterations?: number;  // Maximum iterations for while
  isBackEdge?: boolean;     // Whether this is a back-edge in circular structure
  isInCycle?: boolean;      // Whether this edge is part of a cycle
}

export const SmartEdge = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  data,
}: EdgeProps) => {
  const { setEdges } = useReactFlow();
  const [isEditing, setIsEditing] = useState(false);
  const [conditionInput, setConditionInput] = useState('');

  // 타입 단언으로 data를 SmartEdgeData로 처리
  const edgeData = data as SmartEdgeData | undefined;
  const currentType: BackendEdgeType = (edgeData?.edgeType as BackendEdgeType) || 'edge';
  const typeConfig = EDGE_TYPE_CONFIG[currentType];

  // 엣지 타입 변경 핸들러
  const handleTypeChange = useCallback((newType: BackendEdgeType) => {
    setEdges((edges) =>
      edges.map((edge) =>
        edge.id === id
          ? { 
              ...edge, 
              data: { 
                ...edge.data, 
                edgeType: newType,
                // 조건이 필요 없는 타입으로 변경 시 조건 제거
                condition: EDGE_TYPE_CONFIG[newType].needsCondition ? edge.data?.condition : undefined,
              } 
            }
          : edge
      )
    );
  }, [id, setEdges]);

  // 조건 저장 핸들러
  const handleConditionSave = useCallback(() => {
    setEdges((edges) =>
      edges.map((edge) =>
        edge.id === id
          ? { ...edge, data: { ...edge.data, condition: conditionInput } }
          : edge
      )
    );
    setIsEditing(false);
  }, [id, conditionInput, setEdges]);

  // 1. 베지에 곡선 경로 계산
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX, sourceY, sourcePosition,
    targetX, targetY, targetPosition,
  });

  // Check if this edge is a back-edge (part of a cycle/loop)
  const isBackEdge = edgeData?.isBackEdge === true;
  const isInCycle = edgeData?.isInCycle === true;

  // 2. 타입에 따른 스타일 적용
  const edgeStyle = {
    ...(style as Record<string, unknown>),
    stroke: edgeData?.active ? '#3b82f6' : 
            isBackEdge ? 'hsl(38 92% 50%)' : // Orange for back-edges (loop feedback)
            typeConfig.color,
    strokeWidth: edgeData?.active ? 2.5 : isBackEdge ? 2.5 : 2,
    animation: edgeData?.active ? 'dashdraw 0.5s linear infinite' : 
               isBackEdge ? 'dashdraw 2s linear infinite' : // Animated back-edges
               currentType === 'while' ? 'dashdraw 2s linear infinite' : 'none',
    strokeDasharray: edgeData?.active ? '5' : 
                     isBackEdge ? '8 4' : // Dashed back-edges
                     currentType === 'while' ? '8 4' : 
                     currentType === 'if' ? '4 2' : '0',
  };

  const IconComponent = typeConfig.icon;

  return (
    <>
      {/* 인터랙션용 투명 엣지 (클릭 범위 확장용) */}
      <BaseEdge
        path={edgePath}
        style={{ strokeWidth: 20, stroke: 'transparent', cursor: 'pointer' }}
      />

      {/* 실제 눈에 보이는 엣지 */}
      <BaseEdge path={edgePath} markerEnd={markerEnd} style={edgeStyle} />

      {/* 엣지 위의 인터랙티브 UI 렌더링 */}
      <EdgeLabelRenderer>
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents: 'all',
            zIndex: isBackEdge ? 15 : 10, // Higher z-index for back-edges to prevent overlap issues
          }}
          className="nodrag nopan"
        >
          <div className="flex flex-col items-center gap-1">
            {/* A. 엣지 타입 선택 드롭다운 */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button
                  className="flex items-center gap-1 px-2 py-1 rounded-md bg-background border shadow-sm hover:bg-muted transition-colors text-xs"
                  style={{ 
                    borderColor: isBackEdge ? 'hsl(38 92% 50%)' : typeConfig.color,
                    backgroundColor: isBackEdge ? 'hsla(38 92% 50% / 0.1)' : undefined
                  }}
                >
                  <IconComponent className="w-3 h-3" style={{ color: isBackEdge ? 'hsl(38 92% 50%)' : typeConfig.color }} />
                  <span className="font-medium" style={{ color: isBackEdge ? 'hsl(38 92% 50%)' : typeConfig.color }}>
                    {isBackEdge ? 'Loop Back' : typeConfig.label}
                  </span>
                  <ChevronDown className="w-3 h-3 text-muted-foreground" />
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="center" className="w-48">
                <DropdownMenuLabel className="text-xs text-muted-foreground">
                  Edge Type
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                {Object.entries(EDGE_TYPE_CONFIG).map(([type, config]) => {
                  const Icon = config.icon;
                  return (
                    <DropdownMenuItem
                      key={type}
                      onClick={() => handleTypeChange(type as BackendEdgeType)}
                      className="flex items-center gap-2 cursor-pointer"
                    >
                      <Icon className="w-4 h-4" style={{ color: config.color }} />
                      <div className="flex flex-col">
                        <span className="font-medium">{config.label}</span>
                        <span className="text-[10px] text-muted-foreground">{config.description}</span>
                      </div>
                    </DropdownMenuItem>
                  );
                })}
              </DropdownMenuContent>
            </DropdownMenu>

            {/* B. 조건 입력 (if, while, conditional_edge 타입일 때) */}
            {typeConfig.needsCondition && (
              isEditing ? (
                <div className="flex gap-1">
                  <Input
                    value={conditionInput}
                    onChange={(e) => setConditionInput(e.target.value)}
                    placeholder="예: state.count < 10"
                    className="h-6 text-xs w-32"
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') handleConditionSave();
                      if (e.key === 'Escape') setIsEditing(false);
                    }}
                  />
                  <button
                    onClick={handleConditionSave}
                    className="px-2 h-6 text-xs bg-primary text-primary-foreground rounded"
                  >
                    저장
                  </button>
                </div>
              ) : (
                <Badge
                  variant="outline"
                  className="bg-background text-[10px] px-1.5 py-0 h-5 border-muted-foreground/30 shadow-sm whitespace-nowrap cursor-pointer hover:bg-muted"
                  onClick={() => {
                    setConditionInput((edgeData?.condition as string) || '');
                    setIsEditing(true);
                  }}
                >
                  {edgeData?.condition || edgeData?.label || '조건 입력 클릭'}
                </Badge>
              )
            )}

            {/* C. 데이터 검사 버튼 (호버 시 데이터 표시) */}
            <Tooltip>
              <TooltipTrigger asChild>
                <div
                  className="flex items-center justify-center w-5 h-5 rounded-full bg-background border cursor-pointer hover:bg-muted transition-colors shadow-sm"
                  onClick={(e) => e.stopPropagation()}
                >
                   <Activity className={`w-3 h-3 ${edgeData?.stateDelta ? 'text-blue-500' : 'text-muted-foreground'}`} />
                </div>
              </TooltipTrigger>

              {/* D. 전달 데이터 툴팁 */}
              <TooltipContent className="max-w-[300px] p-2">
                <div className="space-y-1">
                  {isBackEdge && (
                    <div className="mb-2 p-2 bg-orange-500/10 border border-orange-500/30 rounded">
                      <p className="text-xs font-bold text-orange-500">🔄 Circular Structure (Back-Edge)</p>
                      <p className="text-[10px] text-muted-foreground mt-1">
                        This edge is a feedback path for for_each iteration.
                      </p>
                    </div>
                  )}
                  <h4 className="font-medium leading-none text-xs text-muted-foreground mb-2">
                    {isBackEdge && 'Loop Feedback Edge'}
                    {!isBackEdge && currentType === 'while' && 'While Loop 설정'}
                    {!isBackEdge && currentType === 'if' && '조건부 분기'}
                    {!isBackEdge && currentType === 'edge' && 'State Update'}
                    {!isBackEdge && currentType === 'hitp' && '사람 승인 대기'}
                  </h4>
                  {edgeData?.condition && (
                    <p className="text-xs"><strong>조건:</strong> {edgeData.condition as string}</p>
                  )}
                  {edgeData?.max_iterations && (
                    <p className="text-xs"><strong>최대 반복:</strong> {edgeData.max_iterations as number}회</p>
                  )}
                  {edgeData?.stateDelta && (
                    <pre className="text-[10px] bg-muted p-2 rounded overflow-auto max-h-32 font-mono whitespace-pre-wrap">
                      {edgeData.stateDelta}
                    </pre>
                  )}
                </div>
              </TooltipContent>
            </Tooltip>
          </div>
        </div>
      </EdgeLabelRenderer>
    </>
  );
};