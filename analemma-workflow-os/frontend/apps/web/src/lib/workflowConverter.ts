// 프론트엔드 워크플로우 데이터를 백엔드 형식으로 변환하는 유틸리티 함수들

import { analyzeWorkflowGraph, type GraphAnalysisResult, type CycleInfo, type ParallelGroup } from './graphAnalysis';
import { toast } from 'sonner';

// Subgraph 관련 타입
export interface SubgraphMetadata {
  name: string;
  description?: string;
  icon?: string;
  color?: string;
}

export interface SubgraphDefinition {
  id: string;
  nodes: any[];
  edges: any[];
  metadata: SubgraphMetadata;
  input_schema?: Record<string, SchemaField>;
  output_schema?: Record<string, SchemaField>;
}

export interface SchemaField {
  type: 'string' | 'number' | 'object' | 'array' | 'boolean' | 'any';
  description?: string;
  required?: boolean;
  default?: unknown;
}

export interface BackendWorkflow {
  name?: string;
  nodes: BackendNode[];
  edges: BackendEdge[];
  secrets?: BackendSecret[];
  // 서브그래프 지원
  subgraphs?: Record<string, SubgraphDefinition>;
}

export interface BackendNode {
  id: string;
  type: 
    // 사용자가 프론트엔드에서 생성하는 타입들 (코드가 자동 변환)
    | 'operator'       // operator, trigger, control(human) → operator
    | 'llm_chat'       // aiModel → llm_chat
    | 'loop'           // cycle (back-edge) → loop
    | 'for_each'       // control(loop/for_each) → for_each
    | 'parallel_group' // control(parallel) → parallel_group
    | 'aggregator'     // control(aggregator) → aggregator (token aggregation)
    | 'subgraph'       // group → subgraph
    | 'api_call'       // operator(api_call) → api_call
    | 'db_query'       // operator(db_query) → db_query
    | 'safe_operator'  // operator(safe_operator) → safe_operator
    | 'route_condition' // control(conditional) → route_condition
    | 'trigger';       // trigger → trigger (백엔드에서 정규화됨)
  provider?: 'openai' | 'bedrock' | 'anthropic' | 'google';
  model?: string;
  prompt_content?: string;
  system_prompt?: string;
  temperature?: number;
  max_tokens?: number;
  writes_state_key?: string;
  sets?: { [key: string]: any };
  config?: { [key: string]: any };  // nested config object for complex nodes
  position?: { x: number; y: number };
  hitp?: boolean;  // Human-in-the-loop flag
  // Parallel support
  branches?: Array<{ branch_id?: string; nodes?: BackendNode[]; sub_workflow?: { nodes: BackendNode[] } }>;
  // For-each support
  items_path?: string;
  input_list_key?: string;
  sub_workflow?: { nodes: BackendNode[] };
  item_key?: string;
  output_key?: string;
  max_iterations?: number;
  // Subgraph support
  subgraph_ref?: string;
  subgraph_inline?: { nodes: BackendNode[]; edges: BackendEdge[] };
  [key: string]: any;
}

export interface BackendEdge {
  // 백엔드 실제 지원 타입 (builder.py line 747-770 참조)
  type: 'edge' | 'normal' | 'flow' | 'if' | 'hitp' | 'human_in_the_loop' | 'pause' | 'conditional_edge' | 'start' | 'end';
  source: string;
  target: string;
  condition?: string | { lhs: string; op: string; rhs: string };
  // conditional_edge support
  router_func?: string;  // router function name registered in NODE_REGISTRY
  mapping?: { [key: string]: string };  // router return value -> target node mapping
  [key: string]: any;
}

export interface BackendSecret {
  provider: 'secretsmanager' | 'ssm';
  name: string;
  target: string;
}

// 프론트엔드 노드를 백엔드 형식으로 변환
export const convertNodeToBackendFormat = (node: any): BackendNode => {
  const backendNode: BackendNode = {
    id: node.id,
    type: 'operator', // 기본값
  };

  switch (node.type) {
    case 'aiModel':
      backendNode.type = 'llm_chat';
      backendNode.provider = node.data.provider || 'openai';
      backendNode.model = node.data.model || 'gpt-4';
      backendNode.prompt_content = node.data.prompt_content || node.data.prompt || '';
      backendNode.system_prompt = node.data.system_prompt;
      backendNode.temperature = node.data.temperature || 0.7;
      backendNode.max_tokens = node.data.max_tokens || node.data.maxTokens || 256;
      backendNode.writes_state_key = node.data.writes_state_key;
      // Tool definitions for function calling
      if (node.data.tools && Array.isArray(node.data.tools) && node.data.tools.length > 0) {
        backendNode.tool_definitions = node.data.tools.map((tool: any) => ({
          name: tool.name,
          description: tool.description || '',
          parameters: tool.parameters || {},
          required_api_keys: tool.required_api_keys || [],
          handler_type: tool.handler_type,
          handler_config: tool.handler_config,
          // Skill reference for backend resolution
          skill_id: tool.skill_id,
          skill_version: tool.skill_version,
        }));
      }
      break;
    case 'operator':
      backendNode.type = 'operator';
      backendNode.sets = node.data.sets || {};
      // api_call, db_query, safe_operator는 operatorType으로 분기
      if (node.data.operatorType === 'api_call') {
        backendNode.type = 'api_call';
        backendNode.config = {
          url: node.data.url || '',
          method: node.data.method || 'GET',
          headers: node.data.headers || {},
          params: node.data.params || {},
          json: node.data.json || node.data.body,
          timeout: node.data.timeout || 10,
        };
      } else if (node.data.operatorType === 'database' || node.data.operatorType === 'db_query') {
        backendNode.type = 'db_query';
        backendNode.config = {
          query: node.data.query || '',
          connection_string: node.data.connection_string || node.data.connectionString,
        };
      } else if (node.data.operatorType === 'safe_operator' || node.data.operatorType === 'operator_official') {
        backendNode.type = 'safe_operator';
        backendNode.config = {
          strategy: node.data.strategy || 'list_filter',
          input_key: node.data.input_key,
          params: node.data.params || {},
          output_key: node.data.output_key,
        };
      }
      break;
    case 'trigger':
      backendNode.type = 'operator';
      backendNode.sets = {
        _frontend_type: 'trigger',
        trigger_type: node.data.triggerType || 'request',
        triggerHour: node.data.triggerHour,
        triggerMinute: node.data.triggerMinute
      };
      break;
    case 'control':
      // Control type → 백엔드 타입으로 자동 분류
      const controlType = node.data.controlType || 'loop';
      
      if (controlType === 'for_each') {
        // for_each: 리스트 병렬 반복 (ThreadPoolExecutor)
        backendNode.type = 'for_each';
        backendNode.config = {
          items_path: node.data.items_path || node.data.itemsPath || 'state.items',
          item_key: node.data.item_key || node.data.itemKey || 'item',
          output_key: node.data.output_key || node.data.outputKey || 'for_each_results',
          max_iterations: node.data.max_iterations || node.data.maxIterations || 20,
          sub_workflow: node.data.sub_workflow || { nodes: [] },
        };
      } else if (controlType === 'loop') {
        // loop: 조건 기반 순차 반복 (convergence 지원)
        backendNode.type = 'loop';
        backendNode.config = {
          nodes: node.data.sub_workflow?.nodes || [],
          condition: node.data.condition || node.data.whileCondition || 'false',
          max_iterations: node.data.max_iterations || node.data.maxIterations || 5,
          loop_var: node.data.loop_var || 'loop_index',
          convergence_key: node.data.convergence_key,
          target_score: node.data.target_score || 0.9,
        };
      } else if (controlType === 'parallel') {
        // parallel: 병렬 브랜치 실행
        backendNode.type = 'parallel_group';
        backendNode.config = {
          branches: node.data.branches || [],
        };
      } else if (controlType === 'aggregator') {
        // aggregator: 병렬/반복 결과 집계 (토큰 사용량 포함)
        backendNode.type = 'aggregator';
        backendNode.config = {
          // aggregator_runner는 state에서 자동으로 branch_token_details, for_each_result 등을 스캔
          // 추가 설정이 필요한 경우 여기에 포함
          strategy: node.data.strategy || 'auto', // merge, concat, sum 등
          sources: node.data.sources || [], // 특정 소스 지정 (선택사항)
          output_key: node.data.output_key || 'aggregated_result',
        };
      } else if (controlType === 'conditional') {
        // conditional: route_condition 노드로 변환
        backendNode.type = 'route_condition';
        backendNode.config = {
          conditions: node.data.conditions || [],
          default_node: node.data.default_node || node.data.defaultNode,
          evaluation_mode: node.data.evaluation_mode || 'first_match',
        };
      } else if (controlType === 'human' || controlType === 'branch') {
        // human: HITL (Human-in-the-Loop) 엣지로만 표현
        // branch: Graph Analysis용 가상 노드 (conditional_edge로 변환됨)
        return null;
      } else {
        // 기타 control (while 등): operator로 저장하고 엣지로 처리
        backendNode.type = 'operator';
        backendNode.sets = {
          _frontend_type: 'control',
          control_type: controlType,
          whileCondition: node.data.whileCondition,
          max_iterations: node.data.max_iterations || 10
        };
      }
      break;
    case 'group':
      // group: 서브그래프
      backendNode.type = 'subgraph';
      backendNode.subgraph_ref = node.data.subgraph_ref || node.data.subgraphRef || node.data.groupId;
      backendNode.subgraph_inline = node.data.subgraph_inline || node.data.subgraphInline;
      break;
    default:
      backendNode.type = 'operator';
      backendNode.sets = node.data?.sets || {};
  }

  // Persist position if available so frontend layout is preserved
  if (node.position) {
    backendNode.position = node.position;
  }

  return backendNode;
};

// 조건 문자열을 구조화된 형태로 변환 시도
const tryParseCondition = (condition: string): { lhs: string; op: string; rhs: string } | null => {
  if (!condition || typeof condition !== 'string') {
    return null;
  }

  // 단순한 조건만 파싱 (복합 조건은 문자열 그대로 반환)
  const simplePattern = /^\s*([^<>=!]+?)\s*(==|!=|>=|<=|>|<)\s*(.+?)\s*$/;
  const match = condition.trim().match(simplePattern);

  if (match) {
    const lhs = match[1].trim();
    const op = match[2].trim();
    let rhs = match[3].trim();

    // 따옴표로 감싸진 문자열인 경우에만 따옴표 제거
    if ((rhs.startsWith('"') && rhs.endsWith('"')) || (rhs.startsWith("'") && rhs.endsWith("'"))) {
      rhs = rhs.slice(1, -1);
    }

    return { lhs, op, rhs };
  }

  return null;
};

// 프론트엔드 엣지를 백엔드 형식으로 변환
export const convertEdgeToBackendFormat = (edge: any, nodes: any[]): BackendEdge | null => {
  const sourceNode = nodes.find(n => n.id === edge.source);
  const targetNode = nodes.find(n => n.id === edge.target);

  // HITL: source가 control/human 노드인 경우
  if (sourceNode?.type === 'control' && sourceNode?.data?.controlType === 'human') {
    return {
      type: 'hitp',
      source: edge.source,
      target: edge.target,
    };
  }

  // Conditional Branch: source가 control/branch 또는 control/conditional 노드인 경우
  if (sourceNode?.type === 'control' && 
      (sourceNode?.data?.controlType === 'branch' || sourceNode?.data?.controlType === 'conditional')) {
    return {
      type: 'conditional_edge',
      source: edge.source,
      target: edge.target,
      router_func: sourceNode.data.router_func || 'route_by_condition',
      mapping: sourceNode.data.mapping || edge.data?.mapping || {},
      condition: sourceNode.data.condition || edge.data?.condition,
    };
  }

  const backendEdge: BackendEdge = {
    type: 'edge', // 기본값
    source: edge.source,
    target: edge.target,
  };

  // 1. 엣지의 edgeType 데이터 우선 사용 (SmartEdge에서 설정한 값)
  const edgeType = edge.data?.edgeType;
  if (edgeType && ['edge', 'normal', 'flow', 'if', 'hitp', 'human_in_the_loop', 'pause', 'conditional_edge', 'start', 'end'].includes(edgeType)) {
    backendEdge.type = edgeType;
    
    // 조건이 필요한 타입인 경우 condition 추가
    if ((edgeType === 'if' || edgeType === 'conditional_edge') && edge.data?.condition) {
      backendEdge.condition = tryParseCondition(edge.data.condition) || edge.data.condition;
    }
    
    return backendEdge;
  }

  // 2. 레거시 호환: 엣지 타입이나 조건으로 추론
  if (edge.type === 'default' || edge.type === 'smoothstep') {
    backendEdge.type = 'edge';
  } else if (edge.data?.condition) {
    backendEdge.type = 'if';
    backendEdge.condition = tryParseCondition(edge.data.condition) || edge.data.condition;
  }
  
  return backendEdge;
};

// 그래프 분석 기반으로 사이클(back-edge)을 loop 노드로 변환
// 주의: Cycle은 for_each가 아닌 loop 노드로 표현됨
// - loop: 조건이 참인 동안 서브 노드들을 순차 반복 (condition-based)
// - for_each: 리스트의 각 항목에 대해 sub_workflow 병렬 실행 (list-based)
const convertCycleToLoopNode = (cycle: CycleInfo, nodes: any[], edges: any[]): BackendNode => {
  const backEdge = cycle.backEdge;
  
  // 조건 찾기: back-edge나 source 노드에서 조건 추출
  const sourceNode = nodes.find((n: any) => n.id === backEdge.source);
  const condition = 
    backEdge.data?.condition ||
    sourceNode?.data?.whileCondition ||
    sourceNode?.data?.condition ||
    'false'; // 기본값: false (조건이 true가 되면 탈출)
  
  // max_iterations 찾기
  const maxIterations = 
    backEdge.data?.max_iterations ||
    sourceNode?.data?.max_iterations ||
    sourceNode?.data?.maxIterations ||
    5; // loop_runner 기본값

  // 사이클 내부의 노드들을 서브 노드로 변환
  const loopNodeIds = cycle.loopNodes || cycle.path;
  const loopNodes = nodes
    .filter((n: any) => loopNodeIds.includes(n.id))
    .map((node: any) => convertNodeToBackendFormat(node))
    .filter((n: any) => n !== null);

  return {
    id: `loop_${cycle.id}`,
    type: 'loop',
    config: {
      nodes: loopNodes,
      condition: typeof condition === 'string' ? condition : JSON.stringify(condition),
      max_iterations: maxIterations,
      loop_var: 'loop_index', // loop_runner 기본값
    },
  };
};

// 그래프 분석 기반으로 병렬/조건부 분기를 노드로 변환
// 주의: 조건부 분기(conditional)는 parallel_group이 아닌 conditional_edge로 변환해야 함
const convertParallelGroupToNode = (parallelGroup: ParallelGroup, nodes: any[], edges: any[]): BackendNode | null => {
  // 조건부 분기는 노드가 아닌 엣지로 처리해야 함
  // parallel_group은 모든 브랜치를 병렬 실행하는 것임
  if (parallelGroup.branchType === 'conditional') {
    // conditional은 노드로 변환하지 않음 - convertWorkflowToBackendFormat에서 엣지로 처리
    return null;
  }

  const branches = parallelGroup.branches.map((branchNodeIds, index) => {
    // 각 브랜치의 노드들을 백엔드 형식으로 변환
    const branchNodes = nodes.filter((n: any) => branchNodeIds.includes(n.id));
    const convertedNodes = branchNodes.map((node: any) => convertNodeToBackendFormat(node)).filter((n: any) => n !== null);
    
    return {
      branch_id: `branch_${index}`,
      nodes: convertedNodes,
    };
  });

  // 고유 ID 사용 (graphAnalysis에서 생성됨)
  // 백엔드 NodeModel은 branches를 최상위 필드로 지원 (line 282)
  return {
    id: parallelGroup.id,
    type: 'parallel_group',
    branches,  // 최상위 필드로 이동
    config: {
      convergence_node: parallelGroup.convergenceNodeId,
      // 중첩 정보
      _depth: parallelGroup.depth,
      _parentGroupId: parallelGroup.parentGroupId,
    },
  };
};

// 조건부 분기를 conditional_edge로 변환
const convertConditionalBranchToEdges = (
  parallelGroup: ParallelGroup, 
  nodes: any[], 
  edges: any[]
): BackendEdge[] => {
  if (parallelGroup.branchType !== 'conditional') {
    return [];
  }

  const sourceNodeId = parallelGroup.sourceNodeId;
  const conditionalEdges: BackendEdge[] = [];

  // 각 브랜치별로 conditional_edge 생성
  parallelGroup.branches.forEach((branchNodeIds, index) => {
    if (branchNodeIds.length === 0) return;

    const targetNodeId = branchNodeIds[0]; // 브랜치의 첫 번째 노드
    const branchEdge = parallelGroup.branchEdges[index];
    const condition = branchEdge?.data?.condition as string | undefined;

    conditionalEdges.push({
      type: 'conditional_edge',
      source: sourceNodeId,
      target: targetNodeId,
      condition: condition || `branch_${index}`,
      // mapping은 라우터 함수 사용 시 필요
      // router_func가 없으면 condition 값으로 직접 분기
    });
  });

  return conditionalEdges;
};

// 전체 워크플로우를 백엔드 형식으로 변환 (그래프 분석 적용)
export const convertWorkflowToBackendFormat = (workflow: any): BackendWorkflow => {
  const nodes = workflow.nodes || [];
  const edges = workflow.edges || [];
  
  // 1. 그래프 분석 수행
  const analysisResult = analyzeWorkflowGraph(nodes, edges);
  
  // 1-1. Display warnings to user if any
  if (analysisResult.warnings.length > 0) {
    analysisResult.warnings.forEach(warning => {
      const nodeList = warning.nodeIds.slice(0, 3).join(', ') + 
        (warning.nodeIds.length > 3 ? ` +${warning.nodeIds.length - 3} more` : '');
      
      toast.warning(`${warning.message}\nNodes: ${nodeList}`, {
        description: warning.suggestion,
        duration: 5000,
      });
    });
  }
  
  // 2. 분석 결과 기반으로 제외할 노드들 수집
  const excludedNodeIds = new Set<string>();
  
  // 사이클 내부 노드들 (loop 노드로 흡수됨)
  analysisResult.cycles.forEach(cycle => {
    const loopNodeIds = cycle.loopNodes || cycle.path;
    loopNodeIds.forEach(nodeId => excludedNodeIds.add(nodeId));
  });
  
  // 병렬 그룹에 포함된 노드들 (parallel_group으로 변환됨)
  analysisResult.parallelGroups.forEach(pg => {
    pg.branches.forEach(branch => {
      branch.forEach(nodeId => excludedNodeIds.add(nodeId));
    });
  });
  
  // HITL/Branch control 노드 (edge로만 표현됨)
  // 주의: 'branch'는 Graph Analysis용 가상 노드 (제거됨)
  // 'conditional'은 route_condition 노드용 (실제 노드로 유지됨)
  nodes.forEach((node: any) => {
    if (node.type === 'control') {
      const controlType = node.data?.controlType;
      if (controlType === 'human' || controlType === 'branch') {
        excludedNodeIds.add(node.id);
      }
    }
  });
  
  // 3. 제외되지 않은 노드들 변환
  const regularBackendNodes = nodes
    .filter((node: any) => !excludedNodeIds.has(node.id))
    .map((node: any) => convertNodeToBackendFormat(node))
    .filter((n: any) => n !== null);
  
  // 4. 사이클 → loop 노드 생성
  // 주의: cycle은 loop 노드로 변환됨 (조건 기반 반복)
  // for_each는 명시적 control 노드로만 생성됨 (리스트 기반 병렬 반복)
  const loopNodes = analysisResult.cycles.map(cycle => 
    convertCycleToLoopNode(cycle, nodes, edges)
  );
  
  // 5. 병렬 그룹 → parallel_group 노드 생성 (conditional은 제외됨)
  const parallelGroupNodes = analysisResult.parallelGroups.map(pg =>
    convertParallelGroupToNode(pg, nodes, edges)
  ).filter(n => n !== null);
  
  // 6. 조건부 분기 → conditional_edge 생성
  const conditionalEdges = analysisResult.parallelGroups
    .filter(pg => pg.branchType === 'conditional')
    .flatMap(pg => convertConditionalBranchToEdges(pg, nodes, edges));
  
  // 7. 모든 백엔드 노드 합치기
  const backendNodes = [
    ...regularBackendNodes,
    ...loopNodes,
    ...parallelGroupNodes,
  ];
  
  // 8. 엣지 변환 (사이클 back-edge, 병렬 분기 엣지 필터링)
  const backendEdges = edges
    .filter((edge: any) => {
      // back-edge는 제외 (loop 노드 내부로 흡수됨)
      if (analysisResult.backEdgeIds.has(edge.id)) return false;
      
      // source나 target이 제외된 노드인 경우
      if (excludedNodeIds.has(edge.source) && excludedNodeIds.has(edge.target)) {
        // 둘 다 같은 그룹에 속하면 제외
        return false;
      }
      
      return true;
    })
    .map((edge: any) => {
      // HITL/Conditional control 노드를 우회하도록 엣지 재연결
      let actualSource = edge.source;
      let actualTarget = edge.target;
      
      const sourceNode = nodes.find((n: any) => n.id === edge.source);
      
      // source가 human control 노드면 → hitp edge
      if (sourceNode?.type === 'control' && sourceNode?.data?.controlType === 'human') {
        // 이 노드로 들어오는 엣지의 source를 찾아서 연결
        const incomingEdge = edges.find((e: any) => e.target === edge.source);
        if (incomingEdge) {
          actualSource = incomingEdge.source;
        }
        return {
          type: 'hitp',
          source: actualSource,
          target: actualTarget,
        };
      }
      
      // source가 conditional control 노드면 → conditional_edge
      if (sourceNode?.type === 'control' && 
          (sourceNode?.data?.controlType === 'branch' || sourceNode?.data?.controlType === 'conditional')) {
        const incomingEdge = edges.find((e: any) => e.target === edge.source);
        if (incomingEdge) {
          actualSource = incomingEdge.source;
        }
        return {
          type: 'conditional_edge',
          source: actualSource,
          target: actualTarget,
          router_func: sourceNode.data.router_func || 'route_by_condition',
          mapping: sourceNode.data.mapping || edge.data?.mapping || {},
          condition: sourceNode.data.condition || edge.data?.condition,
        };
      }
      
      // 일반 엣지 변환
      return convertEdgeToBackendFormat(edge, nodes);
    })
    .filter((e: any) => e !== null);

  // 9. 조건부 엣지 추가
  const allBackendEdges = [...backendEdges, ...conditionalEdges];

  // secrets 배열 변환 (프론트엔드에 secrets가 있는 경우)
  const backendSecrets = workflow.secrets?.map((secret: any) => ({
    provider: secret.provider || 'secretsmanager',
    name: secret.name,
    target: secret.target,
  })) || [];

  const result: BackendWorkflow = {
    name: workflow.name || 'untitled',
    nodes: backendNodes,
    edges: allBackendEdges,
  };

  // secrets가 있는 경우에만 추가
  if (backendSecrets.length > 0) {
    result.secrets = backendSecrets;
  }

  // subgraphs가 있는 경우 포함 (그룹 노드 지원)
  if (workflow.subgraphs && Object.keys(workflow.subgraphs).length > 0) {
    // 각 서브그래프의 노드/엣지도 백엔드 형식으로 변환
    const convertedSubgraphs: Record<string, SubgraphDefinition> = {};

    for (const [key, subgraph] of Object.entries(workflow.subgraphs as Record<string, any>)) {
      // _root는 내부 네비게이션용이므로 백엔드에 전송하지 않음
      if (key === '_root') continue;

      convertedSubgraphs[key] = {
        id: subgraph.id || key,
        nodes: subgraph.nodes?.map((node: any) => convertNodeToBackendFormat(node)) || [],
        edges: subgraph.edges?.map((edge: any) => convertEdgeToBackendFormat(edge, subgraph.nodes)) || [],
        metadata: subgraph.metadata || { name: key },
        input_schema: subgraph.input_schema,
        output_schema: subgraph.output_schema,
      };
    }

    if (Object.keys(convertedSubgraphs).length > 0) {
      result.subgraphs = convertedSubgraphs;
    }
  }

  return result;
};

// 백엔드 데이터를 프론트엔드 형식으로 변환 (로드 시 사용)
export const convertWorkflowFromBackendFormat = (backendWorkflow: any): any => {
  if (!backendWorkflow || (!backendWorkflow.nodes && !backendWorkflow.edges)) {
    return { nodes: [], edges: [] };
  }

  // Check if data is already in frontend format (has nodes with frontend types)
  const hasNodes = backendWorkflow.nodes && Array.isArray(backendWorkflow.nodes);
  if (hasNodes && backendWorkflow.nodes.length > 0) {
    const firstNode = backendWorkflow.nodes[0];
    // If the node has frontend-specific properties, assume it's already converted
    if (firstNode.type && ['aiModel', 'operator', 'trigger', 'control'].includes(firstNode.type) && firstNode.data) {
      console.log('Data appears to be in frontend format, returning as-is');
      return {
        name: backendWorkflow.name || 'Generated Workflow',
        nodes: backendWorkflow.nodes,
        edges: backendWorkflow.edges || [],
        secrets: backendWorkflow.secrets,
      };
    }
  }

  const frontendNodes = backendWorkflow.nodes?.map((node: BackendNode, index: number) => {
    // 백엔드 타입을 프론트엔드 타입으로 매핑
    let frontendType = 'operator';
    let label = 'Block';
    let nodeData: any = {};

    switch (node.type) {
      case 'llm_chat':
        frontendType = 'aiModel';
        label = 'AI Model';
        nodeData = {
          label,
          prompt_content: node.prompt_content,
          prompt: node.prompt_content,
          system_prompt: node.system_prompt,
          temperature: node.temperature,
          max_tokens: node.max_tokens,
          maxTokens: node.max_tokens,
          model: node.model,
          provider: node.provider,
          writes_state_key: node.writes_state_key,
          // Restore tool definitions
          tools: node.tool_definitions || [],
          toolsCount: node.tool_definitions?.length || 0,
        };
        break;
      case 'operator':
        // sets 내용을 확인하여 프론트엔드 타입 복원
        if (node.sets?._frontend_type === 'trigger') {
          frontendType = 'trigger';
          label = 'Trigger';
          nodeData = {
            label,
            triggerType: node.sets.trigger_type,
            triggerHour: node.sets.triggerHour,
            triggerMinute: node.sets.triggerMinute,
          };
        } else if (node.sets?._frontend_type === 'control') {
          frontendType = 'control';
          label = 'Control';
          nodeData = {
            label,
            controlType: node.sets.control_type,
            whileCondition: node.sets.whileCondition,
            max_iterations: node.sets.max_iterations,
          };
        } else {
          frontendType = 'operator';
          label = 'Operator';
          nodeData = {
            label,
            sets: node.sets,
          };
        }
        break;
      case 'trigger':
        // 백엔드에서 type이 trigger로 저장된 경우 (save_workflow.py의 정규화 로직)
        frontendType = 'trigger';
        label = 'Trigger';
        nodeData = {
          label,
          triggerType: node.sets?.trigger_type || 'request',
          triggerHour: node.sets?.triggerHour,
          triggerMinute: node.sets?.triggerMinute,
          // sets의 다른 속성들도 보존
          ...node.sets
        };
        break;
      case 'loop':
        // loop → control(loop)
        frontendType = 'control';
        label = 'Loop';
        const loopConfig = node.config || {};
        nodeData = {
          label,
          controlType: 'loop',
          condition: loopConfig.condition,
          whileCondition: loopConfig.condition,
          max_iterations: loopConfig.max_iterations || 5,
          sub_workflow: { nodes: loopConfig.nodes || [] },
        };
        break;
      case 'for_each':
        // for_each → control(for_each)
        frontendType = 'control';
        label = 'For Each';
        const forEachConfig = node.config || {};
        nodeData = {
          label,
          controlType: 'for_each',
          items_path: forEachConfig.items_path || node.items_path,
          itemsPath: forEachConfig.items_path || node.items_path,
          item_key: forEachConfig.item_key || node.item_key || 'item',
          output_key: forEachConfig.output_key || node.output_key || 'for_each_results',
          max_iterations: forEachConfig.max_iterations || node.max_iterations || 20,
          sub_workflow: forEachConfig.sub_workflow || node.sub_workflow,
        };
        break;
      case 'parallel_group':
        // parallel_group → control(parallel)
        frontendType = 'control';
        label = 'Parallel';
        const parallelConfig = node.config || {};
        nodeData = {
          label,
          controlType: 'parallel',
          branches: parallelConfig.branches || node.branches || [],
        };
        break;
      case 'aggregator':
        // aggregator → control(aggregator)
        frontendType = 'control';
        label = 'Aggregator';
        const aggregatorConfig = node.config || {};
        nodeData = {
          label,
          controlType: 'aggregator',
          strategy: aggregatorConfig.strategy || 'auto',
          sources: aggregatorConfig.sources || [],
          output_key: aggregatorConfig.output_key || 'aggregated_result',
        };
        break;
      case 'subgraph':
        // subgraph → group
        frontendType = 'group';
        label = 'Group';
        nodeData = {
          label,
          subgraph_ref: node.subgraph_ref || node.config?.subgraph_ref,
          subgraphRef: node.subgraph_ref || node.config?.subgraph_ref,
          subgraph_inline: node.subgraph_inline || node.config?.subgraph_inline,
        };
        break;
      case 'route_condition':
        // route_condition → control(conditional)
        frontendType = 'control';
        label = 'Route Condition';
        const routeConfig = node.config || {};
        nodeData = {
          label,
          controlType: 'conditional',
          conditions: routeConfig.conditions || [],
          default_node: routeConfig.default_node,
          defaultNode: routeConfig.default_node,
          evaluation_mode: routeConfig.evaluation_mode || 'first_match',
        };
        break;
      case 'api_call':
        // api_call → operator(api_call)
        frontendType = 'operator';
        label = 'API Call';
        const apiConfig = node.config || {};
        nodeData = {
          label,
          operatorType: 'api_call',
          url: apiConfig.url || node.url || '',
          method: apiConfig.method || node.method || 'GET',
          headers: apiConfig.headers || node.headers || {},
          params: apiConfig.params || node.params || {},
          json: apiConfig.json || node.json,
          timeout: apiConfig.timeout || node.timeout || 10,
        };
        break;
      case 'db_query':
        // db_query → operator(database)
        frontendType = 'operator';
        label = 'Database Query';
        const dbConfig = node.config || {};
        nodeData = {
          label,
          operatorType: 'database',
          query: dbConfig.query || node.query || '',
          connection_string: dbConfig.connection_string || node.connection_string,
        };
        break;
      default:
        // 기타 런타임 전용 타입(vision, skill_executor 등)은 일반 operator로 표시
        frontendType = 'operator';
        label = node.type || 'Block';
        nodeData = { label, ...(node.config || node.sets || {}) };
    }

    return {
      id: node.id,
      type: frontendType,
      // Preserve stored position when available, otherwise fall back to grid placement
      position: node.position || { x: (index % 3) * 200 + 100, y: Math.floor(index / 3) * 150 + 100 },
      data: nodeData,
    };
  }) || [];

  const frontendEdges = backendWorkflow.edges?.map((edge: BackendEdge, index: number) => ({
    id: `edge-${index}`,
    source: edge.source,
    target: edge.target,
    type: 'smoothstep',
    animated: true,
    style: {
      stroke: edge.type === 'if' ? 'hsl(142 76% 36%)' : edge.type === 'hitp' ? 'hsl(38 92% 50%)' : 'hsl(263 70% 60%)',
      strokeWidth: 2
    },
    data: {
      condition: edge.condition,
      edgeType: edge.type, // 백엔드 엣지 타입을 프론트엔드에서 사용할 수 있도록 저장
    },
  })) || [];

  const result: any = {
    name: backendWorkflow.name,
    nodes: frontendNodes,
    edges: frontendEdges,
  };

  // secrets가 있는 경우 복원
  if (backendWorkflow.secrets && backendWorkflow.secrets.length > 0) {
    result.secrets = backendWorkflow.secrets;
  }

  return result;
};