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
    | 'llm'            // LLM 평가 노드 (자동 생성)
    | 'llm_chat'       // aiModel → llm_chat
    | 'loop'           // cycle (back-edge) → loop
    | 'for_each'       // control(loop/for_each) → for_each
    | 'parallel_group' // control(parallel) → parallel_group
    | 'parallel'       // parallel (alias for parallel_group)
    | 'aggregator'     // control(aggregator) → aggregator (token aggregation)
    | 'subgraph'       // group → subgraph
    | 'api_call'       // operator(api_call) → api_call
    | 'db_query'       // operator(db_query) → db_query
    | 'safe_operator'  // operator(safe_operator) → safe_operator
    | 'operator_official' // operator_official flag setter
    | 'route_condition' // control(conditional) → route_condition
    | 'trigger';       // trigger → trigger (백엔드에서 정규화됨)
  label?: string;
  action?: string;
  hitp?: boolean;  // Human-in-the-loop flag
  config?: { [key: string]: any };  // nested config object for complex nodes
  position?: { x: number; y: number };
  // Parallel support
  branches?: Array<{ branch_id?: string; nodes?: BackendNode[]; sub_workflow?: { nodes: BackendNode[] } }>;
  resource_policy?: { [key: string]: any };
  // Subgraph support
  subgraph_ref?: string;
  subgraph_inline?: { nodes: BackendNode[]; edges: BackendEdge[] };
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
      backendNode.label = node.data?.label || 'AI Model';
      // prompt_content 기본값: 비어있으면 자동으로 기본 프롬프트 설정
      const promptContent = node.data.prompt_content || node.data.prompt || '';
      const finalPromptContent = promptContent.trim() || 'Please perform the task based on the current workflow state and context.';
      
      backendNode.config = {
        provider: node.data.provider || 'openai',
        model: node.data.model || 'gpt-4',
        prompt_content: finalPromptContent,
        system_prompt: node.data.system_prompt,
        temperature: node.data.temperature || 0.7,
        max_tokens: node.data.max_tokens || node.data.maxTokens || 256,
        writes_state_key: node.data.writes_state_key,
      };
      // Tool definitions for function calling
      if (node.data.tools && Array.isArray(node.data.tools) && node.data.tools.length > 0) {
        backendNode.config.tool_definitions = node.data.tools.map((tool: any) => ({
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
      backendNode.label = node.data?.label || 'Operator';
      backendNode.config = {
        sets: node.data.sets || {},
      };
      // api_call, db_query, safe_operator는 operatorType으로 분기
      if (node.data.operatorType === 'api_call') {
        backendNode.type = 'api_call';
        backendNode.label = node.data?.label || 'API Call';
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
        backendNode.label = node.data?.label || 'Database Query';
        backendNode.config = {
          query: node.data.query || '',
          connection_string: node.data.connection_string || node.data.connectionString,
        };
      } else if (node.data.operatorType === 'safe_operator' || node.data.operatorType === 'operator_official') {
        backendNode.type = 'safe_operator';
        backendNode.label = node.data?.label || 'Safe Operator';
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
      backendNode.label = node.data?.label || 'Trigger';
      backendNode.config = {
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
        backendNode.label = node.data?.label || 'For Each';
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
        backendNode.label = node.data?.label || 'Loop';
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
        backendNode.label = node.data?.label || 'Parallel';
        backendNode.config = {
          branches: node.data.branches || [],
        };
      } else if (controlType === 'aggregator') {
        // aggregator: 병렬/반복 결과 집계 (토큰 사용량 포함)
        // aggregator_runner는 state에서 자동으로 병합 (설정 불필요)
        backendNode.type = 'aggregator';
        backendNode.label = node.data?.label || 'Aggregator';
        backendNode.config = {};
      } else if (controlType === 'conditional') {
        // conditional: route_condition 노드로 변환
        backendNode.type = 'route_condition';
        backendNode.label = node.data?.label || 'Route Condition';
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
        backendNode.label = node.data?.label || 'Control';
        backendNode.config = {
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
      backendNode.label = node.data?.label || 'Subgraph';
      backendNode.subgraph_ref = node.data.subgraph_ref || node.data.subgraphRef || node.data.groupId;
      backendNode.subgraph_inline = node.data.subgraph_inline || node.data.subgraphInline;
      break;
    case 'control_block':
      // Control Block: UI 전용 노드, 백엔드 변환 시 제외됨
      // Control Block은 엣지로 변환되어 conditional_edge/parallel_group/loop로 처리됨
      return null;
    default:
      backendNode.type = 'operator';
      backendNode.label = node.data?.label || 'Operator';
      backendNode.config = {
        sets: node.data?.sets || {}
      };
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

// 그래프 분석 기반으로 사이클(back-edge)을 loop 또는 for_each 노드로 변환
// back-edge의 edgeType에 따라 구분:
// - edgeType === 'for_each': 리스트의 각 항목에 대해 서브워크플로우 병렬 실행 (데이터 병렬화)
// - edgeType === 'while' (또는 기본값): 조건이 참인 동안 서브 노드들을 순차 반복 (조건 기반)
const convertCycleToLoopNode = (cycle: CycleInfo, nodes: any[], edges: any[]): BackendNode => {
  const backEdge = cycle.backEdge;
  const edgeType = backEdge.data?.edgeType as string | undefined;
  
  // back-edge 타입에 따라 노드 타입 결정
  const isForEach = edgeType === 'for_each';
  
  if (isForEach) {
    // For Each 노드 생성 (병렬 처리)
    const itemsPath = backEdge.data?.items_path || 'state.items';
    const itemKey = backEdge.data?.item_key || 'item';
    const maxIterations = backEdge.data?.max_iterations || 100;
    
    // 사이클 내부의 노드들을 서브 노드로 변환
    const loopNodeIds = cycle.loopNodes || cycle.path;
    const subNodes = nodes
      .filter((n: any) => loopNodeIds.includes(n.id))
      .map((node: any) => convertNodeToBackendFormat(node))
      .filter((n: any) => n !== null);

    return {
      id: `for_each_${cycle.id}`,
      type: 'for_each',
      config: {
        items_path: itemsPath,
        item_key: itemKey,
        max_iterations: maxIterations,
        sub_workflow: {
          nodes: subNodes,
        },
      },
    };
  } else {
    // While Loop 노드 생성 (순차 처리)
    const sourceNode = nodes.find((n: any) => n.id === backEdge.source);
    const naturalCondition = backEdge.data?.natural_condition;
    const evalMode = backEdge.data?.eval_mode;
    
    // 자연어 조건이 있으면 LLM 평가 기반, 없으면 표현식 기반
    const condition = 
      (evalMode === 'natural_language' && naturalCondition) 
        ? 'state.__loop_should_exit == true'  // 숨겨진 평가 노드가 설정하는 플래그
        : (backEdge.data?.condition ||
           sourceNode?.data?.whileCondition ||
           sourceNode?.data?.condition ||
           'false'); // 기본값: false (조건이 true가 되면 탈출)
    
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
    
    // 🤖 자연어 조건이 있으면 숨겨진 LLM 평가 노드 자동 추가
    if (evalMode === 'natural_language' && naturalCondition) {
      loopNodes.push({
        id: `__loop_condition_evaluator_${cycle.id}`,
        type: 'llm',
        label: 'Loop Condition Evaluator',
        config: {
          model: 'gemini-2.0-flash-exp',
          system_message: `You are a condition evaluator. Evaluate the following condition based on the current workflow state and return ONLY a JSON object.\n\nCondition to evaluate: "${naturalCondition}"\n\nAnalyze the current state and determine if this condition is satisfied.\n\nReturn format: {"should_exit": true/false, "reason": "brief explanation"}`,
          output_key: '__loop_condition_result',
          response_format: 'json',
          temperature: 0.1,
        },
      });
      
      // 평가 결과를 플래그로 변환하는 operator_official 노드 추가
      loopNodes.push({
        id: `__loop_flag_setter_${cycle.id}`,
        type: 'operator_official',
        label: 'Loop Flag Setter',
        config: {
          strategy: 'deep_get',
          input_key: '__loop_condition_result.should_exit',
          output_key: '__loop_should_exit',
        },
      });
    }

    return {
      id: `loop_${cycle.id}`,
      type: 'loop',
      label: 'Loop',
      config: {
        nodes: loopNodes,
        condition: typeof condition === 'string' ? condition : JSON.stringify(condition),
        max_iterations: maxIterations,
        loop_var: 'loop_index', // loop_runner 기본값
      },
    };
  }
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
    label: 'Parallel Group',
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
): { edges: BackendEdge[], routerNode?: BackendNode } => {
  if (parallelGroup.branchType !== 'conditional') {
    return { edges: [] };
  }

  const sourceNodeId = parallelGroup.sourceNodeId;
  
  // 자연어 조건이 있는지 확인
  const hasNaturalLanguageConditions = parallelGroup.branchEdges.some(
    edge => edge?.data?.natural_condition
  );
  
  // Mapping 생성: branch_X → target node
  const mapping: Record<string, string> = {};
  const naturalConditions: Array<{ condition: string; branch: string }> = [];
  
  parallelGroup.branches.forEach((branchNodeIds, index) => {
    if (branchNodeIds.length === 0) return;
    
    const targetNodeId = branchNodeIds[0];
    const branchKey = `branch_${index}`;
    mapping[branchKey] = targetNodeId;
    
    const branchEdge = parallelGroup.branchEdges[index];
    const naturalCondition = branchEdge?.data?.natural_condition as string | undefined;
    
    if (naturalCondition) {
      naturalConditions.push({
        condition: naturalCondition,
        branch: branchKey
      });
    }
  });
  
  // Default branch 추가
  if (parallelGroup.branches.length > 0) {
    const lastBranch = parallelGroup.branches[parallelGroup.branches.length - 1];
    if (lastBranch.length > 0) {
      mapping['default'] = lastBranch[0];
    }
  }
  
  // 자연어 조건이 있으면 LLM 평가 노드 추가 (While 패턴과 동일!)
  let routerNode: BackendNode | undefined;
  
  if (hasNaturalLanguageConditions && naturalConditions.length > 0) {
    const conditionsText = naturalConditions
      .map((c, i) => `${i + 1}. "${c.condition}" → return "${c.branch}"`)
      .join('\n');
    
    routerNode = {
      id: `__router_evaluator_${parallelGroup.id}`,
      type: 'llm',
      label: 'Branch Router',
      config: {
        model: 'gemini-2.0-flash-exp',
        system_message: `You are a branch router. Evaluate the following conditions based on the current workflow state and return the matching branch.

Branch Conditions:
${conditionsText}

Analyze the current state and determine which condition is satisfied.

Return format: {"selected_branch": "branch_X", "reason": "brief explanation"}

If none match, return: {"selected_branch": "default", "reason": "no conditions matched"}`,
        output_key: '__router_result',
        response_format: 'json',
        temperature: 0.1,
      },
    };
  }
  
  // Single conditional_edge with mapping
  const conditionalEdge: BackendEdge = {
    type: 'conditional_edge',
    source: sourceNodeId,
    target: Object.values(mapping)[0] || '', // First target as fallback
    router_func: hasNaturalLanguageConditions ? 'dynamic_router' : 'route_draft_quality',
    mapping: mapping,
  };

  return { 
    edges: [conditionalEdge],
    routerNode: routerNode
  };
};

/**
 * Control Block 노드를 백엔드 형식으로 변환
 * 
 * Control Block은 UI 전용 노드로, 백엔드 실행 시에는:
 * - conditional → conditional_edge + router node
 * - parallel → parallel_group branches
 * - for_each → for_each node
 * - while → loop node (back-edge)
 */
function convertControlBlockToBackend(
  controlBlockNode: any,
  nodes: any[],
  edges: any[]
): { nodes: BackendNode[], edges: BackendEdge[] } {
  const blockType = controlBlockNode.data.blockType;
  const blockId = controlBlockNode.id;
  
  // Control Block로 들어오는 엣지 찾기
  const incomingEdge = edges.find((e: any) => e.target === blockId);
  const sourceNodeId = incomingEdge?.source || blockId;
  
  // Control Block에서 나가는 엣지들
  const outgoingEdges = edges.filter((e: any) => e.source === blockId);
  
  if (blockType === 'conditional') {
    // Conditional Branch → conditional_edge + LLM router
    const branches = controlBlockNode.data.branches || [];
    const mapping: { [key: string]: string } = {};
    const naturalConditions: { branch: string; condition: string }[] = [];
    
    // 엣지와 branch 설정을 매칭하여 mapping 생성
    branches.forEach((branch: any) => {
      const branchEdge = outgoingEdges.find((e: any) => e.sourceHandle === branch.id);
      if (branchEdge) {
        mapping[branch.id] = branchEdge.target;
        
        if (branch.natural_condition) {
          naturalConditions.push({
            branch: branch.id,
            condition: branch.natural_condition
          });
        }
      }
    });
    
    // Default branch
    if (branches.length > 0 && !mapping['default']) {
      mapping['default'] = mapping[branches[0].id];
    }
    
    // LLM router 노드 생성 (natural conditions가 있는 경우)
    const resultNodes: BackendNode[] = [];
    const hasNaturalConditions = naturalConditions.length > 0;
    
    if (hasNaturalConditions) {
      const conditionsText = naturalConditions
        .map((c, i) => `${i + 1}. "${c.condition}" → return "${c.branch}"`)
        .join('\n');
      
      resultNodes.push({
        id: `__router_evaluator_${blockId}`,
        type: 'llm',
        label: 'Branch Router',
        config: {
          model: 'gemini-2.0-flash-exp',
          system_message: `You are a branch router. Evaluate the following conditions based on the current workflow state and return the matching branch.

Branch Conditions:
${conditionsText}

Analyze the current state and determine which condition is satisfied.

Return format: {"selected_branch": "branch_X", "reason": "brief explanation"}

If none match, return: {"selected_branch": "default", "reason": "no conditions matched"}`,
          output_key: '__router_result',
          response_format: 'json',
          temperature: 0.1,
        },
      });
    }
    
    // Conditional edge
    const resultEdges: BackendEdge[] = [{
      type: 'conditional_edge',
      source: sourceNodeId,
      target: Object.values(mapping)[0] || '',
      router_func: hasNaturalConditions ? 'dynamic_router' : 'route_draft_quality',
      mapping: mapping,
    }];
    
    return { nodes: resultNodes, edges: resultEdges };
  }
  
  if (blockType === 'parallel') {
    // Parallel Execution → parallel_group node
    const branches = controlBlockNode.data.branches || [];
    const branchNodes: BackendNode[][] = [];
    
    outgoingEdges.forEach((edge: any) => {
      const targetNode = nodes.find((n: any) => n.id === edge.target);
      if (targetNode) {
        const converted = convertNodeToBackendFormat(targetNode);
        if (converted) {
          branchNodes.push([converted]);
        }
      }
    });
    
    const parallelNode: BackendNode = {
      id: `parallel_${blockId}`,
      type: 'parallel_group',
      config: {
        branches: branchNodes.map((nodeList, idx) => ({
          branch_id: `branch_${idx}`,
          sub_workflow: { nodes: nodeList }
        }))
      }
    };
    
    return { nodes: [parallelNode], edges: [] };
  }
  
  if (blockType === 'for_each') {
    // For Each Loop → for_each node
    const branch = controlBlockNode.data.branches[0];
    const targetEdge = outgoingEdges[0];
    const targetNode = targetEdge ? nodes.find((n: any) => n.id === targetEdge.target) : null;
    
    const forEachNode: BackendNode = {
      id: `for_each_${blockId}`,
      type: 'for_each',
      config: {
        items_path: branch?.items_path || 'state.items',
        item_key: 'item',
        output_key: 'for_each_results',
        max_iterations: 20,
        sub_workflow: targetNode ? { nodes: [convertNodeToBackendFormat(targetNode)!] } : { nodes: [] }
      }
    };
    
    return { nodes: [forEachNode], edges: [] };
  }
  
  if (blockType === 'while') {
    // While Loop → loop node with back-edge
    const maxIterations = controlBlockNode.data.max_iterations || 10;
    const naturalCondition = controlBlockNode.data.natural_condition as string | undefined;
    const backEdgeTarget = controlBlockNode.data.back_edge_source;
    
    // While은 loop 노드 + LLM evaluator로 변환
    const resultNodes: BackendNode[] = [];
    
    if (naturalCondition) {
      // LLM evaluator 노드 추가
      resultNodes.push({
        id: `__loop_condition_evaluator_${blockId}`,
        type: 'llm',
        label: 'Loop Condition Evaluator',
        config: {
          model: 'gemini-2.0-flash-exp',
          system_message: `Evaluate the following loop exit condition based on the current workflow state:

Exit Condition: "${naturalCondition}"

Return format: {"should_exit": true/false, "reason": "brief explanation"}`,
          output_key: '__loop_condition_result',
          response_format: 'json',
          temperature: 0.1,
        },
      });
      
      // Flag setter 노드 추가
      resultNodes.push({
        id: `__loop_flag_setter_${blockId}`,
        type: 'safe_operator',
        label: 'Loop Flag Setter',
        config: {
          strategy: 'set_value',
          input_key: '__loop_condition_result',
          params: { path: 'should_exit', output_key: '__loop_should_exit' },
          output_key: '__loop_should_exit',
        },
      });
    }
    
    // Loop 노드
    const loopNode: BackendNode = {
      id: `loop_${blockId}`,
      type: 'loop',
      label: 'Loop',
      config: {
        condition: naturalCondition ? '__loop_should_exit == true' : 'false',
        max_iterations: maxIterations,
        nodes: resultNodes,
      }
    };
    
    // Back-edge
    const backEdge: BackendEdge = backEdgeTarget ? {
      type: 'edge',
      source: blockId,
      target: backEdgeTarget,
      data: { loopType: 'while' }
    } : { type: 'edge', source: blockId, target: sourceNodeId };
    
    return { nodes: [loopNode], edges: [backEdge] };
  }
  
  return { nodes: [], edges: [] };
}

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
    // control_block 노드도 제외 (엣지로 변환됨)
    if (node.type === 'control_block') {
      excludedNodeIds.add(node.id);
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
  
  // 6. 조건부 분기 → conditional_edge + router 노드 생성
  const conditionalResults = analysisResult.parallelGroups
    .filter(pg => pg.branchType === 'conditional')
    .map(pg => convertConditionalBranchToEdges(pg, nodes, edges));
  
  const conditionalEdges = conditionalResults.flatMap(r => r.edges);
  const routerNodes = conditionalResults
    .map(r => r.routerNode)
    .filter((n): n is BackendNode => n !== undefined);
  
  // 7. Control Block 노드들 변환 (UI 기반 제어 구조)
  const controlBlockNodes = nodes.filter((n: any) => n.type === 'control_block');
  const controlBlockResults = controlBlockNodes.map((node: any) => 
    convertControlBlockToBackend(node, nodes, edges)
  );
  
  const controlBlockBackendNodes = controlBlockResults.flatMap(r => r.nodes);
  const controlBlockEdges = controlBlockResults.flatMap(r => r.edges);
  
  // 8. 모든 백엔드 노드 합치기 (router 노드 + control block 노드 포함!)
  const backendNodes = [
    ...regularBackendNodes,
    ...loopNodes,
    ...parallelGroupNodes,
    ...routerNodes,  // 🤖 자동 생성된 LLM 라우터 노드 추가
    ...controlBlockBackendNodes,  // 🎛️ Control Block에서 변환된 노드들
  ];
  
  // 8. 엣지 변환 (사이클 back-edge, 병렬 분기 엣지 필터링)
  // 유효한 백엔드 노드 ID 집합 생성 (참조 무결성 검증용)
  const validBackendNodeIds = new Set(backendNodes.map(n => n.id));

  const backendEdges = edges
    .filter((edge: any) => {
      // back-edge는 제외 (loop 노드 내부로 흡수됨)
      if (analysisResult.backEdgeIds.has(edge.id)) return false;
      
      // HITL/Branch control 노드와 연결된 엣지는 나중에 변환되므로 유지
      const sourceNode = nodes.find((n: any) => n.id === edge.source);
      const targetNode = nodes.find((n: any) => n.id === edge.target);
      
      const isHitlOrBranchEdge = 
        (sourceNode?.type === 'control' && (sourceNode?.data?.controlType === 'human' || sourceNode?.data?.controlType === 'branch')) ||
        (targetNode?.type === 'control' && (targetNode?.data?.controlType === 'human' || targetNode?.data?.controlType === 'branch'));
      
      if (isHitlOrBranchEdge) {
        // HITL/Branch 관련 엣지는 무조건 유지 (나중에 변환됨)
        return true;
      }
      
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

        // [수정] 참조 무결성 검증: actualSource/Target이 실제 백엔드 노드인지 확인
        if (!validBackendNodeIds.has(actualSource) || !validBackendNodeIds.has(actualTarget)) {
          console.warn(`Skipping HITP edge: missing node reference ${actualSource} -> ${actualTarget}`);
          return null;
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

        // [수정] 참조 무결성 검증
        if (!validBackendNodeIds.has(actualSource) || !validBackendNodeIds.has(actualTarget)) {
          console.warn(`Skipping conditional edge: missing node reference ${actualSource} -> ${actualTarget}`);
          return null;
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

      // [수정] 일반 엣지도 검증
      if (!validBackendNodeIds.has(actualSource) || !validBackendNodeIds.has(actualTarget)) {
        const sourceNode = nodes.find((n: any) => n.id === actualSource);
        const targetNode = nodes.find((n: any) => n.id === actualTarget);
        console.warn(`⚠️ [Converter] Skipping edge: nodes excluded from backend`, {
          edge: `${actualSource} -> ${actualTarget}`,
          sourceType: sourceNode?.type,
          targetType: targetNode?.type,
          reason: !validBackendNodeIds.has(actualSource) 
            ? `Source node excluded (${excludedNodeIds.has(actualSource) ? 'in excluded set' : 'filtered out'})` 
            : `Target node excluded (${excludedNodeIds.has(actualTarget) ? 'in excluded set' : 'filtered out'})`
        });
        return null;
      }

      // 일반 엣지 변환
      return convertEdgeToBackendFormat(edge, nodes);
    })
    .filter((e: any) => e !== null);

  // 9. 조건부 엣지 + Control Block 엣지 추가 (참조 무결성 재검증)
  const additionalEdges = [...conditionalEdges, ...controlBlockEdges];
  const validatedAdditionalEdges = additionalEdges.filter((edge: any) => {
    if (!edge || !edge.source || !edge.target) return false;

    // Control Block에서 생성된 엣지들도 유효한 노드 참조인지 확인
    const hasValidSource = validBackendNodeIds.has(edge.source);
    const hasValidTarget = validBackendNodeIds.has(edge.target);

    if (!hasValidSource || !hasValidTarget) {
      console.warn(`Skipping additional edge: missing node reference ${edge.source} -> ${edge.target}`);
      return false;
    }

    return true;
  });

  const allBackendEdges = [...backendEdges, ...validatedAdditionalEdges];

  // 10. 끝점 노드 처리: outgoing edge가 없는 노드들에 자동으로 end 노드 연결
  const nodeIdsWithOutgoingEdges = new Set(allBackendEdges.map(e => e.source));
  const deadEndNodes = backendNodes.filter(node => !nodeIdsWithOutgoingEdges.has(node.id));
  
  // end 노드가 없으면 자동 생성하고 끝점 노드들과 연결
  if (deadEndNodes.length > 0) {
    const endNodeId = '__auto_end';
    const hasEndNode = backendNodes.some(n => n.type === 'end' || n.id === endNodeId);
    
    if (!hasEndNode) {
      // end 노드 추가
      backendNodes.push({
        id: endNodeId,
        type: 'end',
        label: 'End',
        config: {},
      });
      
      // 각 끝점 노드에서 end 노드로 엣지 연결
      deadEndNodes.forEach(deadEndNode => {
        allBackendEdges.push({
          type: 'edge',
          source: deadEndNode.id,
          target: endNodeId,
        });
      });
      
      console.log(`✅ [Converter] Auto-generated end node and connected ${deadEndNodes.length} dead-end node(s)`);
    }
  }

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
    
    // 타입 검증 및 로깅
    if (!node.type) {
      console.warn(`[WorkflowConverter] Node ${node.id} has no type, defaulting to 'operator'`);
    }

    switch (node.type) {
      case 'llm_chat':
        frontendType = 'aiModel';
        label = 'AI Model';
        // 모든 속성은 config에서만 읽기
        const llmConfig = node.config || {};
        const promptContent = llmConfig.prompt_content || '';
        const systemPrompt = llmConfig.system_prompt || '';
        const temperature = llmConfig.temperature ?? 0.7;
        const maxTokens = llmConfig.max_tokens ?? 1024;
        
        nodeData = {
          label,
          prompt_content: promptContent,
          prompt: promptContent,
          system_prompt: systemPrompt,
          temperature: temperature,
          max_tokens: maxTokens,
          maxTokens: maxTokens,
          model: llmConfig.model || 'gpt-3.5-turbo',
          provider: llmConfig.provider || 'openai',
          writes_state_key: llmConfig.writes_state_key,
          // Restore tool definitions
          tools: llmConfig.tool_definitions || [],
          toolsCount: (llmConfig.tool_definitions || []).length,
        };
        break;
      case 'operator':
        // config.sets 내용을 확인하여 프론트엔드 타입 복원
        const operatorConfig = node.config || {};
        if (operatorConfig._frontend_type === 'trigger') {
          frontendType = 'trigger';
          label = 'Trigger';
          nodeData = {
            label,
            triggerType: operatorConfig.trigger_type,
            triggerHour: operatorConfig.triggerHour,
            triggerMinute: operatorConfig.triggerMinute,
          };
        } else if (operatorConfig._frontend_type === 'control') {
          frontendType = 'control';
          label = 'Control';
          nodeData = {
            label,
            controlType: operatorConfig.control_type,
            whileCondition: operatorConfig.whileCondition,
            max_iterations: operatorConfig.max_iterations,
          };
        } else {
          frontendType = 'operator';
          label = 'Operator';
          nodeData = {
            label,
            sets: operatorConfig.sets || {},
          };
        }
        break;
      case 'trigger':
        // 백엔드에서 type이 trigger로 저장된 경우 (save_workflow.py의 정규화 로직)
        frontendType = 'trigger';
        label = 'Trigger';
        const triggerConfig = node.config || {};
        nodeData = {
          label,
          triggerType: triggerConfig.trigger_type || 'request',
          triggerHour: triggerConfig.triggerHour,
          triggerMinute: triggerConfig.triggerMinute,
          // config의 다른 속성들도 보존
          ...triggerConfig
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
        const itemsPath = forEachConfig.items_path || '';
        const subWorkflow = forEachConfig.sub_workflow;
        
        if (!itemsPath) {
          console.warn(`[WorkflowConverter] for_each node ${node.id} missing items_path`);
        }
        if (!subWorkflow) {
          console.warn(`[WorkflowConverter] for_each node ${node.id} missing sub_workflow`);
        }
        
        nodeData = {
          label,
          controlType: 'for_each',
          items_path: itemsPath,
          itemsPath: itemsPath,
          item_key: forEachConfig.item_key || 'item',
          output_key: forEachConfig.output_key || 'for_each_results',
          max_iterations: forEachConfig.max_iterations || 20,
          sub_workflow: subWorkflow,
        };
        break;
      case 'parallel_group':
      case 'parallel':
        // parallel_group → control(parallel)
        frontendType = 'control';
        label = 'Parallel';
        const parallelConfig = node.config || {};
        // branches는 config.branches 또는 최상위 branches 필드에서 (최상위는 백엔드가 허용함)
        const branches = parallelConfig.branches || node.branches || [];
        
        nodeData = {
          label,
          controlType: 'parallel',
          branches: branches,
        };
        break;
      case 'aggregator':
        // aggregator → control(aggregator)
        frontendType = 'control';
        label = 'Aggregator';
        nodeData = {
          label,
          controlType: 'aggregator',
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
        const apiUrl = apiConfig.url || '';
        
        if (!apiUrl) {
          console.warn(`[WorkflowConverter] api_call node ${node.id} missing url`);
        }
        
        nodeData = {
          label,
          operatorType: 'api_call',
          url: apiUrl,
          method: apiConfig.method || 'GET',
          headers: apiConfig.headers || {},
          params: apiConfig.params || {},
          json: apiConfig.json,
          timeout: apiConfig.timeout || 10,
        };
        break;
      case 'db_query':
        // db_query → operator(database)
        frontendType = 'operator';
        label = 'Database Query';
        const dbConfig = node.config || {};
        const query = dbConfig.query || '';
        
        if (!query) {
          console.warn(`[WorkflowConverter] db_query node ${node.id} missing query`);
        }
        
        nodeData = {
          label,
          operatorType: 'database',
          query: query,
          connection_string: dbConfig.connection_string,
        };
        break;
      default:
        // 기타 런타임 전용 타입(vision, skill_executor 등)은 일반 operator로 표시
        console.warn(`[WorkflowConverter] Unknown node type '${node.type}' for node ${node.id}, treating as operator`);
        frontendType = 'operator';
        label = node.type || 'Block';
        nodeData = { 
          label, 
          rawBackendType: node.type,
          ...(node.config || {}) 
        };
    }

    // Position fallback: 백엔드 규칙과 일치 (x=150 고정, y=50+index*100)
    const position = node.position || {
      x: 150,
      y: 50 + index * 100
    };
    
    return {
      id: node.id,
      type: frontendType,
      position,
      data: nodeData,
    };
  }) || [];

  // 🔄 [역변환 1] HITL 엣지를 control/human 노드 + 일반 엣지로 복원
  const hitlEdges = backendWorkflow.edges?.filter((edge: BackendEdge) => 
    edge.type === 'hitp' || edge.type === 'human_in_the_loop'
  ) || [];

  const hitlNodes = hitlEdges.map((hitlEdge: BackendEdge, index: number) => {
    const hitlNodeId = `hitl_${hitlEdge.source}_${hitlEdge.target}`;
    return {
      id: hitlNodeId,
      type: 'control',
      position: { x: 300 + index * 50, y: 300 + index * 50 },
      data: {
        label: 'Human Review',
        controlType: 'human',
        description: 'Human approval required',
      },
      _isHitlNode: true,
      _originalSource: hitlEdge.source,
      _originalTarget: hitlEdge.target,
    };
  });

  // 🔄 [역변환 2] conditional_edge를 control/branch 노드 + 엣지로 복원
  const conditionalEdges = backendWorkflow.edges?.filter((edge: BackendEdge) => 
    edge.type === 'conditional_edge'
  ) || [];

  // conditional_edge를 source별로 그룹화 (한 노드에서 여러 조건 분기)
  const conditionalEdgesBySource = conditionalEdges.reduce((acc: any, edge: BackendEdge) => {
    if (!acc[edge.source]) {
      acc[edge.source] = [];
    }
    acc[edge.source].push(edge);
    return acc;
  }, {});

  const branchNodes = Object.entries(conditionalEdgesBySource).map(([source, edges]: [string, any], index: number) => {
    const branchNodeId = `branch_${source}`;
    const firstEdge = edges[0];
    
    // mapping 생성: 각 조건 → target 노드
    const mapping: Record<string, string> = {};
    edges.forEach((edge: BackendEdge) => {
      const conditionKey = edge.condition || edge.mapping || 'default';
      mapping[String(conditionKey)] = edge.target;
    });

    return {
      id: branchNodeId,
      type: 'control',
      position: { x: 500 + index * 50, y: 300 + index * 50 },
      data: {
        label: 'Conditional Branch',
        controlType: 'branch',
        router_func: firstEdge.router_func || 'route_by_condition',
        mapping: firstEdge.mapping || mapping,
        condition: firstEdge.condition,
      },
      _isBranchNode: true,
      _originalSource: source,
      _targets: edges.map((e: BackendEdge) => e.target),
    };
  });

  // 모든 노드 합치기 (원본 + HITL 노드 + Branch 노드)
  const allFrontendNodes = [...frontendNodes, ...hitlNodes, ...branchNodes];

  // 엣지 재구성
  const frontendEdges = backendWorkflow.edges?.flatMap((edge: BackendEdge, index: number) => {
    // 1. HITL 엣지인 경우: 2개의 일반 엣지로 분할
    if (edge.type === 'hitp' || edge.type === 'human_in_the_loop') {
      const hitlNodeId = `hitl_${edge.source}_${edge.target}`;
      return [
        // source → HITL 노드
        {
          id: `edge-${index}-in`,
          source: edge.source,
          target: hitlNodeId,
          type: 'smoothstep',
          animated: true,
          style: {
            stroke: 'hsl(38 92% 50%)', // HITL orange
            strokeWidth: 2
          },
          data: {
            edgeType: 'normal',
          },
        },
        // HITL 노드 → target
        {
          id: `edge-${index}-out`,
          source: hitlNodeId,
          target: edge.target,
          type: 'smoothstep',
          animated: true,
          style: {
            stroke: 'hsl(38 92% 50%)', // HITL orange
            strokeWidth: 2
          },
          data: {
            edgeType: 'hitp',
          },
        },
      ];
    }

    // 2. conditional_edge인 경우: 2개의 일반 엣지로 분할
    if (edge.type === 'conditional_edge') {
      const branchNodeId = `branch_${edge.source}`;
      return [
        // source → Branch 노드
        {
          id: `edge-${index}-in`,
          source: edge.source,
          target: branchNodeId,
          type: 'smoothstep',
          animated: true,
          style: {
            stroke: 'hsl(142 76% 36%)', // Conditional green
            strokeWidth: 2
          },
          data: {
            edgeType: 'normal',
          },
        },
        // Branch 노드 → target
        {
          id: `edge-${index}-out`,
          source: branchNodeId,
          target: edge.target,
          type: 'smoothstep',
          animated: true,
          style: {
            stroke: 'hsl(142 76% 36%)', // Conditional green
            strokeWidth: 2
          },
          data: {
            condition: edge.condition,
            edgeType: 'conditional_edge',
            mapping: edge.mapping,
          },
        },
      ];
    }

    // 3. 일반 엣지
    return {
      id: `edge-${index}`,
      source: edge.source,
      target: edge.target,
      type: 'smoothstep',
      animated: true,
      style: {
        stroke: edge.type === 'if' ? 'hsl(142 76% 36%)' : 'hsl(263 70% 60%)',
        strokeWidth: 2
      },
      data: {
        condition: edge.condition,
        edgeType: edge.type,
      },
    };
  }) || [];

  const result: any = {
    name: backendWorkflow.name,
    nodes: allFrontendNodes,
    edges: frontendEdges,
  };

  // secrets가 있는 경우 복원
  if (backendWorkflow.secrets && backendWorkflow.secrets.length > 0) {
    result.secrets = backendWorkflow.secrets;
  }

  return result;
};