# Workflow Configuration Schema & Execution Guide

워크플로우 설정 방식의 스키마, 각 기능 구성 방법, 실제 실행 방식에 대한 종합 문서입니다.

## 목차

1. [워크플로우 스키마 개요](#1-워크플로우-스키마-개요)
2. [노드 타입 및 설정](#2-노드-타입-및-설정)
3. [엣지(연결) 설정](#3-엣지연결-설정)
4. [실행 흐름](#4-실행-흐름)
5. [고급 기능](#5-고급-기능)
6. [실제 사용 예시](#6-실제-사용-예시)
7. [Skill 시스템](#7-skill-시스템)
8. [조건부 처리 패턴](#8-조건부-처리-패턴)
9. [동적 커널 스케줄링 및 워크플로우 구조 수정](#9-동적-커널-스케줄링-및-워크플로우-구조-수정)

---

## 1. 워크플로우 스키마 개요

### 1.1 기본 구조

워크플로우는 JSON 형식으로 정의되며, 다음 핵심 요소로 구성됩니다:

```json
{
  "workflow_id": "unique-workflow-id",
  "name": "워크플로우 이름",
  "description": "워크플로우 설명",
  "version": "1.0.0",
  "nodes": [...],
  "edges": [...],
  "metadata": {...}
}
```

### 1.2 Pydantic 스키마 정의

워크플로우 설정은 Pydantic 모델로 검증됩니다:

**`WorkflowConfigModel`** (from [orchestrator_service.py](analemma-workflow-os/backend/src/services/workflow/orchestrator_service.py#L88-L92))

```python
class WorkflowConfigModel(BaseModel):
    workflow_name: Optional[str] = None  # 최대 256자
    description: Optional[str] = None     # 최대 512자
    nodes: List[NodeModel]                # 최소 1개, 최대 500개
    edges: List[EdgeModel]                # 최대 1000개
    start_node: Optional[str] = None      # 시작 노드 ID
```

**`NodeModel`** (from [orchestrator_service.py](analemma-workflow-os/backend/src/services/workflow/orchestrator_service.py#L47-L69))

```python
class NodeModel(BaseModel):
    id: str                                    # 노드 고유 식별자 (최대 128자)
    type: str                                  # 노드 타입 (아래 참조)
    label: Optional[str] = None                # UI 표시 이름
    action: Optional[str] = None               # 노드 액션 설명
    hitp: Optional[bool] = None                # Human-in-the-loop 플래그
    config: Optional[Dict[str, Any]] = None    # 노드 설정
    branches: Optional[List[Dict]] = None      # 병렬 브랜치 (parallel_group)
    subgraph_ref: Optional[str] = None         # 서브그래프 참조
    subgraph_inline: Optional[Dict] = None     # 인라인 서브그래프
```

**`EdgeModel`** (from [orchestrator_service.py](analemma-workflow-os/backend/src/services/workflow/orchestrator_service.py#L35-L43))

```python
class EdgeModel(BaseModel):
    source: str                                # 소스 노드 ID
    target: str                                # 타겟 노드 ID
    type: str = "edge"                         # 엣지 타입
    router_func: Optional[str] = None          # 라우터 함수명 (조건부)
    mapping: Optional[Dict[str, str]] = None   # 라우터 매핑
    condition: Optional[str] = None            # 조건식
```

### 1.3 Graph DSL 스키마

프론트엔드와 백엔드 간 표준 형식 (from [graph_dsl.py](analemma-workflow-os/backend/src/common/graph_dsl.py#L138-L192)):

```python
class Workflow(BaseModel):
    name: Optional[str]
    description: Optional[str]
    nodes: List[WorkflowNode]
    edges: List[WorkflowEdge]
    subgraphs: Optional[Dict[str, SubgraphMetadata]]  # 서브그래프 정의
```

---

## 2. 노드 타입 및 설정

### 2.1 허용된 노드 타입

**실행 가능 노드** (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py#L75-L103)):

```python
ALLOWED_NODE_TYPES = {
    # 핵심 연산 노드
    "operator",           # 범용 연산자 (sets, code)
    "operator_official",  # Safe 연산자 (50+ 전략)
    "llm_chat",          # LLM 채팅
    
    # 제어 흐름
    "for_each",          # 리스트 반복
    "loop",              # 조건부 반복
    "parallel_group",    # 병렬 실행
    "nested_for_each",   # 중첩 반복
    
    # 데이터 처리
    "api_call",          # HTTP API 호출
    "db_query",          # 데이터베이스 쿼리
    "aggregator",        # 결과 집계
    
    # 고급 기능
    "subgraph",          # 서브그래프 실행
    "skill_executor",    # 스킬 실행
    "vision",            # 멀티모달 분석
    "video_chunker",     # 비디오 분할
    "route_draft_quality",  # 품질 라우팅
}
```

**UI 마커 노드** (실행되지 않고 통과됨):

```python
UI_MARKER_TYPES = {"trigger", "input", "output", "start", "end"}
```

**노드 타입 별칭** (호환성):

```python
NODE_TYPE_ALIASES = {
    "code": "operator",
    "aiModel": "llm_chat",
    "control.loop": "for_each",
    "control.parallel": "parallel_group",
    "group": "subgraph",
}
```

### 2.2 주요 노드 설정 예시

#### 2.2.1 Operator (연산자)

```json
{
  "id": "transform_data",
  "type": "operator",
  "config": {
    "sets": {
      "processed_text": "{{input_text | upper}}",
      "timestamp": "{{current_time}}"
    }
  }
}
```

또는 코드 실행 (MOCK_MODE에서만):

```json
{
  "id": "custom_logic",
  "type": "operator",
  "config": {
    "code": "state['result'] = len(state.get('input_text', ''))"
  }
}
```

#### 2.2.2 LLM Chat

```json
{
  "id": "analyze_sentiment",
  "type": "llm_chat",
  "config": {
    "provider": "gemini",
    "model": "gemini-2.0-flash",
    "prompt_content": "다음 텍스트의 감정을 분석해주세요: {{input_text}}",
    "system_prompt": "당신은 감정 분석 전문가입니다.",
    "temperature": 0.7,
    "max_tokens": 2000,
    "output_key": "sentiment_analysis",
    "retry_config": {
      "max_retries": 2,
      "base_delay": 1.0
    }
  }
}
```

**멀티모달 지원**:

```json
{
  "id": "vision_analysis",
  "type": "llm_chat",
  "config": {
    "vision_enabled": true,
    "image_inputs": ["{{product_image_s3_uri}}"],
    "prompt_content": "이 제품 이미지를 분석해주세요",
    "model": "gemini-1.5-pro"
  }
}
```

#### 2.2.3 For Each (반복)

```json
{
  "id": "process_users",
  "type": "for_each",
  "config": {
    "input_list_key": "users",
    "item_key": "user",
    "max_iterations": 20,
    "output_key": "processed_users",
    "sub_workflow": {
      "nodes": [
        {
          "id": "send_email",
          "type": "llm_chat",
          "config": {
            "prompt_content": "{{user.name}}님께 이메일 작성: {{user.email}}"
          }
        }
      ]
    }
  }
}
```

#### 2.2.4 Parallel Group (병렬 실행)

```json
{
  "id": "parallel_analysis",
  "type": "parallel_group",
  "config": {
    "branches": [
      {
        "branch_id": "branch_summary",
        "nodes": [
          {
            "id": "summarize",
            "type": "llm_chat",
            "config": {
              "prompt_content": "요약: {{document}}"
            }
          }
        ]
      },
      {
        "branch_id": "branch_translate",
        "nodes": [
          {
            "id": "translate",
            "type": "llm_chat",
            "config": {
              "prompt_content": "번역: {{document}}"
            }
          }
        ]
      }
    ]
  }
}
```

#### 2.2.5 Loop (조건부 반복)

```json
{
  "id": "quality_loop",
  "type": "loop",
  "config": {
    "max_iterations": 5,
    "condition": "quality_score >= 0.9",
    "convergence_key": "quality_score",
    "target_score": 0.9,
    "nodes": [
      {
        "id": "generate_draft",
        "type": "llm_chat",
        "config": {
          "prompt_content": "문서 초안 생성"
        }
      },
      {
        "id": "evaluate",
        "type": "operator_official",
        "config": {
          "strategy": "json_parse",
          "input": "{{generate_draft_output}}"
        }
      }
    ]
  }
}
```

#### 2.2.6 API Call

```json
{
  "id": "fetch_weather",
  "type": "api_call",
  "config": {
    "url": "https://api.weather.com/v1/forecast",
    "method": "GET",
    "headers": {
      "Authorization": "Bearer {{api_token}}"
    },
    "params": {
      "city": "{{city_name}}"
    },
    "timeout": 10
  }
}
```

#### 2.2.7 Subgraph (서브그래프)

```json
{
  "id": "email_workflow",
  "type": "subgraph",
  "config": {
    "subgraph_inline": {
      "nodes": [
        {
          "id": "compose",
          "type": "llm_chat",
          "config": {
            "prompt_content": "이메일 작성: {{email_topic}}"
          }
        },
        {
          "id": "send",
          "type": "api_call",
          "config": {
            "url": "https://api.sendgrid.com/v3/mail/send",
            "method": "POST"
          }
        }
      ],
      "edges": [
        {"source": "compose", "target": "send"}
      ]
    },
    "input_mapping": {
      "topic": "email_topic"
    },
    "output_mapping": {
      "compose_output": "email_content"
    }
  }
}
```

---

## 3. 엣지(연결) 설정

### 3.1 기본 엣지

노드 간 순차 연결:

```json
{
  "source": "node1",
  "target": "node2",
  "type": "edge"
}
```

### 3.2 조건부 엣지

```json
{
  "source": "quality_check",
  "target": "approved_path",
  "type": "conditional_edge",
  "condition": "{{quality_score}} > 0.8"
}
```

### 3.3 라우터 엣지

동적 라우팅:

```json
{
  "source": "classifier",
  "target": "router_node",
  "type": "conditional_edge",
  "router_func": "dynamic_router",
  "mapping": {
    "branch_0": "high_priority",
    "branch_1": "medium_priority",
    "default": "low_priority"
  }
}
```

### 3.4 While 엣지

반복 엣지:

```json
{
  "source": "loop_body",
  "target": "loop_start",
  "type": "while",
  "condition": "{{loop_counter}} < 10",
  "max_iterations": 10
}
```

---

## 4. 실행 흐름

### 4.1 실행 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                     Workflow Execution Flow                  │
└─────────────────────────────────────────────────────────────┘

1. API Request
   └─> Lambda Handler (run_workflow.py)
       └─> WorkflowOrchestratorService

2. Config Validation
   └─> Pydantic Schema Validation
       └─> WorkflowConfigModel.model_validate()

3. Dynamic Build
   └─> DynamicWorkflowBuilder
       ├─> StateGraph 생성
       ├─> 노드 등록 (NODE_REGISTRY)
       └─> 엣지 연결

4. Execution
   └─> StateGraph.invoke(initial_state)
       ├─> Node Runner 호출
       │   ├─> llm_chat_runner
       │   ├─> operator_runner
       │   ├─> for_each_runner
       │   └─> ...
       │
       ├─> State Updates
       │   └─> StateBag (Merge Logic)
       │
       └─> Checkpointing (DynamoDB)

5. Result
   └─> Final State with Outputs
```

### 4.2 노드 실행 흐름

각 노드는 **Runner 함수**를 통해 실행됩니다 (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py#L3826-L3860)):

```python
def run_workflow(config_json, initial_state, ...):
    # 1. Config 검증
    validated_config = WorkflowConfigModel.model_validate(raw_config)
    
    # 2. 동적 빌드
    builder = DynamicWorkflowBuilder(validated_config)
    app = builder.build()
    
    # 3. 실행
    result = app.invoke(initial_state)
    
    return result
```

**노드 러너 예시** (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py)):

```python
def llm_chat_runner(state: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    # 1. 설정 추출
    prompt = _render_template(config["prompt_content"], state)
    model = config.get("model", "gemini-2.0-flash")
    
    # 2. LLM 호출
    response = gemini_service.invoke_model(prompt, model)
    
    # 3. 결과 반환
    return {
        f"{node_id}_output": response.text,
        "step_history": state["step_history"] + [node_id]
    }
```

### 4.3 상태 관리

**StateBag 패턴** (from [main.py](analemma-workflow-os/backend/src/services/workflow/builder.py#L60-L85)):

```python
# Annotated 타입으로 자동 병합
DynamicWorkflowState = Annotated[Dict[str, Any], merge_state_dict]

def merge_state_dict(existing: Dict, updated: Dict) -> Dict:
    """노드가 업데이트한 필드만 병합"""
    new_state = dict(existing)
    new_state.update(updated)
    return new_state
```

**예약 키 보호** (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py#L352-L394)):

```python
RESERVED_STATE_KEYS = {
    "workflowId", "execution_id", "loop_counter",
    "segment_id", "state_s3_path", "user_api_keys",
    # ... 총 30+ 예약 키
}

def _validate_output_keys(output: Dict, node_id: str) -> Dict:
    """노드 출력에서 예약 키 제거"""
    return {k: v for k, v in output.items() if k not in RESERVED_STATE_KEYS}
```

### 4.4 템플릿 렌더링

모든 설정 값은 **Jinja2-style 템플릿**으로 렌더링됩니다:

```python
def _render_template(template: Any, state: Dict) -> Any:
    """
    {{variable}} 템플릿을 state 값으로 치환
    
    지원 패턴:
    - {{user.name}} - 중첩 접근
    - {{items | tojson}} - 필터
    - {% if condition %}...{% endif %} - 조건문
    """
```

**사용 예시**:

```json
{
  "prompt_content": "안녕하세요 {{user.name}}님, {{user.email}}로 연락드립니다.",
  "url": "https://api.example.com/users/{{user_id}}"
}
```

---

## 5. 고급 기능

### 5.1 토큰 집계 및 비용 추적

모든 LLM 노드는 토큰 사용량을 자동 추적합니다 (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py)):

```python
# 각 노드 실행 후
result = {
    "total_input_tokens": 1500,
    "total_output_tokens": 500,
    "total_tokens": 2000,
    "estimated_cost_usd": 0.002
}
```

**누적 집계** (for_each, parallel_group):

```python
def aggregate_tokens_from_nested(results: List) -> Dict:
    """중첩 구조의 토큰 재귀 집계"""
    total_input = sum(extract_token_usage(r)['input_tokens'] for r in results)
    total_output = sum(extract_token_usage(r)['output_tokens'] for r in results)
    return {
        'input_tokens': total_input,
        'output_tokens': total_output,
        'total_tokens': total_input + total_output
    }
```

### 5.2 S3 오프로딩

대용량 데이터는 자동으로 S3에 오프로드됩니다:

```python
# 150KB 초과 시 자동 오프로드
if results_size_kb > 150:
    s3_path = state_manager.upload_state_to_s3(results)
    return {
        "__s3_offloaded": True,
        "s3_path": s3_path,
        "size_kb": results_size_kb
    }
```

### 5.3 품질 커널 (Quality Kernel)

LLM 출력 품질을 자동 검증합니다 (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py)):

```python
# LLM 응답 후 자동 실행
interceptor = KernelMiddlewareInterceptor(
    domain=ContentDomain.TECHNICAL_REPORT,
    slop_threshold=0.5
)

result = interceptor.post_process_node(
    node_output=llm_response,
    node_id=node_id,
    context=state
)

# 결과
{
    "action": "PASS",  # or "REJECT"
    "slop_score": 0.23,
    "is_slop": false,
    "recommendation": "High quality output"
}
```

### 5.4 재시도 메커니즘

LLM 호출 실패 시 자동 재시도:

```python
retry_config = {
    "max_retries": 2,
    "base_delay": 1.0
}

# Exponential backoff with jitter
delay = min(60, base_delay * (2 ** attempt)) * (0.5 + random())
```

### 5.5 실행 취소 (Cancellation)

진행 중인 워크플로우를 취소할 수 있습니다:

```python
if check_execution_cancelled(execution_arn):
    raise Exception("Execution cancelled by user")
```

---

## 6. 실제 사용 예시

### 6.1 간단한 텍스트 처리 워크플로우

```json
{
  "workflow_id": "text_analysis_v1",
  "name": "텍스트 분석 워크플로우",
  "nodes": [
    {
      "id": "api_entry",
      "type": "trigger",
      "config": {
        "trigger_type": "request"
      }
    },
    {
      "id": "extract_text",
      "type": "operator",
      "config": {
        "sets": {
          "cleaned_text": "{{input_text | trim}}"
        }
      }
    },
    {
      "id": "analyze",
      "type": "llm_chat",
      "config": {
        "prompt_content": "다음 텍스트를 분석해주세요: {{cleaned_text}}",
        "model": "gemini-2.0-flash",
        "output_key": "analysis_result"
      }
    }
  ],
  "edges": [
    {"source": "api_entry", "target": "extract_text"},
    {"source": "extract_text", "target": "analyze"},
    {"source": "analyze", "target": "END"}
  ]
}
```

### 6.2 병렬 분석 워크플로우

```json
{
  "workflow_id": "parallel_analysis_v1",
  "name": "병렬 문서 분석",
  "nodes": [
    {
      "id": "split_document",
      "type": "operator_official",
      "config": {
        "strategy": "list_chunk",
        "input_key": "document",
        "params": {
          "chunk_size": 1000
        },
        "output_key": "chunks"
      }
    },
    {
      "id": "parallel_analysis",
      "type": "parallel_group",
      "config": {
        "branches": [
          {
            "branch_id": "branch_summary",
            "nodes": [{
              "id": "summarize",
              "type": "llm_chat",
              "config": {
                "prompt_content": "요약: {{document}}",
                "output_key": "summary"
              }
            }]
          },
          {
            "branch_id": "branch_keywords",
            "nodes": [{
              "id": "extract_keywords",
              "type": "llm_chat",
              "config": {
                "prompt_content": "키워드 추출: {{document}}",
                "output_key": "keywords"
              }
            }]
          }
        ]
      }
    },
    {
      "id": "aggregate_results",
      "type": "aggregator",
      "config": {}
    }
  ],
  "edges": [
    {"source": "split_document", "target": "parallel_analysis"},
    {"source": "parallel_analysis", "target": "aggregate_results"},
    {"source": "aggregate_results", "target": "END"}
  ]
}
```

### 6.3 품질 개선 루프 워크플로우

```json
{
  "workflow_id": "quality_loop_v1",
  "name": "반복 품질 개선",
  "nodes": [
    {
      "id": "quality_loop",
      "type": "loop",
      "config": {
        "max_iterations": 5,
        "condition": "quality_score >= 0.9",
        "convergence_key": "quality_score",
        "target_score": 0.9,
        "nodes": [
          {
            "id": "generate_content",
            "type": "llm_chat",
            "config": {
              "prompt_content": "{{topic}}에 대한 기술 문서를 작성해주세요.",
              "output_key": "draft"
            }
          },
          {
            "id": "evaluate_quality",
            "type": "llm_chat",
            "config": {
              "prompt_content": "다음 문서의 품질을 0~1 점수로 평가해주세요: {{draft}}",
              "response_schema": {
                "type": "object",
                "properties": {
                  "score": {"type": "number"},
                  "feedback": {"type": "string"}
                }
              },
              "output_key": "evaluation"
            }
          },
          {
            "id": "parse_score",
            "type": "operator_official",
            "config": {
              "strategy": "deep_get",
              "input": "{{evaluation}}",
              "params": {
                "path": "score"
              },
              "output_key": "quality_score"
            }
          }
        ]
      }
    }
  ],
  "edges": [
    {"source": "quality_loop", "target": "END"}
  ]
}
```

### 6.4 멀티모달 분석 워크플로우

```json
{
  "workflow_id": "vision_analysis_v1",
  "name": "이미지 분석 워크플로우",
  "nodes": [
    {
      "id": "upload_image",
      "type": "api_call",
      "config": {
        "url": "https://storage.example.com/upload",
        "method": "POST",
        "json": {
          "image_data": "{{image_base64}}"
        },
        "output_key": "upload_response"
      }
    },
    {
      "id": "extract_s3_uri",
      "type": "operator_official",
      "config": {
        "strategy": "deep_get",
        "input": "{{upload_response}}",
        "params": {
          "path": "s3_uri"
        },
        "output_key": "image_uri"
      }
    },
    {
      "id": "vision_analysis",
      "type": "vision",
      "config": {
        "image_inputs": ["{{image_uri}}"],
        "prompt_content": "이 이미지에서 제품 스펙을 추출하고 JSON 형식으로 반환해주세요.",
        "model": "gemini-1.5-pro",
        "output_key": "extracted_specs"
      }
    }
  ],
  "edges": [
    {"source": "upload_image", "target": "extract_s3_uri"},
    {"source": "extract_s3_uri", "target": "vision_analysis"},
    {"source": "vision_analysis", "target": "END"}
  ]
}
```

---

## 7. Skill 시스템

### 7.1 Skill 개요

**Skill**은 재사용 가능한 기능 단위로, 워크플로우에 주입하여 사용할 수 있습니다:

- **Tool-based Skill**: 개별 도구(tool) 정의 집합
- **Subgraph-based Skill**: 완전한 서브워크플로우를 캡슐화

#### Skill 스키마 (from [skill_models.py](analemma-workflow-os/backend/src/models/skill_models.py)):

```python
class SkillSchema(BaseModel):
    skill_id: str                           # 고유 식별자
    version: str                            # SemVer 버전 (1.0.0)
    owner_id: str                           # 소유자 ID
    name: str                               # 스킬 이름
    description: str                        # 설명
    category: str                           # 카테고리 (예: "data_processing")
    tags: List[str]                         # 검색 태그
    
    # Tool-based 스킬
    tool_definitions: List[ToolDefinition]  # 도구 목록
    system_instructions: str                # 시스템 프롬프트
    
    # Subgraph-based 스킬
    skill_type: str                         # "tool_based" 또는 "subgraph_based"
    subgraph_config: Dict                   # 서브그래프 정의
    input_schema: Dict                      # 입력 스키마
    output_schema: Dict                     # 출력 스키마
    
    # 의존성 및 권한
    dependencies: List[SkillDependency]     # 다른 스킬 의존성
    required_api_keys: List[str]            # 필요한 API 키
    required_permissions: List[AWSPermission]  # AWS IAM 권한
    
    # 메타데이터
    visibility: str                         # "private", "public", "organization"
    status: str                             # "active", "deprecated", "archived"
    timeout_seconds: int                    # 실행 타임아웃
    retry_config: Dict                      # 재시도 설정
```

### 7.2 Skill 생성 및 관리

#### 7.2.1 Tool-based Skill 생성

```json
{
  "name": "Email Automation",
  "description": "이메일 발송 자동화 스킬",
  "category": "communication",
  "tags": ["email", "automation"],
  "tool_definitions": [
    {
      "name": "send_email",
      "description": "이메일 전송",
      "handler_type": "api_call",
      "handler_config": {
        "url": "https://api.sendgrid.com/v3/mail/send",
        "method": "POST",
        "headers": {
          "Authorization": "Bearer {{api_key}}"
        }
      },
      "parameters": {
        "type": "object",
        "properties": {
          "to": {"type": "string"},
          "subject": {"type": "string"},
          "body": {"type": "string"}
        },
        "required": ["to", "subject", "body"]
      },
      "required_api_keys": ["sendgrid_api_key"]
    }
  ],
  "system_instructions": "당신은 이메일 전송 전문가입니다.",
  "visibility": "private"
}
```

#### 7.2.2 Subgraph-based Skill 생성

Canvas에서 그룹 노드를 Skill로 저장:

```json
{
  "name": "Document Processing Pipeline",
  "description": "문서 분석 및 요약 파이프라인",
  "skill_type": "subgraph_based",
  "subgraph_config": {
    "nodes": [
      {
        "id": "extract_text",
        "type": "operator_official",
        "config": {
          "strategy": "extract_text",
          "input": "{{document}}"
        }
      },
      {
        "id": "summarize",
        "type": "llm_chat",
        "config": {
          "prompt_content": "요약: {{extract_text_output}}"
        }
      }
    ],
    "edges": [
      {"source": "extract_text", "target": "summarize"}
    ]
  },
  "input_schema": {
    "document": {
      "type": "string",
      "description": "분석할 문서 내용"
    }
  },
  "output_schema": {
    "summary": {
      "type": "string",
      "description": "생성된 요약"
    }
  }
}
```

### 7.3 Skill 사용 (skill_executor)

워크플로우에서 Skill을 실행:

```json
{
  "id": "send_notification",
  "type": "skill_executor",
  "config": {
    "skill_ref": "email-automation-abc123",
    "skill_version": "1.0.0",
    "tool_call": "send_email",
    "input_mapping": {
      "recipient_email": "to",
      "email_subject": "subject",
      "email_body": "body"
    },
    "output_key": "email_result",
    "error_handling": "retry"
  }
}
```

**실행 흐름** (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py)):

```python
def skill_executor_runner(state: Dict, config: Dict) -> Dict:
    # 1. Skill 로드
    active_skills = state.get("active_skills", {})
    skill = active_skills.get(config["skill_ref"])
    
    # 2. Tool 정의 찾기
    tool_def = next(t for t in skill["tool_definitions"] 
                    if t["name"] == config["tool_call"])
    
    # 3. 입력 매핑
    rendered_inputs = {
        key: _render_template(template, state)
        for key, template in config["input_mapping"].items()
    }
    
    # 4. Handler 실행
    handler = NODE_REGISTRY[tool_def["handler_type"]]
    result = handler(state, {**tool_def["handler_config"], **rendered_inputs})
    
    # 5. 실행 로그 기록
    skill_execution_log.append({
        "skill_id": config["skill_ref"],
        "tool_name": config["tool_call"],
        "timestamp": datetime.now().isoformat()
    })
    
    return {config["output_key"]: result}
```

### 7.4 Skill 의존성 관리

**SemVer 버전 제약**:

```python
class SkillDependency(BaseModel):
    skill_id: str
    version_constraint: str = "^1.0.0"  # 1.x.x 호환
    constraint_type: VersionConstraint  # EXACT, CARET, TILDE, RANGE, LATEST
    locked_version: Optional[str]       # 배포 시 고정된 버전
```

**버전 호환성 검사**:

```python
# ^1.0.0: 1.x.x (major 버전 일치, minor/patch 업그레이드 허용)
# ~1.0.0: 1.0.x (major.minor 일치, patch 업그레이드만 허용)
# >=1.0.0 <2.0.0: 범위 지정

current_version = SemVer.parse("1.2.3")
constraint = "^1.0.0"
is_compatible = current_version.is_compatible_with(constraint, VersionConstraint.CARET)
```

### 7.5 Skill Context Hydration

워크플로우 실행 전 Skill이 자동으로 로드됩니다:

```python
# 워크플로우 설정에서 skill_executor 노드 감지
skill_refs = extract_skill_refs(workflow_config)

# Skill Repository에서 로드 (의존성 재귀 해결)
hydrated_skills = skill_repository.hydrate_skills(skill_refs)

# 실행 상태에 주입
initial_state["active_skills"] = hydrated_skills
```

### 7.6 AWS IAM 권한 관리

Skill이 AWS 리소스에 접근해야 할 때:

```python
class AWSPermission(BaseModel):
    service: AWSService  # s3, dynamodb, lambda, etc.
    actions: List[str]   # ["GetObject", "PutObject"]
    resources: List[str] # ["arn:aws:s3:::my-bucket/*"]
    conditions: Optional[Dict]

# IAM Policy 생성
policy = generate_iam_policy(skill.required_permissions, skill.skill_id)
```

생성된 IAM Policy는 Lambda 실행 역할에 동적으로 연결되거나, STS AssumeRole로 임시 자격 증명을 획득합니다.

---

## 8. 조건부 처리 패턴

### 8.1 SafeExpressionEvaluator

조건부 로직은 **안전한 표현식 평가기**를 사용합니다 (from [expression_evaluator.py](analemma-workflow-os/backend/src/services/operators/expression_evaluator.py)):

```python
class SafeExpressionEvaluator:
    """
    eval()이나 exec() 없이 안전하게 표현식 평가.
    
    지원 패턴:
    - $.field - 필드 접근
    - $.field.nested - 중첩 접근
    - $.field[0] - 배열 인덱스
    - $.field == 'value' - 비교 연산
    - $.field > 10 - 크기 비교
    - $.field in ['a', 'b'] - 멤버십 테스트
    - $.field and $.other - 논리 연산 (AND, OR, NOT)
    """
    
    def __init__(self, context: Dict[str, Any]):
        self.context = context
    
    def evaluate(self, expression: str) -> bool:
        """표현식을 평가하여 True/False 반환"""
        # 1. 논리 연산자 처리 (and, or, not)
        # 2. 비교 연산자 처리 (==, !=, >, <, >=, <=)
        # 3. 멤버십 연산자 처리 (in, not in)
        # 4. JSONPath 경로 해석 ($.field.nested)
```

**보안 특징**:
- ❌ `eval()` / `exec()` 사용 금지
- ✅ 화이트리스트 연산자만 허용
- ✅ 함수 호출 불가
- ✅ 최대 중첩 깊이 제한 (DoS 방지)

### 8.2 While/Loop 조건부 반복

#### 노드 조합 패턴 1: Loop 노드

```json
{
  "id": "quality_improvement",
  "type": "loop",
  "config": {
    "max_iterations": 5,
    "condition": "quality_score >= 0.9",
    "convergence_key": "quality_score",
    "target_score": 0.9,
    "loop_var": "iteration_count",
    "nodes": [
      {
        "id": "generate_content",
        "type": "llm_chat",
        "config": {
          "prompt_content": "{{topic}}에 대한 문서 작성 (시도 {{iteration_count}})"
        }
      },
      {
        "id": "evaluate_quality",
        "type": "llm_chat",
        "config": {
          "prompt_content": "다음 문서의 품질을 0~1로 평가: {{generate_content_output}}",
          "response_schema": {
            "type": "object",
            "properties": {
              "score": {"type": "number"}
            }
          }
        }
      },
      {
        "id": "extract_score",
        "type": "operator_official",
        "config": {
          "strategy": "deep_get",
          "input": "{{evaluate_quality_output}}",
          "params": {"path": "score"},
          "output_key": "quality_score"
        }
      }
    ]
  }
}
```

**실행 흐름** (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py#L2819-L2913)):

```python
def loop_runner(state: Dict, config: Dict) -> Dict:
    max_iterations = config.get("max_iterations", 5)
    condition = config.get("condition")
    convergence_key = config.get("convergence_key")
    target_score = config.get("target_score", 0.9)
    
    for i in range(max_iterations):
        # 서브 노드 순차 실행
        for node_def in config["nodes"]:
            updates = execute_node(state, node_def)
            state.update(updates)
        
        # 1. 로직 기반 조건 체크
        evaluator = SafeExpressionEvaluator(state)
        if evaluator.evaluate(condition):
            logger.info("조건 충족, 루프 종료")
            break
        
        # 2. 수렴 기반 조건 체크
        if convergence_key:
            score = state.get(convergence_key)
            if score >= target_score:
                logger.info(f"수렴 달성: {score} >= {target_score}")
                break
    
    return {"loop_exit_reason": "convergence_reached"}
```

#### 노드 조합 패턴 2: While 엣지

While 엣지는 LangGraph의 조건부 반복을 사용합니다:

```json
{
  "nodes": [
    {
      "id": "check_condition",
      "type": "operator_official",
      "config": {
        "strategy": "if_else",
        "condition": "$.attempts < 5",
        "true_value": "continue",
        "false_value": "stop",
        "output_key": "loop_decision"
      }
    },
    {
      "id": "process_step",
      "type": "llm_chat",
      "config": {
        "prompt_content": "작업 수행 (시도 {{attempts}})"
      }
    },
    {
      "id": "increment_counter",
      "type": "operator",
      "config": {
        "sets": {
          "attempts": "{{attempts + 1}}"
        }
      }
    }
  ],
  "edges": [
    {"source": "check_condition", "target": "process_step", "condition": "{{loop_decision}} == 'continue'"},
    {"source": "process_step", "target": "increment_counter"},
    {
      "source": "increment_counter",
      "target": "check_condition",
      "type": "while",
      "condition": "{{attempts}} < 5",
      "max_iterations": 10
    }
  ]
}
```

### 8.3 조건부 분기 (Conditional Branching)

#### 패턴 1: operator_official의 if_else

```json
{
  "id": "route_by_score",
  "type": "operator_official",
  "config": {
    "strategy": "if_else",
    "condition": "$.quality_score > 0.8",
    "true_value": "high_quality_path",
    "false_value": "low_quality_path",
    "output_key": "routing_decision"
  }
}
```

#### 패턴 2: Dynamic Router (LLM 기반)

자연어 조건을 LLM이 평가:

```json
{
  "nodes": [
    {
      "id": "evaluate_conditions",
      "type": "llm_chat",
      "config": {
        "prompt_content": "다음 조건들을 평가하고 선택할 브랜치를 반환하세요:\n1. 품질 점수 > 90%: branch_0\n2. 개선 필요: branch_1\n3. 기타: default\n\n현재 품질: {{quality_score}}",
        "response_schema": {
          "type": "object",
          "properties": {
            "selected_branch": {"type": "string"},
            "reason": {"type": "string"}
          }
        },
        "output_key": "__router_result"
      }
    },
    {
      "id": "router",
      "type": "operator",
      "config": {
        "code": "state['next_node'] = state['__router_result']['selected_branch']"
      }
    }
  ],
  "edges": [
    {"source": "evaluate_conditions", "target": "router"},
    {
      "source": "router",
      "target": "dynamic_router",
      "type": "conditional_edge",
      "router_func": "dynamic_router",
      "mapping": {
        "branch_0": "high_priority_handler",
        "branch_1": "improvement_handler",
        "default": "fallback_handler"
      }
    }
  ]
}
```

**Dynamic Router 함수** (from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py)):

```python
def dynamic_router(state: Dict[str, Any]) -> str:
    """
    LLM 평가 결과를 읽어 브랜치 선택.
    
    Expected state:
    {
        "__router_result": {
            "selected_branch": "branch_0",
            "reason": "Quality score is above 90%"
        }
    }
    """
    router_result = state.get("__router_result", {})
    selected = router_result.get("selected_branch", "default")
    logger.info(f"Dynamic Router selected: {selected}")
    return selected
```

### 8.4 조건부 표현식 예시

**비교 연산**:
```python
"$.score > 80"
"$.status == 'approved'"
"$.count >= 10"
```

**논리 연산**:
```python
"$.score > 80 and $.verified == true"
"$.priority == 'high' or $.urgent == true"
"not $.failed"
```

**멤버십 테스트**:
```python
"$.category in ['urgent', 'critical']"
"$.status not in ['archived', 'deleted']"
```

**중첩 접근**:
```python
"$.user.profile.verified == true"
"$.metadata.tags[0] == 'important'"
```

---

## 9. 동적 커널 스케줄링 및 워크플로우 구조 수정

### 9.1 커널 스케줄링 개요

Analemma Workflow OS는 **4단계 방어 메커니즘**을 통해 워크플로우 실행을 지능적으로 관리하고, 필요 시 런타임에 워크플로우 구조를 동적으로 수정합니다.

#### 4단계 아키텍처 (from [concurrency_controller.py](analemma-workflow-os/backend/src/services/quality_kernel/concurrency_controller.py)):

```
┌─────────────────────────────────────────────────────────────┐
│          4단계 방어 메커니즘 (Defense in Depth)               │
└─────────────────────────────────────────────────────────────┘

1단계: Reserved Concurrency (Lambda 레벨)
   └─> template.yaml에서 동시성 제한 설정

2단계: 커널 스케줄링 (Scheduling Layer)
   ├─> Task Buffering: 즉시 실행 대신 버퍼링
   ├─> Batching: 짧은 연산 노드들을 묶어서 처리
   ├─> Throttling: 부하 기반 속도 조절
   ├─> Priority Queue: 중요도 기반 우선순위
   ├─> Fast Track: priority=realtime 시 배치 bypass
   └─> Distributed State: 분산 환경 동시성 추적

3단계: 지능형 재시도 (Execution Layer)
   ├─> 적응형 품질 임계값
   ├─> 정보 증류 (Distillation)
   └─> 커널 내부 재시도 (3회)

4단계: 비용/드리프트 가드레일 (Guardrail Layer)
   ├─> 비용 서킷 브레이커
   ├─> 시맨틱 드리프트 감지
   └─> 예산 감시
```

### 9.2 동적 워크플로우 분할 (Pattern 1: Auto-Split)

**메모리 또는 복잡도 임계값 초과 시 워크플로우를 자동으로 분할합니다.**

#### 분할 트리거 조건 (from [segment_runner_service.py](analemma-workflow-os/backend/src/services/execution/segment_runner_service.py)):

```python
# 메모리 사용률 80% 초과
MEMORY_SAFETY_THRESHOLD = 0.8

# 최소 노드 수
MIN_NODES_PER_SUB_SEGMENT = 2

# 최대 분할 깊이 (무한 분할 방지)
MAX_SPLIT_DEPTH = 3
```

#### 자동 분할 실행 흐름:

```python
def _execute_with_auto_split(segment_config, initial_state, auth_user_id, split_depth=0):
    """
    메모리 기반 동적 분할 실행
    
    동작:
    1. 현재 메모리 사용률 체크
    2. MEMORY_SAFETY_THRESHOLD 초과 시 워크플로우를 2개 서브 세그먼트로 분할
    3. 각 서브 세그먼트 재귀 실행
    4. 결과 병합
    """
    
    # 1. 메모리 체크
    memory_info = get_lambda_memory_info()
    memory_usage_ratio = memory_info['used_mb'] / memory_info['total_mb']
    
    # 2. 분할 필요 여부 판단
    needs_split = (
        memory_usage_ratio > MEMORY_SAFETY_THRESHOLD
        and split_depth < MAX_SPLIT_DEPTH
        and len(segment_config['nodes']) >= MIN_NODES_PER_SUB_SEGMENT * 2
    )
    
    if needs_split:
        logger.warning(
            f"[Kernel Split] Memory threshold exceeded: {memory_usage_ratio:.1%}. "
            f"Splitting segment into 2 sub-segments (depth={split_depth})"
        )
        
        # 3. 워크플로우 분할
        nodes = segment_config['nodes']
        mid_point = len(nodes) // 2
        
        sub_segment_1 = {
            **segment_config,
            'nodes': nodes[:mid_point],
            '_split_depth': split_depth + 1
        }
        sub_segment_2 = {
            **segment_config,
            'nodes': nodes[mid_point:],
            '_split_depth': split_depth + 1
        }
        
        # 4. 순차 실행
        state_1 = _execute_with_auto_split(sub_segment_1, initial_state, auth_user_id, split_depth + 1)
        state_2 = _execute_with_auto_split(sub_segment_2, state_1, auth_user_id, split_depth + 1)
        
        return state_2
    
    # 분할 불필요 - 직접 실행
    return run_workflow(segment_config, initial_state)
```

**실제 사용 예시**:

```json
{
  "workflow_name": "Large Document Processing",
  "nodes": [
    {"id": "node_1", "type": "llm_chat", "config": {...}},
    {"id": "node_2", "type": "llm_chat", "config": {...}},
    {"id": "node_3", "type": "llm_chat", "config": {...}},
    {"id": "node_4", "type": "llm_chat", "config": {...}},
    {"id": "node_5", "type": "llm_chat", "config": {...}},
    {"id": "node_6", "type": "llm_chat", "config": {...}}
  ]
}
```

실행 중 메모리 사용률이 80%를 초과하면:

```
Original Workflow (6 nodes)
   ↓ [Auto-Split at 80% memory]
   ├─> Sub-segment 1 (nodes 1-3)
   │   ↓ [Execute]
   │   └─> state_1
   │
   └─> Sub-segment 2 (nodes 4-6)
       ↓ [Execute with state_1]
       └─> state_2 (final)
```

### 9.3 병렬 브랜치 스케줄링 (Pattern 3: Parallel Scheduler)

**리소스 제약을 고려하여 병렬 브랜치를 배치로 실행합니다.**

#### 스케줄링 전략:

```python
# SPEED_OPTIMIZED: 최대 병렬 실행 (메모리 허용 범위 내)
# RESOURCE_OPTIMIZED: 리소스 효율 우선 (메모리 제약 준수)
# COST_OPTIMIZED: 비용 최소화 (토큰 제한 준수)
```

#### 리소스 정책 설정 (from [test_parallel_scheduler_workflow.json](analemma-workflow-os/backend/src/test_workflows/test_parallel_scheduler_workflow.json)):

```json
{
  "id": "parallel_analysis",
  "type": "parallel_group",
  "config": {
    "resource_policy": {
      "strategy": "RESOURCE_OPTIMIZED",
      "max_concurrent_memory_mb": 3072,
      "max_concurrent_tokens": 100000,
      "max_concurrent_branches": 10,
      "branch_estimates": {
        "memory_mb": 256,
        "tokens": 5000
      }
    },
    "branches": [
      {"branch_id": "branch_0", "nodes": [...]},
      {"branch_id": "branch_1", "nodes": [...]},
      {"branch_id": "branch_2", "nodes": [...]},
      {"branch_id": "branch_3", "nodes": [...]},
      {"branch_id": "branch_4", "nodes": [...]},
      {"branch_id": "branch_5", "nodes": [...]},
      {"branch_id": "branch_6", "nodes": [...]},
      {"branch_id": "branch_7", "nodes": [...]}
    ]
  }
}
```

**실행 시 동적 배치 처리**:

```python
# 리소스 계산
total_branches = 8
estimated_memory_per_branch = 256 MB
total_memory_needed = 8 * 256 = 2048 MB

# 동시성 제한
max_concurrent_memory = 3072 MB
max_concurrent_branches = 3072 / 256 = 12 branches

# 배치 분할
batch_size = min(12, max_concurrent_branches) = 10
batches = [
    [branch_0, branch_1, branch_2, branch_3, branch_4, branch_5, branch_6, branch_7]  # 8개 브랜치 동시 실행 가능
]

# 실제로는 메모리 여유가 있으므로 1개 배치로 모두 병렬 실행
```

메모리가 부족한 경우:

```python
# 리소스 부족 시나리오
total_branches = 20
estimated_memory_per_branch = 512 MB
total_memory_needed = 20 * 512 = 10240 MB

# 동시성 제한
max_concurrent_memory = 3072 MB
max_concurrent_branches = 3072 / 512 = 6 branches

# 배치 분할 (6개씩 실행)
batches = [
    [branch_0 ~ branch_5],   # Batch 1
    [branch_6 ~ branch_11],  # Batch 2
    [branch_12 ~ branch_17], # Batch 3
    [branch_18, branch_19]   # Batch 4
]
```

### 9.4 커널 태스크 스케줄러

#### Fast Track 우선순위 처리

```json
{
  "id": "critical_notification",
  "type": "llm_chat",
  "config": {
    "priority": "realtime",
    "prompt_content": "긴급 알림 생성"
  }
}
```

**실행 흐름**:

```python
def schedule_task(workflow_id, task_config, state, executor):
    # 0. 우선순위 추출
    priority = task_config.get('priority', 'normal')
    
    # Fast Track 판단
    is_fast_track = priority in ['realtime', 'high']
    
    if is_fast_track:
        # 배치 우회, 쓰로틀링 면제, 즉시 실행
        return executor(task_config, state)
    
    # 1. 동시성 스냅샷
    snapshot = get_concurrency_snapshot()
    
    # 2. 쓰로틀링 적용
    if snapshot.load_level == LoadLevel.HIGH:
        time.sleep(0.05)  # 50ms 지연
    elif snapshot.load_level == LoadLevel.CRITICAL:
        time.sleep(0.2)   # 200ms 지연
    
    # 3. 배치 처리 검토
    if should_batch(task_config, priority):
        add_to_batch(workflow_id, task_config, state)
        return {'__batched': True}
    
    # 4. 즉시 실행
    return executor(task_config, state)
```

### 9.5 동적 루프 제한

워크플로우 복잡도에 따라 루프 제한값을 동적으로 계산합니다.

```python
# 동적 계산 공식
L_main = S + 20    # 메인 루프 제한 = 세그먼트 수 + 20
L_branch = S + 10  # 브랜치 루프 제한 = 세그먼트 수 + 10

# 예시
total_segments = 15
max_loop = 15 + 20 = 35
max_branch_loop = 15 + 10 = 25
```

**워크플로우 설정**:

```json
{
  "id": "adaptive_loop",
  "type": "loop",
  "config": {
    "max_iterations": "{{total_segments + 20}}",
    "condition": "quality_score >= 0.9",
    "nodes": [...]
  }
}
```

### 9.6 재시도 및 부분 성공

#### 커널 내부 재시도 (3회)

```python
KERNEL_MAX_RETRIES = 3
KERNEL_RETRY_BASE_DELAY = 1.0  # 지수 백오프

# 재시도 가능 에러 패턴
RETRYABLE_ERROR_PATTERNS = [
    'ThrottlingException',
    'ServiceUnavailable',
    'TooManyRequestsException',
    'ProvisionedThroughputExceeded',
    'InternalServerError',
    'ConnectionError',
    'TimeoutError'
]
```

**실행 흐름**:

```python
for attempt in range(KERNEL_MAX_RETRIES + 1):
    try:
        result = execute_segment(segment_config, initial_state)
        return result  # 성공
    except Exception as e:
        if attempt < KERNEL_MAX_RETRIES and is_retryable_error(e):
            # 지수 백오프: 1s, 2s, 4s
            delay = KERNEL_RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Retry {attempt + 1}/{KERNEL_MAX_RETRIES}: {e}. Waiting {delay:.2f}s")
            time.sleep(delay)
        else:
            raise  # 재시도 불가능 또는 최대 횟수 도달
```

#### 부분 성공 (Partial Success)

```python
ENABLE_PARTIAL_SUCCESS = True

# 세그먼트 실패 시 워크플로우를 중단하지 않고 계속 진행
if segment_failed and ENABLE_PARTIAL_SUCCESS:
    logger.warning(f"Segment {segment_id} failed, but continuing (partial success enabled)")
    result_state['__segment_failures'] = result_state.get('__segment_failures', []) + [segment_id]
    result_state['__partial_success'] = True
```

### 9.7 분산 상태 동기화

Lambda Scale-out 환경에서 전역 동시성을 추적합니다.

```python
# DynamoDB를 이용한 분산 상태 관리
class DistributedStateManager:
    def increment_executions(self, count: int):
        """전역 동시성 증가 (Atomic Counter)"""
        dynamodb.update_item(
            Key={'pk': 'global', 'sk': 'concurrency'},
            UpdateExpression='ADD active_executions :val',
            ExpressionAttributeValues={':val': count}
        )
    
    def get_global_state(self) -> Dict:
        """전역 상태 조회"""
        response = dynamodb.get_item(
            Key={'pk': 'global', 'sk': 'concurrency'}
        )
        return response.get('Item', {'active_executions': 0})
```

**KernelStateTable 스키마** (from [template.yaml](analemma-workflow-os/backend/template.yaml)):

```yaml
KernelStateTable:
  Type: AWS::DynamoDB::Table
  Properties:
    TableName: !Sub "analemma-kernel-state-${StageName}"
    BillingMode: PAY_PER_REQUEST
    AttributeDefinitions:
      - AttributeName: pk
        AttributeType: S
      - AttributeName: sk
        AttributeType: S
    KeySchema:
      - AttributeName: pk
        KeyType: HASH
      - AttributeName: sk
        KeyType: RANGE
    TimeToLiveSpecification:
      AttributeName: ttl
      Enabled: true
```

### 9.8 워크플로우 구조 수정 시나리오 요약

| 시나리오 | 수정 방식 | 트리거 조건 | 구현 패턴 |
|---------|----------|-----------|----------|
| **메모리 부족** | 워크플로우 분할 | 메모리 사용률 > 80% | Pattern 1: Auto-Split |
| **과도한 병렬 브랜치** | 배치 실행 | 브랜치 수 * 메모리 > 제한 | Pattern 3: Parallel Scheduler |
| **높은 부하** | 태스크 버퍼링/배치 | 동시성 > 임계값 | KernelTaskScheduler |
| **실행 실패** | 자동 재시도 | Retryable 에러 발생 | Kernel Internal Retry |
| **복잡한 루프** | 동적 제한 계산 | 세그먼트 수 증가 | Dynamic Loop Limit |
| **긴급 요청** | Fast Track | priority=realtime | Fast Track Bypass |

### 9.9 실제 사용 예시

#### 예시 1: 대규모 병렬 분석 워크플로우

```json
{
  "workflow_name": "Massive Parallel Analysis",
  "nodes": [
    {
      "id": "parallel_processor",
      "type": "parallel_group",
      "config": {
        "resource_policy": {
          "strategy": "RESOURCE_OPTIMIZED",
          "max_concurrent_memory_mb": 3072,
          "branch_estimates": {
            "memory_mb": 512
          }
        },
        "branches": [
          {"branch_id": "branch_0", "nodes": [...]},
          {"branch_id": "branch_1", "nodes": [...]},
          ...
          {"branch_id": "branch_19", "nodes": [...]}
        ]
      }
    }
  ]
}
```

**커널 동작**:
1. 20개 브랜치 감지
2. 각 브랜치 512 MB 추정 → 총 10,240 MB 필요
3. 제한 3,072 MB → 동시 실행 가능: 6개 브랜치
4. 자동으로 4개 배치로 분할하여 순차 실행:
   - Batch 1: branch_0 ~ branch_5
   - Batch 2: branch_6 ~ branch_11
   - Batch 3: branch_12 ~ branch_17
   - Batch 4: branch_18, branch_19

#### 예시 2: 긴급 알림 + 일반 작업 혼합

```json
{
  "nodes": [
    {
      "id": "urgent_alert",
      "type": "llm_chat",
      "config": {
        "priority": "realtime",
        "prompt_content": "긴급 알림 생성"
      }
    },
    {
      "id": "normal_task_1",
      "type": "llm_chat",
      "config": {
        "prompt_content": "일반 작업 1"
      }
    },
    {
      "id": "normal_task_2",
      "type": "llm_chat",
      "config": {
        "prompt_content": "일반 작업 2"
      }
    }
  ]
}
```

**커널 동작**:
1. `urgent_alert` → Fast Track 경로 (배치 우회, 즉시 실행)
2. `normal_task_1`, `normal_task_2` → 부하 확인 후 배치 가능 시 묶어서 실행

---

## 10. 스키마 검증

### 7.1 노드 타입 검증

from [main.py](analemma-workflow-os/backend/src/handlers/core/main.py#L304-L332):

```python
@field_validator('type', mode='before')
@classmethod
def alias_and_validate_node_type(cls, v):
    """
    노드 타입 별칭 변환 및 검증
    - 'code' -> 'operator'
    - 'aiModel' -> 'llm_chat'
    - 'control.loop' -> 'for_each'
    """
    v = v.strip().lower()
    
    # 별칭 변환
    if v in NODE_TYPE_ALIASES:
        return NODE_TYPE_ALIASES[v]
    
    # 허용된 타입 확인
    all_valid_types = ALLOWED_NODE_TYPES | UI_MARKER_TYPES
    if v not in all_valid_types:
        raise ValueError(f"Unknown node type: '{v}'")
    
    return v
```

### 7.2 LLM 노드 검증

from [save_workflow.py](analemma-workflow-os/backend/src/handlers/utils/save_workflow.py#L154-L223):

```python
def _validate_workflow_config(cfg):
    """워크플로우 설정 검증"""
    nodes = cfg.get('nodes', [])
    for node in nodes:
        if node.get('type') in ('llm_chat', 'aiModel'):
            # Temperature 검증
            temp = node.get('temperature')
            if temp is not None:
                if not (0.0 <= float(temp) <= 1.0):
                    return None, 'temperature must be between 0 and 1'
            
            # Max tokens 검증
            max_tokens = node.get('max_tokens')
            if max_tokens is not None:
                if int(max_tokens) <= 0:
                    return None, 'max_tokens must be a positive integer'
            
            # Provider 자동 보정
            model = node.get('model')
            if model:
                correct_provider = get_model_provider(model).value.lower()
                node['provider'] = correct_provider
    
    return cfg, None
```

---

## 8. 실행 컨텍스트

### 8.1 초기 상태

```python
initial_state = {
    "execution_id": "exec-abc123",
    "workflow_id": "wf-xyz789",
    "owner_id": "user-123",
    "input_text": "사용자 입력 데이터",
    "user_api_keys": {
        "gemini": "sk-...",
        "openai": "sk-..."
    },
    "step_history": [],
    "messages": []
}
```

### 8.2 최종 상태

```python
final_state = {
    # 원본 필드 유지
    "execution_id": "exec-abc123",
    "workflow_id": "wf-xyz789",
    
    # 노드 출력
    "analyze_sentiment_output": "긍정적인 감정입니다.",
    
    # 메타데이터
    "analyze_sentiment_meta": {
        "model": "gemini-2.0-flash",
        "provider": "gemini",
        "usage": {
            "input_tokens": 150,
            "output_tokens": 50,
            "total_tokens": 200
        }
    },
    
    # 실행 이력
    "step_history": [
        "api_entry:trigger",
        "extract_text:operator",
        "analyze_sentiment:llm_call"
    ],
    
    # 토큰 집계
    "total_input_tokens": 150,
    "total_output_tokens": 50,
    "total_tokens": 200,
    "estimated_cost_usd": 0.0002
}
```

---

## 9. 보안 및 제약

### 9.1 예약 키 보호

시스템 메타데이터는 노드가 덮어쓸 수 없습니다:

```python
RESERVED_STATE_KEYS = {
    "workflowId", "execution_id", "loop_counter",
    "segment_id", "state_s3_path", "user_api_keys",
    "step_history", "execution_logs", "__s3_offloaded",
    "scheduling_metadata", "guardrail_verified",
    "status", "error_info"
}
```

### 9.2 코드 실행 제한

`operator` 노드의 코드 실행은 **MOCK_MODE**에서만 허용됩니다:

```python
if not mock_mode_enabled:
    raise PermissionError("Operator code execution is disabled outside MOCK_MODE")
```

프로덕션에서는 `operator_official` (Safe Operator)를 사용하세요.

### 9.3 API 호출 제한

- 허용된 메서드: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`
- 허용된 프로토콜: `http`, `https`
- 포트 제한: 80, 443만 허용
- 내부 IP 차단: Private/Loopback/Link-local IP 접근 불가

### 9.4 DB 쿼리 제한

프로덕션 환경에서는 명시적으로 `ALLOW_DB_QUERY=true` 환경 변수 설정 필요.

---

## 10. 성능 최적화

### 10.1 병렬 처리

```python
# ForEach 병렬 실행
cpu_count = os.cpu_count() or 2
max_workers = min(len(items), max(1, cpu_count // 2))

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    results = list(executor.map(process_item, items))
```

### 10.2 S3 오프로딩

대용량 데이터는 자동으로 S3에 저장되고 포인터만 state에 유지:

```json
{
  "for_each_results": {
    "__s3_offloaded": true,
    "s3_path": "s3://bucket/offloaded-results/...",
    "size_kb": 256.5
  }
}
```

### 10.3 Hydration on Demand

S3 오프로드된 데이터는 필요할 때만 다운로드:

```python
hydrated_value = _hydrate_s3_value(state["large_data"])
```

---

## 11. 디버깅 및 모니터링

### 11.1 Step History

모든 노드 실행이 `step_history`에 기록됩니다:

```json
{
  "step_history": [
    "api_entry:trigger",
    "extract_text:operator",
    "analyze_sentiment:llm_call",
    "for_each_node:iteration_0",
    "for_each_node:iteration_1"
  ]
}
```

### 11.2 Glass-Box Logging

LLM 호출의 세부 정보를 추적:

```python
class StateHistoryCallback:
    def on_llm_start(self, prompts):
        self.logs.append({
            "type": "ai_thought",
            "content": "Thinking...",
            "timestamp": int(time.time())
        })
    
    def on_llm_end(self, response):
        self.logs[-1]["status"] = "COMPLETED"
        self.logs[-1]["content"] = response.text
```

### 11.3 토큰 추적

모든 LLM 호출의 토큰 사용량이 자동 집계됩니다:

```json
{
  "total_input_tokens": 1500,
  "total_output_tokens": 500,
  "total_tokens": 2000,
  "estimated_cost_usd": 0.002,
  "iteration_token_details": [
    {
      "iteration": 0,
      "input_tokens": 150,
      "output_tokens": 50
    }
  ]
}
```

---

## 12. 참고 자료

### 12.1 관련 파일

- **스키마 정의**: [orchestrator_service.py](analemma-workflow-os/backend/src/services/workflow/orchestrator_service.py), [graph_dsl.py](analemma-workflow-os/backend/src/common/graph_dsl.py)
- **노드 러너**: [main.py](analemma-workflow-os/backend/src/handlers/core/main.py)
- **워크플로우 빌더**: [builder.py](analemma-workflow-os/backend/src/services/workflow/builder.py)
- **검증 로직**: [save_workflow.py](analemma-workflow-os/backend/src/handlers/utils/save_workflow.py)
- **커널 스케줄러**: [concurrency_controller.py](analemma-workflow-os/backend/src/services/quality_kernel/concurrency_controller.py)
- **세그먼트 러너**: [segment_runner_service.py](analemma-workflow-os/backend/src/services/execution/segment_runner_service.py)
- **Skill 모델**: [skill_models.py](analemma-workflow-os/backend/src/models/skill_models.py)
- **표현식 평가기**: [expression_evaluator.py](analemma-workflow-os/backend/src/services/operators/expression_evaluator.py)

### 12.2 테스트 예시

- **간단한 워크플로우**: [test_trigger_workflow.json](test_trigger_workflow.json)
- **병렬 처리**: [test_hyper_stress_workflow.json](analemma-workflow-os/tests/backend/workflows/test_hyper_stress_workflow.json)
- **동적 루프**: [test_loop_limit_dynamic_workflow.json](analemma-workflow-os/tests/backend/workflows/test_loop_limit_dynamic_workflow.json)
- **병렬 스케줄러**: [test_parallel_scheduler_workflow.json](analemma-workflow-os/backend/src/test_workflows/test_parallel_scheduler_workflow.json)

---

## 요약

Analemma Workflow OS는 다음과 같은 특징을 가진 워크플로우 시스템입니다:

✅ **선언적 JSON 스키마** - 노드와 엣지로 워크플로우 정의
✅ **동적 빌드** - 런타임에 LangGraph로 컴파일
✅ **다양한 노드 타입** - LLM, 연산자, 제어 흐름, API 호출 등
✅ **고급 제어 흐름** - 반복, 병렬, 조건부 분기, 서브그래프
✅ **Skill 시스템** - 재사용 가능한 도구 및 서브워크플로우
✅ **안전한 조건 평가** - SafeExpressionEvaluator로 보안 강화
✅ **지능형 스케줄링** - 4단계 방어 메커니즘, Fast Track, 배치 처리
✅ **동적 워크플로우 수정** - 메모리 기반 자동 분할, 병렬 브랜치 배치 실행
✅ **자동 최적화** - S3 오프로딩, 토큰 집계, 분산 상태 동기화
✅ **안전성** - 예약 키 보호, 스키마 검증, 커널 재시도 메커니즘
✅ **모니터링** - Step History, Glass-Box Logging, 품질 커널

이 문서는 워크플로우 설정의 모든 측면을 다루며, 실제 사용 예시를 통해 쉽게 이해할 수 있도록 구성되었습니다.
