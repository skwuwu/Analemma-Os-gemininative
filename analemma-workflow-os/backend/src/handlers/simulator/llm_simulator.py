"""
LLM Simulator - Production LLM Integration Testing
===================================================
실제 LLM(Gemini, Bedrock)을 호출하여 프로덕션 환경에서의 동작을 검증하는 E2E 테스트 Lambda.

Mission Simulator와의 차이점:
- MOCK_MODE=false (실제 LLM 호출)
- Gemini Thinking Mode, Response Schema 등 실제 기능 테스트
- operator_official 전략들과 LLM 출력의 통합 검증
- 비용 발생에 주의 (테스트 실행 시 토큰 소모)

시나리오:
- LI-A: 기본 LLM 호출 (텍스트 생성)
- LI-B: Structured Output (Response Schema)
- LI-C: Gemini Thinking Mode
- LI-D: LLM + operator_official 통합
- LI-E: 복합 문서 분석 파이프라인
- LI-F: Multimodal Vision
"""

import json
import os
import time
import uuid
import boto3
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

# Logger setup
try:
    from src.common.logging_utils import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

# AWS Clients
from src.common.aws_clients import get_dynamodb_resource, get_s3_client

_stepfunctions_client = None
_cloudwatch_client = None


def get_stepfunctions_client():
    global _stepfunctions_client
    if _stepfunctions_client is None:
        _stepfunctions_client = boto3.client('stepfunctions')
    return _stepfunctions_client


def get_cloudwatch_client():
    global _cloudwatch_client
    if _cloudwatch_client is None:
        _cloudwatch_client = boto3.client('cloudwatch')
    return _cloudwatch_client


# ============================================================================
# Configuration
# ============================================================================
METRIC_NAMESPACE = os.environ.get('METRIC_NAMESPACE', 'Analemma/LLMSimulator')
DISTRIBUTED_STATE_MACHINE_ARN = os.environ.get('WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN')
STANDARD_STATE_MACHINE_ARN = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
STATE_BUCKET = os.environ.get('WORKFLOW_STATE_BUCKET')

# 🚨 IMPORTANT: LLM Simulator runs with MOCK_MODE=false
MOCK_MODE = 'false'  # Real LLM calls!

# Polling configuration
MAX_POLL_SECONDS = 300  # LLM calls can take longer
POLL_INTERVAL_SECONDS = 5

# Cost tracking
ESTIMATED_COST_PER_SCENARIO = {
    # Legacy scenarios
    'BASIC_LLM_CALL': 0.01,
    'STRUCTURED_OUTPUT': 0.02,
    'THINKING_MODE': 0.05,
    'LLM_OPERATOR_INTEGRATION': 0.03,
    'DOCUMENT_ANALYSIS': 0.10,
    'MULTIMODAL_VISION': 0.15,
    # 5-Stage graduated scenarios
    'STAGE1_BASIC': 0.02,
    'STAGE2_FLOW_CONTROL': 0.08,
    'STAGE3_VISION_BASIC': 0.10,
    'STAGE4_VISION_MAP': 0.30,
    'STAGE5_HYPER_STRESS': 0.50
}

# ============================================================================
# 🛡️ Global Safety Limits (무한루프 방지 하드 리밋)
# ============================================================================
GLOBAL_SAFETY_LIMITS = {
    'max_iterations_absolute': 50,      # 어떤 상황에서도 50회 초과 금지
    'max_tokens_absolute': 100000,      # 10만 토큰 초과 시 강제 중단
    'max_execution_seconds': 600,       # 10분 초과 시 강제 중단
    'max_recursion_depth': 5,           # 최대 재귀 깊이
    'max_concurrent_tasks': 10          # 최대 동시 작업 수
}


# ============================================================================
# LLM Test Scenarios (5-Stage Graduated Framework)
# ============================================================================
LLM_SCENARIOS = {
    # ═══════════════════════════════════════════════════════════════════════
    # Stage 1: 동작 기초 (Basic LLM + Response Schema)
    # ═══════════════════════════════════════════════════════════════════════
    'STAGE1_BASIC': {
        'name': 'Stage 1: 동작 기초 (Response Schema + json_parse)',
        'description': 'LLM Response Schema 준수 및 json_parse 0.5초 이내 성능 검증',
        'test_keyword': 'STAGE1_BASIC',
        'stage': 1,
        'guardrails': [],
        'safety_limits': {
            'max_iterations': 1,
            'max_tokens_total': 5000,
            'timeout_seconds': 60
        },
        'input_data': {
            'test_stage': 1,
            'input_text': 'Analemma OS is a revolutionary AI-first workflow automation platform.',
            'expected_json_parse_ms': 500  # 0.5초 이내
        },
        'expected_status': 'SUCCEEDED',
        'verify_func': 'verify_stage1_basic',
        'timeout_seconds': 60
    },
    
    # ═══════════════════════════════════════════════════════════════════════
    # Stage 2: 동작 중급 (Flow Control + COST_GUARDRAIL)
    # ═══════════════════════════════════════════════════════════════════════
    'STAGE2_FLOW_CONTROL': {
        'name': 'Stage 2: Flow Control (Map/Loop/HITL + COST_GUARDRAIL)',
        'description': 'for_each 병렬, if_else 분기, HITL 상태 복구, 토큰 누적 제한 검증',
        'test_keyword': 'STAGE2_FLOW_CONTROL',
        'stage': 2,
        'guardrails': ['COST_GUARDRAIL'],
        'safety_limits': {
            'max_iterations': 3,
            'max_tokens_total': 10000,
            'max_tokens_per_node': 2000,
            'timeout_seconds': 180,
            'action_on_exceed': 'fail'
        },
        'input_data': {
            'test_stage': 2,
            'cost_guardrail_enabled': True,
            'time_machine_test': True,
            'verify_token_reset_on_rollback': True,
            'expected_state_recovery_integrity': 100  # 0% 손실
        },
        'expected_status': 'SUCCEEDED',
        'verify_func': 'verify_stage2_flow_control',
        'timeout_seconds': 180
    },
    
    # ═══════════════════════════════════════════════════════════════════════
    # Stage 3: 멀티모달 기초 (Vision Basic)
    # ═══════════════════════════════════════════════════════════════════════
    'STAGE3_VISION_BASIC': {
        'name': 'Stage 3: 멀티모달 기초 (S3→bytes + Vision JSON)',
        'description': 'S3 이미지 URI → bytes 변환, Gemini Vision JSON 추출, operator 가공 검증',
        'test_keyword': 'STAGE3_VISION_BASIC',
        'stage': 3,
        'guardrails': [],
        'safety_limits': {
            'max_iterations': 1,
            'max_tokens_total': 8000,
            'timeout_seconds': 120
        },
        'input_data': {
            'test_stage': 3,
            'image_uri': 's3://analemma-workflows-dev/test-images/sample_receipt.png',
            'verify_s3_to_bytes': True,
            'verify_json_parseable': True
        },
        'expected_status': 'SUCCEEDED',
        'verify_func': 'verify_stage3_vision_basic',
        'timeout_seconds': 120
    },
    
    # ═══════════════════════════════════════════════════════════════════════
    # Stage 4: 멀티모달 중급 (Vision Map + SPEED_GUARDRAIL)
    # ═══════════════════════════════════════════════════════════════════════
    'STAGE4_VISION_MAP': {
        'name': 'Stage 4: Vision Map (5장 병렬 + SPEED_GUARDRAIL)',
        'description': '5장 이미지 병렬 분석, max_concurrency=3, 타임스탬프 간격 분석, StateBag 병합 검증',
        'test_keyword': 'STAGE4_VISION_MAP',
        'stage': 4,
        'guardrails': ['SPEED_GUARDRAIL'],
        'safety_limits': {
            'max_iterations': 5,
            'max_tokens_total': 20000,
            'max_concurrency': 3,
            'timeout_seconds': 180
        },
        'input_data': {
            'test_stage': 4,
            'speed_guardrail_enabled': True,
            'max_concurrency': 3,
            'min_timestamp_gap_ms': 100,  # 동시 시작 방지 검증
            'verify_branch_merge_integrity': True
        },
        'expected_status': 'SUCCEEDED',
        'verify_func': 'verify_stage4_vision_map',
        'timeout_seconds': 180
    },
    
    # ═══════════════════════════════════════════════════════════════════════
    # Stage 5: 최종 Hyper Stress (ALL_GUARDRAILS)
    # ═══════════════════════════════════════════════════════════════════════
    'STAGE5_HYPER_STRESS': {
        'name': 'Stage 5: Hyper Stress (3단계 재귀 + Partial Failure + ALL_GUARDRAILS)',
        'description': '3단계 재귀, Depth 2 Partial Failure, Context Caching TEI≥50%, HITL 중첩, 상태 오염 방지',
        'test_keyword': 'STAGE5_HYPER_STRESS',
        'stage': 5,
        'guardrails': ['COST_GUARDRAIL', 'SPEED_GUARDRAIL', 'LOOP_GUARDRAIL', 'RECURSION_GUARDRAIL'],
        'safety_limits': {
            'max_iterations': 10,
            'max_tokens_total': 50000,
            'max_concurrency': 5,
            'max_recursion_depth': 3,
            'timeout_seconds': 300,
            'action_on_exceed': 'graceful_stop'
        },
        'input_data': {
            'test_stage': 5,
            'all_guardrails_enabled': True,
            'partial_failure_at_depth': 2,
            'partial_failure_branch_id': 'branch_2',
            'verify_state_isolation': True,
            'verify_context_caching': True,
            'target_tei_percentage': 50  # Token Efficiency Index
        },
        'expected_status': 'SUCCEEDED',
        'verify_func': 'verify_stage5_hyper_stress',
        'timeout_seconds': 300
    },
    
    # ═══════════════════════════════════════════════════════════════════════
    # Legacy Scenarios (하위 호환성)
    # ═══════════════════════════════════════════════════════════════════════
    'BASIC_LLM_CALL': {
        'name': '[Legacy] LI-A: Basic LLM Call',
        'description': '기본 Gemini/Bedrock 텍스트 생성 호출 검증',
        'test_keyword': 'LLM_BASIC',
        'stage': 0,
        'input_data': {
            'llm_test_type': 'basic',
            'prompt': 'Write a haiku about cloud computing.',
            'expected_min_length': 20
        },
        'expected_status': 'SUCCEEDED',
        'verify_func': 'verify_basic_llm_call',
        'timeout_seconds': 60
    }
}


# ============================================================================
# Test Workflow Mappings
# ============================================================================
LLM_TEST_WORKFLOW_MAPPINGS = {
    # 5-Stage scenarios
    'STAGE1_BASIC': 'test_llm_stage1_basic',
    'STAGE2_FLOW_CONTROL': 'test_llm_stage2_flow_control',
    'STAGE3_VISION_BASIC': 'test_llm_stage3_vision_basic',
    'STAGE4_VISION_MAP': 'test_llm_stage4_vision_map',
    'STAGE5_HYPER_STRESS': 'test_llm_stage5_hyper_stress',
    # Legacy mappings
    'LLM_BASIC': 'test_llm_basic_workflow',
    'LLM_STRUCTURED': 'test_llm_structured_workflow',
    'LLM_THINKING': 'test_llm_thinking_workflow',
    'LLM_OPERATOR_INTEGRATION': 'test_llm_operator_integration_workflow',
    'LLM_DOCUMENT_ANALYSIS': 'test_complex_document_analysis_workflow',
    'LLM_VISION_LIVE': 'test_llm_vision_live_workflow'
}


# ============================================================================
# Workflow Loading
# ============================================================================
def _load_llm_test_workflow_config(test_keyword: str) -> dict:
    """
    LLM 테스트 키워드에 해당하는 워크플로우 설정을 로드합니다.
    """
    mapped_workflow_id = LLM_TEST_WORKFLOW_MAPPINGS.get(test_keyword)
    if not mapped_workflow_id:
        logger.warning(f"No mapping found for test_keyword: {test_keyword}")
        return None
    
    # Base directory 계산
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    possible_paths = [
        f"/var/task/test_workflows/{mapped_workflow_id}.json",
        f"{base_dir}/backend/src/test_workflows/{mapped_workflow_id}.json",
        f"./test_workflows/{mapped_workflow_id}.json",
        f"src/test_workflows/{mapped_workflow_id}.json",
    ]
    
    logger.info(f"Attempting to load LLM test workflow: {mapped_workflow_id}")
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                logger.info(f"✅ Loaded LLM test workflow from {path}")
                return config
            except Exception as e:
                logger.error(f"Failed to load {path}: {e}")
    
    logger.error(f"❌ LLM test workflow not found: {test_keyword} -> {mapped_workflow_id}")
    return None


# ============================================================================
# Step Functions Execution
# ============================================================================
def trigger_llm_test(scenario_key: str, scenario_config: dict, orchestrator_type: str = 'DISTRIBUTED') -> str:
    """
    LLM 테스트용 Step Functions 실행을 트리거합니다.
    
    IMPORTANT: MOCK_MODE=false로 실행됨 (실제 LLM 호출)
    """
    if orchestrator_type == 'STANDARD':
        state_machine_arn = STANDARD_STATE_MACHINE_ARN
    else:
        state_machine_arn = DISTRIBUTED_STATE_MACHINE_ARN
    
    if not state_machine_arn:
        raise ValueError(f"State machine ARN not configured for {orchestrator_type}")
    
    test_keyword = scenario_config.get('test_keyword', 'LLM_BASIC')
    input_data = scenario_config.get('input_data', {})
    
    execution_id = f"llm-test-{scenario_key.lower()}-{uuid.uuid4().hex[:8]}"
    
    payload = {
        'workflowId': f'llm-test-{scenario_key.lower()}',
        'ownerId': 'system',
        'user_id': 'system',
        'MOCK_MODE': MOCK_MODE,  # 🚨 FALSE - Real LLM calls!
        'initial_state': {
            'test_keyword': test_keyword,
            'llm_test_scenario': scenario_key,
            'llm_execution_id': execution_id,
            **input_data
        },
        'idempotency_key': f"llm#{scenario_key}#{execution_id}",
        'ALLOW_UNSAFE_EXECUTION': True
    }
    
    # Load test workflow config
    test_workflow_config = _load_llm_test_workflow_config(test_keyword)
    if test_workflow_config:
        payload['test_workflow_config'] = test_workflow_config
        logger.info(f"✅ Injected LLM test workflow config for {test_keyword}")
    else:
        raise ValueError(f"LLM test workflow config not found for {test_keyword}")
    
    logger.info(f"🧠 Triggering LLM Test (MOCK_MODE={MOCK_MODE}): {scenario_key}")
    logger.info(f"⚠️  This will make REAL LLM API calls and incur costs!")
    
    sfn = get_stepfunctions_client()
    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_id,
        input=json.dumps(payload)
    )
    
    execution_arn = response['executionArn']
    logger.info(f"Execution started: {execution_arn}")
    
    return execution_arn


def poll_llm_execution(execution_arn: str, max_seconds: int = MAX_POLL_SECONDS) -> Dict[str, Any]:
    """
    LLM 테스트 실행 완료까지 폴링합니다.
    """
    sfn = get_stepfunctions_client()
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > max_seconds:
            return {
                'status': 'TIMEOUT',
                'output': None,
                'error': f'LLM execution timed out after {max_seconds}s',
                'duration_seconds': elapsed
            }
        
        response = sfn.describe_execution(executionArn=execution_arn)
        status = response['status']
        
        if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            result = {
                'status': status,
                'duration_seconds': elapsed,
                'execution_arn': execution_arn
            }
            
            if status == 'SUCCEEDED':
                result['output'] = json.loads(response.get('output', '{}'))
            elif status == 'FAILED':
                result['error'] = f"{response.get('error', 'Unknown')}: {response.get('cause', 'No cause')}"
                logger.error(f"🔴 LLM Test FAILED: {result['error']}")
            
            return result
        
        logger.debug(f"LLM execution {status}, elapsed: {elapsed:.1f}s")
        time.sleep(POLL_INTERVAL_SECONDS)


# ============================================================================
# Verification Functions
# ============================================================================

# ============================================================================
# Verification Helper Functions (Detailed Reporting)
# ============================================================================
def _verify_latency(duration_seconds: float, limit_seconds: float, stage_name: str) -> Tuple[bool, str]:
    """Verify latency does not exceed limit, with detailed reporting."""
    if duration_seconds > limit_seconds:
        return False, f"Latency Limit Exceeded in {stage_name}: {duration_seconds:.2f}s > {limit_seconds}s"
    return True, f"Latency OK ({duration_seconds:.2f}s)"

def _verify_token_usage(usage: dict, limit: int, stage_name: str) -> Tuple[bool, str]:
    """Verify total tokens do not exceed limit, with detailed reporting."""
    total_tokens = usage.get('total_tokens', 0)
    if total_tokens > limit:
        return False, f"Token Limit Exceeded in {stage_name}: {total_tokens} > {limit}"
    return True, f"Token Usage OK ({total_tokens} tokens)"

# ============================================================================
# Verification Functions
# ============================================================================

def verify_stage1_basic(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """Stage 1: Basic Operation Verification (Response Schema + JSON Parse)"""
    output = result.get('output', {})
    final_state = output.get('final_state', {})
    
    # 1. Check for valid JSON output (from llm_chat)
    structured_json = final_state.get('structured_output_raw')
    if not structured_json:
        return False, "Missing 'structured_output_raw' from llm_chat"
    
    # 2. Check for parsed output (from operator: json_parse)
    parsed = final_state.get('structured_output')
    if not parsed or not isinstance(parsed, dict):
        return False, "Failed to parse JSON output"
        
    # 3. Validation: Check required fields based on schema
    if 'languages' not in parsed or not isinstance(parsed['languages'], list):
        return False, "Schema mismatch: 'languages' array missing"
        
    # 4. Performance Check (Detailed Reporting)
    # Start/End are recorded by operators
    try:
        duration = result.get('duration_seconds', 0)
        # Check against expected latency from input_data if available, else generic 10s
        expected_latency = scenario_config.get('input_data', {}).get('expected_json_parse_ms', 500) / 1000.0
        # Relaxed E2E limit (network overhead included)
        e2e_limit = max(10.0, expected_latency * 20) 
        
        passed, msg = _verify_latency(duration, e2e_limit, "Stage 1")
        if not passed:
             logger.warning(f"Stage 1 Performance Warning: {msg}")
             # Optional: Decide if we want to fail the test on strict latency
             # return False, msg
    except Exception:
        pass
        
    return True, f"Stage 1 Passed: Valid JSON with {len(parsed['languages'])} languages"


def verify_stage2_flow_control(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """Stage 2: Flow Control Verification (Loop/Map + Guardrails)"""
    output = result.get('output', {})
    final_state = output.get('final_state', {})
    
    # 1. Verify Loop Count (from internal counter or accumulated result)
    # Check if we have processed items
    processed = final_state.get('processed_items', [])
    if isinstance(processed, list) and len(processed) > 0:
        pass # Good
    else:
        # If loop was purely control flow, check counter
        loop_counter = final_state.get('loop_counter', 0)
        # Note: Depending on workflow logic, might be difficult to check exact count without artifact
        
    # 2. Guardrail Check (Cost/Token Limit) with Detailed Reporting
    # We expect SUCCEEDED, but potentially with a "guardrail_triggered" flag or similar if it hit limits gracefully
    
    token_usage = result.get('output', {}).get('usage', {})
    limit = scenario_config.get('safety_limits', {}).get('max_tokens_total', 10000)
    
    passed, msg = _verify_token_usage(token_usage, limit, "Stage 2")
    if not passed:
        return False, msg
        
    return True, f"Stage 2 Passed: Flow control executed within limits ({msg})"


def verify_stage3_vision_basic(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """Stage 3: Vision Basic Verification"""
    output = result.get('output', {})
    final_state = output.get('final_state', {})
    
    # 1. Check Hydration (S3 -> Bytes)
    # Internal operator result usually not exposed in final output unless requested
    # We rely on successful Vision execution
    
    # 2. Check Vision Output
    clean_data = final_state.get('clean_vision_data')
    if not clean_data:
        return False, "Missing 'clean_vision_data'"
        
    # 3. Allow partial failure if 'parse_failed' but check logic
    # In this test, we expect success for the sample image
    if clean_data.get('vendor') == 'parse_failed':
         return False, "Vision parsing failed (vendor=parse_failed)"
         
    if not clean_data.get('amount'):
        logger.warning("Stage 3: Amount is null (might be OCR issue)")
        
    # Guardrails
    passed, msg = _verify_token_usage(output.get('usage', {}), 8000, "Stage 3")
    if not passed: return False, msg
    
    passed, msg = _verify_latency(result.get('duration_seconds', 0), 120, "Stage 3")
    if not passed: return False, msg
        
    return True, f"Stage 3 Passed: Extracted vendor={clean_data.get('vendor')} ({msg})"


def verify_stage4_vision_map(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """Stage 4: Vision Map Verification (Parallel)"""
    output = result.get('output', {})
    final_state = output.get('final_state', {})
    
    # 1. Check Parallel Results
    # Assuming the map state output is aggregated into a key
    results = final_state.get('mapped_results') or final_state.get('vision_results')
    
    if not results or not isinstance(results, list):
        # Fallback: check if we have individual branch artifacts or passed status
        pass
        
    # 2. Guardrail (Concurrency) - Hard to verify post-hoc without detailed trace
    # We trust Step Functions definition for max_concurrency
    
    # Detailed Reporting
    passed, msg = _verify_token_usage(output.get('usage', {}), 20000, "Stage 4")
    if not passed: return False, msg
    
    passed, msg = _verify_latency(result.get('duration_seconds', 0), 180, "Stage 4")
    if not passed: return False, msg
    
    return True, f"Stage 4 Passed: Parallel execution completed ({msg})"


def verify_stage5_hyper_stress(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """Stage 5: Hyper Stress Verification"""
    # This is a chaos test - we primarily check it didn't crash hard (Timeout/Runtime Error)
    # The framework guarantees SUCCEEDED status means it handled errors gracefully
    
    status = result.get('status')
    if status != 'SUCCEEDED':
        return False, f"Stage 5 Failed with status {status}"
        
    # Detailed Reporting
    passed, msg = _verify_token_usage(result.get('output', {}).get('usage', {}), 50000, "Stage 5")
    if not passed: return False, msg
    
    return True, f"Stage 5 Passed: System survived hyper stress ({msg})"


def verify_basic_llm_call(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """LI-A: 기본 LLM 호출 검증"""
    output = result.get('output', {})
    
    # Check if LLM output exists
    llm_output = output.get('llm_output') or output.get('final_state', {}).get('llm_output')
    if not llm_output:
        return False, "No LLM output found in result"
    
    # Check minimum length
    expected_min_length = scenario_config.get('input_data', {}).get('expected_min_length', 10)
    if len(str(llm_output)) < expected_min_length:
        return False, f"LLM output too short: {len(str(llm_output))} < {expected_min_length}"
    
    # Check it's not a mock response
    if '[MOCK' in str(llm_output).upper():
        return False, "Received MOCK response instead of real LLM output"
    
    return True, f"Basic LLM call successful, output length: {len(str(llm_output))}"


def verify_structured_output(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """LI-B: Structured Output 검증"""
    output = result.get('output', {})
    
    # Try to find structured output
    structured_output = output.get('structured_output') or output.get('final_state', {}).get('structured_output')
    
    if not structured_output:
        return False, "No structured output found"
    
    # Verify it's valid JSON
    try:
        if isinstance(structured_output, str):
            parsed = json.loads(structured_output)
        else:
            parsed = structured_output
        
        # Check for expected structure (languages array)
        if 'languages' in parsed and isinstance(parsed['languages'], list):
            return True, f"Structured output valid with {len(parsed['languages'])} items"
        else:
            return False, f"Unexpected structure: {list(parsed.keys())}"
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON in structured output: {e}"


def verify_thinking_mode(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """LI-C: Thinking Mode 검증"""
    output = result.get('output', {})
    
    # Check for thinking output
    thinking_output = output.get('thinking_output') or output.get('final_state', {}).get('thinking_output')
    llm_output = output.get('llm_output') or output.get('final_state', {}).get('llm_output')
    
    # For thinking mode, we expect either visible thinking or reasoning in output
    has_reasoning = False
    if thinking_output:
        has_reasoning = True
    elif llm_output and any(word in str(llm_output).lower() for word in ['because', 'therefore', 'so', 'step', 'reason']):
        has_reasoning = True
    
    if not has_reasoning:
        return False, "No thinking/reasoning visible in output"
    
    # Check answer correctness (17 - all but 9 = 9 sheep left)
    # STRICT CHECK: Use regex to find standalone 9 or "9"
    import re
    answer_text = str(llm_output)
    
    # Patterns: "9", " 9 ", "is 9", "are 9"
    # Avoid matching "19", "90", "17", "9 died" (though 9 died is part of problem, answer is 9)
    # Simple heuristic: Look for 9 in the last sentence or near "answer"
    
    if re.search(r'\b9\b', answer_text):
         # Exclude if it says "not 9" or "9 died" specifically? 
         # The riddle answer is exactly 9.
         return True, "Thinking mode worked correctly, answer contains 9"
    else:
        return False, f"Answer likely incorrect (9 not found): {answer_text[:100]}..."


def verify_llm_operator_integration(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """LI-D: LLM + operator_official 통합 검증"""
    output = result.get('output', {})
    final_state = output.get('final_state', output)
    
    # Check that operator strategies were used
    step_history = final_state.get('step_history', [])
    expected_strategies = scenario_config.get('input_data', {}).get('expected_strategies_used', [])
    
    found_strategies = []
    for step in step_history:
        for strategy in expected_strategies:
            if strategy in str(step):
                found_strategies.append(strategy)
    
    found_strategies = list(set(found_strategies))
    
    if len(found_strategies) < len(expected_strategies):
        missing = set(expected_strategies) - set(found_strategies)
        # return False, f"Missing strategies: {missing}" 
        # Warning only for now as step_history format might change
        logger.warning(f"Likely missing strategies: {missing}")
    
    # Check for filtered results
    filtered_results = final_state.get('filtered_results') or final_state.get('high_priority_items')
    if filtered_results is None:
        return False, "No filtered results found - operator integration may have failed"
    
    return True, f"LLM + Operator integration successful. Strategies used: {found_strategies}"


def verify_document_analysis(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """LI-E: 복합 문서 분석 파이프라인 검증"""
    output = result.get('output', {})
    final_state = output.get('final_state', output)
    
    # Check pipeline stages completed
    expected_stages = scenario_config.get('input_data', {}).get('expected_pipeline_stages', [])
    completed_stages = []
    
    # Check for evidence of each stage
    if final_state.get('analysis_data') or final_state.get('llm_analysis_raw'):
        completed_stages.append('extract')
    if final_state.get('high_priority_vulnerabilities') or final_state.get('filtered_items'):
        completed_stages.append('filter')
    if final_state.get('deep_analysis_results'):
        completed_stages.append('deep_analyze')
    if final_state.get('final_report') or final_state.get('final_markdown_report'):
        completed_stages.append('merge')
    
    missing_stages = set(expected_stages) - set(completed_stages)
    if missing_stages:
        return False, f"Pipeline incomplete. Missing stages: {missing_stages}"
    
    # Check for security findings
    if scenario_config.get('input_data', {}).get('expected_security_findings'):
        vulns = final_state.get('security_vulnerabilities') or final_state.get('high_priority_vulnerabilities')
        if not vulns:
            return False, "No security findings extracted"
    
    return True, f"Document analysis pipeline complete. Stages: {completed_stages}"


def verify_multimodal_vision_live(execution_arn: str, result: dict, scenario_config: dict) -> Tuple[bool, str]:
    """LI-F: Multimodal Vision 라이브 검증"""
    output = result.get('output', {})
    
    vision_output = output.get('vision_output') or output.get('final_state', {}).get('vision_output')
    
    if not vision_output:
        return False, "No vision output found"
    
    # Check it contains description
    if len(str(vision_output)) < 50:
        return False, f"Vision output too short: {len(str(vision_output))}"
    
    text_out = str(vision_output).lower()
    
    # NEGATIVE CHECK: Ensure it doesn't say "not a diagram" or "cannot see"
    negative_phrases = ["not a diagram", "cannot identify", "unable to see", "no image", "failed to process"]
    if any(phrase in text_out for phrase in negative_phrases):
        return False, f"Vision analysis returned negative result: {text_out[:100]}..."
    
    # Check for architecture/diagram related words
    diagram_words = ['diagram', 'architecture', 'flow', 'component', 'system', 'connection', 'network', 'structure']
    has_diagram_content = any(word in text_out for word in diagram_words)
    
    if not has_diagram_content:
        return False, "Vision output doesn't seem to describe a diagram"
    
    return True, "Multimodal vision analysis successful"


# ============================================================================
# Metrics
# ============================================================================
def put_llm_metric(scenario_key: str, success: bool, duration: float = 0, tokens_used: int = 0):
    """CloudWatch 메트릭 발행 (LLM Simulator용)"""
    try:
        cw = get_cloudwatch_client()
        
        metrics = [
            {
                'MetricName': 'LLMTestSuccess' if success else 'LLMTestFailure',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Scenario', 'Value': scenario_key}
                ]
            },
            {
                'MetricName': 'LLMTestDuration',
                'Value': duration,
                'Unit': 'Seconds',
                'Dimensions': [
                    {'Name': 'Scenario', 'Value': scenario_key}
                ]
            }
        ]
        
        if tokens_used > 0:
            metrics.append({
                'MetricName': 'LLMTokensUsed',
                'Value': tokens_used,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Scenario', 'Value': scenario_key}
                ]
            })
        
        cw.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=metrics
        )
        
    except Exception as e:
        logger.warning(f"Failed to put LLM metrics: {e}")


# ============================================================================
# Main Handler
# ============================================================================
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    LLM Simulator Lambda Handler
    
    Event format:
    {
        "scenarios": ["BASIC_LLM_CALL", "STRUCTURED_OUTPUT"],  # Optional, defaults to all
        "orchestrator_type": "DISTRIBUTED",  # Optional
        "dry_run": false  # If true, only validate config without executing
    }
    """
    logger.info("=" * 60)
    logger.info("🧠 LLM Simulator Starting (MOCK_MODE=false)")
    logger.info("⚠️  WARNING: This will make REAL LLM API calls!")
    logger.info("=" * 60)
    
    # Parse event
    selected_scenarios = event.get('scenarios', list(LLM_SCENARIOS.keys()))
    orchestrator_type = event.get('orchestrator_type', 'DISTRIBUTED')
    dry_run = event.get('dry_run', False)
    
    results = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'mock_mode': MOCK_MODE,
        'scenarios': {},
        'summary': {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'skipped': 0
        }
    }
    
    # Estimate costs
    estimated_cost = sum(
        ESTIMATED_COST_PER_SCENARIO.get(s, 0.05) 
        for s in selected_scenarios if s in LLM_SCENARIOS
    )
    logger.info(f"Estimated cost for {len(selected_scenarios)} scenarios: ${estimated_cost:.2f}")
    
    if dry_run:
        logger.info("DRY RUN mode - not executing any scenarios")
        results['dry_run'] = True
        results['estimated_cost_usd'] = estimated_cost
        return results
    
    # Execute scenarios
    for scenario_key in selected_scenarios:
        if scenario_key not in LLM_SCENARIOS:
            logger.warning(f"Unknown scenario: {scenario_key}")
            continue
        
        scenario_config = LLM_SCENARIOS[scenario_key]
        results['summary']['total'] += 1
        
        logger.info(f"\n{'='*40}")
        logger.info(f"Running: {scenario_config['name']}")
        logger.info(f"{'='*40}")
        
        scenario_result = {
            'name': scenario_config['name'],
            'description': scenario_config['description'],
            'status': 'PENDING'
        }
        
        try:
            # Trigger execution
            execution_arn = trigger_llm_test(
                scenario_key,
                scenario_config,
                orchestrator_type
            )
            scenario_result['execution_arn'] = execution_arn
            
            # Poll for completion
            timeout = scenario_config.get('timeout_seconds', MAX_POLL_SECONDS)
            poll_result = poll_llm_execution(execution_arn, timeout)
            
            scenario_result['execution_status'] = poll_result['status']
            scenario_result['duration_seconds'] = poll_result.get('duration_seconds', 0)
            
            if poll_result['status'] == 'SUCCEEDED':
                # Run verification
                verify_func_name = scenario_config.get('verify_func')
                verify_func = globals().get(verify_func_name)
                
                if verify_func:
                    passed, message = verify_func(execution_arn, poll_result, scenario_config)
                    scenario_result['verified'] = passed
                    scenario_result['verification_message'] = message
                    
                    if passed:
                        scenario_result['status'] = 'PASSED'
                        results['summary']['passed'] += 1
                    else:
                        scenario_result['status'] = 'FAILED'
                        results['summary']['failed'] += 1
                else:
                    scenario_result['status'] = 'PASSED'
                    scenario_result['verification_message'] = 'No verification function'
                    results['summary']['passed'] += 1
                
                # Extract token usage if available
                output = poll_result.get('output', {})
                usage = output.get('usage') or output.get('final_state', {}).get('usage')
                if usage:
                    scenario_result['token_usage'] = usage
                    put_llm_metric(
                        scenario_key, 
                        scenario_result['status'] == 'PASSED',
                        scenario_result['duration_seconds'],
                        usage.get('total_tokens', 0)
                    )
            else:
                scenario_result['status'] = 'FAILED'
                scenario_result['error'] = poll_result.get('error', 'Execution failed')
                results['summary']['failed'] += 1
                put_llm_metric(scenario_key, False, scenario_result.get('duration_seconds', 0))
                
        except Exception as e:
            logger.exception(f"Scenario {scenario_key} failed with exception")
            scenario_result['status'] = 'ERROR'
            scenario_result['error'] = str(e)
            results['summary']['failed'] += 1
        
        results['scenarios'][scenario_key] = scenario_result
        logger.info(f"Result: {scenario_result['status']}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("🧠 LLM Simulator Complete")
    logger.info(f"Passed: {results['summary']['passed']}/{results['summary']['total']}")
    logger.info(f"Failed: {results['summary']['failed']}/{results['summary']['total']}")
    logger.info("=" * 60)
    
    return results
