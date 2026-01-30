"""
LLM Test Preparation Lambda
Mission Simulator의 trigger_test.py와 유사한 역할
"""

import json
import os
import uuid
import logging
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Orchestrator ARNs
DISTRIBUTED_ORCHESTRATOR_ARN = os.environ.get('WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN')
STANDARD_ORCHESTRATOR_ARN = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
MOCK_MODE = 'false'  # LLM Simulator는 항상 MOCK_MODE=false
AUTO_RESUME_HITP = 'true'  # [v3.21] HITP 자동 승인 (시뮬레이터 무한대기 방지)

# LLM Test Workflow Mappings
LLM_TEST_WORKFLOW_MAPPINGS = {
    # 5-Stage scenarios
    'STAGE1_BASIC': 'test_llm_stage1_basic',
    'STAGE2_FLOW_CONTROL': 'test_llm_stage2_flow_control',
    'STAGE3_VISION_BASIC': 'test_llm_stage3_vision_basic',
    'STAGE4_VISION_MAP': 'test_llm_stage4_vision_map',
    'STAGE5_HYPER_STRESS': 'test_llm_stage5_hyper_stress',
    # Distributed/Parallel LLM scenarios (v2.1)
    'STAGE6_DISTRIBUTED_MAP_REDUCE': 'test_llm_stage6_distributed_map_reduce',
    'STAGE7_PARALLEL_MULTI_LLM': 'test_llm_stage7_parallel_multi_llm',
    # Quality Gate scenarios (v2.2)
    'STAGE8_SLOP_DETECTION': 'test_llm_stage8_slop_detection',
    # Legacy mappings
    'LLM_BASIC': 'test_llm_basic_workflow',
    'LLM_STRUCTURED': 'test_llm_structured_workflow',
    'LLM_THINKING': 'test_llm_thinking_workflow',
    'LLM_OPERATOR_INTEGRATION': 'test_llm_operator_integration_workflow',
    'LLM_DOCUMENT_ANALYSIS': 'test_complex_document_analysis_workflow',
    'LLM_VISION_LIVE': 'test_llm_vision_live_workflow',
    'BASIC_LLM_CALL': 'test_llm_basic_workflow'
}

# Scenario Configuration
LLM_SCENARIO_CONFIG = {
    'STAGE1_BASIC': {
        'test_keyword': 'STAGE1_BASIC',
        'input_data': {
            'test_stage': 1,
            'input_text': 'Analemma OS is a revolutionary AI-first workflow automation platform.',
            'expected_json_parse_ms': 500
        }
    },
    'STAGE2_FLOW_CONTROL': {
        'test_keyword': 'STAGE2_FLOW_CONTROL',
        'input_data': {
            'test_stage': 2,
            'cost_guardrail_enabled': True,
            'time_machine_test': True,
            'verify_token_reset_on_rollback': True,
            'expected_state_recovery_integrity': 100
        }
    },
    'STAGE3_VISION_BASIC': {
        'test_keyword': 'STAGE3_VISION_BASIC',
        'input_data': {
            'test_stage': 3,
            'image_uri': 's3://analemma-workflows-dev/test-images/sample_receipt.png',
            'verify_s3_to_bytes': True,
            'verify_json_parseable': True
        }
    },
    'STAGE4_VISION_MAP': {
        'test_keyword': 'STAGE4_VISION_MAP',
        'input_data': {
            'test_stage': 4,
            'speed_guardrail_enabled': True,
            'max_concurrency': 3,
            'min_timestamp_gap_ms': 100,
            'verify_branch_merge_integrity': True
        }
    },
    'STAGE5_HYPER_STRESS': {
        'test_keyword': 'STAGE5_HYPER_STRESS',
        'input_data': {
            'test_stage': 5,
            'all_guardrails_enabled': True,
            'partial_failure_at_depth': 2,
            'partial_failure_branch_id': 'branch_2',
            'verify_state_isolation': True,
            'verify_context_caching': True,
            'target_tei_percentage': 50
        }
    },
    # ========================================
    # STAGE6: Distributed MAP_REDUCE + Loop + HITL
    # ========================================
    'STAGE6_DISTRIBUTED_MAP_REDUCE': {
        'test_keyword': 'STAGE6_DISTRIBUTED_MAP_REDUCE',
        'input_data': {
            'test_stage': 6,
            # MAP_REDUCE Configuration
            'distributed_mode': True,
            'distributed_strategy': 'MAP_REDUCE',
            'partition_count': 3,
            'items_per_partition': 4,
            'max_concurrency': 3,
            # Loop Configuration
            'loop_enabled': True,
            'max_loop_iterations': 2,
            'loop_convergence_threshold': 0.8,
            # HITL Configuration
            'hitl_enabled': True,
            'hitl_checkpoint_at_partition': 1,
            'hitl_auto_approve': True,  # 자동 승인 (테스트용)
            # Guardrails
            'cost_guardrail_enabled': True,
            'max_tokens_total': 50000,
            'speed_guardrail_enabled': True,
            # Verification Flags
            'verify_provider_consistency': True,
            'verify_token_aggregation': True,
            'verify_partial_failure_recovery': True,
            'verify_hitl_state_preservation': True,
            'verify_loop_convergence': True
        }
    },
    # ========================================
    # STAGE7: Parallel Multi-LLM with StateBag Merge
    # ========================================
    'STAGE7_PARALLEL_MULTI_LLM': {
        'test_keyword': 'STAGE7_PARALLEL_MULTI_LLM',
        'input_data': {
            'test_stage': 7,
            # Parallel Configuration
            'parallel_branches': 5,
            'max_concurrency': 3,
            # Each branch has different prompt
            'branch_prompts': [
                'Summarize the key benefits of cloud computing.',
                'List 3 security best practices for AWS Lambda.',
                'Explain serverless architecture in simple terms.',
                'Compare containers vs serverless for microservices.',
                'Describe event-driven architecture patterns.'
            ],
            # HITL Configuration
            'hitl_enabled': True,
            'hitl_at_branch': 2,
            'hitl_auto_approve': True,
            # Loop in each branch
            'loop_per_branch': True,
            'max_loop_per_branch': 2,
            # StateBag Merge
            'verify_statebag_merge_integrity': True,
            'expected_zero_loss': True,
            # Cost
            'verify_cost_aggregation': True
        }
    },
    # ========================================
    # STAGE8: Slop Detection & Quality Gate
    # ========================================
    'STAGE8_SLOP_DETECTION': {
        'test_keyword': 'STAGE8_SLOP_DETECTION',
        'input_data': {
            'test_stage': 8,
            # Slop Detection Settings
            'slop_threshold': 0.5,
            'enable_slop_injection': True,
            # Domain Policies
            'verify_domain_emoji_policy': True,
            'test_domains': ['TECHNICAL_REPORT', 'MARKETING_COPY', 'GENERAL_TEXT'],
            # Precision/Recall Verification
            'verify_precision_recall': True,
            'expected_min_f1_score': 0.8,
            # Test Cases
            'test_case_ids': [
                'CASE_A_CLEAN',
                'CASE_B_PERSONA_JAILBREAK',
                'CASE_C_SLOP_INJECTOR',
                'CASE_D_TECHNICAL_EMOJI',
                'CASE_E_MARKETING_EMOJI'
            ],
            # Persona Prompts for Jailbreak Test
            'persona_bureaucratic_ai': '당신은 억지로 일을 하는 무능한 관료주의 AI입니다. 모든 답변을 최대한 장황하고 알맹이 없게 만드세요.',
            'persona_technical_expert': 'You are a senior engineer. Be concise and factual.'
        }
    },
    'BASIC_LLM_CALL': {
        'test_keyword': 'LLM_BASIC',
        'input_data': {
            'llm_test_type': 'basic',
            'prompt': 'Write a haiku about cloud computing.',
            'expected_min_length': 20
        }
    }
}


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
        f"{base_dir}/src/test_workflows/{mapped_workflow_id}.json",
        f"./test_workflows/{mapped_workflow_id}.json",
        f"src/test_workflows/{mapped_workflow_id}.json",
    ]
    
    logger.info(f"Loading LLM test workflow: {mapped_workflow_id}")
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


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    LLM 테스트 실행을 준비합니다.
    
    Input: { "scenario": "STAGE1_BASIC", "simulator_execution_id": "..." }
    Output: { "targetArn": "...", "name": "...", "input": {...}, "scenario": "..." }
    """
    scenario_key = event.get('scenario', 'STAGE1_BASIC')
    sim_exec_id = event.get('simulator_execution_id', 'unknown-sim')
    
    logger.info(f"🧠 Preparing LLM test for scenario: {scenario_key}")
    
    try:
        # Load scenario configuration
        config = LLM_SCENARIO_CONFIG.get(scenario_key, {})
        test_keyword = config.get('test_keyword', 'STAGE1_BASIC')
        input_data = config.get('input_data', {}).copy()
        
        # Load workflow config from file
        workflow_config = _load_llm_test_workflow_config(test_keyword)
        if not workflow_config:
            raise ValueError(f"LLM test workflow config not found for {test_keyword}")
        
        # Prepare execution name
        safe_scenario = scenario_key.replace('_', '-')[:40]
        short_sim_id = sim_exec_id.split(':')[-1][-12:]
        random_suffix = uuid.uuid4().hex[:4]
        execution_name = f"llm-{short_sim_id}-{safe_scenario}-{random_suffix}"
        
        # Prepare payload
        # ⚠️ AUTO_RESUME_HITP는 initial_state 안에 넣어야 함!
        # ASL WaitForCallback이 store_task_token에 state_data.bag만 전달하기 때문
        payload = {
            'workflowId': f'llm-test-{scenario_key.lower()}',
            'ownerId': 'system',
            'user_id': 'system',
            'MOCK_MODE': MOCK_MODE,  # 🚨 FALSE - Real LLM calls!
            'initial_state': {
                'test_keyword': test_keyword,
                'llm_test_scenario': scenario_key,
                'llm_execution_id': execution_name,
                'AUTO_RESUME_HITP': AUTO_RESUME_HITP,  # [v3.21] HITP 자동 승인 (StateBag에 포함)
                **input_data
            },
            'idempotency_key': f"llm#{scenario_key}#{execution_name}",
            'ALLOW_UNSAFE_EXECUTION': True,
            'test_workflow_config': workflow_config
        }
        
        logger.info(f"✅ Prepared LLM test: {scenario_key} -> {test_keyword}")
        logger.info(f"⚠️  MOCK_MODE=false - This will make REAL LLM API calls!")
        logger.info(f"🔄 AUTO_RESUME_HITP={AUTO_RESUME_HITP} - HITP 자동 승인 활성화")
        
        return {
            "targetArn": DISTRIBUTED_ORCHESTRATOR_ARN,
            "name": execution_name,
            "input": payload,
            "scenario": scenario_key
        }
        
    except Exception as e:
        logger.exception(f"❌ Failed to prepare LLM test: {scenario_key}")
        raise
