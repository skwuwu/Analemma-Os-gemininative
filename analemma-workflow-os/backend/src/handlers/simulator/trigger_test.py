
import json
import os
import uuid
import logging
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configs
DISTRIBUTED_ORCHESTRATOR_ARN = os.environ.get('WORKFLOW_DISTRIBUTED_ORCHESTRATOR_ARN')
STANDARD_ORCHESTRATOR_ARN = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
MOCK_MODE = os.environ.get('MOCK_MODE', 'true')

# Test workflow mappings - maps test_keyword to actual workflow files
TEST_WORKFLOW_MAPPINGS = {
    'FAIL': 'test_fail_workflow',
    'PAUSED_FOR_HITP': 'test_hitp_workflow',
    'COMPLETE': 'test_complete_workflow',
    'CONTINUE': 'test_continue_workflow',
    'E2E_S3_LARGE_DATA': 'test_s3_large_workflow',
    'MAP_AGGREGATOR_TEST': 'test_map_aggregator_workflow',
    'LOOP_LIMIT_DYNAMIC': 'test_loop_limit_dynamic_workflow',
    'HYPER_REPORT': 'test_hyper_report_workflow',  # 하이퍼-리포트 시나리오
    # [Fix] 누락된 테스트 시나리오 추가
    'PARALLEL_GROUP': 'test_parallel_complex_5_branches_workflow',
    'HITP': 'test_hitp_workflow',
    'MAP_AGGREGATOR_HITP': 'test_map_aggregator_hitp_workflow',
    'ASYNC_LLM': 'test_async_llm_workflow',
    # Multimodal & Advanced Scenarios
    'MULTIMODAL_VISION': 'test_vision_workflow',  # Gemini Vision 멀티모달 이미지 분석
    'HYPER_STRESS_V3': 'test_hyper_stress_workflow',  # V3 하이퍼-스트레스 시나리오 (Nested Map, Multi-HITL)
    'MULTIMODAL_COMPLEX': 'extreme_product_page_workflow',  # 비디오 + 이미지 멀티모달 복합 분석
    # 🔀 Kernel Dynamic Scheduling Test Workflows
    'PARALLEL_SCHEDULER_TEST': 'test_parallel_scheduler_workflow',  # 병렬 스케줄러 RESOURCE_OPTIMIZED
    'COST_OPTIMIZED_PARALLEL_TEST': 'test_cost_optimized_parallel_workflow',  # COST_OPTIMIZED 전략
    'SPEED_GUARDRAIL_TEST': 'test_speed_guardrail_workflow',  # SPEED_OPTIMIZED 가드레일
    'SHARED_RESOURCE_ISOLATION_TEST': 'test_shared_resource_isolation_workflow',  # 공유 자원 격리
    # 🔥 OS Edge Case Test Workflows
    'RACE_CONDITION_TEST': 'test_race_condition_workflow',
    'DEADLOCK_DETECTION_TEST': 'test_deadlock_detection_workflow',
    'MEMORY_LEAK_TEST': 'test_memory_leak_workflow',
    'SPLIT_PARADOX_TEST': 'test_split_paradox_workflow',
    # 🛡️ Ring Protection & Time Machine
    'RING_PROTECTION_ATTACK_TEST': 'test_ring_protection_attack_workflow',
    'TIME_MACHINE_HYPER_STRESS': 'test_time_machine_hyper_stress_workflow',
    'SELF_HEALING_TEST': 'test_self_healing_workflow',
    'COST_GUARDRAILS': 'test_cost_guardrails_workflow',
}


def _load_test_workflow_config(test_keyword: str) -> dict:
    """
    테스트 키워드에 해당하는 워크플로우 설정을 실제 파일에서 로드합니다.
    """
    mapped_workflow_id = TEST_WORKFLOW_MAPPINGS.get(test_keyword)
    if not mapped_workflow_id:
        logger.warning(f"No mapping found for test_keyword: {test_keyword}")
        return None
    
    # Base directory 계산 (backend/src/handlers/simulator -> backend)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    # Workspace root 계산 (backend -> analemma-workflow-os)
    workspace_root = os.path.dirname(base_dir)
    
    # Lambda 컨테이너 및 로컬 개발 환경 경로들
    possible_paths = [
        f"/var/task/test_workflows/{mapped_workflow_id}.json",  # Lambda container (legacy)
        f"/var/task/tests/backend/workflows/{mapped_workflow_id}.json",  # Lambda container (actual location)
        f"{base_dir}/src/test_workflows/{mapped_workflow_id}.json",  # backend/src/test_workflows
        f"{workspace_root}/tests/backend/workflows/{mapped_workflow_id}.json",  # analemma-workflow-os/tests/backend/workflows
        f"./test_workflows/{mapped_workflow_id}.json",
        f"./tests/backend/workflows/{mapped_workflow_id}.json",
        f"src/test_workflows/{mapped_workflow_id}.json",
        f"backend/src/test_workflows/{mapped_workflow_id}.json",
        f"tests/backend/workflows/{mapped_workflow_id}.json",
    ]
    
    logger.info(f"Loading test workflow for {test_keyword} -> {mapped_workflow_id}")
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                logger.info(f"✅ Loaded test workflow from {path}")
                return config
            except Exception as e:
                logger.error(f"Failed to load {path}: {e}")
    
    logger.error(f"❌ Test workflow not found: {mapped_workflow_id}")
    logger.error(f"Searched paths: {possible_paths}")
    return None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Prepares input for SFN startExecution.sync.
    Input: { "scenario": "HAPPY_PATH", "simulator_execution_id": "..." }
    Output: { "targetArn": "...", "input": {...}, "name": "..." }
    """
    scenario_key = event.get('scenario', 'HAPPY_PATH')
    sim_exec_id = event.get('simulator_execution_id', 'unknown-sim')
    
    logger.info(f"Preparing test for scenario: {scenario_key} (Sim: {sim_exec_id})")
    
    try:
        # 1. Determine Orchestrator & Config
        orchestrator_type = 'DISTRIBUTED'
        
        # Load Config
        config = SCENARIO_CONFIG.get(scenario_key, {})
        target_type = config.get('target_type', 'SFN')
        
        if target_type == 'LOCAL':
            target_arn = 'LOCAL' # Logic handled by Choice state
        elif scenario_key.startswith('STANDARD_'):
            orchestrator_type = 'STANDARD'
            target_arn = STANDARD_ORCHESTRATOR_ARN
        else:
            target_arn = DISTRIBUTED_ORCHESTRATOR_ARN
            
        if target_type == 'SFN' and not target_arn:
            raise ValueError(f"Orchestrator ARN not found for type: {orchestrator_type}")

        # 2. Prepare Execution ID (Idempotency Key)
        # deterministic naming allow retries without duplication
        # Clean scenario key for SFN naming rules (max 80 chars)
        safe_scenario = scenario_key.replace('_', '-')[:40] # Truncate to ensure length safety
        
        # SFN execution name constraints: 1-80 chars, A-Z, a-z, 0-9, - _ .
        # taking last 12 chars of sim_exec_id if it's an ARN
        short_sim_id = sim_exec_id.split(':')[-1][-12:] 
        
        # [Fix] Add random suffix to prevent "ExecutionAlreadyExists" on quick retries
        # while preserving traceability to the simulator run
        random_suffix = uuid.uuid4().hex[:4]
        
        # sim-{12}-{40}-{4} + hyphens = ~60 chars max, safely under 80
        execution_name = f"sim-{short_sim_id}-{safe_scenario}-{random_suffix}"

        # 3. Prepare Payload (Input)
        test_keyword = config.get('test_keyword', 'COMPLETE')
        input_data = config.get('input_data', {}).copy()  # 복사본 사용하여 원본 보호
        
        # LOCAL 시나리오는 워크플로 파일이 필요 없음 - 바로 LocalRunner Lambda 실행
        if target_type == 'LOCAL':
            logger.info(f"✅ Prepared LOCAL scenario: {scenario_key}")
            return {
                "targetArn": target_arn,
                "name": execution_name,
                "input": {},  # LOCAL 시나리오는 input 불필요
                "scenario": scenario_key,
                "targetType": target_type
            }
        
        # SFN 시나리오만 워크플로 파일 로드
        workflow_config = _load_test_workflow_config(test_keyword)
        if not workflow_config:
            raise ValueError(f"Test workflow config not found for test_keyword: {test_keyword}")
        
        # [Safety] 테스트 워크플로우에 max_loop_iterations 강제 설정
        # 테스트 워크플로우는 최대 5개 노드이므로 10회면 충분 (5 × 2배 여유)
        # 이렇게 하면 무한 루프 발생 시 빠르게 감지 가능
        node_count = len(workflow_config.get('nodes', []))
        safe_max_loop = max(10, node_count * 2)  # 최소 10회, 노드 수 × 2
        workflow_config['max_loop_iterations'] = workflow_config.get('max_loop_iterations', safe_max_loop)
        
        payload = {
            'workflowId': f'e2e-test-{scenario_key.lower()}',
            'ownerId': 'system',
            'user_id': 'system',
            'MOCK_MODE': MOCK_MODE,
            'initial_state': {
                'test_keyword': test_keyword,
                'e2e_test_scenario': scenario_key,
                'e2e_execution_id': execution_name,
                **input_data
            },
            'idempotency_key': f"e2e#{scenario_key}#{execution_name}", 
            'ALLOW_UNSAFE_EXECUTION': True,
            'test_workflow_config': workflow_config  # 실제 테스트 워크플로 주입
        }
        
        logger.info(f"✅ Prepared payload for {scenario_key}: target={target_type}, workflow={test_keyword}")
        
        return {
            "targetArn": target_arn,
            "name": execution_name,
            "input": payload,
            "scenario": scenario_key,
            "targetType": target_type
        }
        
    except Exception as e:
        logger.exception(f"❌ Failed to prepare test for scenario: {scenario_key}")
        raise

# ============================================================================
# Scenario Configuration
# - test_keyword는 TEST_WORKFLOW_MAPPINGS와 매칭되어 실제 워크플로 파일 로드
# - input_data는 initial_state에 추가 주입되는 테스트 데이터
# ============================================================================
SCENARIO_CONFIG = {
    # Distributed SFN Scenarios
    'HAPPY_PATH': {'test_keyword': 'COMPLETE'},
    'PII_TEST': {
        'test_keyword': 'COMPLETE',
        'input_data': {
            'test_pii': 'Contact: john@example.com, Phone: 010-1234-5678',
            'pii_test_enabled': True
        }
    },
    'LARGE_PAYLOAD': {'test_keyword': 'E2E_S3_LARGE_DATA'},
    'ERROR_HANDLING': {'test_keyword': 'FAIL'},
    'MAP_AGGREGATOR': {'test_keyword': 'MAP_AGGREGATOR_TEST'},
    'LOOP_LIMIT': {'test_keyword': 'LOOP_LIMIT_DYNAMIC'},
    'REALTIME_DISTILLER': {
        'test_keyword': 'COMPLETE',
        'input_data': {'distiller_test_enabled': True, 'notify_on_complete': True}
    },
    'DLQ_RECOVERY': {'target_type': 'LOCAL'},  # [Fix] Local Runner로 변경 - 완전한 DLQ 흐름 테스트
    'COST_GUARDRAIL': {'test_keyword': 'COST_GUARDRAILS'},
    'ATOMICITY': {'test_keyword': 'COMPLETE', 'input_data': {'atomicity_test': True}},
    'XRAY_TRACEABILITY': {'test_keyword': 'COMPLETE'},
    
    # [Fix] 누락된 핵심 기능 테스트 시나리오 추가
    'HITP_TEST': {'test_keyword': 'HITP'},  # PAUSED_FOR_HITP 경로 테스트
    'PARALLEL_GROUP_TEST': {'test_keyword': 'PARALLEL_GROUP'},  # PARALLEL_GROUP 경로 테스트
    'ASYNC_LLM_TEST': {'test_keyword': 'ASYNC_LLM'},  # PAUSED_FOR_ASYNC_LLM 경로 테스트
    'MAP_AGGREGATOR_HITP': {'test_keyword': 'MAP_AGGREGATOR_HITP'},  # Map + HITP 조합 테스트
    
    # Ultimate Stress Test
    'HYPER_REPORT': {
        'test_keyword': 'HYPER_REPORT',
        'input_data': {
            'categories': ['AI', 'Cloud', 'Security', 'SaaS'],
            'expected_payload_size_kb': 350,
            'enable_failure_injection': True,
            'enable_hitl': True,
            'enable_distiller': True
        }
    },
    
    # Multimodal & Advanced Scenarios
    'MULTIMODAL_VISION': {
        'test_keyword': 'MULTIMODAL_VISION',
        'input_data': {
            'product_image': 's3://test-bucket/sample_product.jpg',
            'vision_test_enabled': True
        }
    },
    'HYPER_STRESS_V3': {
        'test_keyword': 'HYPER_STRESS_V3',
        'input_data': {
            'test_nested_map': True,
            'test_multi_hitl': True,
            'test_partial_sync': True,
            'expected_outer_count': 4,
            'expected_inner_total': 10
        }
    },
    'MULTIMODAL_COMPLEX': {
        'test_keyword': 'MULTIMODAL_COMPLEX',
        'input_data': {
            'video_input_uri': 's3://test-bucket/sample_video.mp4',
            'image_input_uris': [
                's3://test-bucket/spec_sheet_1.jpg',
                's3://test-bucket/spec_sheet_2.jpg',
                's3://test-bucket/spec_sheet_3.jpg'
            ],
            'multimodal_test_enabled': True
        }
    },
    
    # 🔀 Kernel Dynamic Scheduling Scenarios
    'PARALLEL_SCHEDULER_TEST': {
        'test_keyword': 'PARALLEL_SCHEDULER_TEST',
        'input_data': {'resource_policy_strategy': 'RESOURCE_OPTIMIZED'}
    },
    'COST_OPTIMIZED_PARALLEL_TEST': {
        'test_keyword': 'COST_OPTIMIZED_PARALLEL_TEST',
        'input_data': {'resource_policy_strategy': 'COST_OPTIMIZED'}
    },
    'SPEED_GUARDRAIL_TEST': {
        'test_keyword': 'SPEED_GUARDRAIL_TEST',
        'input_data': {
            'guardrail_test': True,
            'resource_policy_strategy': 'SPEED_OPTIMIZED',
            'force_branch_count': 120,
            'concurrency_limit': 10
        }
    },
    'SHARED_RESOURCE_ISOLATION_TEST': {
        'test_keyword': 'SHARED_RESOURCE_ISOLATION_TEST',
        'input_data': {'shared_resource_test': True}
    },
    # 🔥 OS Edge Case Scenarios
    'RACE_CONDITION_TEST': {'test_keyword': 'RACE_CONDITION_TEST'},
    'DEADLOCK_DETECTION_TEST': {'test_keyword': 'DEADLOCK_DETECTION_TEST'},
    'MEMORY_LEAK_TEST': {'test_keyword': 'MEMORY_LEAK_TEST'},
    'SPLIT_PARADOX_TEST': {'test_keyword': 'SPLIT_PARADOX_TEST'},
    'RING_PROTECTION_ATTACK_TEST': {'test_keyword': 'RING_PROTECTION_ATTACK_TEST'},
    'TIME_MACHINE_HYPER_STRESS': {'test_keyword': 'TIME_MACHINE_HYPER_STRESS'},
    'SELF_HEALING_TEST': {'test_keyword': 'SELF_HEALING_TEST'},
    
    # Standard SFN Scenarios
    'STANDARD_HAPPY_PATH': {'test_keyword': 'COMPLETE'},
    'STANDARD_ERROR_HANDLING': {'test_keyword': 'FAIL'},
    'STANDARD_PII_TEST': {
        'test_keyword': 'COMPLETE',
        'input_data': {
            'test_pii': 'Contact: john@example.com, Phone: 010-1234-5678',
            'pii_test_enabled': True
        }
    },
    'STANDARD_LARGE_PAYLOAD': {'test_keyword': 'E2E_S3_LARGE_DATA'},
    'STANDARD_MAP_AGGREGATOR': {'test_keyword': 'MAP_AGGREGATOR_TEST'},
    'STANDARD_LOOP_LIMIT': {'test_keyword': 'LOOP_LIMIT_DYNAMIC'},
    'STANDARD_COST_GUARDRAIL': {'test_keyword': 'COST_GUARDRAILS'},
    'STANDARD_ATOMICITY': {'test_keyword': 'COMPLETE', 'input_data': {'atomicity_test': True}},
    'STANDARD_XRAY_TRACEABILITY': {'test_keyword': 'COMPLETE'},
    'STANDARD_IDEMPOTENCY': {
        'test_keyword': 'COMPLETE',
        'input_data': {'idempotency_test': True},
        'target_type': 'LOCAL'
    },

    # Local Runner Scenarios (SFN 워크플로 불필요)
    'API_CONNECTIVITY': {'target_type': 'LOCAL'},
    'WEBSOCKET_CONNECT': {'target_type': 'LOCAL'},
    'AUTH_FLOW': {'target_type': 'LOCAL'},
    'REALTIME_NOTIFICATION': {'target_type': 'LOCAL'},
    'MULTI_TENANT_ISOLATION': {'target_type': 'LOCAL'},
    'CONCURRENT_BURST': {'target_type': 'LOCAL'},
    'CORS_SECURITY': {'target_type': 'LOCAL'},
    'CANCELLATION': {'target_type': 'LOCAL'},
    'IDEMPOTENCY': {'target_type': 'LOCAL', 'input_data': {'idempotency_test': True}},
}
