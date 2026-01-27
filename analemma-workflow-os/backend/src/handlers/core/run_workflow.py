import json
import os
import boto3
import time
import uuid
import hashlib
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from src.services.workflow.repository import WorkflowRepository
import urllib.request
import urllib.error

# [v2.1] 중앙 집중식 재시도 유틸리티
try:
    from src.common.retry_utils import retry_call, retry_stepfunctions
    RETRY_UTILS_AVAILABLE = True
except ImportError:
    RETRY_UTILS_AVAILABLE = False

# Skills integration
try:
    from src.services.skill_repository import SkillRepository, get_skill_repository
except ImportError:
    SkillRepository = None
    get_skill_repository = None

# --- 공통 모듈 임포트 (개선된 구조화 로깅 및 에러 처리) ---
try:
    from src.common.auth_utils import require_authentication
    from src.common.logging_utils import get_logger, log_execution_context, log_workflow_event
    from src.common.error_handlers import handle_dynamodb_error, handle_stepfunctions_error, handle_s3_error
    from src.common.constants import (
        EnvironmentVariables, get_table_name, get_inline_threshold,
        HTTPStatusCodes, WorkflowConfig
    )
    from src.common.aws_clients import get_dynamodb_resource, get_s3_client, get_stepfunctions_client, get_lambda_client
    
    # AWS 클라이언트 초기화
    dynamodb = get_dynamodb_resource()
    s3 = get_s3_client()
    stepfunctions = get_stepfunctions_client()
    lambda_client = get_lambda_client()
    
    # 테이블 참조
    EXECUTIONS_TABLE = get_table_name(EnvironmentVariables.EXECUTIONS_TABLE)
    executions_table = dynamodb.Table(EXECUTIONS_TABLE)
    
except ImportError as e:
    # Fallback to basic logging for critical security dependency
    import logging
    logger = logging.getLogger(__name__)
    logger.critical("Critical security dependency missing: %s", str(e))
    raise RuntimeError("Critical Security Dependency Missing") from e

# 구조화된 로거 초기화
logger = get_logger(__name__)


def hydrate_skills_for_workflow(input_data: dict, owner_id: str) -> dict:
    """
    Context Hydration: Load skills referenced in input_data and inject into state.
    
    This function:
    1. Checks for 'injected_skills' in input_data
    2. Loads skill definitions from src.DynamoDB
    3. Resolves dependencies recursively
    4. Injects hydrated skills into 'active_skills' state
    
    Args:
        input_data: The workflow input data (may contain 'injected_skills')
        owner_id: The owner ID for access control
        
    Returns:
        Modified input_data with 'active_skills' populated
    """
    if not get_skill_repository:
        return input_data
    
    # Check for skills to inject
    skill_refs = input_data.get('injected_skills', [])
    if not skill_refs:
        return input_data
    
    try:
        repo = get_skill_repository()
        
        # Hydrate all referenced skills (including dependencies)
        hydrated = repo.hydrate_skills(skill_refs)
        
        if hydrated:
            # Inject into input_data
            input_data['active_skills'] = hydrated
            input_data['skill_execution_log'] = []  # Initialize execution log
            logger.info("Context Hydration: Loaded %d skills: %s", 
                       len(hydrated), list(hydrated.keys()))
        
        return input_data
        
    except Exception as e:
        logger.warning("Context Hydration failed (non-blocking): %s", str(e))
        return input_data


def generate_content_hash(input_data: dict) -> str:
    """
    Generate a deterministic hash based on input_data content.
    This ensures true idempotency based on actual request content.
    """
    try:
        # Sort keys to ensure consistent hashing regardless of key order
        content = json.dumps(input_data, sort_keys=True, ensure_ascii=False)
        hash_obj = hashlib.sha256(content.encode('utf-8'))
        return hash_obj.hexdigest()[:16]  # Use first 16 chars for brevity
    except Exception:
        # Fallback to empty hash if input_data is not serializable
        return "empty"



def _convert_floats_to_decimals(obj):
    """
    Recursively converts float values in a dictionary or list to Decimal.
    DynamoDB does not support float types; they must be converted to Decimal.
    """
    from decimal import Decimal
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_floats_to_decimals(i) for i in obj]
    return obj


def lambda_handler(event, context):
    # NOTE: background push worker logic removed. This Lambda now only acts as
    # the API "gatekeeper": authenticate, check quota/idempotency and start
    # the Step Functions execution. Asynchronous notification of completion
    # is handled by Step Functions -> EventBridge -> dedicated Notify lambda.
    # Handle OPTIONS request for CORS early (skip heavy logging/parsing)
    if event.get('httpMethod') == 'OPTIONS' or event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {"statusCode": 200, "body": ""}

    # NOTE: The GET /status handling has been intentionally removed from
    # this Lambda to avoid unauthenticated access and tenant data leakage.
    # Use the dedicated `get_status` Lambda which performs authentication
    # and authorization (ownerId comparison) before calling
    # Step Functions DescribeExecution.

    # Log invocation early to help debug routing / pathParameters issues
    try:
        logger.info("run_workflow.lambda_handler invoked")
        # Avoid dumping Authorization or other sensitive headers; show high-level event keys
        keys = list(event.keys()) if isinstance(event, dict) else []
        logger.info("event keys: %s", keys)
        try:
            logger.debug("event full: %s", json.dumps(event))
        except Exception:
            # If event is not JSON-serializable, skip detailed dump
            logger.debug("event full (non-serializable)")
    except Exception:
        # Ensure logging never prevents handler from src.proceeding
        pass
    
    try:
        # 경로 파라미터에서 workflowId 추출 (API Gateway 통합 시)
        workflow_id = event.get('pathParameters', {}).get('id')
        if not workflow_id:
            logger.error(f"STOP: Missing workflow id in path. pathParameters: {event.get('pathParameters')}")
            logger.debug(f"Event structure: {json.dumps(event, default=str)}")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing workflow id in path'})
            }

        # 🚀 동적 오케스트레이터 선택 로직
        # 워크플로우 복잡도에 따라 Standard vs Distributed Map 오케스트레이터를 동적으로 선택
        orchestrator_arn = None
        orchestrator_type = 'standard'  # 기본값
        selection_metadata = {}
        cache_hit = False
        selection_start_time = time.time()  # 성능 측정용
        
        try:
            # 워크플로우 설정 가져오기 (우선순위: test_config > DB config)
            workflow_config = None
            
            # 1. 테스트 설정이 있으면 우선 사용 (MOCK_MODE)
            if test_config_to_inject:
                workflow_config = test_config_to_inject
                logger.info("🧪 Using test workflow config for orchestrator selection")
            
            # 2. DB에서 워크플로우 설정 가져오기 (캐시 사용)
            if not workflow_config:
                try:
                    from src.services.workflow.cache_manager import cached_get_workflow_config
                    
                    # 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
                    WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
                    if WORKFLOWS_TABLE:
                        wf_table = dynamodb.Table(WORKFLOWS_TABLE)
                        
                        # 🚀 캐시를 사용한 워크플로우 설정 조회 (레이턴시 최적화)
                        cache_lookup_start = time.time()
                        workflow_config = cached_get_workflow_config(wf_table, owner_id, workflow_id)
                        cache_lookup_time = (time.time() - cache_lookup_start) * 1000
                        
                        if workflow_config:
                            cache_hit = True
                            logger.info(f"✅ Loaded workflow config (cached, {cache_lookup_time:.1f}ms) for orchestrator selection: {workflow_id}")
                        else:
                            logger.warning(f"⚠️ Workflow not found for orchestrator selection: {workflow_id}")
                    else:
                        logger.warning("⚠️ WORKFLOWS_TABLE not configured")
                        
                except ImportError:
                    # 캐시 모듈이 없으면 기본 DB 조회
                    logger.warning("⚠️ Cache module not available, using direct DB query")
                    try:
                        # 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
                        WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
                        if WORKFLOWS_TABLE:
                            wf_table = dynamodb.Table(WORKFLOWS_TABLE)
                            wf_resp = wf_table.get_item(Key={'ownerId': owner_id, 'workflowId': workflow_id})
                            if 'Item' in wf_resp:
                                workflow_config = wf_resp['Item'].get('config')
                                logger.info(f"📦 Loaded workflow config (direct DB) for orchestrator selection: {workflow_id}")
                    except Exception as e:
                        logger.warning(f"⚠️ Direct DB query failed: {e}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Cached workflow config fetch failed: {e}")
            
            # 3. 오케스트레이터 선택 로직 실행
            if workflow_config:
                from src.services.workflow.orchestrator_selector import select_orchestrator, get_orchestrator_selection_summary
                
                # 복잡도 분석 및 오케스트레이터 선택
                orchestrator_arn, orchestrator_type, selection_metadata = select_orchestrator(workflow_config)
                
                # 선택 결과 로깅
                selection_summary = get_orchestrator_selection_summary(orchestrator_type, selection_metadata)
                logger.info(f"🎯 Orchestrator selected: {selection_summary}")
                
                # 메타데이터를 payload에 추가 (디버깅 및 모니터링용)
                payload['orchestrator_selection'] = {
                    'type': orchestrator_type,
                    'metadata': selection_metadata,
                    'summary': selection_summary,
                    'cache_hit': cache_hit
                }
            else:
                # 워크플로우 설정이 없으면 기본 Standard 오케스트레이터 사용
                orchestrator_arn = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
                orchestrator_type = 'standard'
                logger.warning("⚠️ No workflow config available, using default Standard orchestrator")
                
                payload['orchestrator_selection'] = {
                    'type': 'standard',
                    'metadata': {'selection_reason': 'No workflow config available, using default'},
                    'summary': 'STANDARD selected: No config available, using default',
                    'cache_hit': cache_hit
                }
                
        except ImportError as e:
            # 선택 모듈이 없으면 기본 오케스트레이터 사용
            orchestrator_arn = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
            orchestrator_type = 'standard'
            logger.warning(f"⚠️ Orchestrator selector module not available, using Standard: {e}")
            
        except Exception as e:
            # 선택 로직 실패 시 기본 오케스트레이터로 폴백
            orchestrator_arn = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
            orchestrator_type = 'standard'
            logger.warning(f"⚠️ Orchestrator selection failed, falling back to Standard: {e}")
            
            payload['orchestrator_selection'] = {
                'type': 'standard',
                'metadata': {'selection_reason': f'Selection failed: {str(e)}', 'fallback_used': True},
                'summary': f'STANDARD selected: Selection failed, using fallback',
                'cache_hit': cache_hit
            }
        
        # 🚀 성능 메트릭 수집
        try:
            from src.services.orchestrator_metrics import record_orchestrator_selection_metrics
            record_orchestrator_selection_metrics(
                orchestrator_type=orchestrator_type,
                selection_metadata=selection_metadata,
                selection_start_time=selection_start_time,
                workflow_id=workflow_id,
                owner_id=owner_id,
                cache_hit=cache_hit
            )
        except ImportError:
            logger.debug("Metrics module not available, skipping metrics collection")
        except Exception as e:
            logger.warning(f"Failed to record orchestrator selection metrics: {e}")
        
        if not orchestrator_arn:
            logger.error("STOP: Orchestrator ARN not configured")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Orchestrator ARN not configured'})
            }

        # 입력값: 필요시 event['body']에서 파싱
        # Support two shapes from src.clients:
        # 1) { "input_data": { ... } } (preferred)
        # 2) { ... } (raw initial state)
        parsed_body = None
        input_data = {}
        raw_body = event.get('body')
        if raw_body:
            try:
                parsed_body = json.loads(raw_body)
            except (json.JSONDecodeError, ValueError):
                parsed_body = None

        if isinstance(parsed_body, dict):
            # If client wrapped inputs under 'input_data', prefer that.
            input_data = parsed_body.get('input_data', parsed_body)
        else:
            # Non-dict body (or absent): fallback to empty dict
            input_data = {}

        # initial_state와 input_data 모두에서 테스트 키워드 감지 (MOCK_MODE에서만 활성화)
        mock_mode_enabled = os.environ.get('MOCK_MODE', 'false').strip().lower() in {'true', '1', 'yes', 'on'}
        initial_state = input_data.get('initial_state', {})
        test_keyword = None

        if mock_mode_enabled:
            test_keyword = input_data.get('test_keyword')
            
            # initial_state가 dict인 경우에만 get 메서드 사용
            if isinstance(initial_state, dict) and not test_keyword:
                test_keyword = initial_state.get('test_keyword')
            
            # initial_state가 문자열인 경우도 처리
            if isinstance(initial_state, str):
                try:
                    parsed_initial_state = json.loads(initial_state)
                    if isinstance(parsed_initial_state, dict):
                        test_keyword = test_keyword or parsed_initial_state.get('test_keyword')
                        initial_state = parsed_initial_state
                except (json.JSONDecodeError, TypeError):
                    pass
            
            # 키워드가 명시적으로 없으면 initial_state 전체에서 키워드 패턴 검색
            if not test_keyword and isinstance(initial_state, dict):
                test_keywords = [
                    # Basic Status Tests
                    'FAIL', 'PAUSED_FOR_HITP', 'COMPLETE', 'CONTINUE',
                    # S3 Tests
                    'E2E_S3_LARGE_DATA', 'E2E_S3_MIXED_DATA', 'E2E_S3_PROGRESSIVE',
                    'S3_INIT_TEST',
                    # Async Tests
                    'ASYNC_LLM_TEST', 'ASYNC_HEAVY_PROMPT', 'ASYNC_S3_HEAVY_FILE',
                    # Edge Cases
                    'NULL_FINAL_STATE',
                    # Map State Tests
                    'MAP_AGGREGATOR_TEST', 'MAP_AGGREGATOR_HITP_TEST',
                    # Step Functions Improvements Tests (NEW)
                    'IDEMPOTENCY_DUPLICATE', 'PAYLOAD_COMPRESSION', 'PAYLOAD_S3_OFFLOAD',
                    'PARALLEL_COMPLEX_5', 'LOOP_LIMIT_DYNAMIC'
                ]
                
                # initial_state의 모든 값에서 키워드 검색
                state_str = json.dumps(initial_state, default=str).upper()
                for keyword in test_keywords:
                    if keyword in state_str:
                        test_keyword = keyword
                        logger.info(f"Auto-detected test keyword '{keyword}' in initial_state (MOCK_MODE)")
                        break
            
            # 키워드가 있으면 해당 테스트용 워크플로우 ID로 변경
            if test_keyword:
                test_workflow_mappings = {
                    # Basic Status Tests
                    'FAIL': 'test_fail_workflow',
                    'PAUSED_FOR_HITP': 'test_hitp_workflow',
                    'COMPLETE': 'test_complete_workflow', 
                    'CONTINUE': 'test_continue_workflow',
                    
                    # S3 Tests
                    'E2E_S3_LARGE_DATA': 'test_s3_large_workflow',
                    'E2E_S3_MIXED_DATA': 'test_s3_mixed_workflow',
                    'E2E_S3_PROGRESSIVE': 'test_s3_progressive_workflow',
                    'S3_INIT_TEST': 'test_s3_init_workflow',  # S3에서 초기 상태 로드
                    
                    # Async Tests
                    'ASYNC_LLM_TEST': 'test_async_llm_workflow',
                    'ASYNC_HEAVY_PROMPT': 'test_async_heavy_workflow', 
                    'ASYNC_S3_HEAVY_FILE': 'test_async_s3_workflow',
                    
                    # Edge Cases
                    'NULL_FINAL_STATE': 'test_null_state_workflow',
                    
                    # Map State Tests
                    'MAP_AGGREGATOR_TEST': 'test_map_aggregator_workflow',
                    'MAP_AGGREGATOR_HITP_TEST': 'test_map_aggregator_hitp_workflow',
                    
                    # Step Functions Improvements Tests (NEW)
                    'IDEMPOTENCY_DUPLICATE': 'test_idempotency_duplicate_workflow',
                    'PAYLOAD_COMPRESSION': 'test_payload_compression_workflow',
                    'PAYLOAD_S3_OFFLOAD': 'test_payload_s3_offload_workflow',
                    'PARALLEL_COMPLEX_5': 'test_parallel_complex_5_branches_workflow',
                    'LOOP_LIMIT_DYNAMIC': 'test_loop_limit_dynamic_workflow'
                }
                
                original_workflow_id = workflow_id
                workflow_id = test_workflow_mappings.get(test_keyword, workflow_id)
                
                if workflow_id != original_workflow_id:
                    logger.info(f"Test keyword '{test_keyword}' detected: {original_workflow_id} -> {workflow_id} (MOCK_MODE)")
                    input_data['is_test_workflow'] = True
                    input_data['original_workflow_id'] = original_workflow_id
                    
                    # S3 테스트의 경우 대용량 input_data를 강제 생성하여 S3 오프로드 트리거
                    if test_keyword in ['E2E_S3_LARGE_DATA', 'E2E_S3_MIXED_DATA', 'E2E_S3_PROGRESSIVE', 'S3_INIT_TEST']:
                        threshold = int(os.environ.get('STREAM_INLINE_THRESHOLD_BYTES', '250000'))
                        # 임계값보다 50KB 더 크게 생성하여 확실히 S3 오프로드 트리거
                        large_data_size = threshold + 50000
                        large_test_data = f"S3_TEST_{test_keyword}_" * (large_data_size // 20)
                        
                        input_data['s3_test_payload'] = large_test_data[:large_data_size]
                        input_data['s3_test_metadata'] = {
                            'generated_size': len(input_data['s3_test_payload']),
                            'threshold': threshold,
                            'should_trigger_s3': len(input_data['s3_test_payload']) > threshold,
                            'test_keyword': test_keyword
                        }
                        logger.info(f"S3 테스트용 대용량 데이터 생성: {len(input_data['s3_test_payload']):,} bytes (임계값: {threshold:,}) (MOCK_MODE)")

        # --- Stability: ensure skeleton cache exists before charging user ---
        # [REMOVED] S3 skeleton check is obsolete. We now use DynamicWorkflowBuilder
        # which compiles the graph from src.JSON at runtime.
        pass
        # 사용자 인증 (JWT 서명 검증 포함)
        try:
            owner_id = require_authentication(event)
        except ValueError as e:
            logger.error("STOP: Authentication failed: %s", str(e))
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Unauthorized'})
            }

        # --- Idempotency check (tenant-scoped) ---
        # Perform idempotency check BEFORE consuming the user's quota so
        # duplicate requests don't decrement usage.
        idempotency_key = None
        headers = event.get('headers') or {}
        # Prefer explicit HTTP header. For backward compatibility check
        # the top-level parsed body (parsed_body) for idempotency key (if present).
        idempotency_key = headers.get('Idempotency-Key') or headers.get('idempotency-key') or (parsed_body.get('idempotency_key') if isinstance(parsed_body, dict) else (input_data.get('idempotency_key') if isinstance(input_data, dict) else None))

        idemp_table_name = os.environ.get('IDEMPOTENCY_TABLE')
        idemp_table = None
        # TTL for idempotency entries (seconds). Default 5 minutes (300s).
        TTL_DURATION_SECONDS = int(os.environ.get('IDEMPOTENCY_TTL_SECONDS', 5 * 60))
        now_epoch = int(time.time())
        ttl_timestamp = now_epoch + TTL_DURATION_SECONDS

        # Predeclare tenant_scoped_key so it's available later for update_item.
        tenant_scoped_key = None

        # Generate content-based hash for true idempotency
        content_hash = generate_content_hash(input_data)
        
        # Build tenant-scoped key that includes content hash
        if idempotency_key:
            # Client-provided key + content hash for guaranteed uniqueness per content
            tenant_scoped_key = f"{owner_id}#{idempotency_key}#{content_hash}"
        elif idemp_table_name and owner_id:
            # Auto-generate idempotency key based on workflow and content
            tenant_scoped_key = f"{owner_id}#auto#{workflow_id}#{content_hash}"
        else:
            tenant_scoped_key = None
        
        if tenant_scoped_key and idemp_table_name and owner_id:
            try:
                idemp_table = dynamodb.Table(idemp_table_name)
                claim_item = {
                    'idempotency_key': tenant_scoped_key,
                    'ownerId': owner_id,
                    # record created_at as epoch seconds and ttl for DynamoDB TTL
                    'created_at': now_epoch,
                    'ttl': ttl_timestamp,
                    'workflow_id': workflow_id,
                    'content_hash': content_hash,  # Store content hash for debugging
                    # Mark as in-progress until the finalizer marks COMPLETED/FAILED
                    'status': 'IN_PROGRESS'
                }
                idemp_table.put_item(Item=claim_item, ConditionExpression='attribute_not_exists(idempotency_key)')
                logger.info("Idempotency: claimed key %s (status=IN_PROGRESS, content_hash=%s)", tenant_scoped_key, content_hash)
            except ClientError as e:
                # [Fix] None defense: e.response['Error']가 None일 수 있음
                err_code = (e.response.get('Error') or {}).get('Code')
                # If the key already exists, consult its status to decide next action
                if err_code == 'ConditionalCheckFailedException':
                    existing = idemp_table.get_item(Key={'idempotency_key': tenant_scoped_key}).get('Item')
                    if existing:
                        existing_status = existing.get('status')
                        # Completed: return recorded execution
                        if existing_status == 'COMPLETED' and existing.get('executionArn'):
                            logger.info("Idempotency: existing COMPLETED execution for key %s executionArn=%s startDate=%s",
                                        tenant_scoped_key, existing.get('executionArn'), existing.get('startDate'))
                            return {
                                'statusCode': 200,
                                'body': json.dumps({
                                    'executionArn': existing.get('executionArn'),
                                    'startDate': existing.get('startDate')
                                })
                            }
                        # In-progress: tell caller to wait
                        elif existing_status == 'IN_PROGRESS':
                            logger.info("Idempotency: request already in progress for key %s existing_item=%s",
                                        tenant_scoped_key, existing)
                            return {
                                'statusCode': 409,
                                'body': json.dumps({'error': 'Idempotent request already in progress'})
                            }
                        # Failed: attempt to atomically re-claim the key for a retry
                        elif existing_status == 'FAILED':
                            try:
                                idemp_table.update_item(
                                    Key={'idempotency_key': tenant_scoped_key},
                                    UpdateExpression='SET #status = :inprog, created_at = :now, content_hash = :ch',
                                    ExpressionAttributeNames={'#status': 'status'},
                                    ExpressionAttributeValues={':inprog': 'IN_PROGRESS', ':now': now_epoch, ':ch': content_hash},
                                    ConditionExpression='#status = :failed',
                                )
                                logger.info("Idempotency: re-claimed key %s (was FAILED) -> now IN_PROGRESS", tenant_scoped_key)
                                # proceed to start execution
                            except ClientError:
                                logger.info("Idempotency: failed to re-claim key %s, treating as in-progress", tenant_scoped_key)
                                return {
                                    'statusCode': 409,
                                    'body': json.dumps({'error': 'Idempotent request already in progress'})
                                }
                        else:
                            # Unknown state: be conservative and treat as in-progress
                            logger.info("Idempotency: unknown existing item state for key %s existing_item=%s",
                                        tenant_scoped_key, existing)
                            return {
                                'statusCode': 409,
                                'body': json.dumps({'error': 'Idempotent request already in progress'})
                            }
                    else:
                        logger.exception("Idempotency: conditional put failed but no existing item found for key %s", tenant_scoped_key)
                        return {
                            'statusCode': 500,
                            'body': json.dumps({'error': 'Idempotency table inconsistent state'})
                        }
                else:
                    # Unexpected DynamoDB error: surface as 500
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': f'Idempotency table error: {str(e)}'})
                    }

        # NOTE: Quota checking removed from src.this Lambda for security architecture.
        # SegmentRunner Lambda now handles quota validation using the secure proxy
        # pattern. This centralizes quota management in one place and prevents
        # quota bypass vulnerabilities.

        # --- [NEW] Quota Pre-check and Reservation ---
        # Check quota once at workflow start and pass reservation to segments.
        # This reduces DynamoDB calls by 60-70% (no per-segment quota checks).
        quota_reservation_id = None
        try:
            from src.services.workflow.repository import WorkflowRepository
            repo = WorkflowRepository()
            user_item = repo.get_user(owner_id)
            if user_item:
                subscription_plan = user_item.get('subscription_plan', 'free')
                # Check if user has remaining quota (don't consume yet)
                current_usage = user_item.get('monthly_runs', 0)
                plan_limits = {'free': 50, 'basic': 500, 'pro': 5000, 'enterprise': 50000}
                limit = plan_limits.get(subscription_plan, 50)
                
                if current_usage >= limit:
                    logger.error("Quota exceeded for owner=%s (usage=%d, limit=%d)", owner_id, current_usage, limit)
                    return {
                        'statusCode': 429,
                        'body': json.dumps({'error': 'Monthly quota exceeded', 'usage': current_usage, 'limit': limit})
                    }
                
                # Generate reservation ID for segment runner to skip quota checks
                quota_reservation_id = f"{owner_id}#{workflow_id}#{uuid.uuid4().hex[:8]}"
                logger.info("Quota pre-check passed: owner=%s, usage=%d/%d, reservation=%s", 
                           owner_id, current_usage, limit, quota_reservation_id)
        except ImportError:
            logger.warning("WorkflowRepository not available, skipping quota pre-check")
        except Exception as e:
            logger.warning("Quota pre-check failed (non-blocking): %s", str(e))

        # --- Context Hydration: Load skills into state ---
        # If input_data contains 'injected_skills', load them from src.DynamoDB
        # and inject hydrated skill definitions into 'active_skills'
        input_data = hydrate_skills_for_workflow(input_data, owner_id)

        # Step Functions 실행 준비
        payload = {
            'workflowId': workflow_id,
            'ownerId': owner_id,
            'user_id': owner_id,  # segment runner에서 user_id도 확인하므로 동일한 값으로 설정
            'initial_state': input_data,
            
            # MOCK_MODE 환경변수 전달 (테스트 전용 경로 활성화용)
            'MOCK_MODE': os.environ.get('MOCK_MODE', 'false')
        }
        
        # Add quota reservation to payload if available
        if quota_reservation_id:
            payload['quota_reservation_id'] = quota_reservation_id
        
        # 🚀 MOCK_MODE 전용: 테스트 키워드나 직접 설정 주입으로 DynamoDB 우회
        mock_mode = os.environ.get('MOCK_MODE', 'false').lower()
        test_config_to_inject = None
        
        # 1. 직접 test_workflow_config가 API Body에 있는 경우
        if mock_mode == 'true' and isinstance(parsed_body, dict) and 'test_workflow_config' in parsed_body:
            test_config_to_inject = parsed_body['test_workflow_config']
            logger.info("🧪 MOCK_MODE: Direct test_workflow_config injection")
        
        # 2. 키워드 검출로 테스트 워크플로우를 로드하는 경우
        elif mock_mode == 'true' and test_keyword:
            try:
                # 키워드에 매핑된 워크플로우 ID 사용
                mapped_workflow_id = test_workflow_mappings.get(test_keyword)
                if mapped_workflow_id:
                    # Base directory 계산 (backend/src/handlers/core -> backend)
                    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
                    
                    # JSON 파일 경로 탐색
                    logger.info(f"Looking for test workflow: {mapped_workflow_id}")
                    possible_paths = [
                        f"/var/task/test_workflows/{mapped_workflow_id}.json",  # Lambda container (context=./backend/src)
                        f"./test_workflows/{mapped_workflow_id}.json",
                        f"../test_workflows/{mapped_workflow_id}.json",       # Local dev / legacy
                        f"/opt/test_workflows/{mapped_workflow_id}.json",     # Lambda Layer
                        f"backend/src/test_workflows/{mapped_workflow_id}.json",  # Local development
                        f"{base_dir}/src/test_workflows/{mapped_workflow_id}.json",  # Absolute path from backend
                    ]
                    
                    test_workflow_path = None
                    for path in possible_paths:
                        if os.path.exists(path):
                            test_workflow_path = path
                            break
                    
                    if test_workflow_path:
                        with open(test_workflow_path, 'r', encoding='utf-8') as f:
                            test_config_to_inject = json.load(f)
                        logger.info(f"🧪 MOCK_MODE: Loaded test workflow config from {test_workflow_path} for keyword '{test_keyword}'")
                    else:
                        logger.error(f"🚨 Test workflow file not found for keyword '{test_keyword}' -> {mapped_workflow_id}")
                        
            except Exception as e:
                logger.error(f"🚨 Failed to load test workflow for keyword '{test_keyword}': {e}")
        
        # 테스트 설정이 있으면 Step Functions payload에 추가
        if test_config_to_inject:
            payload['test_workflow_config'] = test_config_to_inject
            logger.info("🧪 MOCK_MODE: test_workflow_config injected, will bypass DynamoDB")
        elif workflow_config:
            # 실제 워크플로우 설정을 payload에 추가 (이미 로드됨)
            # Ensure config is serializable (Decimal conversion)
            config = _convert_floats_to_decimals(workflow_config)
            payload['workflow_config'] = config
            logger.info(f"✅ Real workflow_config injected for {workflow_id}")
        else:
            # [FALLBACK] 워크플로우 설정을 찾을 수 없는 경우 캐시를 사용한 DB 재시도
            try:
                from src.services.workflow.cache_manager import cached_get_workflow_config
                
                # 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
                WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
                if WORKFLOWS_TABLE:
                    wf_table = dynamodb.Table(WORKFLOWS_TABLE)
                    
                    # 🚀 캐시를 사용한 fallback 조회
                    workflow_config = cached_get_workflow_config(wf_table, owner_id, workflow_id)
                    
                    if workflow_config:
                        # Ensure config is serializable (Decimal conversion)
                        config = _convert_floats_to_decimals(workflow_config)
                        payload['workflow_config'] = config
                        
                        # 워크플로우 이름도 가져오기 (별도 조회 필요)
                        try:
                            wf_resp = wf_table.get_item(Key={'ownerId': owner_id, 'workflowId': workflow_id})
                            if 'Item' in wf_resp:
                                payload['workflow_name'] = wf_resp['Item'].get('name')
                        except Exception:
                            pass  # 이름은 선택사항
                        
                        logger.info(f"📦 Fallback: workflow_config loaded (cached) for {workflow_id}")
                    else:
                        logger.warning(f"⚠️ Fallback: Workflow not found in DB: {workflow_id}")
                        
            except ImportError:
                # 캐시 모듈이 없으면 기본 DB 조회
                try:
                    # 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
                    WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
                    if WORKFLOWS_TABLE:
                        wf_table = dynamodb.Table(WORKFLOWS_TABLE)
                        wf_resp = wf_table.get_item(Key={'ownerId': owner_id, 'workflowId': workflow_id})
                        if 'Item' in wf_resp:
                            wf_item = wf_resp['Item']
                            config = wf_item.get('config')
                            # Ensure config is serializable (Decimal conversion)
                            config = _convert_floats_to_decimals(config)
                            payload['workflow_config'] = config
                            payload['workflow_name'] = wf_item.get('name')
                            logger.info(f"📦 Fallback: workflow_config loaded (direct DB) for {workflow_id}")
                        else:
                            logger.warning(f"⚠️ Fallback: Workflow not found in DB: {workflow_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Fallback direct DB query failed: {e}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Fallback cached workflow config fetch failed: {e}")
        
        # NOTE: API key injection removed from src.this Lambda for security reasons.
        # SegmentRunner Lambda handles API key retrieval (user keys + Secrets Manager
        # fallback) using the secure proxy pattern. This avoids exposing secrets
        # in Step Functions input/logs and centralizes key management in one place.

        # If a workflow state bucket is configured, consider offloading large
        # initial_state to S3 and pass a pointer instead to avoid Step Functions
        # input size limits. We mirror the same threshold env var used elsewhere.
        try:
            SKELETON_STATE_BUCKET = os.environ.get('SKELETON_S3_BUCKET') or os.environ.get('WORKFLOW_STATE_BUCKET')
            # Use a conservative default below the Step Functions 256KB limit
            STREAM_INLINE_THRESHOLD = int(os.environ.get('STREAM_INLINE_THRESHOLD_BYTES', '250000'))
            if SKELETON_STATE_BUCKET and input_data:
                serialized = json.dumps(input_data, ensure_ascii=False)
                if len(serialized.encode('utf-8')) > STREAM_INLINE_THRESHOLD:
                    # upload to S3
                    # SECURITY: include owner_id (tenant) in the key path so that
                    # downstream Lambdas can validate the pointer belongs to the
                    # authenticated user and avoid IDOR. Require owner_id exists.
                    if not owner_id:
                        raise PermissionError("Missing owner_id for S3 state offload")

                    key_prefix = f"workflow-states/{owner_id}/{workflow_id}/execution-inputs"
                    key = f"{key_prefix.rstrip('/')}/{uuid.uuid4()}.json"
                    s3.put_object(Bucket=SKELETON_STATE_BUCKET, Key=key, Body=serialized.encode('utf-8'))
                    payload.pop('initial_state', None)
                    payload['initial_state_s3_path'] = f"s3://{SKELETON_STATE_BUCKET}/{key}"
        except Exception as e:
            # S3 offload failed while the payload is above the safe threshold.
            # Returning 500 is safer than attempting to send a too-large
            # payload to Step Functions (which would fail with PayloadTooLarge).
            logger.exception("CRITICAL: Failed to offload large initial_state to S3 before starting execution")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to upload large input state to S3 before execution',
                    'detail': str(e)
                })
            }

        # Include idempotency key in the state machine input so the finalizer
        # can mark the idempotency record COMPLETED/FAILED based on terminal state.
        if tenant_scoped_key:
            payload['idempotency_key'] = tenant_scoped_key

        # [v2.1] Start the Step Functions execution with retry
        # API Throttling(ProvisionedThroughputExceededException) 대응
        def _start_sfn_execution():
            return stepfunctions.start_execution(
                stateMachineArn=orchestrator_arn,
                input=json.dumps(payload)
            )
        
        if RETRY_UTILS_AVAILABLE:
            response = retry_call(
                _start_sfn_execution,
                max_retries=2,
                base_delay=0.5,
                max_delay=5.0,
                exceptions=(ClientError,)
            )
        else:
            response = _start_sfn_execution()

        execution_arn = response.get('executionArn')
        start_date = response.get('startDate')

        # [FIX] Synchronously create the initial execution record in DynamoDB
        # This prevents the "404 Not Found" race condition where the frontend queries
        # state_history before the async ExecutionProgressNotifier has run.
        if executions_table and owner_id and execution_arn:
            try:
                now_epoch = int(time.time())
                start_date_iso = start_date.isoformat() if hasattr(start_date, 'isoformat') else str(start_date)
                
                initial_item = {
                    'ownerId': owner_id,
                    'executionArn': execution_arn,
                    'workflowId': workflow_id,
                    'status': 'STARTED',
                    'startDate': start_date_iso,
                    'created_at': now_epoch,
                    'updated_at': now_epoch,
                    'initial_input': input_data,  # Persist initial input separately to prevent overwrite
                    # Initialize with empty state structure to avoid schema issues in frontend
                    'step_function_state': {
                        'input': input_data,
                        'start_time': now_epoch,
                        'status': 'STARTED'
                    }
                }
                
                # Use ConditionExpression to avoid overwriting if the async notifier was faster
                # [FIX] Convert floats to Decimals for DynamoDB compatibility
                initial_item = _convert_floats_to_decimals(initial_item)
                
                executions_table.put_item(
                    Item=initial_item,
                    ConditionExpression='attribute_not_exists(executionArn)'
                )
                logger.info(f"Synchronously created initial execution record: {execution_arn}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    # Async notifier beat us to it - this is fine, race condition resolved naturally
                    logger.info(f"Execution record already exists (async notifier won race): {execution_arn}")
                else:
                    # Log but don't fail the request - let eventual consistency handle it
                    logger.warning(f"Failed to synchronously create execution record: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error creating execution record: {e}")

        # NOTE: auto-push self-invocation removed. Notification of completion
        # should be performed by Step Functions publishing to EventBridge and
        # a dedicated Notify lambda processing those events.

        # NOTE: synchronous client-side waiting (wait_for_completion) has been
        # removed in favor of the auto_push (background push) and async-pull
        # models. The function immediately returns an executionArn and the
        # client may poll a dedicated status endpoint if desired.

        # If we used an idempotency table, persist the executionArn/startDate for
        # future duplicate requests.
        if idemp_table is not None and tenant_scoped_key:
            try:
                start_date = response.get('startDate')
                start_date_iso = start_date.isoformat() if hasattr(start_date, 'isoformat') else str(start_date)
                # Use tenant-scoped key (we created it earlier)
                key_to_use = tenant_scoped_key

                # Update executionArn, startDate and ttl (ensure TTL remains set)
                # 'ttl' is a DynamoDB reserved word for UpdateExpression; use
                # an expression attribute name to avoid ValidationException.
                idemp_table.update_item(
                    Key={'idempotency_key': key_to_use},
                    UpdateExpression='SET executionArn = :e, startDate = :s, #ttl = :t',
                    ExpressionAttributeNames={'#ttl': 'ttl'},
                    ExpressionAttributeValues={
                        ':e': response.get('executionArn'),
                        ':s': start_date_iso,
                        ':t': ttl_timestamp
                    }
                )
            except Exception as exc:
                # CRITICAL: failing to record the executionArn against the
                # idempotency key leaves the system in an inconsistent state.
                # Return 500 so the client is aware the request succeeded but
                # idempotency recording failed (client can retry safely).
                logger.exception("CRITICAL: Failed to update idempotency table for key %s", tenant_scoped_key)
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'error': 'Failed to record idempotency key after starting execution',
                        'executionArn': response.get('executionArn'),
                        'detail': str(exc)
                    })
                }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'executionArn': response.get('executionArn'),
                'startDate': response.get('startDate').isoformat() if response.get('startDate') else None
            })
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
