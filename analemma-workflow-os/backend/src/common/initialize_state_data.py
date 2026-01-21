import os
import json
import time
import logging
import boto3

# [Priority 1 Optimization] Pre-compilation: Load partition_map from DB
# Runtime partitioning used only as fallback
try:
    from src.services.workflow.partition_service import partition_workflow_advanced
    _HAS_PARTITION = True
except ImportError:
    try:
        from src.services.workflow.partition_service import partition_workflow_advanced
        _HAS_PARTITION = True
    except ImportError:
        _HAS_PARTITION = False
        partition_workflow_advanced = None

# DynamoDB client (warm start optimization)
try:
    from src.common.aws_clients import get_dynamodb_resource
    _dynamodb = get_dynamodb_resource()
except ImportError:
    _dynamodb = boto3.resource('dynamodb')

# 🚨 [Critical Fix] Match default values with template.yaml
WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def _calculate_distributed_strategy(
    total_segments: int,
    llm_segments: int,
    hitp_segments: int,
    partition_map: list
) -> dict:
    """
    🚀 하이브리드 분산 실행 전략 결정
    
    Returns:
        dict: {
            "strategy": "SAFE" | "BATCHED" | "MAP_REDUCE" | "RECURSIVE",
            "max_concurrency": int,
            "batch_size": int (for BATCHED mode),
            "reason": str
        }
    """
    # 🔍 워크플로우 특성 분석
    llm_ratio = llm_segments / max(total_segments, 1)
    hitp_ratio = hitp_segments / max(total_segments, 1)
    
    # 🔗 의존성 분석: 독립 실행 가능한 세그먼트 그룹 계산
    independent_segments = 0
    max_dependency_depth = 0
    
    for segment in partition_map:
        deps = segment.get("dependencies", [])
        if not deps:
            independent_segments += 1
        max_dependency_depth = max(max_dependency_depth, len(deps))
    
    independence_ratio = independent_segments / max(total_segments, 1)
    
    logger.info(f"[Strategy Analysis] segments={total_segments}, llm_ratio={llm_ratio:.2f}, "
                f"hitp_ratio={hitp_ratio:.2f}, independence_ratio={independence_ratio:.2f}, "
                f"max_dep_depth={max_dependency_depth}")
    
    # 📊 전략 결정 로직
    # 1. HITP가 포함된 경우: 반드시 SAFE 모드 (인간 승인 필요)
    if hitp_segments > 0:
        return {
            "strategy": "SAFE",
            "max_concurrency": 1,
            "batch_size": 1,
            "reason": f"HITP segments detected ({hitp_segments}), requires sequential human approval"
        }
    
    # 2. 소규모 워크플로우: SAFE 모드
    if total_segments <= 10:
        return {
            "strategy": "SAFE",
            "max_concurrency": min(total_segments, 5),
            "batch_size": 1,
            "reason": f"Small workflow ({total_segments} segments), SAFE mode sufficient"
        }
    
    # 3. 대규모 + 높은 독립성 + LLM 비율 높음: MAP_REDUCE 모드
    if total_segments > 100 and independence_ratio > 0.5 and llm_ratio > 0.3:
        return {
            "strategy": "MAP_REDUCE",
            "max_concurrency": 100,  # 높은 병렬성
            "batch_size": 25,
            "reason": f"High independence ({independence_ratio:.1%}), LLM-heavy ({llm_ratio:.1%}), optimal for Map-Reduce"
        }
    
    # 4. 중간 규모 또는 혼합 워크플로우: BATCHED 모드
    if total_segments > 10:
        # 배치 크기 동적 결정: LLM 비율 높으면 작은 배치
        if llm_ratio > 0.5:
            batch_size = 10
            max_concurrency = 10
        else:
            batch_size = 20
            max_concurrency = 20
            
        return {
            "strategy": "BATCHED",
            "max_concurrency": max_concurrency,
            "batch_size": batch_size,
            "reason": f"Medium workflow ({total_segments} segments), batched processing optimal"
        }
    
    # 5. 기본: SAFE 모드
    return {
        "strategy": "SAFE",
        "max_concurrency": 2,
        "batch_size": 1,
        "reason": "Default fallback to SAFE mode"
    }

def _calculate_dynamic_concurrency(
    total_segments: int,
    llm_segments: int,
    hitp_segments: int,
    partition_map: list,
    owner_id: str
) -> int:
    """
    Calculate dynamic concurrency based on workflow complexity and user tier
    
    🚨 [Critical] Specification clarification:
    The max_concurrency returned by this function determines **parallel processing capacity within chunks**.
    
    - Distributed Map's MaxConcurrency is fixed at 1 in ASL (ensures state continuity)
    - This value is used for parallel branch execution within each chunk
    - Applied to parallel group processing within ProcessSegmentChunk
    
    Args:
        total_segments: Total number of segments
        llm_segments: Number of LLM segments
        hitp_segments: Number of HITP segments
        partition_map: Partition map
        owner_id: User ID
        
    Returns:
        int: Calculated chunk-internal MaxConcurrency value (range 5-50)
             ※ Separate from Distributed Map's own MaxConcurrency
    """
    # Default value
    base_concurrency = 15
    
    # 1. Calculate number of parallel branches
    max_parallel_branches = 0
    if partition_map:
        for segment in partition_map:
            if segment.get('type') == 'parallel_group':
                branches = segment.get('branches', [])
                max_parallel_branches = max(max_parallel_branches, len(branches))
    
    # 2. Adjust based on workflow complexity
    calculated_concurrency = base_concurrency # [Safety] Initialize default
    
    if max_parallel_branches == 0:
        # Use default value if no parallel branches
        calculated_concurrency = base_concurrency
    elif max_parallel_branches <= 5:

        # Small-scale parallel (5 or fewer): Execute all concurrently
        calculated_concurrency = max_parallel_branches
    elif max_parallel_branches <= 10:
        # Medium-scale parallel (6-10): Execute 80% concurrently
        calculated_concurrency = int(max_parallel_branches * 0.8)
    elif max_parallel_branches <= 20:
        # Large-scale parallel (11-20): Execute 60% concurrently
        calculated_concurrency = int(max_parallel_branches * 0.6)
    else:
        # Ultra-large-scale parallel (21+): Limit to maximum 30
        calculated_concurrency = min(30, int(max_parallel_branches * 0.5))
    
    # 3. Adjust based on user tier (optional)
    try:
        user_table = _dynamodb.Table(os.environ.get('USERS_TABLE', 'UsersTableV3'))
        user_response = user_table.get_item(Key={'userId': owner_id})
        user_item = user_response.get('Item')
        
        if user_item:
            subscription_plan = user_item.get('subscription_plan', 'free')
            
            # Apply tier multipliers
            tier_multipliers = {
                'free': 0.5,      # 50% limit
                'basic': 0.75,    # 75%
                'pro': 1.0,       # 100%
                'enterprise': 1.5 # 150% (up to 50 max)
            }
            
            multiplier = tier_multipliers.get(subscription_plan, 1.0)
            calculated_concurrency = int(calculated_concurrency * multiplier)
            
            logger.info(f"User tier '{subscription_plan}' applied multiplier {multiplier}")
    except Exception as e:
        logger.warning(f"Failed to load user tier for concurrency calculation: {e}")
    
    # 4. 🛡️ [Concurrency Protection] OS-level upper limit clamping
    # Ensure overall system stability according to account concurrency limit (10)
    MAX_OS_LIMIT = 2  # Response to account concurrency limit of 10 (to be increased upon approval request)
    clamped_concurrency = min(calculated_concurrency, MAX_OS_LIMIT)
    
    if calculated_concurrency > MAX_OS_LIMIT:
        logger.warning(
            f"[Concurrency Protection] Clamping requested concurrency {calculated_concurrency} "
            f"to OS limit {MAX_OS_LIMIT}"
        )
    
    # 5. Final range limit (1 ~ MAX_OS_LIMIT)
    final_concurrency = max(1, clamped_concurrency)
    
    logger.info(
        f"Chunk-internal concurrency calculated: {final_concurrency} "
        f"(branches: {max_parallel_branches}, segments: {total_segments}, "
        f"llm: {llm_segments}, hitp: {hitp_segments}, OS_LIMIT: {MAX_OS_LIMIT}) "
        f"※ Distributed Map MaxConcurrency remains 1 for state continuity"
    )
    
    return final_concurrency


def _load_workflow_config(owner_id: str, workflow_id: str) -> dict:
    """
    Load workflow config from Workflows table.
    Retrieve full config including subgraphs.
    """
    if not owner_id or not workflow_id:
        return None
    
    try:
        table = _dynamodb.Table(WORKFLOWS_TABLE)
        response = table.get_item(
            Key={'ownerId': owner_id, 'workflowId': workflow_id},
            ProjectionExpression='config, partition_map, total_segments, llm_segments_count, hitp_segments_count'
        )
        item = response.get('Item')
        if item and item.get('config'):
            config = item.get('config')
            # Parse if JSON string
            if isinstance(config, str):
                import json
                config = json.loads(config)
            logger.info(f"Loaded workflow config from DB: {workflow_id}")
            return {
                'config': config,
                'partition_map': item.get('partition_map'),
                'total_segments': item.get('total_segments'),
                'llm_segments_count': item.get('llm_segments_count'),
                'hitp_segments_count': item.get('hitp_segments_count')
            }
        return None
    except Exception as e:
        logger.warning(f"Failed to load workflow config: {e}")
        return None


def _load_precompiled_partition(owner_id: str, workflow_id: str) -> dict:
    """
    Load pre-compiled partition_map from Workflows table.
    Retrieve partition_map calculated at save time.
    """
    if not owner_id or not workflow_id:
        return None
    
    try:
        table = _dynamodb.Table(WORKFLOWS_TABLE)
        response = table.get_item(
            Key={'ownerId': owner_id, 'workflowId': workflow_id},
            ProjectionExpression='partition_map, total_segments, llm_segments_count, hitp_segments_count'
        )
        item = response.get('Item')
        if item and item.get('partition_map'):
            logger.info(f"Loaded pre-compiled partition_map from DB: {item.get('total_segments', 0)} segments")
            return item
        return None
    except Exception as e:
        logger.warning(f"Failed to load pre-compiled partition_map: {e}")
        return None


def lambda_handler(event, context):
    """
    Initializes state data.
    [Priority 1 Optimization] Load pre-compiled partition_map from DB.
    Fallback: Runtime calculation (maintain backward compatibility)
    
    [Subgraph Support] If workflow_config includes subgraphs,
    DynamicWorkflowBuilder will build recursively.
    """
    logger.info("Initializing state data")
    
    # ====================================================================
    # 🛡️ [v2.6 P0 Fix] 최우선 초기화 - 에러 발생 시에도 필수 필드 보존
    # Step Functions ASL이 $.total_segments를 참조할 때 null 방지
    # ====================================================================
    raw_input = event.get('input', event)
    if not isinstance(raw_input, dict):
        raw_input = {}
    
    # 🛡️ [P0] total_segments 안전 초기화 - try 블록 외부에서 먼저 계산
    _early_partition_map = raw_input.get('partition_map', [])
    _safe_total_segments = len(_early_partition_map) if isinstance(_early_partition_map, list) and _early_partition_map else 1
    
    # [FIX] 1. Move initialization to top (prevent NameError in S3 Metadata)
    current_time = int(time.time())
        
    # [FIX] Explicit variable initialization (prevent UnboundLocalError and Missing Field)
    # Keys must always exist to prevent errors when SFN references $.field_name
    partition_map_s3_path = ""
    manifest_s3_path = ""
    state_s3_path = ""
    
    # 2. Refine config determination priority
    workflow_config = raw_input.get('test_workflow_config') or raw_input.get('workflow_config')
    owner_id = raw_input.get('ownerId', "")
    workflow_id = raw_input.get('workflowId', "")
    idempotency_key = raw_input.get('idempotency_key', "")
    quota_reservation_id = raw_input.get('quota_reservation_id', "")
    
    # [Fix] Extract MOCK_MODE - for simulator E2E testing
    mock_mode = raw_input.get('MOCK_MODE', 'false')
    
    # [Robustness Fix] Load from DB if workflow_config missing (when test_config absent)
    if not workflow_config and workflow_id and owner_id:
        logger.info(f"workflow_config missing in input, loading from DB: {workflow_id}")
        db_data = _load_workflow_config(owner_id, workflow_id)
        if db_data and db_data.get('config'):
            workflow_config = db_data.get('config')
            # Also load partition_map from DB if available
            if db_data.get('partition_map'):
                event['_db_partition_map'] = db_data.get('partition_map')
                event['_db_total_segments'] = db_data.get('total_segments')
                event['_db_llm_segments'] = db_data.get('llm_segments_count')
                event['_db_hitp_segments'] = db_data.get('hitp_segments_count')
        else:
            logger.warning(f"Workflow not found in DB: {workflow_id}")

    # Set to empty dict if workflow_config still missing to prevent crash
    if not workflow_config:
        workflow_config = {}
        logger.warning("Proceeding with empty workflow_config (risk of later failure)")

    # [Fix] Merge top-level initial_state into workflow_config for test mode
    # When using test_workflow_config, initial_state may be at top level, not inside workflow_config
    if raw_input.get('initial_state') and not workflow_config.get('initial_state'):
        workflow_config['initial_state'] = raw_input.get('initial_state')
        logger.info(f"Merged top-level initial_state into workflow_config: {list(raw_input.get('initial_state', {}).keys())}")

    # 3. MOCK_MODE: Force partitioning (prepare for no DB data)
    if raw_input.get('test_workflow_config'):
         if not _HAS_PARTITION:
             logger.warning("MOCK_MODE but partition logic unavailable!")
         else:
             logger.info("MOCK_MODE detected: Forcing runtime partition calculation")
             try:
                 partition_result = partition_workflow_advanced(workflow_config)
                 
                 # 🛡️ [v2.6 P0 Fix] 유령 'code' 타입 박멸 로직
                 # 상위 데이터 오염을 런타임에서 교정
                 for seg in partition_result.get('partition_map', []):
                     for node in seg.get('nodes', []):
                         if isinstance(node, dict) and node.get('type') == 'code':
                             logger.warning(f"🛡️ [Self-Healing] Aliasing 'code' to 'operator' for node {node.get('id')}")
                             node['type'] = 'operator'
                 
                 # Override raw inputs/event cache with fresh calculation
                 raw_input['partition_map'] = partition_result.get('partition_map', [])
                 raw_input['total_segments'] = partition_result.get('total_segments', 0)
                 raw_input['llm_segments'] = partition_result.get('llm_segments', 0)
                 raw_input['hitp_segments'] = partition_result.get('hitp_segments', 0)
                 
                 # 🛡️ [P0] 조기 계산된 _safe_total_segments 업데이트
                 _safe_total_segments = max(1, partition_result.get('total_segments', 1))
                 
             except Exception as e:
                 logger.error(f"MOCK_MODE partitioning failed: {e}")

    # 2. Partitioning (priority: input > DB pre-compiled > runtime calculation)
    partition_map = None
    total_segments = 0
    llm_segments = 0
    hitp_segments = 0
    
    # 2a. Use partition_map if already in input (retry/Child Workflow)
    if raw_input.get('partition_map'):
        logger.info("Using provided partition_map from input")
        partition_map = raw_input.get('partition_map')
        total_segments = raw_input.get('total_segments', len(partition_map))
        llm_segments = raw_input.get('llm_segments', 0)
        hitp_segments = raw_input.get('hitp_segments', 0)
    
    # 2a-1. Use partition_map loaded with config
    if not partition_map and event.get('_db_partition_map'):
        logger.info("Using partition_map from DB config load")
        partition_map = event.get('_db_partition_map')
        total_segments = event.get('_db_total_segments', len(partition_map) if partition_map else 0)
        llm_segments = event.get('_db_llm_segments', 0)
        hitp_segments = event.get('_db_hitp_segments', 0)
    
    # 2b. Load pre-compiled partition_map from DB (priority 1 optimization)
    if not partition_map:
        precompiled = _load_precompiled_partition(owner_id, workflow_id)
        if precompiled:
            partition_map = precompiled.get('partition_map')
            total_segments = precompiled.get('total_segments', len(partition_map) if partition_map else 0)
            llm_segments = precompiled.get('llm_segments_count', 0)
            hitp_segments = precompiled.get('hitp_segments_count', 0)
    
    # 2c. Fallback: Runtime calculation (existing logic, maintain backward compatibility)
    if not partition_map:
        if not _HAS_PARTITION:
            raise RuntimeError("partition_workflow_advanced not available and no pre-compiled partition_map found")
        
        logger.info("Calculating partition_map at runtime (fallback)...")
        try:
            partition_result = partition_workflow_advanced(workflow_config)
            partition_map = partition_result.get('partition_map', [])
            total_segments = partition_result.get('total_segments', 0)
            llm_segments = partition_result.get('llm_segments', 0)
            hitp_segments = partition_result.get('hitp_segments', 0)
            
            # 🛡️ [v2.6 P0 Fix] 유령 'code' 타입 박멸 로직
            for seg in partition_map:
                for node in seg.get('nodes', []):
                    if isinstance(node, dict) and node.get('type') == 'code':
                        logger.warning(f"🛡️ [Self-Healing] Aliasing 'code' to 'operator' for node {node.get('id')}")
                        node['type'] = 'operator'
            
        except Exception as e:
            logger.error(f"Partitioning failed: {e}")
            raise RuntimeError(f"Failed to partition workflow: {str(e)}")

    # 🎯 [Unification Strategy] Create segment_manifest (list of segments to execute)
    segment_manifest = []
    for idx, segment in enumerate(partition_map):
        segment_manifest.append({
            "segment_id": idx,
            "segment_config": segment,
            "execution_order": idx,
            "dependencies": segment.get("dependencies", []),
            "type": segment.get("type", "normal")
        })
    
    # � [MOVED UP] Hybrid Distributed Strategy Calculation - BEFORE S3 offload decision
    distributed_strategy = _calculate_distributed_strategy(
        total_segments=total_segments,
        llm_segments=llm_segments,
        hitp_segments=hitp_segments,
        partition_map=partition_map
    )
    
    logger.info(f"[Distributed Strategy] {distributed_strategy['strategy']}: {distributed_strategy['reason']}")
    
    # 🚨 [Critical Fix] Detect distributed mode and offload large data to S3
    is_distributed_mode = total_segments > 300  # Distributed mode threshold
    partition_map_for_return = partition_map  # Default: return all
    
    # 🚀 [Hybrid Mode Compatibility] MAP_REDUCE/BATCHED 모드에서는 segment_manifest 인라인 유지
    uses_inline_manifest = distributed_strategy["strategy"] in ("MAP_REDUCE", "BATCHED")
    
    # AWS Clients
    s3_client = None
    bucket = os.environ.get('WORKFLOW_STATE_BUCKET')
    
    # Initialize boto3 client only when S3 upload needed
    if bucket and owner_id and workflow_id:
        if (len(segment_manifest) > 50 and not uses_inline_manifest) or is_distributed_mode:
            import boto3
            s3_client = boto3.client('s3')

    # 1. Manifest Offloading (when exceeding 50 items AND not using inline manifest for hybrid mode)
    if len(segment_manifest) > 50 and s3_client and not uses_inline_manifest:
        try:
            manifest_key = f"workflow-manifests/{owner_id}/{workflow_id}/segment_manifest.json"
            manifest_data = json.dumps(segment_manifest, ensure_ascii=False)
            s3_client.put_object(
                Bucket=bucket,
                Key=manifest_key,
                Body=manifest_data.encode('utf-8')
            )
            manifest_s3_path = f"s3://{bucket}/{manifest_key}"
            logger.info(f"Segment manifest uploaded to S3: {manifest_s3_path}")
        except Exception as e:
            logger.warning(f"Failed to upload segment manifest to S3: {e}")
            # Keep manifest_s3_path as initial value "" on failure

    # 2. Partition Map Offloading (distributed mode)
    # 🚨 [Critical Fix] Optimize partition_map size in distributed mode
    if is_distributed_mode:
        partition_map_json = json.dumps(partition_map, ensure_ascii=False)
        partition_map_size_kb = len(partition_map_json.encode('utf-8')) / 1024
        
        logger.info(f"Distributed mode detected: {total_segments} segments, partition_map size: {partition_map_size_kb:.1f}KB")
        
        # Offload partition_map larger than 100KB to S3
        if partition_map_size_kb > 100 and s3_client:
            try:
                partition_map_key = f"workflow-partitions/{owner_id}/{workflow_id}/partition_map.json"
                s3_client.put_object(
                    Bucket=bucket,
                    Key=partition_map_key,
                    Body=partition_map_json.encode('utf-8'),
                    ContentType='application/json',
                    Metadata={
                        'total_segments': str(total_segments),
                        'size_kb': str(int(partition_map_size_kb)),
                        'created_at': str(current_time)
                    }
                )
                
                partition_map_s3_path = f"s3://{bucket}/{partition_map_key}"
                logger.info(f"Large partition_map offloaded to S3: {partition_map_s3_path}")
                
                # 🚨 [Critical] Exclude partition_map in distributed mode
                partition_map_for_return = None
                
            except Exception as e:
                logger.warning(f"Failed to offload partition_map to S3: {e}")
                # Warn and fallback to inline on S3 failure
                logger.warning(f"Proceeding with inline partition_map ({partition_map_size_kb:.1f}KB)")
        else:
            logger.info(f"partition_map size acceptable for inline return: {partition_map_size_kb:.1f}KB")

    # 3. Initial state configuration
    current_time = int(time.time())
    
    # Set current_state to null if S3 path exists, otherwise use initial_state
    initial_state_s3_path = workflow_config.get('initial_state_s3_path')
    if initial_state_s3_path:
        # Large payload: Load from S3 (handled by segment_runner)
        current_state = None
        state_s3_path = initial_state_s3_path
        input_value = initial_state_s3_path
    else:
        # Inline state
        current_state = workflow_config.get('initial_state', {})
        # state_s3_path = "" # Already initialized above
        input_value = current_state
    
    # 4. Dynamic concurrency calculation (Map state optimization)
    max_concurrency = _calculate_dynamic_concurrency(
        total_segments=total_segments,
        llm_segments=llm_segments,
        hitp_segments=hitp_segments,
        partition_map=partition_map,
        owner_id=owner_id
    )
    
    # 🚀 Override max_concurrency with strategy-recommended value (strategy calculated earlier)
    max_concurrency = distributed_strategy.get("max_concurrency", max_concurrency)
    
    logger.info(f"[FIXED_ROBUST] Returning state data. partition_map_s3_path: '{partition_map_s3_path}'")

    # 🛡️ [Critical Fix] Ensure total_segments is always int (never None)
    # 조기 초기화된 _safe_total_segments와 계산된 total_segments 중 큰 값 사용
    safe_total_segments = max(1, int(total_segments or _safe_total_segments))

    return {
        "workflow_config": workflow_config,
        "current_state": current_state,
        "input": input_value,
        "state_s3_path": state_s3_path,
        "state_history": [],
        "ownerId": owner_id,
        "workflowId": workflow_id,
        "segment_to_run": 0,
        "idempotency_key": idempotency_key,
        "quota_reservation_id": quota_reservation_id,
        
        # 🚨 [Critical Fix] Conditional partition_map return in distributed mode
        "total_segments": safe_total_segments,  # 🛡️ Always int, never None
        "partition_map": partition_map_for_return,
        "partition_map_s3_path": partition_map_s3_path,  # Always exists (minimum "")
        
        # Set segment_manifest to None if manifest_s3_path exists (payload optimization)
        "segment_manifest": segment_manifest if manifest_s3_path == "" else None,
        "segment_manifest_s3_path": manifest_s3_path,    # Always exists (minimum "")
        
        # Metadata
        "state_durations": {},
        "last_update_time": current_time,
        "start_time": current_time,
        "loop_counter": 0,
        "llm_segments": llm_segments,
        "hitp_segments": hitp_segments,
        
        # Distributed Mode Flag
        "distributed_mode": is_distributed_mode,
        "max_concurrency": max_concurrency,
        
        # 🚀 Hybrid Distributed Strategy
        "distributed_strategy": distributed_strategy["strategy"],
        "distributed_strategy_detail": distributed_strategy,
        
        # [Fix] Pass MOCK_MODE - for simulator E2E testing (HITP auto resume, etc.)
        "MOCK_MODE": mock_mode,
        
        # 🚨 [Critical Fix] Loop Control Parameters - Force int to prevent SFN comparison failure
        "max_loop_iterations": int(workflow_config.get("max_loop_iterations", 100)),
        "max_branch_iterations": int(workflow_config.get("max_branch_iterations", 100))
    }

