import os
import json
import time
import logging
import boto3

# [1순위 최적화] Pre-compilation: DB에서 partition_map 로드
# 런타임 파티셔닝은 fallback으로만 사용
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

# DynamoDB 클라이언트 (웜 스타트 최적화)
try:
    from src.common.aws_clients import get_dynamodb_resource
    _dynamodb = get_dynamodb_resource()
except ImportError:
    _dynamodb = boto3.resource('dynamodb')

WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'Workflows')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _calculate_dynamic_concurrency(
    total_segments: int,
    llm_segments: int,
    hitp_segments: int,
    partition_map: list,
    owner_id: str
) -> int:
    """
    워크플로우 복잡도와 사용자 티어 기반으로 동적 동시성 계산
    
    🚨 [Critical] 규격 명확화:
    이 함수가 반환하는 max_concurrency는 **청크 내부의 병렬 처리량**을 결정합니다.
    
    - Distributed Map의 MaxConcurrency는 ASL에서 1로 고정 (상태 연속성 보장)
    - 이 값은 각 청크 내에서 병렬 브랜치 실행 시 사용됨
    - ProcessSegmentChunk 내부의 병렬 그룹 처리에 적용
    
    Args:
        total_segments: 총 세그먼트 수
        llm_segments: LLM 세그먼트 수
        hitp_segments: HITP 세그먼트 수
        partition_map: 파티션 맵
        owner_id: 사용자 ID
        
    Returns:
        int: 계산된 청크 내부 MaxConcurrency 값 (5-50 범위)
             ※ Distributed Map 자체의 MaxConcurrency와는 별개
    """
    # 기본값
    base_concurrency = 15
    
    # 1. 병렬 브랜치 수 계산
    max_parallel_branches = 0
    if partition_map:
        for segment in partition_map:
            if segment.get('type') == 'parallel_group':
                branches = segment.get('branches', [])
                max_parallel_branches = max(max_parallel_branches, len(branches))
    
    # 2. 워크플로우 복잡도 기반 조정
    calculated_concurrency = base_concurrency # [Safety] Initialize default
    
    if max_parallel_branches == 0:
        # 병렬 브랜치가 없으면 기본값 사용
        calculated_concurrency = base_concurrency
    elif max_parallel_branches <= 5:

        # 소규모 병렬 (5개 이하): 모두 동시 실행
        calculated_concurrency = max_parallel_branches
    elif max_parallel_branches <= 10:
        # 중규모 병렬 (6-10개): 80% 동시 실행
        calculated_concurrency = int(max_parallel_branches * 0.8)
    elif max_parallel_branches <= 20:
        # 대규모 병렬 (11-20개): 60% 동시 실행
        calculated_concurrency = int(max_parallel_branches * 0.6)
    else:
        # 초대규모 병렬 (21개 이상): 최대 30개로 제한
        calculated_concurrency = min(30, int(max_parallel_branches * 0.5))
    
    # 3. 사용자 티어 기반 조정 (선택적)
    try:
        user_table = _dynamodb.Table(os.environ.get('USERS_TABLE', 'Users'))
        user_response = user_table.get_item(Key={'userId': owner_id})
        user_item = user_response.get('Item')
        
        if user_item:
            subscription_plan = user_item.get('subscription_plan', 'free')
            
            # 티어별 배수 적용
            tier_multipliers = {
                'free': 0.5,      # 50% 제한
                'basic': 0.75,    # 75%
                'pro': 1.0,       # 100%
                'enterprise': 1.5 # 150% (최대 50까지)
            }
            
            multiplier = tier_multipliers.get(subscription_plan, 1.0)
            calculated_concurrency = int(calculated_concurrency * multiplier)
            
            logger.info(f"User tier '{subscription_plan}' applied multiplier {multiplier}")
    except Exception as e:
        logger.warning(f"Failed to load user tier for concurrency calculation: {e}")
    
    # 4. 🛡️ [Concurrency Protection] OS 레벨 상한 클램핑
    # 계정 동시성 한도(10)에 맞춰 전체 시스템 안정성 보장
    MAX_OS_LIMIT = 2  # 계정 Concurrency 10 제한 대응 (상향 요청 완료 시 증가 예정)
    clamped_concurrency = min(calculated_concurrency, MAX_OS_LIMIT)
    
    if calculated_concurrency > MAX_OS_LIMIT:
        logger.warning(
            f"[Concurrency Protection] Clamping requested concurrency {calculated_concurrency} "
            f"to OS limit {MAX_OS_LIMIT}"
        )
    
    # 5. 최종 범위 제한 (1 ~ MAX_OS_LIMIT)
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
    Workflows 테이블에서 workflow config를 로드.
    subgraphs를 포함한 전체 config를 가져옴.
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
            # JSON string이면 파싱
            if isinstance(config, str):
                import json
                config = json.loads(config)
            logger.info(f"Loaded workflow config from src.DB: {workflow_id}")
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
    Workflows 테이블에서 pre-compiled partition_map 로드.
    저장 시점에 계산된 partition_map을 가져옴.
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
            logger.info(f"Loaded pre-compiled partition_map from src.DB: {item.get('total_segments', 0)} segments")
            return item
        return None
    except Exception as e:
        logger.warning(f"Failed to load pre-compiled partition_map: {e}")
        return None


def lambda_handler(event, context):
    """
    Initializes state data.
    [1순위 최적화] Pre-compiled partition_map을 DB에서 로드.
    Fallback: 런타임 계산 (하위 호환성 유지)
    
    [서브그래프 지원] workflow_config에 subgraphs가 포함되어 있으면
    DynamicWorkflowBuilder에서 재귀적으로 빌드됨.
    """
    logger.info("Initializing state data")
    
    # [FIX] 1. Move initialization to top (prevent NameError in S3 Metadata)
    current_time = int(time.time())
    
    # 1. 입력 데이터 추출
    raw_input = event.get('input', event)
    if not isinstance(raw_input, dict):
        raw_input = {}
        
    # [FIX] 변수 명시적 초기화 (UnboundLocalError 및 Missing Field 방지)
    # SFN에서 $.field_name 참조 시 에러를 방지하기 위해 키가 항상 존재해야 함
    partition_map_s3_path = ""
    manifest_s3_path = ""
    state_s3_path = ""
    
    # 2. Config 결정 우선순위 정교화
    workflow_config = raw_input.get('test_workflow_config') or raw_input.get('workflow_config')
    owner_id = raw_input.get('ownerId', "")
    workflow_id = raw_input.get('workflowId', "")
    idempotency_key = raw_input.get('idempotency_key', "")
    quota_reservation_id = raw_input.get('quota_reservation_id', "")
    
    # [Fix] MOCK_MODE 추출 - 시뮬레이터 E2E 테스트용
    mock_mode = raw_input.get('MOCK_MODE', 'false')
    
    # [Robustness Fix] workflow_config가 없으면 DB에서 로드 (test_config 없을 때)
    if not workflow_config and workflow_id and owner_id:
        logger.info(f"workflow_config missing in input, loading from src.DB: {workflow_id}")
        db_data = _load_workflow_config(owner_id, workflow_id)
        if db_data and db_data.get('config'):
            workflow_config = db_data.get('config')
            # DB에서 partition_map도 함께 로드된 경우 사용
            if db_data.get('partition_map'):
                event['_db_partition_map'] = db_data.get('partition_map')
                event['_db_total_segments'] = db_data.get('total_segments')
                event['_db_llm_segments'] = db_data.get('llm_segments_count')
                event['_db_hitp_segments'] = db_data.get('hitp_segments_count')
        else:
            logger.warning(f"Workflow not found in DB: {workflow_id}")

    # workflow_config가 여전히 없으면 빈 딕셔너리로 설정하여 crash 방지
    if not workflow_config:
        workflow_config = {}
        logger.warning("Proceeding with empty workflow_config (risk of later failure)")

    # 3. MOCK_MODE: 강제 파티셔닝 (DB 데이터 없음 대비)
    if raw_input.get('test_workflow_config'):
         if not _HAS_PARTITION:
             logger.warning("MOCK_MODE but partition logic unavailable!")
         else:
             logger.info("MOCK_MODE detected: Forcing runtime partition calculation")
             try:
                 partition_result = partition_workflow_advanced(workflow_config)
                 # Override raw inputs/event cache with fresh calculation
                 raw_input['partition_map'] = partition_result.get('partition_map', [])
                 raw_input['total_segments'] = partition_result.get('total_segments', 0)
                 raw_input['llm_segments'] = partition_result.get('llm_segments', 0)
                 raw_input['hitp_segments'] = partition_result.get('hitp_segments', 0)
             except Exception as e:
                 logger.error(f"MOCK_MODE partitioning failed: {e}")

    # 2. 파티셔닝 (우선순위: 입력 > DB pre-compiled > 런타임 계산)
    partition_map = None
    total_segments = 0
    llm_segments = 0
    hitp_segments = 0
    
    # 2a. 입력에 이미 partition_map이 있다면 사용 (재시도/Child Workflow)
    if raw_input.get('partition_map'):
        logger.info("Using provided partition_map from src.input")
        partition_map = raw_input.get('partition_map')
        total_segments = raw_input.get('total_segments', len(partition_map))
        llm_segments = raw_input.get('llm_segments', 0)
        hitp_segments = raw_input.get('hitp_segments', 0)
    
    # 2a-1. config 로드 시 함께 가져온 partition_map 사용
    if not partition_map and event.get('_db_partition_map'):
        logger.info("Using partition_map from src.DB config load")
        partition_map = event.get('_db_partition_map')
        total_segments = event.get('_db_total_segments', len(partition_map) if partition_map else 0)
        llm_segments = event.get('_db_llm_segments', 0)
        hitp_segments = event.get('_db_hitp_segments', 0)
    
    # 2b. DB에서 pre-compiled partition_map 로드 (1순위 최적화)
    if not partition_map:
        precompiled = _load_precompiled_partition(owner_id, workflow_id)
        if precompiled:
            partition_map = precompiled.get('partition_map')
            total_segments = precompiled.get('total_segments', len(partition_map) if partition_map else 0)
            llm_segments = precompiled.get('llm_segments_count', 0)
            hitp_segments = precompiled.get('hitp_segments_count', 0)
    
    # 2c. Fallback: 런타임 계산 (기존 로직, 하위 호환성)
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
        except Exception as e:
            logger.error(f"Partitioning failed: {e}")
            raise RuntimeError(f"Failed to partition workflow: {str(e)}")

    # 🎯 [단일화 전략] 실행할 세그먼트 리스트(segment_manifest) 생성
    segment_manifest = []
    for idx, segment in enumerate(partition_map):
        segment_manifest.append({
            "segment_id": idx,
            "segment_config": segment,
            "execution_order": idx,
            "dependencies": segment.get("dependencies", []),
            "type": segment.get("type", "normal")
        })
    
    # 🚨 [Critical Fix] 분산 모드 감지 및 대용량 데이터 S3 오프로딩
    is_distributed_mode = total_segments > 300  # 분산 모드 임계값
    partition_map_for_return = partition_map  # 기본값: 전체 반환
    
    # AWS Clients
    s3_client = None
    bucket = os.environ.get('WORKFLOW_STATE_BUCKET')
    
    # S3 업로드 필요 시에만 boto3 클라이언트 초기화
    if bucket and owner_id and workflow_id:
        if len(segment_manifest) > 50 or is_distributed_mode:
            import boto3
            s3_client = boto3.client('s3')

    # 1. Manifest Offloading (50개 초과 시)
    if len(segment_manifest) > 50 and s3_client:
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
            # 실패 시 manifest_s3_path는 초기값 "" 유지됨

    # 2. Partition Map Offloading (분산 모드)
    # 🚨 [Critical Fix] 분산 모드에서 partition_map 크기 최적화
    if is_distributed_mode:
        partition_map_json = json.dumps(partition_map, ensure_ascii=False)
        partition_map_size_kb = len(partition_map_json.encode('utf-8')) / 1024
        
        logger.info(f"Distributed mode detected: {total_segments} segments, partition_map size: {partition_map_size_kb:.1f}KB")
        
        # 100KB 이상의 partition_map은 S3로 오프로딩
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
                
                # 🚨 [Critical] 분산 모드에서는 partition_map 제외
                partition_map_for_return = None
                
            except Exception as e:
                logger.warning(f"Failed to offload partition_map to S3: {e}")
                # S3 실패 시 경고하고 인라인으로 폴백
                logger.warning(f"Proceeding with inline partition_map ({partition_map_size_kb:.1f}KB)")
        else:
            logger.info(f"partition_map size acceptable for inline return: {partition_map_size_kb:.1f}KB")

    # 3. 초기 상태 구성
    current_time = int(time.time())
    
    # S3 경로가 있으면 current_state를 null로, 아니면 initial_state 사용
    initial_state_s3_path = workflow_config.get('initial_state_s3_path')
    if initial_state_s3_path:
        # Large payload: S3에서 로드 (segment_runner에서 처리)
        current_state = None
        state_s3_path = initial_state_s3_path
        input_value = initial_state_s3_path
    else:
        # Inline state
        current_state = workflow_config.get('initial_state', {})
        # state_s3_path = "" # 위에서 이미 초기화됨
        input_value = current_state
    
    # 4. 동적 동시성 계산 (Map 상태 최적화)
    max_concurrency = _calculate_dynamic_concurrency(
        total_segments=total_segments,
        llm_segments=llm_segments,
        hitp_segments=hitp_segments,
        partition_map=partition_map,
        owner_id=owner_id
    )
    
    logger.info(f"[FIXED_ROBUST] Returning state data. partition_map_s3_path: '{partition_map_s3_path}'")

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
        
        # 🚨 [Critical Fix] 분산 모드에서 partition_map 조건부 반환
        "total_segments": total_segments,
        "partition_map": partition_map_for_return,
        "partition_map_s3_path": partition_map_s3_path,  # 항상 존재 (최소 "")
        
        # manifest_s3_path가 있으면 segment_manifest는 None으로 (페이로드 최적화)
        "segment_manifest": segment_manifest if manifest_s3_path == "" else None,
        "segment_manifest_s3_path": manifest_s3_path,    # 항상 존재 (최소 "")
        
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
        
        # [Fix] MOCK_MODE 전달 - 시뮬레이터 E2E 테스트용 (HITP 자동 resume 등)
        "MOCK_MODE": mock_mode,
        
        # 🚨 [Critical Fix] Loop Control Parameters
        "max_loop_iterations": workflow_config.get("max_loop_iterations", 100),
        "max_branch_iterations": workflow_config.get("max_branch_iterations", 100)
    }

