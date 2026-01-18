import logging
import os
import time
from typing import Dict, Any, Optional

# [v2.1] 중앙 집중식 재시도 유틸리티
try:
    from src.common.retry_utils import retry_call, retry_stepfunctions, retry_s3
    RETRY_UTILS_AVAILABLE = True
except ImportError:
    RETRY_UTILS_AVAILABLE = False

# Services
from src.services.state.state_manager import StateManager
from src.services.recovery.self_healing_service import SelfHealingService
# Legacy Imports (for now, until further refactoring)
from src.services.workflow.repository import WorkflowRepository
# Using generic imports from main handler file as source of truth
from src.handlers.core.main import run_workflow, partition_workflow as _partition_workflow_dynamically, _build_segment_config
from src.common.statebag import normalize_inplace


logger = logging.getLogger(__name__)

class SegmentRunnerService:
    def __init__(self):
        self.state_manager = StateManager()
        self.healer = SelfHealingService()
        self.repo = WorkflowRepository()
        self.threshold = int(os.environ.get("STATE_SIZE_THRESHOLD", 256000))

    def _trigger_child_workflow(self, event: Dict[str, Any], branch_config: Dict[str, Any], auth_user_id: str, quota_id: str) -> Optional[Dict[str, Any]]:
        """
        Triggers a Child Step Function (Standard Orchestrator) for complex branches.
        "Fire and Forget" pattern to avoid Lambda timeouts.
        """
        try:
            import boto3
            import json
            import time
            
            sfn_client = boto3.client('stepfunctions')
            
            # 1. Resolve Orchestrator ARN
            orchestrator_arn = os.environ.get('WORKFLOW_ORCHESTRATOR_ARN')
            if not orchestrator_arn:
                logger.error("WORKFLOW_ORCHESTRATOR_ARN not set. Cannot trigger child workflow.")
                return None

            # 2. Construct Payload (Full 23 fields injection)
            payload = event.copy()
            payload['workflow_config'] = branch_config
            
            parent_workflow_id = payload.get('workflowId') or payload.get('workflow_id', 'unknown')
            parent_idempotency_key = payload.get('idempotency_key', str(time.time()))
            
            # 3. Generate Child Idempotency Key
            branch_id = branch_config.get('id') or f"branch_{int(time.time()*1000)}"
            child_idempotency_key = f"{parent_idempotency_key}_{branch_id}"[:80]
            
            payload['idempotency_key'] = child_idempotency_key
            payload['parent_workflow_id'] = parent_workflow_id
            
            # 4. Start Execution with retry
            safe_exec_name = "".join(c for c in child_idempotency_key if c.isalnum() or c in "-_")
            
            logger.info(f"Triggering Child SFN: {safe_exec_name}")
            
            # [v2.1] Step Functions start_execution에 재시도 적용
            def _start_child_execution():
                return sfn_client.start_execution(
                    stateMachineArn=orchestrator_arn,
                    name=safe_exec_name,
                    input=json.dumps(payload)
                )
            
            if RETRY_UTILS_AVAILABLE:
                response = retry_call(
                    _start_child_execution,
                    max_retries=2,
                    base_delay=0.5,
                    max_delay=5.0
                )
            else:
                response = _start_child_execution()
            
            return {
                "status": "ASYNC_CHILD_WORKFLOW_STARTED",
                "executionArn": response['executionArn'],
                "startDate": response['startDate'].isoformat(),
                "executionName": safe_exec_name
            }
            
        except Exception as e:
            logger.error(f"Failed to trigger child workflow: {e}")
            return None

    def execute_segment(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution logic for a workflow segment.
        """
        # [Fix] 이벤트에서 MOCK_MODE를 읽어서 환경 변수로 주입
        # 이렇게 하면 모든 하위 함수들(invoke_bedrock_model 등)이 MOCK_MODE를 인식함
        event_mock_mode = event.get('MOCK_MODE', '').lower()
        if event_mock_mode in ('true', '1', 'yes', 'on'):
            os.environ['MOCK_MODE'] = 'true'
            logger.info("🧪 MOCK_MODE enabled from event payload")
        
        # 0. Check for Branch Offloading
        branch_config = event.get('branch_config')
        if branch_config:
            force_child = os.environ.get('FORCE_CHILD_WORKFLOW', 'false').lower() == 'true'
            node_count = len(branch_config.get('nodes', [])) if isinstance(branch_config.get('nodes'), list) else 0
            has_hitp = branch_config.get('hitp', False) or any(n.get('hitp') for n in branch_config.get('nodes', []))
            
            should_offload = force_child or node_count > 20 or has_hitp
            
            if should_offload:
                auth_user_id = event.get('ownerId') or event.get('owner_id')
                quota_id = event.get('quota_reservation_id')
                
                child_result = self._trigger_child_workflow(event, branch_config, auth_user_id, quota_id)
                if child_result:
                    return child_result

        # 1. State Bag Normalization
        normalize_inplace(event, remove_state_data=True)
        
        # 2. Extract Context
        auth_user_id = event.get('ownerId') or event.get('owner_id') or event.get('user_id')
        workflow_id = event.get('workflowId') or event.get('workflow_id')
        segment_id = event.get('segment_to_run', 0)
        s3_bucket = os.environ.get("S3_BUCKET") or os.environ.get("SKELETON_S3_BUCKET")
        
        # 3. Load State (Inline or S3)
        state_s3_path = event.get('state_s3_path')
        initial_state = event.get('state', {})
        
        if state_s3_path:
            initial_state = self.state_manager.download_state_from_s3(state_s3_path)
            
        # 4. Resolve Segment Config
        # [Critical Fix] Support both test_workflow_config (E2E tests) and workflow_config
        workflow_config = event.get('test_workflow_config') or event.get('workflow_config')
        partition_map = event.get('partition_map')
        partition_map_s3_path = event.get('partition_map_s3_path')
        
        # [Critical Fix] Support S3 Offloaded Partition Map with retry
        if not partition_map and partition_map_s3_path:
            try:
                import boto3
                import json
                s3 = boto3.client('s3')
                bucket_name = partition_map_s3_path.replace("s3://", "").split("/")[0]
                key_name = "/".join(partition_map_s3_path.replace("s3://", "").split("/")[1:])
                
                logger.info(f"Loading partition_map from S3: {partition_map_s3_path}")
                
                # [v2.1] S3 get_object에 재시도 적용
                def _get_partition_map():
                    obj = s3.get_object(Bucket=bucket_name, Key=key_name)
                    return json.loads(obj['Body'].read().decode('utf-8'))
                
                if RETRY_UTILS_AVAILABLE:
                    partition_map = retry_call(
                        _get_partition_map,
                        max_retries=2,
                        base_delay=0.5,
                        max_delay=5.0
                    )
                else:
                    partition_map = _get_partition_map()
                    
            except Exception as e:
                logger.error(f"Failed to load partition_map from S3 after retries: {e}")
                # Fallback to dynamic partitioning (handled in _resolve_segment_config)
        
        segment_config = self._resolve_segment_config(workflow_config, partition_map, segment_id)
        
        # [Critical Fix] parallel_group 타입 세그먼트는 바로 PARALLEL_GROUP status 반환
        # ASL의 ProcessParallelSegments가 branches를 받아서 Map으로 병렬 실행함
        segment_type = segment_config.get('type') if isinstance(segment_config, dict) else None
        if segment_type == 'parallel_group':
            branches = segment_config.get('branches', [])
            logger.info(f"🔀 Parallel group detected with {len(branches)} branches")
            return {
                "status": "PARALLEL_GROUP",
                "final_state": initial_state,
                "final_state_s3_path": None,
                "next_segment_to_run": segment_id + 1,  # aggregator로 이동
                "new_history_logs": [],
                "error_info": None,
                "branches": branches,
                "segment_type": "parallel_group"
            }
        
        # 5. Apply Self-Healing (Prompt Injection / Refinement)
        self.healer.apply_healing(segment_config, event.get("_self_healing_metadata"))
        
        # 6. Check User Quota / Secret Resolution (Repo access)
        # Note: In a full refactor, this should move to a UserService or AuthMiddleware
        # For now, we keep it simple.
        if auth_user_id:
            try:
                self.repo.get_user(auth_user_id) # Just validating access/existence
            except Exception as e:
                logger.warning("User check failed, but proceeding if possible: %s", e)

        # 7. Execute Workflow Segment (Legacy function call)
        # We are wrapping the old `run_workflow` runner logic here.
        # Ideally, run_workflow logic moves into this class method over time.
        user_api_keys = {} # Should be resolved from Secrets Manager or Repo
        
        # We need to pass secrets if encryption is enabled, but for this pilot we assume basic flow
        start_time = time.time()
        
        # Run logic
        # Note: We are reusing the existing 'run_workflow' function which performs the actual node traversal.
        # In a deep refactor, 'run_workflow' would be decomposed.
        result_state = run_workflow(
            config_json=segment_config,
            initial_state=initial_state,
            ddb_table_name=os.environ.get("JOB_TABLE"),
            user_api_keys=user_api_keys,
            run_config={"user_id": auth_user_id}
        )
        
        # 8. Handle Output State Storage
        final_state, output_s3_path = self.state_manager.handle_state_storage(
            state=result_state,
            auth_user_id=auth_user_id,
            workflow_id=workflow_id,
            segment_id=segment_id,
            bucket=s3_bucket,
            threshold=self.threshold
        )
        
        # Extract history logs from result_state if available
        new_history_logs = result_state.get('__new_history_logs', []) if isinstance(result_state, dict) else []
        
        # [Critical Fix] 워크플로우 완료 여부 결정
        # 1. test_workflow_config가 주입된 경우 (E2E 테스트): 한 번에 전체 실행 후 완료
        # 2. partition_map이 없는 경우: 전체 워크플로우를 한 번에 실행했으므로 완료
        # 3. partition_map이 있는 경우: 다음 세그먼트가 있는지 확인
        is_e2e_test = event.get('test_workflow_config') is not None
        has_partition_map = partition_map is not None and len(partition_map) > 0
        
        if is_e2e_test or not has_partition_map:
            # E2E 테스트 또는 파티션 없는 단일 실행: 워크플로우 완료
            return {
                "status": "COMPLETE",  # ASL이 기대하는 상태값
                "final_state": final_state,
                "final_state_s3_path": output_s3_path,
                "next_segment_to_run": None,  # 다음 세그먼트 없음
                "new_history_logs": new_history_logs,
                "error_info": None,
                "branches": None,
                "segment_type": "final",
                "state_s3_path": output_s3_path,
                "segment_id": segment_id
            }
        
        # 파티션 맵이 있는 경우: 다음 세그먼트 존재 여부 확인
        total_segments = event.get('total_segments', len(partition_map))
        next_segment = segment_id + 1
        
        if next_segment >= total_segments:
            # 마지막 세그먼트 완료
            return {
                "status": "COMPLETE",
                "final_state": final_state,
                "final_state_s3_path": output_s3_path,
                "next_segment_to_run": None,
                "new_history_logs": new_history_logs,
                "error_info": None,
                "branches": None,
                "segment_type": "final",
                "state_s3_path": output_s3_path,
                "segment_id": segment_id
            }
        
        # 아직 실행할 세그먼트가 남아있음
        return {
            "status": "SUCCEEDED",
            "final_state": final_state,
            "final_state_s3_path": output_s3_path,
            "next_segment_to_run": next_segment,
            "new_history_logs": new_history_logs,
            "error_info": None,
            "branches": None,
            "segment_type": "normal",
            "state_s3_path": output_s3_path,
            "segment_id": segment_id
        }

    def _resolve_segment_config(self, workflow_config, partition_map, segment_id):
        """
        Identical logic to original handler for partitioning.
        """
        if workflow_config:
             # Basic full workflow or pre-chunked
             # If we are strictly running a segment, we might need to simulate partitioning if map is missing
             # For simplicity, we assume workflow_config IS the segment config if partition_map is missing
             # OR we call the dynamic partitioner.
             if not partition_map:
                 # Fallback to dynamic partitioning logic
                 parts = _partition_workflow_dynamically(workflow_config) # arbitrary chunks removed
                 if 0 <= segment_id < len(parts):
                     return parts[segment_id]
                 return workflow_config # Fallback

        # 🚨 [Critical Fix] partition_map이 list 또는 dict일 수 있음
        if partition_map:
            if isinstance(partition_map, list):
                # list인 경우: 인덱스로 접근
                if 0 <= segment_id < len(partition_map):
                    return partition_map[segment_id]
            elif isinstance(partition_map, dict):
                # dict인 경우: 문자열 키로 접근
                if str(segment_id) in partition_map:
                    return partition_map[str(segment_id)]
            
        # Simplified fallback for readability in pilot
        return workflow_config
