
"""
Distributed Map Worker Handler - "Tiny Handler" Refactored
Delegates logic to:
- PartitionCacheService (Infra: S3/Cache)
- DistributedChunkService (Domain: Loop/Validation)
- SegmentRunnerService (Execution)
"""

import json
import logging
import os
import time
from typing import Dict, Any

import boto3

# Service Improts
from src.services.infrastructure.partition_cache_service import PartitionCacheService
from src.services.distributed.distributed_chunk_service import DistributedChunkService

# Legacy/Existing Handler Import (for segment execution)
# Ideally we should use SegmentRunnerService directly, but the existing codebase
# wraps it in segment_runner_handler.lambda_handler. 
# To minimize breakage, we reuse the existing handler as the "runner callable".
try:
    from src.handlers.core.segment_runner_handler import lambda_handler as segment_runner_handler
except ImportError:
    # Safe fallback for import errors during structural transitions
    import logging
    logging.getLogger(__name__).warning("Could not import segment_runner_handler directly. Ensure package structure.")
    segment_runner_handler = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialization (Cold Start)
_cache_service = PartitionCacheService()
_chunk_service = DistributedChunkService(_cache_service)

def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Refactored Entry Point for Distributed Chunk Processing.
    """
    logger.info(f"=== ProcessSegmentChunk (Refactored) Started ===")
    
    try:
        # 1. Extract Context
        chunk_data = event.get('chunk_data', {})
        # Flatten structure if s3 path is passed inside event root but not chunk_data
        if 'partition_map_s3_path' in event and 'partition_map_s3_path' not in chunk_data:
            chunk_data['partition_map_s3_path'] = event['partition_map_s3_path']
        
        # 2. Resolve Previous State (Infra/Support Logic - could be moved to service later)
        previous_state = event.get('previous_state', {})
        state_bucket = event.get('state_bucket') or os.environ.get('WORKFLOW_STATE_BUCKET')
        current_state = _resolve_previous_state(previous_state, state_bucket)
        
        # 3. Prepare Service Context
        service_context = {
            'current_state': current_state,
            'owner_id': event.get('owner_id'),
            'workflow_id': event.get('workflowId'),
            'execution_id': event.get('execution_id'),
            'workflow_config': event.get('workflow_config', {}),
            'max_concurrency': event.get('max_concurrency')
        }
        
        # 4. Delegate to Domain Service
        response = _chunk_service.process_chunk(
            chunk_data=chunk_data,
            context=service_context,
            segment_runner_callable=_run_segment_wrapper, # Wrapper to adapt signature
            token_storage_callable=_store_distributed_task_token
        )
        
        logger.info(f"Chunk {response.get('chunk_id')} finished: {response.get('status')}")
        return response

    except Exception as e:
        logger.exception("Top-level handler failure")
        # [Fix] Must include all fields expected by ASL ResultSelector to prevent JSONPath errors
        chunk_id = event.get('chunk_data', {}).get('chunk_id', 'unknown')
        return {
            "chunk_id": chunk_id,
            "status": "FAILED",
            "error": str(e),
            "processed_segments": 0,
            # Fields required by ASL - provided as None/empty values
            "final_state": event.get('previous_state', {}),  # Preserve last known state
            "paused_segment_id": None
        }

def _run_segment_wrapper(segment_event):
    """Adapter to match expected signature of segment_runner_handler"""
    # Context is optional in some mocks, pass None or mock if needed
    fake_context = type('obj', (object,), {'aws_request_id': 'distributed-worker'})
    return segment_runner_handler(segment_event, fake_context)

# -------------------------------------------------------------------------
# Support Functions (Legacy logic that might move to shared Utils later)
# -------------------------------------------------------------------------

def _resolve_previous_state(previous_state: Any, state_bucket: str) -> Dict[str, Any]:
    """Helper to resolve state from Inline or S3"""
    # Simply reused logic (condensed)
    if not previous_state: return {}
    if not isinstance(previous_state, dict): return {}
    
    s3_uri = previous_state.get('previous_state_s3_uri')
    if s3_uri and previous_state.get('payload_type') == 's3_reference':
        # Use Cache Service to load? Or keep ad-hoc?
        # State files are dynamic, not cacheable like partitions.
        return _load_state_from_s3_uri(s3_uri)
    
    if 'previous_state' in previous_state:
        return previous_state['previous_state']
        
    # Default: direct dict minus metadata
    clean = {k:v for k,v in previous_state.items() if not k.startswith('__') and k != 'payload_type'}
    return clean

def _load_state_from_s3_uri(s3_uri: str) -> Dict[str, Any]:
    try:
        if not s3_uri.startswith('s3://'): return {}
        parts = s3_uri[5:].split('/', 1)
        s3 = boto3.client('s3')
        resp = s3.get_object(Bucket=parts[0], Key=parts[1])
        return json.loads(resp['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to load state {s3_uri}: {e}")
        return {}

def _store_distributed_task_token(
    chunk_id: str,
    segment_id: int,
    task_token: str,
    owner_id: str,
    workflow_id: str,
    execution_id: str
) -> None:
    """Store token in DynamoDB"""
    try:
        dynamodb = boto3.resource('dynamodb')
        # 🚨 [Critical Fix] 환경변수명 통일: TASK_TOKENS_TABLE_NAME 우선 (DynamoDBConfig와 일치)
        token_table = dynamodb.Table(os.environ.get('TASK_TOKENS_TABLE_NAME', os.environ.get('TASK_TOKEN_TABLE', 'TaskTokensTableV3')))
        token_table.put_item(Item={
            'conversation_id': f"{chunk_id}_segment_{segment_id}",
            'task_token': task_token,
            'chunk_id': chunk_id,
            'segment_id': segment_id,
            'owner_id': owner_id,
            'workflow_id': workflow_id,
            'distributed_execution': True,
            'parent_execution_id': execution_id,
            'created_at': int(time.time()),
            'ttl': int(time.time()) + 86400,
            'status': 'WAITING_FOR_CALLBACK'
        })
    except Exception as e:
        logger.error(f"Failed to store token: {e}")