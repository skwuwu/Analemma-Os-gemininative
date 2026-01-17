"""
State Data Manager Lambda Function
Lambda function responsible for payload size management and S3 offloading
"""

import json
import boto3
import gzip
import base64
from typing import Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import os
import sys

# Add the common directory to the path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from src.common.logging_utils import get_logger
from src.common.error_handlers import handle_lambda_error

logger = get_logger(__name__)

# AWS clients
s3_client = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')

# Environment variables
S3_BUCKET = os.environ.get('STATE_STORAGE_BUCKET')
MAX_PAYLOAD_SIZE_KB = int(os.environ.get('MAX_PAYLOAD_SIZE_KB', '200'))


def calculate_payload_size(data: Dict[str, Any]) -> int:
    """Calculate payload size in KB"""
    try:
        json_str = json.dumps(data, separators=(',', ':'))
        size_bytes = len(json_str.encode('utf-8'))
        size_kb = size_bytes / 1024
        return int(size_kb)
    except Exception as e:
        logger.warning(f"Failed to calculate payload size: {e}")
        return 0


def compress_data(data: Any) -> str:
    """Compress data using gzip and return base64 encoded string"""
    try:
        json_str = json.dumps(data, separators=(',', ':'))
        compressed = gzip.compress(json_str.encode('utf-8'))
        return base64.b64encode(compressed).decode('utf-8')
    except Exception as e:
        logger.error(f"Failed to compress data: {e}")
        raise


def decompress_data(compressed_str: str) -> Any:
    """Decompress base64 encoded gzip data"""
    try:
        compressed = base64.b64decode(compressed_str.encode('utf-8'))
        decompressed = gzip.decompress(compressed)
        return json.loads(decompressed.decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to decompress data: {e}")
        raise


def store_to_s3(data: Any, key: str) -> str:
    """Store data to S3 and return the S3 path"""
    try:
        json_str = json.dumps(data, separators=(',', ':'))
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json_str,
            ContentType='application/json',
            ServerSideEncryption='AES256'
        )
        
        s3_path = f"s3://{S3_BUCKET}/{key}"
        logger.info(f"Stored data to S3: {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"Failed to store data to S3: {e}")
        raise


def generate_s3_key(idempotency_key: str, data_type: str) -> str:
    """Generate S3 key for storing data"""
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    return f"workflow-state/{idempotency_key}/{data_type}_{timestamp}.json"


def optimize_state_history(state_history: list, idempotency_key: str, max_entries: int = 50) -> Tuple[list, Optional[str]]:
    """Optimize state history by keeping recent entries and storing old ones to S3"""
    if not state_history or len(state_history) <= max_entries:
        return state_history, None
    
    # Keep recent entries
    recent_history = state_history[-max_entries:]
    
    # Store old entries to S3 if there are many
    old_history = state_history[:-max_entries]
    if len(old_history) > 10:  # Only store if significant amount
        try:
            # idempotency_key used for S3 path generation
            s3_key = generate_s3_key(idempotency_key, "history_archive")
            s3_path = store_to_s3(old_history, s3_key)

            
            # Add reference to archived history
            archive_ref = {
                "type": "history_archive",
                "s3_path": s3_path,
                "entry_count": len(old_history),
                "archived_at": datetime.now(timezone.utc).isoformat()
            }
            recent_history.insert(0, archive_ref)
            
            return recent_history, s3_path
        except Exception as e:
            logger.warning(f"Failed to archive old history: {e}")
            return state_history, None
    
    return recent_history, None


def optimize_current_state(current_state: Dict[str, Any], idempotency_key: str) -> Tuple[Dict[str, Any], bool]:
    """Optimize current state by moving large fields to S3"""
    if not current_state:
        return current_state, False
    
    optimized_state = current_state.copy()
    s3_offloaded = False
    
    # Fields that can be large and moved to S3
    large_fields = ['context', 'llm_responses', 'generated_content', 'analysis_results']
    
    for field in large_fields:
        if field in optimized_state:
            field_data = optimized_state[field]
            field_size = calculate_payload_size({field: field_data})
            
            # Move to S3 if field is larger than 50KB
            if field_size > 50:
                try:
                    s3_key = generate_s3_key(idempotency_key, f"state_{field}")
                    s3_path = store_to_s3(field_data, s3_key)
                    
                    # Replace with S3 reference
                    optimized_state[field] = {
                        "type": "s3_reference",
                        "s3_path": s3_path,
                        "size_kb": field_size,
                        "stored_at": datetime.now(timezone.utc).isoformat()
                    }
                    
                    s3_offloaded = True
                    logger.info(f"Moved {field} ({field_size}KB) to S3: {s3_path}")
                    
                except Exception as e:
                    logger.warning(f"Failed to move {field} to S3: {e}")
    
    return optimized_state, s3_offloaded


def update_and_compress_state_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """Main function to update and compress state data"""
    state_data = event.get('state_data', {})
    execution_result = event.get('execution_result', {})
    max_payload_size_kb = event.get('max_payload_size_kb', MAX_PAYLOAD_SIZE_KB)
    
    # Extract key information
    idempotency_key = state_data.get('idempotency_key', 'unknown')
    
    # Update state data with execution result
    updated_state_data = {
        'workflow_config': state_data.get('workflow_config'),
        'current_state': execution_result.get('final_state', state_data.get('current_state')),
        'state_s3_path': execution_result.get('final_state_s3_path', state_data.get('state_s3_path')),
        'state_history': execution_result.get('new_history_logs', state_data.get('state_history', [])),
        'ownerId': state_data.get('ownerId'),
        'workflowId': state_data.get('workflowId'),
        'segment_to_run': state_data.get('segment_to_run'),
        'idempotency_key': idempotency_key,
        'quota_reservation_id': state_data.get('quota_reservation_id'),
        'total_segments': state_data.get('total_segments'),
        'partition_map': state_data.get('partition_map'),
        # [FIX] Add missing fields for Step Functions loop and Distributed Map
        'partition_map_s3_path': state_data.get('partition_map_s3_path'),
        # 🚨 [Critical] Distributed Map Manifest Fields
        'segment_manifest': state_data.get('segment_manifest'),
        'segment_manifest_s3_path': state_data.get('segment_manifest_s3_path'),
        
        'distributed_mode': state_data.get('distributed_mode'),
        'max_concurrency': state_data.get('max_concurrency'),
        
        # 🚨 [Critical] Statistics Fields for Scenario J
        'llm_segments': state_data.get('llm_segments'),
        'hitp_segments': state_data.get('hitp_segments'),
        
        'state_durations': state_data.get('state_durations'),
        'last_update_time': state_data.get('last_update_time'),
        'start_time': state_data.get('start_time'),
        'max_loop_iterations': state_data.get('max_loop_iterations', 100),
        'max_branch_iterations': state_data.get('max_branch_iterations', 100),
        'loop_counter': state_data.get('loop_counter', 0)
    }

    
    # Calculate initial payload size
    initial_size_kb = calculate_payload_size(updated_state_data)
    logger.info(f"Initial payload size: {initial_size_kb}KB")
    
    compression_applied = False
    s3_offloaded = False
    
    # If payload is too large, apply optimizations
    if initial_size_kb > max_payload_size_kb:
        logger.info(f"Payload size ({initial_size_kb}KB) exceeds limit ({max_payload_size_kb}KB), applying optimizations")
        
        # 1. Optimize state history
        if updated_state_data.get('state_history'):
            optimized_history, history_s3_path = optimize_state_history(
                updated_state_data['state_history'], 
                idempotency_key=idempotency_key,
                max_entries=30
            )
            updated_state_data['state_history'] = optimized_history
            if history_s3_path:
                compression_applied = True
        
        # 2. Optimize current state
        if updated_state_data.get('current_state'):
            optimized_state, state_s3_offloaded = optimize_current_state(
                updated_state_data['current_state'], 
                idempotency_key
            )
            updated_state_data['current_state'] = optimized_state
            if state_s3_offloaded:
                s3_offloaded = True
        
        # 3. If still too large, compress workflow_config
        final_size_kb = calculate_payload_size(updated_state_data)
        if final_size_kb > max_payload_size_kb and updated_state_data.get('workflow_config'):
            try:
                compressed_config = compress_data(updated_state_data['workflow_config'])
                updated_state_data['workflow_config'] = {
                    "type": "compressed",
                    "data": compressed_config,
                    "compressed_at": datetime.now(timezone.utc).isoformat()
                }
                compression_applied = True
                logger.info("Applied compression to workflow_config")
            except Exception as e:
                logger.warning(f"Failed to compress workflow_config: {e}")
    
    # Final size calculation
    final_size_kb = calculate_payload_size(updated_state_data)
    
    # Add metadata
    updated_state_data['payload_size_kb'] = final_size_kb
    updated_state_data['compression_applied'] = compression_applied
    updated_state_data['s3_offloaded'] = s3_offloaded
    updated_state_data['last_optimization'] = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"Final payload size: {final_size_kb}KB (compression: {compression_applied}, s3_offload: {s3_offloaded})")
    
    # Send CloudWatch metrics
    _send_cloudwatch_metrics(
        initial_size_kb=initial_size_kb,
        final_size_kb=final_size_kb,
        compression_applied=compression_applied,
        s3_offloaded=s3_offloaded,
        idempotency_key=idempotency_key
    )
    
    return updated_state_data


def _send_cloudwatch_metrics(
    initial_size_kb: int,
    final_size_kb: int,
    compression_applied: bool,
    s3_offloaded: bool,
    idempotency_key: str
) -> None:
    """CloudWatch 메트릭 발송"""
    try:
        metric_data = [
            {
                'MetricName': 'PayloadSizeKB',
                'Value': final_size_kb,
                'Unit': 'Kilobytes',
                'Dimensions': [
                    {
                        'Name': 'OptimizationType',
                        'Value': 'Final'
                    }
                ]
            },
            {
                'MetricName': 'PayloadSizeKB',
                'Value': initial_size_kb,
                'Unit': 'Kilobytes',
                'Dimensions': [
                    {
                        'Name': 'OptimizationType',
                        'Value': 'Initial'
                    }
                ]
            },
            {
                'MetricName': 'PayloadOptimization',
                'Value': 1 if compression_applied or s3_offloaded else 0,
                'Unit': 'Count',
                'Dimensions': [
                    {
                        'Name': 'OptimizationType',
                        'Value': 'Applied'
                    }
                ]
            }
        ]
        
        if initial_size_kb > 0:
            compression_ratio = (initial_size_kb - final_size_kb) / initial_size_kb * 100
            metric_data.append({
                'MetricName': 'CompressionRatio',
                'Value': compression_ratio,
                'Unit': 'Percent'
            })
        
        cloudwatch_client.put_metric_data(
            Namespace='Workflow/StateDataManager',
            MetricData=metric_data
        )
        
        logger.info(f"Sent CloudWatch metrics for {idempotency_key}")
        
    except Exception as e:
        logger.warning(f"Failed to send CloudWatch metrics: {e}")


@handle_lambda_error
def lambda_handler(event, context):
    """Lambda handler for state data management"""
    logger.info(f"Processing state data management request: {event.get('action')}")
    
    action = event.get('action')
    
    if action == 'update_and_compress':
        result = update_and_compress_state_data(event)
        return result
    
    elif action == 'decompress':
        # Handle decompression requests
        compressed_data = event.get('compressed_data')
        if not compressed_data:
            raise ValueError("compressed_data is required for decompress action")
        
        decompressed = decompress_data(compressed_data)
        return decompressed
    
    else:
        raise ValueError(f"Unknown action: {action}")