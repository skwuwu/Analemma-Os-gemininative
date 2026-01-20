"""
StatePersistenceService - Unified State Load/Save Service

Extracted from `load_latest_state.py` and `save_latest_state.py`.
Implements Single Source of Truth (SSOT) pattern with S3 as primary store.

Key Features:
- S3 is the Source of Truth for state data
- DynamoDB is used as a read-through cache and fallback pointer
- Dual-write with rollback on failure
- Consistency checks for distributed execution
"""

import json
import logging
import os
import time
import random
from typing import Dict, Any, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class StatePersistenceService:
    """
    Unified service for state persistence in distributed workflows.
    
    SSOT Policy:
    - S3 is the authoritative source of state data
    - DynamoDB stores metadata/pointers for fast lookup
    - On conflict: S3 data takes precedence
    
    Rollback Policy:
    - If S3 write succeeds but DynamoDB fails, S3 write is rolled back
    - If S3 write fails, DynamoDB is never updated
    """
    
    SIZE_LIMIT_BYTES = 200 * 1024  # 200KB (Step Functions limit is 256KB)
    DEFAULT_TTL_SECONDS = 86400 * 7  # 7 days

    def __init__(
        self, 
        state_bucket: Optional[str] = None,
        workflows_table: Optional[str] = None
    ):
        self._s3_client = None
        self._dynamodb = None
        self._state_bucket = state_bucket or os.environ.get('WORKFLOW_STATE_BUCKET')
        # 🚨 [Critical Fix] Match default values with template.yaml
        self._workflows_table = workflows_table or os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')

    @property
    def s3_client(self):
        """Lazy S3 client initialization."""
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')
        return self._s3_client

    @property
    def dynamodb(self):
        """Lazy DynamoDB resource initialization."""
        if self._dynamodb is None:
            self._dynamodb = boto3.resource('dynamodb')
        return self._dynamodb

    def set_bucket(self, bucket_name: str) -> None:
        """
        [v2.3] Dynamically set state bucket.
        
        Instead of directly accessing private members from handlers
        use this method to follow encapsulation principles.
        
        Args:
            bucket_name: S3 bucket name
        """
        if bucket_name:
            self._state_bucket = bucket_name

    # =========================================================================
    # LOAD STATE (Read Path)
    # =========================================================================

    def load_state(
        self,
        execution_id: str,
        owner_id: str,
        workflow_id: str,
        chunk_index: int = 0,
        chunk_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Load the latest state for a distributed workflow chunk.
        
        Read Order (SSOT):
        1. S3 (Primary) → Return if found and consistent
        2. DynamoDB (Fallback) → Use pointer to load from S3
        3. Return empty state if not found
        
        Args:
            execution_id: Workflow execution ID
            owner_id: Owner user ID
            workflow_id: Workflow ID
            chunk_index: Current chunk index (0 = first chunk, no state needed)
            chunk_data: Optional chunk metadata for consistency verification
            
        Returns:
            State response with `previous_state` or `previous_state_s3_uri`
        """
        # First chunk has no previous state
        if chunk_index == 0:
            logger.info(f"First chunk, no previous state needed")
            return self._build_load_response(state_loaded=False, reason="first_chunk")

        if not self._state_bucket:
            logger.warning("No state bucket configured")
            return self._build_load_response(state_loaded=False, reason="no_bucket_configured")

        # S3 Path for latest state
        s3_key = f"distributed-states/{owner_id}/{workflow_id}/{execution_id}/latest_state.json"
        
        try:
            # Attempt S3 load
            result = self._load_from_s3(s3_key, chunk_index, chunk_data)
            if result["state_loaded"]:
                return result
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') != 'NoSuchKey':
                raise

        # Fallback to DynamoDB pointer
        logger.info("S3 state not found, falling back to DynamoDB pointer")
        return self._fallback_to_dynamodb(execution_id, owner_id, workflow_id)

    def _load_from_s3(
        self, 
        s3_key: str, 
        chunk_index: int, 
        chunk_data: Optional[Dict] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """Load state from S3 with consistency verification."""
        response = self.s3_client.get_object(Bucket=self._state_bucket, Key=s3_key)
        state_data = json.loads(response['Body'].read().decode('utf-8'))
        metadata = response.get('Metadata', {})
        
        latest_segment_id = self._parse_segment_id(metadata.get('latest_segment_id'))
        
        # Consistency check
        if not self._verify_consistency(chunk_index, latest_segment_id, metadata, state_data, chunk_data):
            # Retry with backoff
            return self._retry_load_with_backoff(s3_key, chunk_index, chunk_data, max_retries)
        
        # Handle large payloads
        return self._handle_payload_size(state_data, latest_segment_id, metadata, s3_key)

    def _verify_consistency(
        self,
        chunk_index: int,
        latest_segment_id: Optional[int],
        metadata: Dict[str, str],
        state_data: Dict[str, Any],
        chunk_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Verify S3 read-after-write consistency.
        
        Checks:
        1. Segment ID is >= expected minimum for this chunk
        2. Timestamp is recent (within 5 minutes)
        3. Internal state matches metadata
        """
        try:
            if latest_segment_id is not None:
                if chunk_data and 'start_segment' in chunk_data:
                    expected_min = max(0, chunk_data['start_segment'] - 1) if chunk_index > 0 else -1
                else:
                    # Fallback with warning
                    expected_min = max(0, chunk_index * 100 - 1) if chunk_index > 0 else -1
                    logger.warning("Using fallback segment calculation")

                if latest_segment_id < expected_min:
                    logger.warning(f"Segment ID too low: {latest_segment_id} < {expected_min}")
                    return False

            # Timestamp freshness
            updated_at = metadata.get('updated_at')
            if updated_at:
                update_time = int(updated_at)
                if time.time() - update_time > 300:  # 5 minutes
                    logger.warning("State timestamp too old")
                    return False

            return True
        except Exception as e:
            logger.error(f"Consistency check failed: {e}")
            return False

    def _retry_load_with_backoff(
        self, 
        s3_key: str, 
        chunk_index: int, 
        chunk_data: Optional[Dict], 
        max_retries: int
    ) -> Dict[str, Any]:
        """Retry S3 load with exponential backoff."""
        for retry in range(max_retries):
            delay = (2 ** retry) + random.uniform(0.1, 0.5)
            logger.info(f"Retry {retry + 1}/{max_retries}, waiting {delay:.2f}s")
            time.sleep(delay)

            try:
                response = self.s3_client.get_object(Bucket=self._state_bucket, Key=s3_key)
                state_data = json.loads(response['Body'].read().decode('utf-8'))
                metadata = response.get('Metadata', {})
                latest_segment_id = self._parse_segment_id(metadata.get('latest_segment_id'))

                if self._verify_consistency(chunk_index, latest_segment_id, metadata, state_data, chunk_data):
                    return self._handle_payload_size(state_data, latest_segment_id, metadata, s3_key)
            except Exception as e:
                logger.warning(f"Retry {retry + 1} failed: {e}")

        return self._build_load_response(state_loaded=False, reason="consistency_timeout")

    def _fallback_to_dynamodb(
        self, 
        execution_id: str, 
        owner_id: str, 
        workflow_id: str
    ) -> Dict[str, Any]:
        """Load state from DynamoDB pointer."""
        try:
            table = self.dynamodb.Table(self._workflows_table)
            response = table.get_item(Key={'execution_id': execution_id, 'state_type': 'LATEST'})

            if 'Item' not in response:
                return self._build_load_response(state_loaded=False, reason="not_found")

            item = response['Item']
            s3_path = item.get('state_s3_path')

            if s3_path:
                bucket, key = s3_path.replace('s3://', '').split('/', 1)
                s3_response = self.s3_client.get_object(Bucket=bucket, Key=key)
                state_data = json.loads(s3_response['Body'].read().decode('utf-8'))
                return self._build_load_response(
                    state_data=state_data,
                    latest_segment_id=item.get('segment_id'),
                    source="dynamodb_pointer"
                )
            else:
                return self._build_load_response(
                    state_data=item.get('state_data', {}),
                    latest_segment_id=item.get('segment_id'),
                    source="dynamodb_inline"
                )
        except Exception as e:
            logger.error(f"DynamoDB fallback failed: {e}")
            return self._build_load_response(state_loaded=False, reason="dynamodb_fallback_failed", error=str(e))

    # =========================================================================
    # SAVE STATE (Write Path)
    # =========================================================================

    def save_state(
        self,
        execution_id: str,
        owner_id: str,
        workflow_id: str,
        chunk_id: str,
        segment_id: int,
        state_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save state with dual-write to S3 and DynamoDB.
        
        Write Order (with Rollback):
        1. Write to S3 (Primary)
        2. If S3 succeeds, write to DynamoDB
        3. If DynamoDB fails, rollback S3 write
        
        Args:
            execution_id: Workflow execution ID
            owner_id: Owner user ID
            workflow_id: Workflow ID
            chunk_id: Chunk identifier
            segment_id: Latest segment ID
            state_data: State data to persist
            
        Returns:
            Save result with s3_path and status
        """
        if not self._state_bucket:
            raise ValueError("State bucket not configured")

        s3_key = f"distributed-states/{owner_id}/{workflow_id}/{execution_id}/latest_state.json"
        s3_path = f"s3://{self._state_bucket}/{s3_key}"
        timestamp = int(time.time())

        # Prepare state JSON
        state_json = json.dumps(state_data, ensure_ascii=False)
        
        # S3 Metadata
        metadata = {
            'execution_id': execution_id,
            'chunk_id': chunk_id,
            'latest_segment_id': str(segment_id),
            'updated_at': str(timestamp),
            'owner_id': owner_id,
            'workflow_id': workflow_id
        }

        # Phase 1: Write to S3
        try:
            self.s3_client.put_object(
                Bucket=self._state_bucket,
                Key=s3_key,
                Body=state_json,
                ContentType='application/json',
                Metadata=metadata
            )
            logger.info(f"State saved to S3: {s3_path}")
        except Exception as e:
            logger.error(f"S3 write failed: {e}")
            return {"saved": False, "error": str(e), "phase": "s3_write"}

        # Phase 2: Write to DynamoDB
        try:
            self._update_dynamodb_pointer(
                execution_id=execution_id,
                owner_id=owner_id,
                workflow_id=workflow_id,
                chunk_id=chunk_id,
                segment_id=segment_id,
                s3_path=s3_path,
                timestamp=timestamp
            )
            logger.info(f"DynamoDB pointer updated for {execution_id}")
        except Exception as e:
            # Rollback S3 write
            logger.error(f"DynamoDB write failed, rolling back S3: {e}")
            try:
                self.s3_client.delete_object(Bucket=self._state_bucket, Key=s3_key)
                logger.info("S3 rollback successful")
            except Exception as rollback_err:
                logger.error(f"S3 rollback failed: {rollback_err}")
            
            return {"saved": False, "error": str(e), "phase": "dynamodb_write", "rollback": True}

        return {
            "saved": True,
            "s3_path": s3_path,
            "timestamp": timestamp,
            "segment_id": segment_id,
            "chunk_id": chunk_id
        }

    def delete_state(
        self,
        execution_id: str,
        owner_id: str = None,
        workflow_id: str = None
    ) -> Dict[str, Any]:
        """
        Delete state from both S3 and DynamoDB.
        
        Used for cleanup in smoke tests or when explicitly removing state.
        
        Args:
            execution_id: Workflow execution ID
            owner_id: Owner user ID (required for DynamoDB delete)
            workflow_id: Workflow ID (required for DynamoDB delete)
            
        Returns:
            Delete result with status
        """
        result = {"deleted": False, "s3_deleted": False, "ddb_deleted": False}
        
        # Delete from S3
        if self._state_bucket and owner_id and workflow_id:
            s3_key = f"distributed-states/{owner_id}/{workflow_id}/{execution_id}/latest_state.json"
            try:
                self.s3_client.delete_object(Bucket=self._state_bucket, Key=s3_key)
                result["s3_deleted"] = True
                logger.info(f"Deleted S3 state: {s3_key}")
            except Exception as e:
                logger.warning(f"S3 delete failed: {e}")
        
        # Delete from DynamoDB (requires composite key: ownerId + workflowId)
        if owner_id and workflow_id:
            try:
                table = self.dynamodb.Table(self._workflows_table)
                table.delete_item(Key={
                    'ownerId': owner_id,  # [FIX] camelCase to match table schema
                    'workflowId': workflow_id  # [FIX] camelCase to match table schema
                })
                result["ddb_deleted"] = True
                logger.info(f"Deleted DynamoDB state: ownerId={owner_id}, workflowId={workflow_id}")
            except Exception as e:
                logger.warning(f"DynamoDB delete failed: {e}")
        else:
            logger.warning(f"Cannot delete DynamoDB state: missing owner_id or workflow_id")
        
        result["deleted"] = result["s3_deleted"] or result["ddb_deleted"]
        return result



    def _update_dynamodb_pointer(
        self,
        execution_id: str,
        owner_id: str,
        workflow_id: str,
        chunk_id: str,
        segment_id: int,
        s3_path: str,
        timestamp: int
    ):
        """Update DynamoDB state pointer."""
        table = self.dynamodb.Table(self._workflows_table)
        table.put_item(
            Item={
                'ownerId': owner_id,  # [FIX] camelCase to match table schema
                'workflowId': workflow_id,  # [FIX] camelCase to match table schema
                'executionId': execution_id,
                'stateType': 'LATEST',
                'chunkId': chunk_id,
                'segmentId': segment_id,
                'stateS3Path': s3_path,
                'updatedAt': timestamp,
                'ttl': timestamp + self.DEFAULT_TTL_SECONDS
            }
        )


    # =========================================================================
    # Helpers
    # =========================================================================

    def _handle_payload_size(
        self, 
        state_data: Dict, 
        latest_segment_id: Optional[int], 
        metadata: Dict, 
        s3_key: str
    ) -> Dict[str, Any]:
        """Handle Step Functions 256KB limit by offloading large payloads."""
        state_json = json.dumps(state_data, ensure_ascii=False)
        size = len(state_json.encode('utf-8'))

        if size <= self.SIZE_LIMIT_BYTES:
            return self._build_load_response(
                state_data=state_data,
                latest_segment_id=latest_segment_id,
                payload_type="inline",
                payload_size=size
            )

        # Offload to S3
        logger.warning(f"Large state ({size} bytes), offloading to S3")
        # 🛡️ [P1] S3 파티셔닝 강화: owner_id를 경로 최상단으로 올려서 분산 저장
        # 기존: large-states/{execution_id}/{timestamp}.json
        # 개선: large-states/{owner_id}/{workflow_id}/{execution_id}/{timestamp}.json
        path_parts = s3_key.split('/')
        owner_id = path_parts[1] if len(path_parts) > 1 else 'unknown'
        workflow_id = path_parts[2] if len(path_parts) > 2 else 'unknown'
        execution_id = path_parts[3] if len(path_parts) > 3 else 'unknown'
        large_key = f"large-states/{owner_id}/{workflow_id}/{execution_id}/{int(time.time())}.json"
        
        self.s3_client.put_object(
            Bucket=self._state_bucket,
            Key=large_key,
            Body=state_json,
            ContentType='application/json'
        )

        return {
            "previous_state_s3_uri": f"s3://{self._state_bucket}/{large_key}",
            "latest_segment_id": latest_segment_id,
            "state_loaded": True,
            "payload_type": "s3_reference",
            "payload_size_bytes": size
        }

    def _build_load_response(
        self,
        state_data: Optional[Dict] = None,
        latest_segment_id: Optional[int] = None,
        state_loaded: bool = True,
        reason: Optional[str] = None,
        source: Optional[str] = None,
        error: Optional[str] = None,
        payload_type: str = "inline",
        payload_size: int = 0,
        total_segments: Optional[int] = None  # 🛡️ [P0] ASL null 참조 방지
    ) -> Dict[str, Any]:
        """Build standardized load response."""
        response = {
            "previous_state": state_data or {},
            "latest_segment_id": latest_segment_id,
            "state_loaded": state_loaded,
            "total_segments": total_segments if total_segments is not None else 1  # 🛡️ [P0] 기본값 보장
        }
        if reason:
            response["reason"] = reason
        if source:
            response["source"] = source
        if error:
            response["error"] = error
        if state_loaded:
            response["payload_type"] = payload_type
            response["payload_size_bytes"] = payload_size
        return response

    def _parse_segment_id(self, value: Optional[str]) -> Optional[int]:
        """Safely parse segment ID from metadata."""
        if value:
            try:
                return int(value)
            except (ValueError, TypeError):
                pass
        return None


# Singleton
_service_instance = None

def get_state_persistence_service() -> StatePersistenceService:
    global _service_instance
    if _service_instance is None:
        _service_instance = StatePersistenceService()
    return _service_instance
