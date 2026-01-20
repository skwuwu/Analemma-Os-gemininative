"""
[Tiny Handler] Save Latest State

Delegates all logic to StatePersistenceService.
This handler is a thin wrapper for Lambda/Step Functions compatibility.
"""

import logging
import os
from typing import Dict, Any

from src.services.state.state_persistence_service import get_state_persistence_service

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Save latest state for distributed workflow chunk.
    
    Delegates to StatePersistenceService.save_state().
    
    Args:
        event: {
            "chunk_data": { "chunk_id": "chunk_0001", "segment_id": 99, ... },
            "execution_id": "exec-123",
            "owner_id": "user-456",
            "workflow_id": "wf-789",
            "state_bucket": "my-bucket",
            "final_state": {...}
        }
    
    Returns:
        {
            "saved": bool,
            "s3_path": str,
            "timestamp": int,
            "segment_id": int
        }
    """
    try:
        chunk_data = event.get('chunk_data', {})
        execution_id = event.get('execution_id')
        owner_id = event.get('owner_id')
        workflow_id = event.get('workflow_id')
        state_bucket = event.get('state_bucket') or os.environ.get('WORKFLOW_STATE_BUCKET')
        
        chunk_id = chunk_data.get('chunk_id', 'unknown')
        segment_id = chunk_data.get('segment_id') or chunk_data.get('latest_segment_id', 0)
        final_state = event.get('final_state', {})
        
        # 🛡️ [P0] 컨텍스트 추출 (total_segments) - 워크플로우 흐름 보존
        total_segments = event.get('total_segments')
        
        # 🛡️ [P0] 데이터 정화 (유령 'code' 타입 박멸)
        # 상태 데이터 내부에 오염된 노드 타입이 저장되지 않도록 방어
        if isinstance(final_state, dict):
            if 'partition_map' in final_state and isinstance(final_state['partition_map'], list):
                for seg in final_state['partition_map']:
                    if isinstance(seg, dict):
                        for node in seg.get('nodes', []):
                            if isinstance(node, dict) and node.get('type') == 'code':
                                logger.warning(f"🛡️ [SaveHandler] Fixing 'code' type to 'operator' in state for node {node.get('id')}")
                                node['type'] = 'operator'
        
        logger.info(f"SaveLatestState: chunk={chunk_id}, segment={segment_id}")
        
        # Delegate to service
        service = get_state_persistence_service()
        
        # 🛡️ [P0] 캡슐화 준수 - set_bucket 메서드 사용
        if state_bucket:
            service.set_bucket(state_bucket)
        
        save_result = service.save_state(
            execution_id=execution_id,
            owner_id=owner_id,
            workflow_id=workflow_id,
            chunk_id=chunk_id,
            segment_id=segment_id,
            state_data=final_state
        )
        
        # 🛡️ [P0] 컨텍스트 보존 (total_segments 누락 방지)
        # Step Functions의 다음 State가 null을 참조하여 TypeError가 발생하지 않도록 함
        if isinstance(save_result, dict):
            save_result['total_segments'] = int(total_segments) if total_segments is not None else 1
            
        return save_result
        
    except Exception as e:
        logger.exception(f"SaveLatestState failed: {e}")
        return {
            "saved": False,
            "error": str(e),
            "phase": "handler_exception"
        }
