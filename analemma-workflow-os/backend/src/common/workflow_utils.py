import hashlib
import os
import time
from src.common.auth_utils import extract_owner_id_from_event

# Backward compatibility alias
def get_owner_id(event):
    """JWT 토큰에서 owner_id 추출 (레거시 호환용 - extract_owner_id_from_event 사용 권장)"""
    return extract_owner_id_from_event(event)

def get_current_timestamp():
    """현재 시간을 표준 Unix Timestamp(초 단위, 정수)로 반환"""
    return int(time.time())

def generate_workflow_id(owner_id: str, workflow_name: str) -> str:
    """Generate deterministic workflowId from src.ownerId and workflow name.
    
    Args:
        owner_id: The owner's unique identifier (from src.JWT sub claim)
        workflow_name: The workflow name from src.config
        
    Returns:
        32-character hex string that uniquely identifies the workflow
    """
    # Add salt to prevent workflow name guessing attacks
    try:
        from src.common.constants import WorkflowConfig
        salt = WorkflowConfig.WORKFLOW_NAME_SALT
        hash_length = WorkflowConfig.WORKFLOW_ID_HASH_LENGTH
    except ImportError:
        salt = "analemma_workflow_v1"
        hash_length = 32
    
    combined = f"{salt}:{owner_id}:{workflow_name}"
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()[:hash_length]


def ensure_edge_ids(cfg: dict) -> dict:
    """Ensure every edge in a workflow config has an 'id' field.

    This mutates the provided dict in-place and also returns it for convenience.
    Missing edge ids are filled with a UUID4 string.
    """
    try:
        import uuid
        if not isinstance(cfg, dict):
            return cfg
        edges = cfg.get('edges')
        if isinstance(edges, list):
            for e in edges:
                if isinstance(e, dict):
                    if not e.get('id'):
                        e['id'] = str(uuid.uuid4())
    except Exception:
        # If anything goes wrong, return cfg unchanged
        return cfg
    return cfg