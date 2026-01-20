import logging
import os
import json
from typing import Dict, Any

from src.services.execution.segment_runner_service import SegmentRunnerService

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 🚨 [Critical] Validate S3 bucket at module load time (Cold Start)
_S3_BUCKET = os.environ.get("S3_BUCKET") or os.environ.get("SKELETON_S3_BUCKET") or ""
_S3_BUCKET = _S3_BUCKET.strip() if _S3_BUCKET else ""
if not _S3_BUCKET:
    logger.error("🚨 [CRITICAL CONFIG ERROR] S3_BUCKET or SKELETON_S3_BUCKET environment variable is NOT SET! "
                f"S3_BUCKET='{os.environ.get('S3_BUCKET')}', "
                f"SKELETON_S3_BUCKET='{os.environ.get('SKELETON_S3_BUCKET')}'. "
                "Payloads exceeding 256KB will cause Step Functions failures.")
else:
    logger.info(f"✅ S3 bucket configured for state offloading: {_S3_BUCKET}")

def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Entry point for Segment Executions.
    
    Refactored to "Tiny Handler" pattern.
    Logic delegated to:
    - src.services.execution.segment_runner_service.SegmentRunnerService
    """
    try:
        # PII / Logging safety check
        # Limit log size
        event_str = json.dumps(event)
        log_size = len(event_str)
        if log_size > 10000:
             logger.info("🚀 Segment Runner started. Event size: %s (large event truncated)", log_size)
        else:
             logger.info("🚀 Segment Runner started. Event: %s", event_str)
        
        service = SegmentRunnerService()
        result = service.execute_segment(event)
        
        logger.info("✅ Segment Runner finished successfully.")
        return result

    except Exception as e:
        logger.exception("❌ Segment Runner failed")
        # Return error state that Step Functions can catch
        # [Fix] ASL ResultSelector가 기대하는 모든 필드를 포함해야 JSONPath 에러 방지
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__
        }
        return {
            "status": "FAILED",
            "error": str(e),
            "error_type": type(e).__name__,
            # ASL이 필수로 요구하는 필드들 - None/빈값으로 제공
            "final_state": event.get('current_state', {}),  # 마지막 알려진 상태 보존
            "final_state_s3_path": None,
            "next_segment_to_run": None,
            "new_history_logs": [],
            "error_info": error_info,
            "branches": None,
            "segment_type": "ERROR"
        }

# --- Legacy Helper Imports Preservation ---
# To avoid breaking other files that import from here during transition
# (though ideally they should import from src.services now)
from src.services.state.state_manager import StateManager
from src.services.workflow.repository import WorkflowRepository
# We re-export run_workflow from main to keep interface if used as lib
from src.handlers.core.main import run_workflow, partition_workflow, _build_segment_config

