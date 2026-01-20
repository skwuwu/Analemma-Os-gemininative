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
        
        # 🛡️ [v2.5] S3 bucket 강제 동기화 - 핸들러에서 서비스로 전달
        service = SegmentRunnerService(s3_bucket=_S3_BUCKET)
        result = service.execute_segment(event)
        
        # 🛡️ [v2.5] TypeError 방어 코드 - total_segments 보장 (강제 캐스팅)
        if result and isinstance(result, dict):
            # 🛡️ [Critical Fix] 어떤 타입이든 int로 강제 변환 시도
            raw_total = result.get('total_segments')
            if raw_total is None:
                raw_total = event.get('total_segments')
            
            try:
                # Dict, List 등 잘못된 타입이 들어와도 int()로 변환 시도
                if isinstance(raw_total, (int, float)):
                    result['total_segments'] = max(1, int(raw_total))
                elif isinstance(raw_total, str) and raw_total.strip().isdigit():
                    result['total_segments'] = max(1, int(raw_total.strip()))
                elif raw_total is None:
                    # 최후의 보루: partition_map 크기 체크 또는 1로 강제
                    p_map = event.get('partition_map', [])
                    result['total_segments'] = len(p_map) if isinstance(p_map, list) and p_map else 1
                    logger.info(f"🛡️ [v2.5] total_segments forced to {result['total_segments']}")
                else:
                    # Dict, List 등 예상치 못한 타입
                    logger.error(f"🚨 Invalid total_segments type: {type(raw_total).__name__} = {raw_total}")
                    p_map = event.get('partition_map', [])
                    result['total_segments'] = len(p_map) if isinstance(p_map, list) and p_map else 1
            except (TypeError, ValueError) as e:
                # 모든 변환 시도 실패 시
                logger.error(f"🚨 Failed to convert total_segments: {e}, raw_value={raw_total}")
                result['total_segments'] = 1
            
            # 🛡️ [v2.5] threshold도 result에 포함 (디버깅용)
            if 'state_size_threshold' not in result:
                result['state_size_threshold'] = service.threshold
        
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
        
        # 🛡️ [P0 Fix] total_segments 안전 추출 - TypeError 방지
        p_map = event.get('partition_map', [])
        raw_total = event.get('total_segments')
        if raw_total is not None and isinstance(raw_total, (int, float)):
            safe_total_segments = max(1, int(raw_total))
        elif isinstance(p_map, list) and p_map:
            safe_total_segments = len(p_map)
        else:
            safe_total_segments = 1
        
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
            "segment_type": "ERROR",
            # 🛡️ [P0 Fix] Step Functions Choice에서 참조하는 필수 필드
            "total_segments": safe_total_segments,
            "segment_id": event.get('segment_id', 0)
        }

# --- Legacy Helper Imports Preservation ---
# To avoid breaking other files that import from here during transition
# (though ideally they should import from src.services now)
from src.services.state.state_manager import StateManager
from src.services.workflow.repository import WorkflowRepository
# We re-export run_workflow from main to keep interface if used as lib
from src.handlers.core.main import run_workflow, partition_workflow, _build_segment_config

