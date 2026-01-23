import json
import logging
import re
import boto3
from typing import Dict, Any, Optional, Tuple, Set

logger = logging.getLogger(__name__)

# PII 마스킹 패턴 정의 (RFC 5322 이메일, 한국 전화번호, API 키 등)
PII_REGEX_PATTERNS = {
    'email': (re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'), '[EMAIL_MASKED]'),
    'phone_kr': (re.compile(r'0\d{1,2}-\d{3,4}-\d{4}'), '[PHONE_MASKED]'),
    'phone_intl': (re.compile(r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}'), '[PHONE_MASKED]'),
    'api_key': (re.compile(r'(?:api[_-]?key|apikey|secret[_-]?key)\s*[:=]\s*["\']?[\w\-]+["\']?', re.IGNORECASE), '[API_KEY_MASKED]'),
}

# [Perf Optimization] 마스킹 제외 대상 키 (대용량 바이너리/S3 참조 데이터)
SKIP_MASKING_KEYS: Set[str] = {
    'large_s3_payload', 's3_path', 's3_url', 'final_state_s3_path',
    '__s3_reference', 'binary_data', 'file_content', 'image_data',
    '__kernel_actions',  # 커널 액션은 시스템 데이터로 PII 없음
}


def mask_pii(text: str) -> str:
    """문자열에서 PII를 마스킹"""
    if not isinstance(text, str):
        return text
    for _, (pattern, replacement) in PII_REGEX_PATTERNS.items():
        text = pattern.sub(replacement, text)
    return text


def mask_pii_in_state(state: Any, skip_keys: Set[str] = SKIP_MASKING_KEYS, depth: int = 0, max_depth: int = 50) -> Any:
    """
    상태 객체 내의 모든 문자열에서 PII를 재귀적으로 마스킹.
    
    [Perf Optimization v2]
    - skip_keys: 대용량 데이터가 포함된 키는 마스킹 우회
    - max_depth: 무한 재귀 방지 (기본 50 레벨)
    - 대용량 문자열(>100KB)은 마스킹 스킵
    """
    # 무한 재귀 방지
    if depth > max_depth:
        logger.warning("⚠️ PII masking: max depth (%d) reached, returning as-is", max_depth)
        return state
    
    if isinstance(state, str):
        # [Perf] 100KB 이상 문자열은 마스킹 스킵 (성능 최적화)
        if len(state) > 102400:
            logger.debug("⚡ Skipping PII masking for large string (%d bytes)", len(state))
            return state
        return mask_pii(state)
    elif isinstance(state, dict):
        result = {}
        for k, v in state.items():
            # [Perf] 대용량 키는 마스킹 우회
            if k in skip_keys:
                result[k] = v
            else:
                result[k] = mask_pii_in_state(v, skip_keys, depth + 1, max_depth)
        return result
    elif isinstance(state, list):
        return [mask_pii_in_state(item, skip_keys, depth + 1, max_depth) for item in state]
    else:
        return state


class StateManager:
    def __init__(self, s3_client=None):
        self.s3_client = s3_client or boto3.client("s3")

    def download_state_from_s3(self, s3_path: str) -> Dict[str, Any]:
        """
        Download state JSON from S3.
        """
        try:
            logger.info("⬇️ Downloading state from: %s", s3_path)
            bucket, key = s3_path.replace("s3://", "").split("/", 1)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            state_data = json.loads(response["Body"].read().decode("utf-8"))
            logger.info("✅ State downloaded successfully (%d keys)", len(state_data.keys()))
            return state_data
        except Exception as e:
            logger.error("❌ Failed to download state from %s: %s", s3_path, e)
            raise RuntimeError(f"Failed to download state from S3: {e}")

    def upload_state_to_s3(self, bucket: str, prefix: str, state: Dict[str, Any], deterministic_filename: Optional[str] = None) -> str:
        """
        Upload state JSON to S3.
        [Deprecated] Use _upload_raw_bytes_to_s3 for pre-serialized data.
        """
        try:
            import time
            import uuid
            
            file_name = deterministic_filename if deterministic_filename else f"{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
            key = f"{prefix}/{file_name}"
            s3_path = f"s3://{bucket}/{key}"
            
            logger.info("⬆️ Uploading state to: %s", s3_path)
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(state, ensure_ascii=False),
                ContentType="application/json"
            )
            return s3_path
        except Exception as e:
            logger.error("❌ Failed to upload state to %s: %s", bucket, e)
            raise RuntimeError(f"Failed to upload state to S3: {e}")

    def _upload_raw_bytes_to_s3(self, bucket: str, prefix: str, serialized_bytes: bytes, deterministic_filename: Optional[str] = None) -> str:
        """
        [Perf Optimization] Upload pre-serialized bytes directly to S3.
        Eliminates double serialization overhead.
        """
        try:
            import time
            import uuid
            
            file_name = deterministic_filename if deterministic_filename else f"{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
            key = f"{prefix}/{file_name}"
            s3_path = f"s3://{bucket}/{key}"
            
            logger.info("⬆️ [Optimized] Uploading pre-serialized bytes to: %s (%d bytes)", s3_path, len(serialized_bytes))
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=serialized_bytes,
                ContentType="application/json"
            )
            return s3_path
        except Exception as e:
            logger.error("❌ Failed to upload raw bytes to %s: %s", bucket, e)
            raise RuntimeError(f"Failed to upload raw bytes to S3: {e}")

    def handle_state_storage(self, state: Dict[str, Any], auth_user_id: str, workflow_id: str, segment_id: int, bucket: Optional[str], threshold: Optional[int] = None, loop_counter: Optional[int] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Decide whether to store state inline or in S3 based on size threshold.
        PII data is masked before storage to ensure privacy compliance.
        
        [Critical] Step Functions has a 256KB payload limit. If state exceeds this:
        - With bucket: Upload to S3, return (None, s3_path)
        - Without bucket: Return truncated state with error marker to prevent SF failure
        
        [Perf Optimization v2]
        - Single serialization: 직렬화 결과를 재사용하여 중복 연산 제거
        - Selective masking: 대용량 키는 마스킹 우회
        - Safe threshold: 180KB로 하향하여 SF 래퍼 오버헤드 고려
        
        [v3.10] Loop Support:
        - loop_counter arg added to prevent S3 overwrite during loops
        """
        try:
            # 🛡️ PII 마스킹 적용 (개인정보 보호) - 대용량 키 우회 적용됨
            masked_state = mask_pii_in_state(state)
            logger.debug("🔒 PII masking applied to state before storage")
            
            # [Perf Optimization] Single Serialization - 직렬화 한 번만 수행
            serialized_bytes = json.dumps(masked_state, ensure_ascii=False).encode("utf-8")
            state_size = len(serialized_bytes)
            
            # [Critical Fix] Step Functions hard limit with safety buffer
            # 256KB = 262,144 bytes, but AWS wrapper adds ~10-15KB overhead
            # Using 180KB (180,000 bytes) for safe margin
            SF_HARD_LIMIT = 180000  # ~175KB safe threshold
            
            # [Fix] Handle None threshold - default to 180KB (safe Step Functions limit)
            if threshold is None:
                threshold = 180000
                logger.warning("⚠️ threshold parameter was None, using default 180KB (safe SF limit)")
            
            if state_size > threshold:
                if not bucket:
                    logger.error("🚨 CRITICAL: State size (%d bytes, %.1fKB) exceeds threshold (%d) but no S3 bucket provided!", 
                                state_size, state_size/1024, threshold)
                    
                    # [Critical Fix] Instead of returning the full state (which causes SF failure),
                    # return a truncated state with error information
                    if state_size > SF_HARD_LIMIT:
                        logger.error("🚨 State exceeds Step Functions safe limit (180KB)! Creating safe fallback state.")
                        
                        # Create a minimal safe state that won't exceed limits
                        safe_state = {
                            "__state_truncated": True,
                            "__original_size_bytes": state_size,
                            "__original_size_kb": round(state_size / 1024, 2),
                            "__truncation_reason": "State exceeded 180KB Step Functions safe limit but no S3 bucket available",
                            "__error": "PAYLOAD_TOO_LARGE_NO_S3_BUCKET",
                            # Preserve essential metadata if present
                            "workflowId": masked_state.get("workflowId") if isinstance(masked_state, dict) else None,
                            "ownerId": masked_state.get("ownerId") if isinstance(masked_state, dict) else None,
                            "segment_id": segment_id,
                        }
                        
                        # Try to preserve test result if this is a test workflow
                        if isinstance(masked_state, dict):
                            for key in ['TEST_RESULT', 'VALIDATION_STATUS', '__kernel_actions']:
                                if key in masked_state:
                                    safe_state[key] = masked_state[key]
                        
                        logger.warning("⚠️ Returning truncated safe state (%d bytes) instead of full state (%d bytes)", 
                                      len(json.dumps(safe_state)), state_size)
                        return safe_state, None
                    else:
                        # State is below SF limit but above our threshold - return with warning
                        logger.warning("⚠️ State size (%d) exceeds threshold but below SF safe limit. Returning inline (risky).", state_size)
                        return masked_state, None

                if not auth_user_id:
                    raise PermissionError("Missing authenticated user id for S3 upload")
                
                # [v3.10] Loop-Safe Path Construction
                if loop_counter is not None and isinstance(loop_counter, int) and loop_counter >= 0:
                    # e.g. .../segments/10/5/output.json (Loop #5)
                    prefix = f"workflow-states/{auth_user_id}/{workflow_id}/segments/{segment_id}/{loop_counter}"
                else:
                    prefix = f"workflow-states/{auth_user_id}/{workflow_id}/segments/{segment_id}"
                
                # [Perf Optimization] 이미 직렬화된 바이트를 직접 S3에 업로드 (중복 직렬화 제거)
                s3_path = self._upload_raw_bytes_to_s3(bucket, prefix, serialized_bytes, deterministic_filename="output.json")
                logger.info("📦 State uploaded to S3: %s (%d bytes, %.1fKB)", s3_path, state_size, state_size/1024)
                return None, s3_path
            else:
                logger.info("📦 Returning state inline (%d bytes <= %d threshold)", state_size, threshold)
                return masked_state, None
        except Exception as e:
            logger.exception("Failed to handle state storage")
            raise RuntimeError(f"Failed to handle state storage: {e}")
