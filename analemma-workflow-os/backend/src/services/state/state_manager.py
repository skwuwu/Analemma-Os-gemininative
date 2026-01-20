import json
import logging
import re
import boto3
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# PII 마스킹 패턴 정의 (RFC 5322 이메일, 한국 전화번호, API 키 등)
PII_REGEX_PATTERNS = {
    'email': (re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'), '[EMAIL_MASKED]'),
    'phone_kr': (re.compile(r'0\d{1,2}-\d{3,4}-\d{4}'), '[PHONE_MASKED]'),
    'phone_intl': (re.compile(r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}'), '[PHONE_MASKED]'),
    'api_key': (re.compile(r'(?:api[_-]?key|apikey|secret[_-]?key)\s*[:=]\s*["\']?[\w\-]+["\']?', re.IGNORECASE), '[API_KEY_MASKED]'),
}


def mask_pii(text: str) -> str:
    """문자열에서 PII를 마스킹"""
    if not isinstance(text, str):
        return text
    for _, (pattern, replacement) in PII_REGEX_PATTERNS.items():
        text = pattern.sub(replacement, text)
    return text


def mask_pii_in_state(state: Any) -> Any:
    """상태 객체 내의 모든 문자열에서 PII를 재귀적으로 마스킹"""
    if isinstance(state, str):
        return mask_pii(state)
    elif isinstance(state, dict):
        return {k: mask_pii_in_state(v) for k, v in state.items()}
    elif isinstance(state, list):
        return [mask_pii_in_state(item) for item in state]
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

    def handle_state_storage(self, state: Dict[str, Any], auth_user_id: str, workflow_id: str, segment_id: int, bucket: Optional[str], threshold: Optional[int] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Decide whether to store state inline or in S3 based on size threshold.
        PII data is masked before storage to ensure privacy compliance.
        
        [Critical] Step Functions has a 256KB payload limit. If state exceeds this:
        - With bucket: Upload to S3, return (None, s3_path)
        - Without bucket: Return truncated state with error marker to prevent SF failure
        """
        try:
            # 🛡️ PII 마스킹 적용 (개인정보 보호)
            masked_state = mask_pii_in_state(state)
            logger.debug("🔒 PII masking applied to state before storage")
            
            serialized = json.dumps(masked_state, ensure_ascii=False)
            state_size = len(serialized.encode("utf-8"))
            
            # Step Functions hard limit: 256KB (262,144 bytes)
            # Use 250KB as safe threshold to account for wrapper overhead
            SF_HARD_LIMIT = 250000
            
            # [Fix] Handle None threshold - default to 256KB (Step Functions limit)
            if threshold is None:
                threshold = 256000
                logger.warning("⚠️ threshold parameter was None, using default 256KB")
            
            if state_size > threshold:
                if not bucket:
                    logger.error("🚨 CRITICAL: State size (%d bytes, %.1fKB) exceeds threshold (%d) but no S3 bucket provided!", 
                                state_size, state_size/1024, threshold)
                    
                    # [Critical Fix] Instead of returning the full state (which causes SF failure),
                    # return a truncated state with error information
                    if state_size > SF_HARD_LIMIT:
                        logger.error("🚨 State exceeds Step Functions 256KB limit! Creating safe fallback state.")
                        
                        # Create a minimal safe state that won't exceed limits
                        safe_state = {
                            "__state_truncated": True,
                            "__original_size_bytes": state_size,
                            "__original_size_kb": round(state_size / 1024, 2),
                            "__truncation_reason": "State exceeded 256KB Step Functions limit but no S3 bucket available",
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
                        logger.warning("⚠️ State size (%d) exceeds threshold but below SF limit. Returning inline (risky).", state_size)
                        return masked_state, None

                if not auth_user_id:
                    raise PermissionError("Missing authenticated user id for S3 upload")
                
                prefix = f"workflow-states/{auth_user_id}/{workflow_id}/segments/{segment_id}"
                s3_path = self.upload_state_to_s3(bucket, prefix, masked_state, deterministic_filename="output.json")
                logger.info("📦 State uploaded to S3: %s (%d bytes, %.1fKB)", s3_path, state_size, state_size/1024)
                return None, s3_path
            else:
                logger.info("📦 Returning state inline (%d bytes <= %d threshold)", state_size, threshold)
                return masked_state, None
        except Exception as e:
            logger.exception("Failed to handle state storage")
            raise RuntimeError(f"Failed to handle state storage: {e}")
