"""
BedrockService - LLM Invocation Service (Lazy Initialization)

Extracted from `main.py` to reduce Cold Start latency.
Only initializes boto3 client when first invoked.
"""

import os
import json
import logging
from typing import Any, Dict, Optional

from botocore.config import Config
from botocore.exceptions import ReadTimeoutError

# 공통 유틸리티 import
try:
    from src.common.constants import is_mock_mode as _common_is_mock_mode
    from src.common.aws_clients import get_bedrock_client
except ImportError:
    _common_is_mock_mode = None
    get_bedrock_client = None

logger = logging.getLogger(__name__)


class AsyncLLMRequiredException(Exception):
    """Exception raised when async LLM processing is required."""
    pass


class BedrockService:
    """
    LLM invocation service with Lazy Initialization.
    
    Performance Optimization:
    - boto3 client is NOT created in __init__
    - Client is lazily initialized on first call to `client` property
    - Reduces Cold Start time when Bedrock is not used in a request path
    """
    
    # Async processing threshold (configurable via environment variable)
    ASYNC_TOKEN_THRESHOLD = int(os.getenv('ASYNC_TOKEN_THRESHOLD', '2000'))

    def __init__(self):
        self._client = None
        self._config = Config(
            retries={"max_attempts": 3, "mode": "standard"},
            read_timeout=int(os.environ.get("BEDROCK_READ_TIMEOUT_SECONDS", "60")),
            connect_timeout=int(os.environ.get("BEDROCK_CONNECT_TIMEOUT_SECONDS", "5")),
        )

    @property
    def client(self):
        """Lazy client initialization - only creates client on first access."""
        if self._client is None:
            import boto3
            region = os.environ.get("AWS_REGION", "us-east-1")
            try:
                self._client = boto3.client(
                    "bedrock-runtime", 
                    region_name=region, 
                    config=self._config
                )
                logger.info(f"BedrockService: Client initialized for region {region}")
            except Exception as e:
                logger.warning(f"Failed to create Bedrock client: {e}")
                return None
        return self._client

    def is_mock_mode(self) -> bool:
        """Check if running in mock mode."""
        if _common_is_mock_mode is not None:
            return _common_is_mock_mode()
        return os.getenv("MOCK_MODE", "false").strip().lower() in {"true", "1", "yes", "on"}

    def invoke_model(
        self,
        model_id: str,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        read_timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Invoke a Bedrock model.
        
        Args:
            model_id: The model identifier (e.g., "anthropic.claude-3-sonnet")
            user_prompt: The user's prompt
            system_prompt: Optional system prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            read_timeout_seconds: Custom read timeout (overrides default)
            
        Returns:
            Raw Bedrock response dictionary
            
        Raises:
            AsyncLLMRequiredException: When SDK read timeout occurs
        """
        if self.is_mock_mode():
            logger.info(f"MOCK_MODE: Skipping Bedrock call for {model_id}")
            return {"content": [{"text": f"[MOCK_MODE] Response from {model_id}. Prompt: {user_prompt[:50]}..."}]}

        # Client selection: use custom timeout if specified
        if read_timeout_seconds and read_timeout_seconds != self._config.read_timeout:
            import boto3
            custom_config = self._config.merge(Config(read_timeout=read_timeout_seconds))
            client = boto3.client("bedrock-runtime", config=custom_config)
        else:
            client = self.client

        if not client:
            return {"content": [{"text": "[Error] Bedrock client unavailable"}]}

        try:
            messages = [{"role": "user", "content": [{"type": "text", "text": user_prompt}]}]
            payload = {"messages": messages}
            
            # Model-specific configurations
            if "gemini" not in (model_id or "").lower():
                payload["anthropic_version"] = "bedrock-2023-05-31"
            if system_prompt:
                payload["system"] = system_prompt
            if max_tokens:
                payload["max_tokens"] = int(max_tokens)
            if temperature:
                payload["temperature"] = float(temperature)

            resp = client.invoke_model(body=json.dumps(payload), modelId=model_id)
            return json.loads(resp['body'].read())

        except ReadTimeoutError:
            logger.warning(
                f"🚨 [Bedrock Timeout] Model: {model_id}, "
                f"Read timeout occurred, triggering async mode"
            )
            raise AsyncLLMRequiredException("SDK read timeout")
        except Exception as e:
            # 🛡️ [Enhanced Error Logging] 구조화된 Bedrock 에러 분류
            error_type = type(e).__name__
            error_msg = str(e).lower()
            
            if hasattr(e, 'response') and e.response:
                # boto3 ClientError with response
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                error_message = e.response.get('Error', {}).get('Message', str(e))
                
                if error_code in ['ThrottlingException', 'TooManyRequestsException']:
                    logger.warning(
                        f"🚨 [Bedrock Rate Limit] Model: {model_id}, "
                        f"Error: {error_message}"
                    )
                elif error_code in ['ValidationException', 'InvalidRequestException']:
                    logger.warning(
                        f"🚨 [Bedrock Validation Error] Model: {model_id}, "
                        f"Error: {error_message}"
                    )
                elif error_code in ['AccessDeniedException', 'UnauthorizedOperation']:
                    logger.error(
                        f"🚨 [Bedrock Auth Error] Model: {model_id}, "
                        f"Error: {error_message}, Check IAM permissions"
                    )
                elif error_code == 'ResourceNotFoundException':
                    logger.error(
                        f"🚨 [Bedrock Model Not Found] Model: {model_id}, "
                        f"Error: {error_message}, Check model availability"
                    )
                else:
                    logger.error(
                        f"🚨 [Bedrock API Error] Model: {model_id}, "
                        f"Code: {error_code}, Error: {error_message}"
                    )
            else:
                # Generic exception
                if "timeout" in error_msg or "deadline" in error_msg:
                    logger.warning(
                        f"🚨 [Bedrock Network Timeout] Model: {model_id}, "
                        f"Error: {e}"
                    )
                elif "connection" in error_msg or "network" in error_msg:
                    logger.warning(
                        f"🚨 [Bedrock Network Error] Model: {model_id}, "
                        f"Error: {e}"
                    )
                else:
                    logger.exception(
                        f"🚨 [Bedrock Unknown Error] Model: {model_id}, "
                        f"Type: {error_type}, Error: {e}"
                    )
            
            raise e

    def extract_text(self, response: Any) -> str:
        """Extract text from standard Bedrock response format."""
        try:
            if isinstance(response, dict):
                content = response.get("content")
                if isinstance(content, list) and content:
                    if "text" in content[0]:
                        return content[0]["text"]
            return str(response)
        except Exception:
            return str(response)

    def should_use_async(self, config: Dict[str, Any]) -> bool:
        """
        Heuristic to check if async processing is needed.
        
        Criteria:
        - High token count (> ASYNC_TOKEN_THRESHOLD)
        - Heavy model (e.g., claude-3-opus)
        - Explicit force_async flag
        """
        max_tokens = config.get("max_tokens", 0)
        model = config.get("model", "")
        force_async = config.get("force_async", False)
        
        high_token_count = max_tokens > self.ASYNC_TOKEN_THRESHOLD
        heavy_model = "claude-3-opus" in model
        
        if high_token_count or heavy_model or force_async:
            logger.info(f"Async required: tokens={max_tokens}, model={model}, force={force_async}")
            return True
        return False


# Singleton instance for module-level access (optional)
_bedrock_service_instance = None

def get_bedrock_service() -> BedrockService:
    """Get or create the singleton BedrockService instance."""
    global _bedrock_service_instance
    if _bedrock_service_instance is None:
        _bedrock_service_instance = BedrockService()
    return _bedrock_service_instance
