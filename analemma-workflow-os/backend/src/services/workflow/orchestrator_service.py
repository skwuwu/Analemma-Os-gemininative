"""
WorkflowOrchestratorService - Workflow Execution Engine

Extracted from `main.py` to separate orchestration logic from handler.
Handles:
- Workflow validation and config parsing
- Dynamic workflow building  
- Execution with callbacks
- Glass-Box logging
"""

import json
import os
import time
import uuid
import logging
from typing import Dict, Any, List, Optional, Union

from pydantic import BaseModel, Field, conlist, constr, ValidationError, field_validator

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Schema Definitions
# -----------------------------------------------------------------------------

class EdgeModel(BaseModel):
    source: constr(min_length=1, max_length=128)
    target: constr(min_length=1, max_length=128)
    type: constr(min_length=1, max_length=64) = "edge"


class NodeModel(BaseModel):
    id: constr(min_length=1, max_length=128)
    type: constr(min_length=1, max_length=64)
    label: Optional[constr(min_length=0, max_length=256)] = None
    action: Optional[constr(min_length=0, max_length=256)] = None
    hitp: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None
    # [Fix] parallel_group support - branches와 resource_policy 필드 추가
    branches: Optional[List[Dict[str, Any]]] = None
    resource_policy: Optional[Dict[str, Any]] = None
    # [Fix] subgraph support
    subgraph_ref: Optional[str] = None
    subgraph_inline: Optional[Dict[str, Any]] = None
    
    @field_validator('type', mode='before')
    @classmethod
    def alias_node_type(cls, v):
        """Alias 'code' type to 'operator' to prevent ValueError in NODE_REGISTRY."""
        if v == 'code':
            return 'operator'
        return v


class WorkflowConfigModel(BaseModel):
    workflow_name: Optional[constr(min_length=0, max_length=256)] = None
    description: Optional[constr(min_length=0, max_length=512)] = None
    nodes: conlist(NodeModel, min_length=1, max_length=500)
    edges: conlist(EdgeModel, min_length=0, max_length=1000)
    start_node: Optional[constr(min_length=1, max_length=128)] = None


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class AsyncLLMRequiredException(Exception):
    """Exception raised when async LLM processing is required."""
    pass


# -----------------------------------------------------------------------------
# Glass-Box Callback
# -----------------------------------------------------------------------------

class StateHistoryCallback:
    """Capture AI thoughts and tool usage with PII masking for Glass-Box UI."""
    
    def __init__(self, mask_pii_fn=None):
        self.logs = []
        self.mask_pii = mask_pii_fn or (lambda x: x)
    
    def on_chain_start(self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs) -> None:
        pass
    
    def on_llm_start(self, serialized: Dict[str, Any], prompts: List[str], **kwargs) -> None:
        prompt_safe = self.mask_pii(prompts[0]) if prompts else ""
        self.logs.append({
            "id": str(uuid.uuid4()),
            "type": "ai_thought",
            "name": serialized.get("name", "LLM Thinking"),
            "node_id": serialized.get("name", "llm_node"),
            "content": "Thinking...",
            "timestamp": int(time.time()),
            "status": "RUNNING",
            "details": {"prompts": [prompt_safe] if prompt_safe else []}
        })
    
    def on_llm_end(self, response: Any, **kwargs) -> None:
        if self.logs and self.logs[-1]["type"] == "ai_thought":
            self.logs[-1]["status"] = "COMPLETED"
            if getattr(response, "llm_output", None) and "usage" in response.llm_output:
                self.logs[-1]["usage"] = response.llm_output["usage"]
            try:
                text = response.generations[0][0].text
                self.logs[-1]["content"] = self.mask_pii(text)
            except Exception:
                pass
    
    def on_llm_error(self, error: BaseException, **kwargs) -> None:
        if self.logs and self.logs[-1]["type"] == "ai_thought":
            self.logs[-1]["status"] = "FAILED"
            self.logs[-1]["error"] = {"message": str(error), "type": type(error).__name__}
    
    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs) -> None:
        self.logs.append({
            "id": str(uuid.uuid4()),
            "type": "tool_usage",
            "name": serialized.get("name", "Tool"),
            "node_id": serialized.get("name", "tool_node"),
            "status": "RUNNING",
            "timestamp": int(time.time()),
            "input": self.mask_pii(input_str)
        })
    
    def on_tool_end(self, output: str, **kwargs) -> None:
        if self.logs and self.logs[-1]["type"] == "tool_usage":
            self.logs[-1]["status"] = "COMPLETED"
            self.logs[-1]["output"] = self.mask_pii(str(output))
    
    def on_tool_error(self, error: BaseException, **kwargs) -> None:
        if self.logs and self.logs[-1]["type"] == "tool_usage":
            self.logs[-1]["status"] = "FAILED"
            self.logs[-1]["error"] = str(error)


# -----------------------------------------------------------------------------
# Orchestrator Service
# -----------------------------------------------------------------------------

class WorkflowOrchestratorService:
    """
    Workflow execution orchestrator.
    
    Responsibilities:
    - Config validation (Pydantic schema)
    - Dynamic workflow building
    - Execution with DynamoDB checkpointing
    - Glass-Box callback integration
    """
    
    def __init__(self, mask_pii_fn=None):
        self._mask_pii = mask_pii_fn
    
    def run_workflow(
        self,
        config_json: Union[str, Dict[str, Any]],
        initial_state: Optional[Dict[str, Any]] = None,
        user_api_keys: Optional[Dict[str, str]] = None,
        use_cache: bool = True,
        conversation_id: Optional[str] = None,
        ddb_table_name: Optional[str] = None,
        run_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for workflow execution.
        
        Args:
            config_json: Workflow configuration (JSON string or dict)
            initial_state: Initial state for the workflow
            user_api_keys: User-provided API keys
            use_cache: Whether to use caching
            conversation_id: Conversation thread ID
            ddb_table_name: DynamoDB table for checkpointing
            run_config: Additional runtime configuration
            
        Returns:
            Workflow execution result
        """
        initial_state = dict(initial_state) if initial_state else {}
        
        # 1. Check for Mock/Test Request
        mock_behavior = initial_state.get("mock_behavior")
        if mock_behavior:
            logger.info(f"Mock behavior detected: {mock_behavior}")
            config_json = json.dumps(self._get_mock_config(mock_behavior))
            
            if mock_behavior == "PAUSED_FOR_HITP":
                return {"status": "PAUSED_FOR_HITP", "next_segment_to_run": 1}
        
        # 2. Config Validation
        raw_config = self._parse_config(config_json)
        workflow_config = self._validate_config(raw_config)
        
        # 3. Dynamic Build
        from src.services.workflow.builder import DynamicWorkflowBuilder
        
        logger.info("Building workflow dynamically...")
        builder = DynamicWorkflowBuilder(workflow_config)
        app = builder.build()
        
        # 4. Apply Checkpointer
        if ddb_table_name:
            app = self._apply_checkpointer(app, ddb_table_name)
        
        # 5. Prepare Execution Config
        final_config = self._prepare_run_config(run_config, conversation_id)
        
        # 6. Setup State
        if user_api_keys:
            initial_state.setdefault("user_api_keys", {}).update(user_api_keys)
        initial_state.setdefault("step_history", [])
        
        # 7. Setup Glass-Box Callback
        history_callback = StateHistoryCallback(mask_pii_fn=self._mask_pii)
        final_config.setdefault("callbacks", []).append(history_callback)
        
        # 8. Execute
        logger.info("Invoking workflow...")
        try:
            result = app.invoke(initial_state, config=final_config)
            
            if isinstance(result, dict):
                # 🛡️ total_segments 주입 - NoneType 에러 원천 차단
                if "total_segments" not in result:
                    result["total_segments"] = initial_state.get("total_segments") or 1
                
                result["__new_history_logs"] = history_callback.logs
                prev_logs = result.get("execution_logs", [])
                result["execution_logs"] = prev_logs + history_callback.logs
            
            return result
            
        except AsyncLLMRequiredException:
            return {"status": "PAUSED_FOR_ASYNC_LLM"}
        except Exception as e:
            logger.exception("Workflow execution failed")
            raise
    
    def partition_workflow(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Partition workflow into segments for distributed execution."""
        from src.services.workflow.partition_service import partition_workflow_advanced
        result = partition_workflow_advanced(config)
        return result.get("partition_map", [])
    
    def build_segment_config(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Convert segment object to executable workflow config."""
        return {
            "nodes": segment.get("nodes", []),
            "edges": segment.get("edges", [])
        }
    
    # =========================================================================
    # Private Helpers
    # =========================================================================
    
    def _parse_config(self, config_json: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Parse JSON config."""
        if isinstance(config_json, dict):
            return config_json
        try:
            return json.loads(config_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON config: {e}")
    
    def _validate_config(self, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate config against Pydantic schema."""
        try:
            validated = WorkflowConfigModel.model_validate(raw_config)
            return validated.model_dump()
        except ValidationError as ve:
            raise ValueError(f"Invalid workflow config: {ve}")
    
    def _apply_checkpointer(self, app, ddb_table_name: str):
        """Apply DynamoDB checkpointer if available."""
        try:
            from langgraph_checkpoint_dynamodb import DynamoDBSaver
            saver = DynamoDBSaver(table_name=ddb_table_name)
            return app.with_checkpointer(saver)
        except ImportError:
            logger.warning("DynamoDBSaver not found, skipping persistence")
            return app
    
    def _prepare_run_config(
        self, 
        run_config: Optional[Dict[str, Any]], 
        conversation_id: Optional[str]
    ) -> Dict[str, Any]:
        """Prepare runtime configuration."""
        final_config = dict(run_config) if run_config else {}
        
        if "configurable" not in final_config:
            final_config["configurable"] = {}
        
        configurable = final_config["configurable"]
        if not configurable.get("thread_id"):
            configurable["thread_id"] = conversation_id or "default_thread"
        if conversation_id and not configurable.get("conversation_id"):
            configurable["conversation_id"] = conversation_id
        
        return final_config
    
    def _get_mock_config(self, mock_behavior: str) -> Dict[str, Any]:
        """Return mock configurations for testing."""
        if mock_behavior == "E2E_S3_LARGE_DATA":
            return {
                "nodes": [{
                    "id": "large_data_generator", "type": "operator",
                    "config": {"code": "state['res'] = 'X'*300000", "output_key": "res"}
                }],
                "edges": [], "start_node": "large_data_generator"
            }
        elif mock_behavior == "CONTINUE":
            return {
                "nodes": [
                    {"id": "step1", "type": "operator", "config": {"sets": {"step": 1}}},
                    {"id": "step2", "type": "operator", "config": {"sets": {"step": 2}}}
                ],
                "edges": [{"source": "step1", "target": "step2"}], "start_node": "step1"
            }
        return {"nodes": [], "edges": []}


# Singleton
_orchestrator_instance = None

def get_workflow_orchestrator() -> WorkflowOrchestratorService:
    global _orchestrator_instance
    if _orchestrator_instance is None:
        # Import mask_pii from template renderer
        try:
            from src.services.common.template_renderer import get_template_renderer
            renderer = get_template_renderer()
            _orchestrator_instance = WorkflowOrchestratorService(mask_pii_fn=renderer.mask_pii)
        except ImportError:
            _orchestrator_instance = WorkflowOrchestratorService()
    return _orchestrator_instance
