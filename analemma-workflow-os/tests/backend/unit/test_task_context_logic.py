
import pytest
from datetime import datetime
from src.models.task_context import TaskContext, TaskStatus, ArtifactPreview, ArtifactType

class TestTaskContextLogic:
    
    def test_task_context_initialization(self):
        """Test default initialization values."""
        task = TaskContext(task_id="task-1")
        assert task.status == TaskStatus.QUEUED
        assert task.progress_percentage == 0
        assert task.thought_history == []
        assert task.artifacts == []

    def test_add_thought_history_limit(self):
        """Test that thought history is capped at 10 items."""
        task = TaskContext(task_id="task-1")
        
        # Add 15 thoughts
        for i in range(15):
            task.add_thought(f"Thought {i}")
            
        assert len(task.thought_history) == 10
        # Should verify the LAST 10 are kept (FIFO)
        assert task.thought_history[0].message == "Thought 5"
        assert task.thought_history[-1].message == "Thought 14"
        assert task.current_thought == "Thought 14"

    def test_to_websocket_payload_structure(self):
        """Test the websocket payload format."""
        task = TaskContext(
            task_id="task-1",
            agent_name="TestAgent",
            status=TaskStatus.IN_PROGRESS,
            progress_percentage=50
        )
        task.add_thought("Processing...")
        
        payload = task.to_websocket_payload()
        
        assert payload["task_id"] == "task-1"
        assert payload["display_status"] == "In Progress" # Mapped value
        assert payload["thought"] == "Processing..."
        assert payload["progress"] == 50
        assert payload["agent_name"] == "TestAgent"
        assert "updated_at" in payload

    def test_set_pending_decision(self):
        """Test setting a pending decision updates status."""
        task = TaskContext(task_id="task-1", status=TaskStatus.IN_PROGRESS)
        
        task.set_pending_decision(
            question="Approve?", 
            context="Review needed",
            options=[{"label": "Yes", "value": "yes"}]
        )
        
        assert task.status == TaskStatus.PENDING_APPROVAL
        assert task.pending_decision is not None
        assert task.pending_decision.question == "Approve?"
        assert len(task.pending_decision.options) == 1
        
        # Verify websocket payload reflects interruption
        payload = task.to_websocket_payload()
        assert payload["is_interruption"] == True
        assert payload["display_status"] == "Pending Approval"

    def test_clear_pending_decision(self):
        """Test clearing decision resumes status."""
        task = TaskContext(task_id="task-1")
        task.set_pending_decision("Q", "C")
        
        assert task.status == TaskStatus.PENDING_APPROVAL
        
        task.clear_pending_decision()
        
        assert task.pending_decision is None
        assert task.status == TaskStatus.IN_PROGRESS
        assert task.to_websocket_payload()["is_interruption"] == False

    def test_add_artifact(self):
        """Test adding artifacts."""
        task = TaskContext(task_id="task-1")
        
        preview = ArtifactPreview(
            artifact_id="art-1",
            artifact_type=ArtifactType.TEXT,
            title="Summary",
            preview_content="Snippet"
        )
        
        task.add_artifact(preview)
        
        assert len(task.artifacts) == 1
        assert task.artifacts[0].title == "Summary"
        assert task.to_websocket_payload()["artifacts_count"] == 1

