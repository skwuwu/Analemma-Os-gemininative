# 📡 API Reference

> [← Back to Main README](../README.md)

This document provides comprehensive API documentation for Analemma OS, including REST endpoints, WebSocket protocol, and SDK integration patterns.

---

## Table of Contents

1. [Authentication](#1-authentication)
2. [REST API Endpoints](#2-rest-api-endpoints)
3. [WebSocket Protocol](#3-websocket-protocol)
4. [Error Handling](#4-error-handling)
5. [Rate Limiting](#5-rate-limiting)
6. [SDK Integration](#6-sdk-integration)

---

## 1. Authentication

Analemma OS uses **AWS Cognito** for authentication. All API requests require a valid JWT token.

### 1.1 Token Format

```http
Authorization: Bearer <JWT_TOKEN>
```

### 1.2 Token Claims

| Claim | Description |
|-------|-------------|
| `sub` | User ID (owner_id) - immutable identifier |
| `email` | User email address |
| `cognito:groups` | User group memberships |
| `exp` | Token expiration timestamp |

### 1.3 Authentication Flow

```
┌─────────────────────────────────────────────────────────┐
│                  Authentication Flow                     │
├─────────────────────────────────────────────────────────┤
│                                                          │
│   1. User logs in via Cognito Hosted UI                 │
│                     │                                    │
│                     ▼                                    │
│   2. Cognito returns JWT tokens                         │
│      - id_token (user info)                             │
│      - access_token (API access)                        │
│      - refresh_token (token renewal)                    │
│                     │                                    │
│                     ▼                                    │
│   3. Client includes access_token in API requests       │
│                     │                                    │
│                     ▼                                    │
│   4. API Gateway validates token via Cognito            │
│                     │                                    │
│                     ▼                                    │
│   5. Lambda extracts owner_id from jwt.claims.sub       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## 2. REST API Endpoints

### 2.1 Workflows API

#### Create/Save Workflow

```http
POST /workflows
Content-Type: application/json
Authorization: Bearer <token>

{
  "name": "My Workflow",
  "description": "Workflow description",
  "nodes": [...],
  "edges": [...],
  "category": "automation",
  "visibility": "private"
}
```

**Response:**
```json
{
  "workflow_id": "wf_abc123",
  "name": "My Workflow",
  "created_at": "2026-01-14T10:00:00Z",
  "updated_at": "2026-01-14T10:00:00Z"
}
```

#### Get Workflow

```http
GET /workflows/{workflow_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "workflow_id": "wf_abc123",
  "owner_id": "user_123",
  "name": "My Workflow",
  "nodes": [...],
  "edges": [...],
  "version": 3,
  "created_at": "2026-01-14T10:00:00Z"
}
```

#### List Workflows

```http
GET /workflows?limit=20&next_token=<token>
Authorization: Bearer <token>
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 20 | Max items per page (1-100) |
| `next_token` | string | - | Pagination token |
| `category` | string | - | Filter by category |
| `visibility` | string | - | Filter by visibility (private/public) |

**Response:**
```json
{
  "workflows": [...],
  "next_token": "eyJrIjp7InBrIjp7...",
  "count": 20
}
```

#### Delete Workflow

```http
DELETE /workflows/{workflow_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "message": "Workflow deleted successfully",
  "workflow_id": "wf_abc123"
}
```

---

### 2.2 Executions API

#### Run Workflow

```http
POST /executions
Content-Type: application/json
Authorization: Bearer <token>

{
  "workflow_id": "wf_abc123",
  "input": {
    "user_query": "Process this data",
    "parameters": {...}
  },
  "idempotency_key": "unique_key_123"
}
```

**Response:**
```json
{
  "execution_id": "exec_xyz789",
  "execution_arn": "arn:aws:states:...",
  "status": "RUNNING",
  "started_at": "2026-01-14T10:05:00Z"
}
```

#### Get Execution Status

```http
GET /executions/{execution_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "execution_id": "exec_xyz789",
  "workflow_id": "wf_abc123",
  "status": "RUNNING",
  "current_segment": 5,
  "total_segments": 12,
  "started_at": "2026-01-14T10:05:00Z",
  "progress_percent": 41.67
}
```

**Possible Status Values:**

| Status | Description |
|--------|-------------|
| `RUNNING` | Execution in progress |
| `SUCCEEDED` | Completed successfully |
| `FAILED` | Execution failed |
| `PAUSED_FOR_HITP` | Waiting for human input |
| `TIMED_OUT` | Execution timed out |
| `ABORTED` | Manually stopped |

#### List Executions

```http
GET /executions?limit=20&status=RUNNING
Authorization: Bearer <token>
```

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Max items per page |
| `status` | string | Filter by status |
| `workflow_id` | string | Filter by workflow |
| `start_date` | string | Filter by start date (ISO 8601) |

#### Stop Execution

```http
POST /executions/{execution_id}/stop
Authorization: Bearer <token>
```

**Response:**
```json
{
  "execution_id": "exec_xyz789",
  "status": "ABORTED",
  "stopped_at": "2026-01-14T10:10:00Z"
}
```

#### Resume Execution (HITP Response)

```http
POST /executions/{execution_id}/resume
Content-Type: application/json
Authorization: Bearer <token>

{
  "user_response": "Approved",
  "additional_data": {...}
}
```

**Response:**
```json
{
  "execution_id": "exec_xyz789",
  "status": "RUNNING",
  "resumed_at": "2026-01-14T10:15:00Z"
}
```

---

### 2.3 Co-design API

#### Stream Co-design Response

```http
POST /codesign
Content-Type: application/json
Authorization: Bearer <token>

{
  "user_request": "Add a loop that processes each item",
  "current_workflow": {
    "nodes": [...],
    "edges": [...]
  },
  "recent_changes": [...],
  "session_id": "session_abc"
}
```

**Response (JSONL Stream):**
```
Content-Type: application/x-ndjson

{"type": "node", "data": {"id": "node_1", "type": "loop", ...}}
{"type": "node", "data": {"id": "node_2", "type": "llm_chat", ...}}
{"type": "edge", "data": {"source": "node_1", "target": "node_2"}}
{"type": "suggestion", "data": {"text": "Consider adding error handling", "confidence": 0.85}}
{"type": "status", "data": "done"}
```

**Canvas Modes:**

| Mode | Trigger | Behavior |
|------|---------|----------|
| `agentic-designer` | Empty canvas, no history | Full workflow generation |
| `co-design` | Existing workflow or history | Incremental modifications |

#### Explain Workflow

```http
POST /codesign/explain
Content-Type: application/json
Authorization: Bearer <token>

{
  "workflow": {
    "nodes": [...],
    "edges": [...]
  }
}
```

**Response:**
```json
{
  "summary": "This workflow processes customer data...",
  "node_explanations": [
    {
      "node_id": "node_1",
      "explanation": "Fetches data from the API..."
    }
  ],
  "flow_description": "1. Start -> 2. Fetch Data -> 3. Process..."
}
```

#### Generate Suggestions

```http
POST /codesign/suggestions
Content-Type: application/json
Authorization: Bearer <token>

{
  "workflow": {...},
  "max_suggestions": 5
}
```

**Response:**
```json
{
  "suggestions": [
    {
      "id": "sug_1",
      "type": "optimization",
      "text": "Add retry logic to API calls",
      "confidence": 0.92,
      "affected_nodes": ["node_3", "node_5"]
    }
  ],
  "count": 3
}
```

---

### 2.4 Skills API

#### List Skills

```http
GET /skills?category=data-processing
Authorization: Bearer <token>
```

**Response:**
```json
{
  "skills": [
    {
      "skill_id": "skill_csv_parser",
      "name": "CSV Parser",
      "category": "data-processing",
      "version": "1.2.0",
      "description": "Parses CSV files with configurable options"
    }
  ]
}
```

#### Get Skill Details

```http
GET /skills/{skill_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "skill_id": "skill_csv_parser",
  "name": "CSV Parser",
  "schema": {
    "input": {...},
    "output": {...},
    "config": {...}
  },
  "examples": [...]
}
```

---

### 2.5 Time Machine API

#### Get Execution Timeline

```http
GET /executions/{execution_id}/timeline
Authorization: Bearer <token>
```

**Response:**
```json
{
  "execution_id": "exec_xyz789",
  "timeline": [
    {
      "timestamp": "2026-01-14T10:05:00Z",
      "event_type": "segment_start",
      "segment_id": 0,
      "data": {...}
    },
    {
      "timestamp": "2026-01-14T10:05:05Z",
      "event_type": "llm_call",
      "segment_id": 0,
      "data": {"model": "gemini-3-pro", "tokens": 1250}
    }
  ]
}
```

#### Compare Checkpoints

```http
POST /executions/{execution_id}/checkpoints/compare
Content-Type: application/json
Authorization: Bearer <token>

{
  "checkpoint_a": "cp_001",
  "checkpoint_b": "cp_005"
}
```

**Response:**
```json
{
  "added_keys": ["new_variable"],
  "removed_keys": ["temp_data"],
  "modified_keys": ["counter", "status"],
  "state_diff": {
    "counter": {"before": 5, "after": 10},
    "status": {"before": "processing", "after": "validated"}
  }
}
```

---

## 3. WebSocket Protocol

Analemma OS uses WebSocket for real-time updates during workflow execution.

### 3.1 Connection

```
wss://api.analemma.io/ws?token=<JWT_TOKEN>
```

### 3.2 Connection Lifecycle

```
┌──────────────────────────────────────────────────────────┐
│                WebSocket Lifecycle                        │
├──────────────────────────────────────────────────────────┤
│                                                           │
│   Client                          Server                  │
│      │                               │                    │
│      │──── Connect (JWT) ───────────>│                    │
│      │                               │                    │
│      │<─── Connection Ack ───────────│                    │
│      │     {type: "connected",       │                    │
│      │      connection_id: "..."}    │                    │
│      │                               │                    │
│      │<─── Execution Updates ────────│                    │
│      │     {type: "progress", ...}   │                    │
│      │                               │                    │
│      │<─── HITP Request ─────────────│                    │
│      │     {type: "hitp_required"}   │                    │
│      │                               │                    │
│      │──── HITP Response ───────────>│                    │
│      │     {action: "resume", ...}   │                    │
│      │                               │                    │
│      │<─── Completion ───────────────│                    │
│      │     {type: "completed"}       │                    │
│      │                               │                    │
│      │──── Disconnect ──────────────>│                    │
│      │                               │                    │
└──────────────────────────────────────────────────────────┘
```

### 3.3 Message Types

#### Server → Client Messages

**Progress Update:**
```json
{
  "type": "progress",
  "execution_id": "exec_xyz789",
  "segment": 5,
  "total_segments": 12,
  "current_node": "node_process_data",
  "progress_percent": 41.67
}
```

**Glass-Box Log (AI Transparency):**
```json
{
  "type": "glass_box",
  "execution_id": "exec_xyz789",
  "log": {
    "event": "llm_call",
    "model": "gemini-3-pro",
    "prompt_preview": "Process the following...",
    "tokens_used": 1250,
    "duration_ms": 850
  }
}
```

**HITP Request:**
```json
{
  "type": "hitp_required",
  "execution_id": "exec_xyz789",
  "prompt": "Please review and approve the following action:",
  "context": {...},
  "timeout_seconds": 3600
}
```

**Execution Completed:**
```json
{
  "type": "completed",
  "execution_id": "exec_xyz789",
  "status": "SUCCEEDED",
  "output": {...},
  "duration_seconds": 45.3
}
```

**Error Notification:**
```json
{
  "type": "error",
  "execution_id": "exec_xyz789",
  "error_code": "LLM_TIMEOUT",
  "message": "LLM call timed out after 30 seconds",
  "recoverable": true
}
```

#### Client → Server Messages

**Subscribe to Execution:**
```json
{
  "action": "subscribe",
  "execution_id": "exec_xyz789"
}
```

**HITP Response:**
```json
{
  "action": "resume",
  "execution_id": "exec_xyz789",
  "response": "Approved",
  "additional_data": {...}
}
```

**Ping (Keep-alive):**
```json
{
  "action": "ping"
}
```

### 3.4 Payload Optimization

WebSocket payloads are automatically optimized to stay under API Gateway limits:

| Field | Optimization |
|-------|--------------|
| `current_thought` | Truncated to 200 chars |
| `thought_history` | Latest 3 entries only |
| `artifacts.preview_content` | Truncated to 100 chars |

---

## 4. Error Handling

### 4.1 Error Response Format

```json
{
  "error": "WorkflowNotFound",
  "message": "Workflow with ID 'wf_abc123' not found",
  "statusCode": 404,
  "request_id": "req_xyz789"
}
```

### 4.2 Error Codes

| HTTP Status | Error Type | Description |
|-------------|------------|-------------|
| 400 | `ValidationError` | Invalid request parameters |
| 401 | `AuthenticationError` | Invalid or missing token |
| 403 | `AuthorizationError` | Insufficient permissions |
| 404 | `NotFound` | Resource not found |
| 409 | `ConflictError` | Resource already exists |
| 429 | `RateLimitExceeded` | Too many requests |
| 500 | `InternalError` | Server error |
| 502 | `ExternalServiceError` | LLM or external service failure |

### 4.3 Retry Guidance

| Error Type | Retry | Backoff |
|------------|-------|---------|
| `RateLimitExceeded` | Yes | Exponential (1s, 2s, 4s...) |
| `ExternalServiceError` | Yes | Fixed 5s |
| `InternalError` | Yes | Exponential |
| `ValidationError` | No | - |
| `AuthenticationError` | No | Refresh token first |

---

## 5. Rate Limiting

### 5.1 Limits by Tier

| Tier | Requests/min | WebSocket Connections | Concurrent Executions |
|------|--------------|----------------------|----------------------|
| Free | 60 | 5 | 3 |
| Pro | 300 | 20 | 10 |
| Enterprise | 1000 | 100 | 50 |

### 5.2 Rate Limit Headers

```http
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 45
X-RateLimit-Reset: 1705234800
```

---

## 6. SDK Integration

### 6.1 Python SDK (Planned)

```python
from analemma import AnalemmaClient

client = AnalemmaClient(
    api_url="https://api.analemma.io",
    token="<JWT_TOKEN>"
)

# Create workflow
workflow = client.workflows.create(
    name="My Workflow",
    nodes=[...],
    edges=[...]
)

# Run workflow
execution = client.executions.run(
    workflow_id=workflow.id,
    input={"user_query": "Process data"}
)

# Stream updates
for event in execution.stream():
    if event.type == "progress":
        print(f"Progress: {event.progress_percent}%")
    elif event.type == "hitp_required":
        execution.resume(response="Approved")
    elif event.type == "completed":
        print(f"Result: {event.output}")
```

### 6.2 TypeScript SDK (Planned)

```typescript
import { AnalemmaClient } from '@analemma/sdk';

const client = new AnalemmaClient({
  apiUrl: 'https://api.analemma.io',
  token: '<JWT_TOKEN>'
});

// Create and run workflow
const workflow = await client.workflows.create({
  name: 'My Workflow',
  nodes: [...],
  edges: [...]
});

const execution = await client.executions.run({
  workflowId: workflow.id,
  input: { userQuery: 'Process data' }
});

// Subscribe to updates
execution.on('progress', (event) => {
  console.log(`Progress: ${event.progressPercent}%`);
});

execution.on('hitp_required', async (event) => {
  await execution.resume({ response: 'Approved' });
});

execution.on('completed', (event) => {
  console.log('Result:', event.output);
});
```

---

## 7. Task Manager API

The Task Manager API provides business-friendly endpoints for managing and monitoring workflow executions.

### 7.1 List Tasks

```http
GET /tasks?limit=20&status=in_progress&next_token=<token>
Authorization: Bearer <token>
```

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 20 | Max items per page (1-100) |
| `status` | string | - | Filter by status |
| `next_token` | string | - | Pagination token |
| `sort_by` | string | `created_at` | Sort field |
| `sort_order` | string | `desc` | `asc` or `desc` |

**Response:**
```json
{
  "tasks": [
    {
      "task_id": "task_abc123",
      "title": "Data Processing Job",
      "status": "in_progress",
      "progress": 45.5,
      "created_at": "2026-01-14T10:00:00Z",
      "updated_at": "2026-01-14T10:05:00Z"
    }
  ],
  "next_token": "eyJrIjp7...",
  "count": 20
}
```

### 7.2 Get Task Details

```http
GET /tasks/{task_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "task_id": "task_abc123",
  "title": "Data Processing Job",
  "description": "Processing customer data for Q4 report",
  "status": "in_progress",
  "progress": 45.5,
  "current_step": "Analyzing data patterns...",
  "created_at": "2026-01-14T10:00:00Z",
  "updated_at": "2026-01-14T10:05:00Z",
  "estimated_completion": "2026-01-14T10:15:00Z"
}
```

### 7.3 Get Task Context

```http
GET /tasks/{task_id}/context
Authorization: Bearer <token>
```

**Response:**
```json
{
  "task_id": "task_abc123",
  "context": {
    "artifacts": [
      {
        "type": "data",
        "name": "processed_records.json",
        "preview": "1,234 records processed",
        "size_bytes": 45678,
        "download_url": "https://..."
      }
    ],
    "agent_thoughts": [
      {
        "timestamp": "2026-01-14T10:05:00Z",
        "thought": "Identified 3 anomalies in dataset",
        "confidence": 0.92
      }
    ],
    "business_metrics": {
      "total_cost_usd": 0.045,
      "tokens_used": 12500,
      "execution_time_seconds": 300
    }
  }
}
```

### 7.4 Cancel Task

```http
POST /tasks/{task_id}/cancel
Authorization: Bearer <token>

{
  "reason": "User requested cancellation"
}
```

**Response:**
```json
{
  "task_id": "task_abc123",
  "status": "cancelled",
  "cancelled_at": "2026-01-14T10:06:00Z"
}
```

### 7.5 Retry Failed Task

```http
POST /tasks/{task_id}/retry
Authorization: Bearer <token>

{
  "from_checkpoint": "cp_003",
  "modified_input": {}
}
```

**Response:**
```json
{
  "task_id": "task_abc123",
  "new_execution_id": "exec_xyz789",
  "status": "pending",
  "retried_at": "2026-01-14T10:07:00Z"
}
```

---

## 8. Timeline API (Extended)

Extended documentation for the Time Machine timeline endpoints.

### 8.1 Timeline Event Types

| Event Type | Description |
|------------|-------------|
| `execution_start` | Workflow execution began |
| `segment_start` | Segment processing started |
| `segment_end` | Segment processing completed |
| `llm_call` | LLM API invocation |
| `tool_call` | External tool/API call |
| `checkpoint_created` | State checkpoint saved |
| `hitp_triggered` | Human approval requested |
| `hitp_resumed` | Execution resumed after approval |
| `error` | Error occurred |
| `self_healing` | Automatic error recovery |
| `execution_end` | Workflow execution completed |

### 8.2 Get Timeline with Filters

```http
GET /executions/{execution_id}/timeline?event_types=llm_call,error&segment_id=0
Authorization: Bearer <token>
```

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `event_types` | string | Comma-separated event types |
| `segment_id` | integer | Filter by segment |
| `start_time` | ISO8601 | Filter events after this time |
| `end_time` | ISO8601 | Filter events before this time |
| `include_state` | boolean | Include full state in response |

**Response:**
```json
{
  "execution_id": "exec_xyz789",
  "filtered": true,
  "timeline": [
    {
      "event_id": "evt_001",
      "timestamp": "2026-01-14T10:05:01.200Z",
      "event_type": "llm_call",
      "segment_id": 0,
      "data": {
        "model": "gemini-3-pro",
        "input_tokens": 850,
        "output_tokens": 400,
        "duration_ms": 1200,
        "cost_usd": 0.00125
      }
    }
  ],
  "summary": {
    "total_events": 1,
    "duration_ms": 5000
  }
}
```

### 8.3 Get Checkpoint State

```http
GET /executions/{execution_id}/checkpoints/{checkpoint_id}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "checkpoint_id": "cp_003",
  "execution_id": "exec_xyz789",
  "segment_id": 2,
  "created_at": "2026-01-14T10:05:05Z",
  "state": {
    "variables": {...},
    "results": {...}
  },
  "state_size_bytes": 12500,
  "s3_path": "s3://bucket/checkpoints/cp_003.json"
}
```

### 8.4 Resume from Checkpoint

```http
POST /executions/{execution_id}/checkpoints/{checkpoint_id}/resume
Content-Type: application/json
Authorization: Bearer <token>

{
  "modified_state": {
    "retry_count": 0
  },
  "skip_failed_segment": false
}
```

**Response:**
```json
{
  "new_execution_id": "exec_abc456",
  "resumed_from": "cp_003",
  "status": "RUNNING",
  "started_at": "2026-01-14T10:10:00Z"
}
```

---

## Appendix: JSONL Stream Format

All streaming endpoints return **JSONL** (JSON Lines) format:

```
{"type": "node", "data": {...}}\n
{"type": "edge", "data": {...}}\n
{"type": "status", "data": "done"}\n
```

**Content-Type:** `application/x-ndjson`

**Parsing Example (JavaScript):**
```javascript
const reader = response.body.getReader();
const decoder = new TextDecoder();
let buffer = '';

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  
  buffer += decoder.decode(value, { stream: true });
  
  while (buffer.includes('\n')) {
    const [line, rest] = buffer.split('\n', 2);
    buffer = rest;
    
    if (line.trim()) {
      const event = JSON.parse(line);
      handleEvent(event);
    }
  }
}
```

---

> [<- Back to Main README](../README.md)
