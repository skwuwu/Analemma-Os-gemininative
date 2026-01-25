import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

# [v2.1] 중앙 집중식 재시도 유틸리티
try:
    from src.common.retry_utils import retry_call, with_retry_sync, CircuitBreaker
    RETRY_UTILS_AVAILABLE = True
except ImportError:
    RETRY_UTILS_AVAILABLE = False

try:  # Allow local imports during tests and packaged Lambda execution
    from src.handlers.core.segment_runner_handler import lambda_handler as segment_runner  # type: ignore
except ImportError:  # pragma: no cover - fallback when module executed as script
    from src.handlers.core.segment_runner_handler import lambda_handler as segment_runner  # type: ignore


logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    payload = _coerce_event(event)
    # Normalize payload to prefer top-level keys but fall back to state_data bag
    if isinstance(payload, dict) and isinstance(payload.get("state_data"), dict):
        sd = payload.get("state_data")
        # do not overwrite explicit top-level keys
        for k, v in sd.items():
            if k not in payload:
                payload[k] = v
    mode = str(payload.get("mode") or "").lower()
    if mode == "worker":
        return _execute_worker(payload, context)
    return _dispatch_worker(payload, context)


def _dispatch_worker(payload: Dict[str, Any], context: Any) -> Dict[str, Any]:
    task_token = payload.get("TaskToken") or payload.get("taskToken")
    if not task_token:
        raise KeyError("TaskToken missing from src.asynchronous request")

    workflow_config = payload.get("workflow_config")
    segment_to_run = int(payload.get("segment_to_run", 0))
    user_api_keys = payload.get("user_api_keys")
    owner_id = _extract_owner_id(payload, workflow_config)
    workflow_id = _extract_workflow_id(payload, workflow_config)

    state_s3_path = payload.get("state_s3_path")
    current_state = payload.get("current_state") or {}
    if not isinstance(current_state, dict):
        current_state = {}

    bucket = _resolve_state_bucket()
    inline_threshold = _resolve_inline_threshold()

    if not state_s3_path and bucket and current_state:
        try:
            serialized_state = json.dumps(current_state, ensure_ascii=False)
            if len(serialized_state.encode("utf-8")) > inline_threshold:
                if not owner_id:
                    raise PermissionError("ownerId required to offload state to S3")
                key_prefix = _build_state_prefix(owner_id, workflow_id, segment_to_run)
                state_s3_path = _upload_state(serialized_state, bucket, key_prefix)
                current_state = {}
        except Exception as exc:
            logger.exception("Failed to offload current state to S3")
            _send_task_failure_safe(task_token, "AsyncLLMInitializationFailed", str(exc))
            raise

    worker_event: Dict[str, Any] = {
        "mode": "worker",
        "task_token": task_token,
        "workflow_config": workflow_config,
        "segment_to_run": segment_to_run,
        "ownerId": owner_id,
        "workflow_id": workflow_id,
    }

    if state_s3_path:
        worker_event["state_s3_path"] = state_s3_path
    if current_state:
        worker_event["current_state"] = current_state

    for key in ("conversation_id", "execution_id", "request_context", "MOCK_MODE"):
        if key in payload:
            worker_event[key] = payload[key]
    # Propagate idempotency_key if present so downstream workers/segment runner can use it
    if payload.get('idempotency_key'):
        worker_event['idempotency_key'] = payload.get('idempotency_key')

    # 🚀 FARGATE 비동기 LLM 워커 (현재 비활성화)
    # 활성화 방법: fargate-async-config/ACTIVATION_GUIDE.md 참조
    cluster_name = os.environ.get("ASYNC_WORKER_ECS_CLUSTER")
    task_definition = os.environ.get("ASYNC_WORKER_TASK_DEFINITION") 
    
    if not cluster_name or not task_definition:
        # 현재 비활성화 상태: 기존 동기 처리로 폴백
        logger.warning("🚨 Fargate async worker is DISABLED - initiating sync fallback")
        logger.warning("📊 Async Processing Analysis:")
        logger.warning(f"   🏗️ Workflow: {workflow_id or 'unknown'}")
        logger.warning(f"   🔧 Segment: {segment_to_run}")
        logger.warning(f"   👤 Owner: {owner_id or 'unknown'}")
        logger.warning(f"   🐳 ECS Cluster: {cluster_name or 'NOT_SET'}")
        logger.warning(f"   📋 Task Definition: {task_definition or 'NOT_SET'}")
        logger.warning("⚠️ RISK: Lambda 15-minute timeout may occur for heavy processing")
        logger.info("📁 Fargate config available in: fargate-async-config/")
        logger.info("📖 Activation guide: fargate-async-config/ACTIVATION_GUIDE.md")
        logger.info("🔄 Attempting synchronous fallback processing...")
        
        # 동기 처리로 폴백 (기존 segment_runner 직접 호출)
        try:
            segment_event: Dict[str, Any] = {
                "workflow_config": workflow_config,
                "segment_to_run": segment_to_run,
            }
            
            if owner_id:
                segment_event["ownerId"] = owner_id
            if workflow_id:
                segment_event["workflow_id"] = workflow_id
            if state_s3_path:
                segment_event["state_s3_path"] = state_s3_path
            if current_state:
                segment_event["current_state"] = current_state
                
            for key in ("conversation_id", "execution_id", "request_context", "idempotency_key"):
                if key in payload:
                    segment_event[key] = payload[key]
                    
            result = segment_runner(segment_event, context)
            final_payload = _build_final_payload(result, segment_to_run)
            # Preserve existing messages/system_data before sending to Step Functions
            final_payload = _merge_state_preserving_messages(final_payload, current_state)
            _send_task_success(task_token, final_payload)
            
            logger.info("✅ Synchronous fallback completed successfully")
            logger.info("📊 Fallback Processing Summary:")
            logger.info(f"   🎯 Result status: {final_payload.get('status', 'unknown')}")
            logger.info(f"   🔄 Next segment: {final_payload.get('next_segment_to_run', 'unknown')}")
            logger.info(f"   📝 Has final state: {bool(final_payload.get('final_state'))}")
            logger.info("⚠️ Note: This processing was within Lambda 15-minute limit")
            
            return {"status": "SYNC_FALLBACK_SUCCESS"}
            
        except Exception as exc:
            logger.error("❌ Synchronous fallback FAILED")
            logger.error("📊 Failure Analysis:")
            logger.error(f"   🚨 Error type: {type(exc).__name__}")
            logger.error(f"   📝 Error message: {str(exc)}")
            logger.error(f"   🏗️ Workflow: {workflow_id or 'unknown'}")
            logger.error(f"   🔧 Segment: {segment_to_run}")
            logger.error("💡 Recommendation: Consider activating Fargate for this workload")
            logger.exception("Detailed error traceback:")
            
            _send_task_failure_safe(task_token, "AsyncLLMSyncFallbackFailed", str(exc))
            raise
    
    # === 아래는 Fargate 활성화 시에만 실행되는 코드 ===
    subnet_ids = os.environ.get("ASYNC_WORKER_SUBNET_IDS", "").split(",")
    security_group_ids = os.environ.get("ASYNC_WORKER_SECURITY_GROUP_IDS", "").split(",")

    # Fargate Task에 전달할 환경변수로 worker_event 변환
    container_overrides = {
        "containerOverrides": [
            {
                "name": "async-llm-worker-container",  # 컨테이너 이름
                "environment": [
                    {"name": "WORKER_MODE", "value": "true"},
                    {"name": "WORKER_EVENT", "value": json.dumps(worker_event, ensure_ascii=False)},
                    {"name": "LOG_LEVEL", "value": os.environ.get("LOG_LEVEL", "INFO")}
                ]
            }
        ]
    }
    
    # 네트워크 구성
    network_config = {
        "awsvpcConfiguration": {
            "subnets": [s.strip() for s in subnet_ids if s.strip()],
            "assignPublicIp": "ENABLED"  # NAT Gateway 없는 경우 필요
        }
    }
    
    if security_group_ids and security_group_ids[0].strip():
        network_config["awsvpcConfiguration"]["securityGroups"] = [s.strip() for s in security_group_ids if s.strip()]

    try:
        ecs_client = boto3.client("ecs")
        
        # [v2.1] ECS run_task는 Exponential Backoff로 재시도
        # 프로비저닝 지연, API Throttling 등 일시적 장애 대응
        def _run_ecs_task():
            return ecs_client.run_task(
                cluster=cluster_name,
                taskDefinition=task_definition,
                launchType="FARGATE",
                networkConfiguration=network_config,
                overrides=container_overrides,
                tags=[
                    {"key": "WorkflowId", "value": str(workflow_id or "unknown")},
                    {"key": "OwnerId", "value": str(owner_id or "unknown")},
                    {"key": "SegmentToRun", "value": str(segment_to_run)},
                    {"key": "Purpose", "value": "AsyncLLMWorker"}
                ]
            )
        
        if RETRY_UTILS_AVAILABLE:
            response = retry_call(
                _run_ecs_task,
                max_retries=3,
                base_delay=1.0,
                max_delay=10.0,
                exceptions=(ClientError, ConnectionError, Exception)
            )
        else:
            response = _run_ecs_task()
        
        task_arn = response["tasks"][0]["taskArn"] if response.get("tasks") else None
        logger.info(f"Started Fargate async worker task: {task_arn}")
        
    except Exception as exc:
        logger.exception("Failed to start Fargate async worker after retries")
        _send_task_failure_safe(task_token, "AsyncLLMDispatchFailed", str(exc))
        raise

    return {"status": "QUEUED"}


def _execute_worker(payload: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Fargate 컨테이너에서 실행되는 Worker 함수
    Lambda context는 Fargate에서 사용할 수 없으므로 Optional로 처리
    """
    task_token = payload.get("task_token") or payload.get("TaskToken")
    if not task_token:
        raise KeyError("task_token missing from src.worker event")

    workflow_config = payload.get("workflow_config")
    segment_to_run = int(payload.get("segment_to_run", 0))
    owner_id = payload.get("ownerId") or payload.get("owner_id")
    workflow_id = payload.get("workflow_id") or _extract_workflow_id(payload, workflow_config)
    
    logger.info(f"Fargate worker starting: workflow_id={workflow_id}, segment={segment_to_run}, owner={owner_id}")

    segment_event: Dict[str, Any] = {
        "workflow_config": workflow_config,
        "segment_to_run": segment_to_run,
    }

    if owner_id:
        segment_event["ownerId"] = owner_id
    if workflow_id:
        segment_event["workflow_id"] = workflow_id
    if payload.get("state_s3_path"):
        segment_event["state_s3_path"] = payload["state_s3_path"]
    if payload.get("current_state"):
        segment_event["current_state"] = payload["current_state"]
    if payload.get("conversation_id"):
        segment_event["conversation_id"] = payload["conversation_id"]
    if payload.get("execution_id"):
        segment_event["execution_id"] = payload["execution_id"]
    if payload.get("idempotency_key"):
        segment_event["idempotency_key"] = payload["idempotency_key"]
    if payload.get("MOCK_MODE"):
        segment_event["MOCK_MODE"] = payload["MOCK_MODE"]

    try:
        # Fargate Worker에서도 S3 상태 크기 검사 (추가 안전장치)
        current_state = segment_event.get("current_state", {})
        if isinstance(current_state, dict):
            _check_state_s3_references(current_state)
        
        result = segment_runner(segment_event, context)
    except Exception as exc:
        logger.exception("Segment runner failed in async worker")
        _send_task_failure(task_token, "AsyncLLMProcessingFailed", str(exc))
        # Return after sending task failure to avoid raising and causing retries
        return {"status": "CALLBACK_SENT_FAILURE"}

    # Inspect result for failure indicators. Some runner implementations may
    # return a dict containing a 'status' key or error fields instead of
    # raising. Treat explicit failure statuses or presence of error fields
    # as a task failure so Step Functions transitions into the Catch/Fail
    # branch instead of being treated as success.
    try:
        is_failure = False
        failure_reason = None
        if result is None:
            is_failure = True
            failure_reason = "segment_runner returned None"
        elif isinstance(result, dict):
            status = result.get("status")
            if isinstance(status, str) and status.strip().upper() in {"FAILED", "ERROR", "CRITICAL"}:
                is_failure = True
                failure_reason = f"status={status}"
            if not is_failure:
                # If runner returned explicit error details, treat as failure
                if any(k in result for k in ("error", "exception", "traceback", "err")):
                    is_failure = True
                    failure_reason = "result contains error fields"
        else:
            # Non-dict non-None return value -> consider success (will be sent as final_state)
            is_failure = False

        if is_failure:
            cause = None
            try:
                cause = json.dumps(result, ensure_ascii=False)
            except Exception:
                cause = str(result)
            logger.error("Segment runner returned failure-like result: %s", failure_reason)
            _send_task_failure(task_token, "AsyncLLMProcessingFailed", _truncate(cause, 32768))
            return {"status": "CALLBACK_SENT_FAILURE"}
    except Exception:
        # If our inspection logic fails unexpectedly, log and send failure
        logger.exception("Error while evaluating segment runner result")
        _send_task_failure(task_token, "AsyncLLMProcessingFailed", "Failed to evaluate runner result")
        return {"status": "CALLBACK_SENT_FAILURE"}

    final_payload = _build_final_payload(result, segment_to_run)
    # Ensure messages/system_data are preserved so States.JsonMerge does not discard history
    final_payload = _merge_state_preserving_messages(final_payload, current_state)

    try:
        _send_task_success(task_token, final_payload)
    except Exception as exc:
        logger.exception("Failed to send task success for async worker")
        # If callback failed, report failure to Step Functions and surface exception
        _send_task_failure_safe(task_token, "AsyncLLMCallbackFailed", str(exc))
        raise

    return {"status": "CALLBACK_SENT"}


def _build_final_payload(result: Any, segment_index: int) -> Dict[str, Any]:
    """
    세그먼트 실행 결과를 Step Functions 호환 페이로드로 변환.
    
    🚨 [Critical Fix] States.JsonMerge 호환성을 위해 final_state는 항상 dict 보장
    """
    payload: Dict[str, Any] = {
        "next_segment_to_run": segment_index + 1,
        "status": "COMPLETE",
    }

    if isinstance(result, dict):
        status = result.get("status")
        if isinstance(status, str):
            payload["status"] = status
        if result.get("next_segment_to_run") is not None:
            payload["next_segment_to_run"] = result.get("next_segment_to_run")

        # --- 방어 코드 시작 ---
        # 'result'가 명시적인 상태 키를 포함하는지 확인하기 위한 플래그
        has_explicit_state_key = False

        pointer = result.get("final_state_s3_path") or result.get("output_state_s3_path")
        if pointer:
            payload["final_state_s3_path"] = pointer
            has_explicit_state_key = True  # 명시적 키 (S3) 발견

        if result.get("final_state") is not None:
            fs = result.get("final_state")
            # 🚨 [Critical Fix] final_state가 dict가 아니면 래핑
            if isinstance(fs, dict):
                payload["final_state"] = fs
            else:
                payload["final_state"] = {"value": fs, "_wrapped": True}
            has_explicit_state_key = True  # 명시적 키 (inline) 발견
        elif result.get("output_state") is not None:
            os_val = result.get("output_state")
            # 🚨 [Critical Fix] output_state가 dict가 아니면 래핑
            if isinstance(os_val, dict):
                payload["final_state"] = os_val
            else:
                payload["final_state"] = {"value": os_val, "_wrapped": True}
            has_explicit_state_key = True  # 명시적 키 (inline) 발견

        # 'result'가 딕셔너리임에도 불구하고,
        # 위에서 확인한 명시적인 상태 키(s3_path, final_state, output_state)가
        # 하나도 발견되지 않았을 경우, 'result' 자체를 'final_state'로 간주합니다.
        if not has_explicit_state_key:
            payload["final_state"] = result
        # --- 방어 코드 종료 ---
            
    else:
        # 'result'가 딕셔너리가 아닌 경우 (예: 문자열, 숫자, 리스트 등)
        # 🚨 [Critical Fix] States.JsonMerge 호환성을 위해 dict로 래핑
        payload["final_state"] = {"value": result, "_wrapped": True}

    # 🚨 [Critical Fix] final_state가 None이거나 dict가 아닌 경우 최종 보장
    if not isinstance(payload.get("final_state"), dict):
        payload["final_state"] = {"value": payload.get("final_state"), "_wrapped": True}
    
    return payload


def _merge_state_preserving_messages(final_payload: Dict[str, Any], original_state: Any) -> Dict[str, Any]:
    """Ensure final_payload['final_state'] preserves and appends messages and system_data

    - If original_state contains a `messages` list and the final_state contains `messages`,
      concatenate them (original first), trimming to MESSAGES_WINDOW if set.
    - Merge `system_data` dictionaries with final_state taking precedence.
    - Return the possibly-modified final_payload in-place for convenience.
    """
    try:
        if not isinstance(final_payload, dict):
            return final_payload
        fs = final_payload.get('final_state')
        if not isinstance(fs, dict):
            return final_payload

        orig = original_state if isinstance(original_state, dict) else {}

        # messages
        orig_msgs = orig.get('messages') if isinstance(orig.get('messages'), list) else []
        new_msgs = fs.get('messages') if isinstance(fs.get('messages'), list) else []
        if orig_msgs and new_msgs:
            merged = list(orig_msgs) + list(new_msgs)
            try:
                window = int(os.environ.get('MESSAGES_WINDOW', '20'))
            except Exception:
                window = 20
            if len(merged) > window:
                merged = merged[-window:]
            fs['messages'] = merged
        elif orig_msgs and not new_msgs:
            # preserve prior messages if result didn't return any
            fs['messages'] = list(orig_msgs)

        # system_data merge (final_state priority)
        orig_sys = orig.get('system_data') if isinstance(orig.get('system_data'), dict) else {}
        new_sys = fs.get('system_data') if isinstance(fs.get('system_data'), dict) else {}
        merged_sys = dict(orig_sys)
        merged_sys.update(new_sys)
        if merged_sys:
            fs['system_data'] = merged_sys

        final_payload['final_state'] = fs
    except Exception:
        logger.exception('Failed to merge messages/system_data into final_payload (non-fatal)')
    return final_payload


def _coerce_event(event: Any) -> Dict[str, Any]:
    if isinstance(event, dict):
        return event
    if isinstance(event, (bytes, bytearray)):
        try:
            return json.loads(event.decode("utf-8"))
        except Exception:
            logger.warning("Received non-JSON byte payload")
            return {}
    if isinstance(event, str):
        try:
            return json.loads(event)
        except Exception:
            logger.warning("Received non-JSON string payload")
            return {}
    return {}


def _extract_owner_id(payload: Dict[str, Any], workflow_config: Any) -> Optional[str]:
    candidates = [
        payload.get("ownerId"),
        payload.get("owner_id"),
    ]
    if isinstance(workflow_config, dict):
        candidates.extend([
            workflow_config.get("ownerId"),
            workflow_config.get("owner_id"),
        ])
    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


def _extract_workflow_id(payload: Dict[str, Any], workflow_config: Any) -> Optional[str]:
    candidates = [
        payload.get("workflow_id"),
        payload.get("workflowId"),
    ]
    if isinstance(workflow_config, dict):
        candidates.extend([
            workflow_config.get("id"),
            workflow_config.get("workflowId"),
        ])
    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


def _build_state_prefix(owner_id: str, workflow_id: Optional[str], segment_to_run: int) -> str:
    workflow_part = workflow_id or "unknown"
    return f"workflow-states/{owner_id}/{workflow_part}/segments/{segment_to_run}"


def _upload_state(serialized_state: str, bucket: str, key_prefix: str) -> str:
    s3 = boto3.client("s3")
    key = f"{key_prefix.rstrip('/')}/{uuid.uuid4()}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=serialized_state.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def _resolve_state_bucket() -> Optional[str]:
    return os.environ.get("SKELETON_S3_BUCKET") or os.environ.get("WORKFLOW_STATE_BUCKET")


def _resolve_inline_threshold() -> int:
    try:
        return int(os.environ.get("STREAM_INLINE_THRESHOLD_BYTES", "250000"))
    except Exception:
        return 250000


def _send_task_success(task_token: str, data: Dict[str, Any]) -> None:
    client = boto3.client("stepfunctions")
    client.send_task_success(taskToken=task_token, output=json.dumps(data, ensure_ascii=False))


def _send_task_failure(task_token: str, error: str, cause: str) -> None:
    client = boto3.client("stepfunctions")
    client.send_task_failure(
        taskToken=task_token,
        error=_truncate(error, 256),
        cause=_truncate(cause, 32768),
    )


def _send_task_failure_safe(task_token: str, error: str, cause: str) -> None:
    try:
        _send_task_failure(task_token, error, cause)
    except Exception:
        logger.exception("Failed to send task failure callback for token %s", task_token)


def _truncate(value: Any, limit: int) -> str:
    if not isinstance(value, str):
        value = str(value)
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def _check_state_s3_references(current_state: Dict[str, Any]) -> None:
    """
    Fargate Worker에서 추가 안전장치: 현재 상태에 포함된 S3 참조 검사
    매우 큰 상태 객체가 S3에 저장되어 있을 가능성 체크
    """
    import re
    
    # S3 경로 패턴 (state 내부의 s3:// 링크들)
    s3_pattern = r's3://([^/\s]+)/([^\s]+)'
    
    # 상태를 JSON 문자열로 변환해서 S3 링크 찾기
    try:
        state_str = json.dumps(current_state, ensure_ascii=False)
        s3_matches = re.findall(s3_pattern, state_str)
        
        if s3_matches:
            logger.info(f"Found {len(s3_matches)} S3 references in current state")
            
            s3_client = boto3.client('s3')
            LARGE_STATE_THRESHOLD = 500 * 1024 * 1024  # 500MB (상태용은 더 큰 임계값)
            
            for bucket, key in s3_matches:
                try:
                    response = s3_client.head_object(Bucket=bucket, Key=key)
                    file_size_bytes = response['ContentLength']
                    file_size_mb = file_size_bytes / (1024 * 1024)
                    
                    logger.info(f"State S3 reference size: {file_size_mb:.1f}MB (s3://{bucket}/{key})")
                    
                    if file_size_bytes > LARGE_STATE_THRESHOLD:
                        logger.warning(f"Large state S3 reference: {file_size_mb:.1f}MB > 500MB")
                        # Fargate에서는 이미 비동기 처리 중이므로 경고만 로그
                        
                except Exception as e:
                    logger.warning(f"Could not check state S3 reference s3://{bucket}/{key}: {e}")
                    continue
                    
    except Exception as e:
        logger.warning(f"Could not check state S3 references: {e}")
        return


def fargate_worker_main():
    """
    Fargate 컨테이너에서 실행되는 메인 엔트리포인트
    환경변수로부터 worker_event를 읽어서 처리
    """
    import sys
    
    try:
        # 환경변수에서 worker event 로드
        worker_event_json = os.environ.get("WORKER_EVENT")
        if not worker_event_json:
            logger.error("WORKER_EVENT environment variable not found")
            sys.exit(1)
            
        worker_event = json.loads(worker_event_json)
        logger.info("Fargate worker started with event")
        
        # Worker 실행 (context는 None - Fargate에서는 Lambda context 없음)
        result = _execute_worker(worker_event, context=None)
        
        logger.info(f"Fargate worker completed successfully: {result}")
        sys.exit(0)
        
    except Exception as exc:
        logger.exception("Fargate worker failed")
        
        # Task Token으로 실패 알림 시도
        try:
            worker_event = json.loads(os.environ.get("WORKER_EVENT", "{}"))
            task_token = worker_event.get("task_token") or worker_event.get("TaskToken")
            if task_token:
                _send_task_failure_safe(task_token, "FargateWorkerFailed", str(exc))
        except Exception:
            logger.exception("Failed to send task failure from src.Fargate worker")
        
        sys.exit(1)


if __name__ == "__main__":
    # Fargate 컨테이너에서 직접 실행될 때
    fargate_worker_main()
