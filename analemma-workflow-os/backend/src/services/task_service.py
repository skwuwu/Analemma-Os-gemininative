# -*- coding: utf-8 -*-
"""
Task Service

Task Manager UI를 위한 비즈니스 로직 서비스입니다.
기술적인 실행 로그를 비즈니스 친화적인 Task 정보로 변환합니다.

[v2.0] 개선사항:
- GSI 기반 효율적인 task_id 조회 (FilterExpression 제거)
- 중요 이벤트 DynamoDB 영속화 (State Drift 방지)
- S3 Presigned URL 지원 (대용량 아티팩트)
- 타임스탬프 검증 강화
"""

import os
import logging
import json
import asyncio
import time
import uuid
from functools import partial
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from src.common.aws_clients import get_dynamodb_resource, get_s3_client
from src.common.constants import DynamoDBConfig
from src.models.task_context import (
    TaskContext,
    TaskStatus,
    ArtifactType,
    ArtifactPreview,
    AgentThought,
    convert_technical_status,
    get_friendly_error_message,
)

# 비즈니스 메트릭스 계산 모듈 임포트
try:
    from backend.services.business_metrics_calculator import (
        calculate_all_business_metrics,
    )
except ImportError:
    try:
        from src.services.business_metrics_calculator import (
            calculate_all_business_metrics,
        )
    except ImportError:
        # 폴백: 모듈 없으면 빈 딕셔너리 반환
        def calculate_all_business_metrics(*args, **kwargs):
            return {}

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# 상수 정의
# ═══════════════════════════════════════════════════════════════════════════════
# S3 Presigned URL 만료 시간 (1시간)
PRESIGNED_URL_EXPIRY_SECONDS = int(os.environ.get('PRESIGNED_URL_EXPIRY_SECONDS', '3600'))
# 이벤트 스토어용 테이블
TASK_EVENTS_TABLE = os.environ.get('TASK_EVENTS_TABLE', 'TaskEventsTable')
# S3 버킷
WORKFLOW_STATE_BUCKET = os.environ.get('WORKFLOW_STATE_BUCKET', '')


class TaskService:
    """
    Task Manager 서비스
    
    - 기술적 실행 로그를 Task 컨텍스트로 변환
    - Task 목록 조회 (필터링 지원)
    - Task 상세 정보 조회
    - 실시간 Task 컨텍스트 업데이트
    """
    
    def __init__(
        self,
        execution_table: Optional[str] = None,
        notification_table: Optional[str] = None,
        task_events_table: Optional[str] = None,
    ):
        """
        Args:
            execution_table: 실행 상태 테이블 이름
            notification_table: 알림 테이블 이름
            task_events_table: 태스크 이벤트 스토어 테이블 이름 (중요 이벤트 영속화)
        """
        self.execution_table_name = execution_table or os.environ.get(
            'EXECUTIONS_TABLE', DynamoDBConfig.EXECUTIONS_TABLE if hasattr(DynamoDBConfig, 'EXECUTIONS_TABLE') else 'ExecutionsTableV3'
        )
        self.notification_table_name = notification_table or os.environ.get(
            'PENDING_NOTIFICATIONS_TABLE', DynamoDBConfig.PENDING_NOTIFICATIONS_TABLE if hasattr(DynamoDBConfig, 'PENDING_NOTIFICATIONS_TABLE') else 'PendingNotificationsTableV3'
        )
        self.task_events_table_name = task_events_table or TASK_EVENTS_TABLE
        self._execution_table = None
        self._notification_table = None
        self._task_events_table = None
        self._s3_client = None
    
    @property
    def execution_table(self):
        """지연 초기화된 실행 테이블"""
        if self._execution_table is None:
            dynamodb_resource = get_dynamodb_resource()
            self._execution_table = dynamodb_resource.Table(self.execution_table_name)
        return self._execution_table

    @property
    def notification_table(self):
        """지연 초기화된 알림 테이블"""
        if self._notification_table is None:
            dynamodb_resource = get_dynamodb_resource()
            self._notification_table = dynamodb_resource.Table(self.notification_table_name)
        return self._notification_table

    @property
    def task_events_table(self):
        """지연 초기화된 태스크 이벤트 스토어 테이블"""
        if self._task_events_table is None:
            dynamodb_resource = get_dynamodb_resource()
            self._task_events_table = dynamodb_resource.Table(self.task_events_table_name)
        return self._task_events_table

    @property
    def s3_client(self):
        """지연 초기화된 S3 클라이언트"""
        if self._s3_client is None:
            self._s3_client = get_s3_client()
        return self._s3_client

    async def get_tasks(
        self,
        owner_id: str,
        status_filter: Optional[str] = None,
        limit: int = 50,
        include_completed: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Task 목록 조회
        
        Args:
            owner_id: 사용자 ID
            status_filter: 상태 필터 (pending_approval, in_progress, completed 등)
            limit: 최대 조회 개수
            include_completed: 완료된 Task 포함 여부
            
        Returns:
            Task 요약 정보 목록
        """
        try:
            # 기존 notification 테이블에서 조회
            query_func = partial(
                self.notification_table.query,
                KeyConditionExpression="ownerId = :oid",
                ExpressionAttributeValues={
                    ":oid": owner_id
                },
                ScanIndexForward=False,  # 최신순
                Limit=limit
            )
            response = await asyncio.get_event_loop().run_in_executor(None, query_func)
            
            items = response.get('Items', [])
            tasks = []
            
            for item in items:
                task = self._convert_notification_to_task(item)
                if task:
                    # 필터 적용
                    if status_filter:
                        if task.get('status') != status_filter:
                            continue
                    if not include_completed and task.get('status') == 'completed':
                        continue
                    
                    tasks.append(task)
            
            return tasks[:limit]
            
        except ClientError as e:
            logger.error(f"Failed to get tasks: {e}")
            return []

    async def get_task_detail(
        self,
        task_id: str,
        owner_id: str,
        include_technical_logs: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Task 상세 정보 조회
        
        [v2.0] GSI 기반 효율적인 조회로 개선
        - 기존: FilterExpression으로 전체 스캔 후 필터링 (RCU 낭비)
        - 개선: ExecutionIdIndex GSI로 직접 조회 (O(1) 조회)
        
        Args:
            task_id: Task ID (execution_id)
            owner_id: 사용자 ID
            include_technical_logs: 기술 로그 포함 여부 (권한에 따라)
            
        Returns:
            Task 상세 정보
        """
        try:
            # [v2.0] GSI를 사용한 효율적인 조회
            # ExecutionIdIndex: ownerId (PK), execution_id (SK)
            execution_id_index = os.environ.get(
                'EXECUTION_ID_INDEX', 
                DynamoDBConfig.EXECUTION_ID_INDEX if hasattr(DynamoDBConfig, 'EXECUTION_ID_INDEX') else 'ExecutionIdIndex'
            )
            
            query_func = partial(
                self.notification_table.query,
                IndexName=execution_id_index,
                KeyConditionExpression="ownerId = :oid AND execution_id = :eid",
                ExpressionAttributeValues={
                    ":oid": owner_id,
                    ":eid": task_id
                },
                ScanIndexForward=False,  # 최신순
                Limit=1
            )
            
            try:
                response = await asyncio.get_event_loop().run_in_executor(None, query_func)
            except ClientError as gsi_error:
                # GSI가 없는 경우 폴백 (단, 경고 로그 출력)
                if 'ValidationException' in str(gsi_error) or 'ResourceNotFoundException' in str(gsi_error):
                    logger.warning(
                        f"ExecutionIdIndex GSI not found, falling back to scan. "
                        f"Consider adding GSI for better performance. Error: {gsi_error}"
                    )
                    return await self._get_task_detail_fallback(task_id, owner_id, include_technical_logs)
                raise
            
            items = response.get('Items', [])
            if not items:
                # GSI에서 못찾은 경우 이벤트 스토어에서 조회 시도
                return await self._get_task_from_event_store(task_id, owner_id, include_technical_logs)
            
            # 가장 최신 항목 사용
            latest = items[0]
            task = self._convert_notification_to_task(latest, detailed=True)
            
            if include_technical_logs and task:
                task['technical_logs'] = self._extract_technical_logs(latest)
            
            return task
            
        except ClientError as e:
            logger.error(f"Failed to get task detail: {e}")
            return None

    async def _get_task_detail_fallback(
        self,
        task_id: str,
        owner_id: str,
        include_technical_logs: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        GSI가 없는 경우의 폴백 조회 (레거시 호환)
        
        ⚠️ 경고: 이 방식은 RCU를 많이 소모합니다.
        프로덕션에서는 반드시 ExecutionIdIndex GSI를 추가하세요.
        """
        logger.warning(
            f"Using fallback query for task_id={task_id}. "
            "This is inefficient - add ExecutionIdIndex GSI!"
        )
        
        query_func = partial(
            self.notification_table.query,
            KeyConditionExpression="ownerId = :oid",
            FilterExpression="contains(notification, :tid)",
            ExpressionAttributeValues={
                ":oid": owner_id,
                ":tid": task_id
            },
            ScanIndexForward=False,
            Limit=50  # 더 많이 읽어서 필터링
        )
        response = await asyncio.get_event_loop().run_in_executor(None, query_func)
        
        items = response.get('Items', [])
        if not items:
            return None
        
        latest = items[0]
        task = self._convert_notification_to_task(latest, detailed=True)
        
        if include_technical_logs and task:
            task['technical_logs'] = self._extract_technical_logs(latest)
        
        return task

    async def _get_task_from_event_store(
        self,
        task_id: str,
        owner_id: str,
        include_technical_logs: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        이벤트 스토어에서 태스크 정보 조회
        
        notification 테이블에 없는 경우 이벤트 스토어에서 재구성
        """
        try:
            query_func = partial(
                self.task_events_table.query,
                KeyConditionExpression="task_id = :tid",
                ExpressionAttributeValues={
                    ":tid": task_id
                },
                ScanIndexForward=True,  # 시간순
            )
            response = await asyncio.get_event_loop().run_in_executor(None, query_func)
            
            events = response.get('Items', [])
            if not events:
                return None
            
            # 이벤트들로부터 태스크 상태 재구성
            return self._reconstruct_task_from_events(events, include_technical_logs)
            
        except ClientError as e:
            # 테이블이 없는 경우 무시
            if 'ResourceNotFoundException' in str(e):
                return None
            logger.error(f"Failed to get task from event store: {e}")
            return None

    def _convert_notification_to_task(
        self,
        notification: Dict[str, Any],
        detailed: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Notification 데이터를 Task 형식으로 변환
        
        Args:
            notification: DynamoDB notification 항목
            detailed: 상세 정보 포함 여부
            
        Returns:
            Task 정보 딕셔너리
        """
        try:
            payload = notification.get('notification', {}).get('payload', {})
            if not payload:
                return None
            
            execution_id = payload.get('execution_id', '')
            technical_status = payload.get('status', 'UNKNOWN')
            task_status = convert_technical_status(technical_status)
            
            # 진행률 계산
            current_segment = payload.get('current_segment', 0)
            total_segments = payload.get('total_segments', 1)
            progress = int((current_segment / max(total_segments, 1)) * 100)
            
            # 기본 Task 정보
            task = {
                "task_id": execution_id,
                "task_summary": self._generate_task_summary(payload),
                "agent_name": "AI Assistant",
                "status": task_status.value,
                "progress_percentage": progress,
                "current_step_name": payload.get('current_step_label', ''),
                "current_thought": self._generate_current_thought(payload),
                "is_interruption": technical_status == 'PAUSED_FOR_HITP',
                "started_at": self._format_timestamp(payload.get('start_time')),
                "updated_at": self._format_timestamp(notification.get('timestamp')),
                "workflow_name": payload.get('workflow_name', ''),
                "workflow_id": payload.get('workflowId', ''),
            }
            
            # 에러 정보 (사용자 친화적으로 변환)
            if task_status == TaskStatus.FAILED:
                error = payload.get('error', '')
                if error:
                    message, suggestion = get_friendly_error_message(str(error))
                    task['error_message'] = message
                    task['error_suggestion'] = suggestion
            
            # 비즈니스 가치 지표 추가 (새로운 계산 로직)
            business_metrics = calculate_all_business_metrics(
                payload=payload,
                execution_id=execution_id,
                progress=progress,
                status=technical_status
            )
            task.update(business_metrics)
            
            # 상세 정보
            if detailed:
                task['pending_decision'] = None
                if technical_status == 'PAUSED_FOR_HITP':
                    task['pending_decision'] = {
                        "question": "계속 진행하시겠습니까?",
                        "context": payload.get('message', ''),
                        "pre_hitp_output": payload.get('pre_hitp_output', {}),
                    }
                
                task['artifacts'] = self._extract_artifacts(payload)
                task['thought_history'] = self._extract_thought_history(payload)
                
                # 비용 정보
                step_function_state = payload.get('step_function_state', {})
                if step_function_state:
                    task['token_usage'] = step_function_state.get('token_usage', {})
            
            return task
            
        except Exception as e:
            logger.error(f"Failed to convert notification to task: {e}")
            return None

    def _generate_task_summary(self, payload: Dict[str, Any]) -> str:
        """워크플로우 정보로부터 Task 요약 생성"""
        workflow_name = payload.get('workflow_name', '')
        if workflow_name:
            return f"{workflow_name} 실행"
        
        workflow_id = payload.get('workflowId', '')
        if workflow_id:
            return f"워크플로우 {workflow_id[:8]}... 실행"
        
        return "AI 작업 진행 중"

    def _generate_current_thought(self, payload: Dict[str, Any]) -> str:
        """현재 상태에 따른 에이전트 사고 메시지 생성"""
        status = payload.get('status', '')
        current_step = payload.get('current_step_label', '')
        
        if status == 'PAUSED_FOR_HITP':
            return "사용자의 승인을 기다리고 있습니다."
        
        if status == 'RUNNING':
            if current_step:
                return f"{current_step} 단계를 처리하고 있습니다..."
            return "작업을 처리하고 있습니다..."
        
        if status == 'COMPLETE' or status == 'COMPLETED':
            return "작업이 완료되었습니다."
        
        if status == 'FAILED':
            return "작업 중 문제가 발생했습니다."
        
        return "작업을 준비하고 있습니다..."

    def _format_timestamp(self, ts: Any) -> Optional[str]:
        """
        타임스탬프를 ISO 형식으로 변환
        
        [v2.0] 검증 강화: 잘못된 입력에 대해 None 반환 (프론트엔드 파싱 에러 방지)
        """
        if ts is None:
            return None
        
        try:
            # 숫자형 타임스탬프 처리
            if isinstance(ts, (int, float)):
                # 음수나 비정상적으로 작은 값 거부
                if ts <= 0:
                    return None
                
                # 밀리초/초 판단 (13자리 이상이면 밀리초)
                if ts > 1e12:
                    ts_seconds = ts / 1000
                else:
                    ts_seconds = ts
                
                # 합리적인 범위 검증 (1970년 ~ 2100년)
                if ts_seconds < 0 or ts_seconds > 4102444800:  # 2100-01-01
                    return None
                
                return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).isoformat()
            
            # 문자열 타임스탬프 처리
            if isinstance(ts, str):
                # 이미 ISO 형식인 경우 검증 후 반환
                try:
                    # ISO 형식 파싱 시도
                    parsed = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    return parsed.isoformat()
                except ValueError:
                    pass
                
                # 숫자 문자열인 경우 재귀 처리
                try:
                    numeric_ts = float(ts)
                    return self._format_timestamp(numeric_ts)
                except ValueError:
                    pass
                
                # 파싱 불가능한 문자열은 None 반환
                logger.warning(f"Unable to parse timestamp: {ts}")
                return None
            
            # 기타 타입은 None 반환
            return None
            
        except (ValueError, OSError, OverflowError) as e:
            logger.warning(f"Timestamp parsing error for {ts}: {e}")
            return None

    def _extract_artifacts(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        실행 결과물 추출
        
        [v2.0] S3 Presigned URL 지원
        - 대용량 JSON/이미지 파일은 S3에서 Presigned URL 생성
        - 작은 데이터는 직접 포함
        """
        artifacts = []
        
        # step_function_state에서 output 추출
        step_state = payload.get('step_function_state', {})
        final_output = step_state.get('final_state', {}) or step_state.get('output', {})
        
        if isinstance(final_output, dict):
            for key, value in final_output.items():
                if key.startswith('_'):  # 내부 필드 제외
                    continue
                
                artifact = self._create_artifact(key, value)
                if artifact:
                    artifacts.append(artifact)
        
        return artifacts

    def _create_artifact(self, key: str, value: Any) -> Optional[Dict[str, Any]]:
        """
        단일 아티팩트 생성
        
        S3 참조인 경우 Presigned URL 생성
        """
        if value is None:
            return None
        
        artifact = {
            "artifact_id": f"output_{key}",
            "title": key,
        }
        
        # S3 참조 감지 (s3:// URL 또는 S3 메타데이터 객체)
        if isinstance(value, str) and value.startswith('s3://'):
            artifact.update(self._handle_s3_artifact(value))
        elif isinstance(value, dict) and 's3_bucket' in value and 's3_key' in value:
            s3_uri = f"s3://{value['s3_bucket']}/{value['s3_key']}"
            artifact.update(self._handle_s3_artifact(s3_uri, value))
        else:
            # 일반 데이터 처리
            artifact.update(self._handle_inline_artifact(key, value))
        
        return artifact

    def _handle_s3_artifact(
        self, 
        s3_uri: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        S3 아티팩트 처리: Presigned URL 생성
        """
        try:
            # s3://bucket/key 파싱
            if s3_uri.startswith('s3://'):
                parts = s3_uri[5:].split('/', 1)
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else ''
            else:
                return {"artifact_type": "error", "preview_content": "Invalid S3 URI"}
            
            # 파일 확장자로 타입 추론
            extension = key.rsplit('.', 1)[-1].lower() if '.' in key else ''
            artifact_type = self._infer_artifact_type(extension)
            
            # Presigned URL 생성
            try:
                presigned_url = self.s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket, 'Key': key},
                    ExpiresIn=PRESIGNED_URL_EXPIRY_SECONDS
                )
            except ClientError as e:
                logger.error(f"Failed to generate presigned URL for {s3_uri}: {e}")
                presigned_url = None
            
            result = {
                "artifact_type": artifact_type,
                "s3_uri": s3_uri,
                "download_url": presigned_url,
                "preview_content": f"[S3 파일] {key.rsplit('/', 1)[-1]}",
            }
            
            # 메타데이터 추가
            if metadata:
                result["file_size"] = metadata.get('size', metadata.get('file_size'))
                result["content_type"] = metadata.get('content_type')
            
            return result
            
        except Exception as e:
            logger.error(f"S3 artifact handling error: {e}")
            return {"artifact_type": "error", "preview_content": str(e)}

    def _handle_inline_artifact(self, key: str, value: Any) -> Dict[str, Any]:
        """
        인라인 아티팩트 처리 (작은 데이터)
        """
        # 데이터 크기 확인
        str_value = str(value) if not isinstance(value, str) else value
        
        if len(str_value) > 10000:  # 10KB 이상은 잘라서 프리뷰만
            return {
                "artifact_type": "data",
                "preview_content": str_value[:500] + "... [truncated]",
                "is_truncated": True,
                "full_size": len(str_value),
            }
        
        # 타입 추론
        artifact_type = "data"
        if isinstance(value, dict):
            artifact_type = "json"
        elif isinstance(value, list):
            artifact_type = "array"
        elif key.endswith('_html') or '<html' in str_value.lower()[:100]:
            artifact_type = "html"
        elif key.endswith('_markdown') or key.endswith('_md'):
            artifact_type = "markdown"
        
        return {
            "artifact_type": artifact_type,
            "preview_content": str_value[:500] if len(str_value) > 500 else str_value,
            "full_content": value if len(str_value) <= 10000 else None,
        }

    def _infer_artifact_type(self, extension: str) -> str:
        """파일 확장자로 아티팩트 타입 추론"""
        type_map = {
            # 이미지
            'jpg': 'image', 'jpeg': 'image', 'png': 'image', 
            'gif': 'image', 'webp': 'image', 'svg': 'image',
            # 문서
            'pdf': 'document', 'doc': 'document', 'docx': 'document',
            'xls': 'spreadsheet', 'xlsx': 'spreadsheet', 'csv': 'spreadsheet',
            # 코드/데이터
            'json': 'json', 'xml': 'data', 'yaml': 'data', 'yml': 'data',
            'html': 'html', 'md': 'markdown',
            # 미디어
            'mp3': 'audio', 'wav': 'audio', 'mp4': 'video', 'webm': 'video',
            # 압축
            'zip': 'archive', 'tar': 'archive', 'gz': 'archive',
        }
        return type_map.get(extension, 'file')

    def _reconstruct_task_from_events(
        self, 
        events: List[Dict[str, Any]],
        include_technical_logs: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        이벤트들로부터 태스크 상태 재구성
        
        WebSocket 메시지를 놓친 경우 이벤트 스토어에서 복원
        """
        if not events:
            return None
        
        # 최신 이벤트에서 기본 정보 추출
        latest = events[-1]
        first = events[0]
        
        task = {
            "task_id": latest.get('task_id'),
            "task_summary": latest.get('task_summary', ''),
            "agent_name": "AI Assistant",
            "status": latest.get('status', 'unknown'),
            "progress_percentage": latest.get('progress', 0),
            "current_step_name": latest.get('current_step', ''),
            "current_thought": latest.get('thought', ''),
            "started_at": self._format_timestamp(first.get('timestamp')),
            "updated_at": self._format_timestamp(latest.get('timestamp')),
            "workflow_name": latest.get('workflow_name', ''),
            "workflow_id": latest.get('workflow_id', ''),
        }
        
        # 사고 히스토리 재구성
        task['thought_history'] = [
            {
                "thought_id": e.get('event_id', str(i)),
                "timestamp": self._format_timestamp(e.get('timestamp')),
                "thought_type": e.get('thought_type', 'progress'),
                "message": e.get('thought', ''),
            }
            for i, e in enumerate(events)
            if e.get('thought')
        ]
        
        return task

    def _extract_thought_history(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """사고 과정 히스토리 추출"""
        from src.models.task_context import THOUGHT_HISTORY_MAX_LENGTH
        
        thoughts = []
        
        # state_history에서 추출
        state_history = payload.get('state_history', [])
        if not state_history:
            step_state = payload.get('step_function_state', {})
            state_history = step_state.get('state_history', [])
        
        for entry in state_history[-THOUGHT_HISTORY_MAX_LENGTH:]:  # 상수 사용
            thought = {
                "thought_id": entry.get('id', str(hash(str(entry)))),
                "timestamp": entry.get('entered_at', ''),
                "thought_type": "progress",
                "message": self._state_to_thought_message(entry),
                "node_id": entry.get('node_id', entry.get('state_name', '')),
            }
            thoughts.append(thought)
        
        return thoughts

    def _state_to_thought_message(self, state_entry: Dict[str, Any]) -> str:
        """상태 엔트리를 사고 메시지로 변환"""
        state_name = state_entry.get('state_name', state_entry.get('name', ''))
        status = state_entry.get('status', '')
        
        if status == 'COMPLETED':
            return f"'{state_name}' 단계를 완료했습니다."
        elif status == 'RUNNING':
            return f"'{state_name}' 단계를 처리하고 있습니다..."
        elif status == 'FAILED':
            return f"'{state_name}' 단계에서 문제가 발생했습니다."
        
        return f"'{state_name}' 단계 진행 중"

    def _extract_technical_logs(self, notification: Dict[str, Any]) -> List[Dict[str, Any]]:
        """기술 로그 추출 (개발자 모드용)"""
        logs = []
        
        payload = notification.get('notification', {}).get('payload', {})
        step_state = payload.get('step_function_state', {})
        state_history = step_state.get('state_history', [])
        
        for entry in state_history:
            log = {
                "timestamp": entry.get('entered_at'),
                "node_id": entry.get('state_name', entry.get('node_id', '')),
                "input": entry.get('input'),
                "output": entry.get('output'),
                "duration": entry.get('duration', 0),
                "error": entry.get('error'),
            }
            logs.append(log)
        
        return logs

    def update_task_context(
        self,
        execution_id: str,
        thought: str,
        progress: Optional[int] = None,
        current_step: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Task 컨텍스트 업데이트 (노드 실행 중 호출)
        
        실시간으로 Task의 상태를 업데이트합니다.
        WebSocket과 DynamoDB에 동시에 반영됩니다.
        
        Args:
            execution_id: 실행 ID
            thought: 에이전트 사고 메시지
            progress: 진행률 (0-100)
            current_step: 현재 단계 이름
            
        Returns:
            WebSocket 페이로드
        """
        ws_payload = {
            "type": "task_update",
            "task_id": execution_id,
            "thought": thought,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        if progress is not None:
            ws_payload["progress"] = progress
        if current_step:
            ws_payload["current_step"] = current_step
        
        return ws_payload


class ContextAwareLogger:
    """
    컨텍스트 인식 로거
    
    기존 logging.Logger를 확장하여 Task 컨텍스트 업데이트를 함께 수행합니다.
    
    [v2.0] 개선사항:
    - 중요 이벤트(is_important=True)는 DynamoDB에도 영속화
    - WebSocket 연결 끊김 시에도 이벤트 스토어에서 복구 가능
    - State Drift 방지
    """
    
    # 중요 이벤트 타입 (DynamoDB에 영속화)
    IMPORTANT_THOUGHT_TYPES = {'decision', 'success', 'error', 'warning'}
    
    def __init__(
        self,
        execution_id: str,
        owner_id: Optional[str] = None,
        task_service: Optional[TaskService] = None,
        websocket_callback: Optional[Callable] = None,
        persist_important_events: bool = True,
    ):
        """
        Args:
            execution_id: 실행 ID
            owner_id: 소유자 ID (이벤트 영속화에 필요)
            task_service: TaskService 인스턴스
            websocket_callback: WebSocket 전송 콜백 함수
            persist_important_events: 중요 이벤트를 DynamoDB에 저장할지 여부
        """
        self.execution_id = execution_id
        self.owner_id = owner_id
        self.task_service = task_service or TaskService()
        self.websocket_callback = websocket_callback
        self.persist_important_events = persist_important_events
        self._logger = logging.getLogger(f"task.{execution_id[:8]}")
        self._progress = 0
        self._current_step = ""
        self._workflow_name = ""
        self._workflow_id = ""

    def set_workflow_context(self, workflow_name: str, workflow_id: str) -> None:
        """워크플로우 컨텍스트 설정"""
        self._workflow_name = workflow_name
        self._workflow_id = workflow_id

    def report_thought(
        self,
        message: str,
        thought_type: str = "progress",
        progress: Optional[int] = None,
        current_step: Optional[str] = None,
        is_important: Optional[bool] = None,
    ) -> None:
        """
        에이전트 사고 보고
        
        사용자에게 보여줄 메시지를 기록하고 WebSocket으로 전송합니다.
        
        [v2.0] 중요 이벤트는 DynamoDB에도 저장하여 State Drift 방지
        
        Args:
            message: 사용자에게 보여줄 메시지
            thought_type: 사고 유형 (progress, decision, warning, success, error)
            progress: 진행률 업데이트
            current_step: 현재 단계 업데이트
            is_important: 중요 이벤트 여부 (None이면 thought_type으로 자동 판단)
        """
        if progress is not None:
            self._progress = progress
        if current_step:
            self._current_step = current_step
        
        # 기술 로그도 기록
        self._logger.info(f"[{thought_type.upper()}] {message}")
        
        # WebSocket 페이로드 생성
        payload = self.task_service.update_task_context(
            self.execution_id,
            thought=message,
            progress=self._progress,
            current_step=self._current_step,
        )
        payload["thought_type"] = thought_type
        
        # WebSocket 전송
        if self.websocket_callback:
            try:
                self.websocket_callback(payload)
            except Exception as e:
                self._logger.error(f"Failed to send WebSocket message: {e}")
        
        # [v2.0] 중요 이벤트는 DynamoDB에 영속화
        should_persist = is_important if is_important is not None else (
            thought_type in self.IMPORTANT_THOUGHT_TYPES
        )
        
        if should_persist and self.persist_important_events:
            self._persist_event(payload, thought_type, message)

    def _persist_event(
        self, 
        payload: Dict[str, Any], 
        thought_type: str,
        message: str
    ) -> None:
        """
        중요 이벤트를 DynamoDB 이벤트 스토어에 저장
        
        WebSocket 연결이 끊겨도 이 데이터로 상태 복구 가능
        """
        if not self.owner_id:
            self._logger.debug("owner_id not set, skipping event persistence")
            return
        
        try:
            event_item = {
                'task_id': self.execution_id,
                'event_id': f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}",
                'owner_id': self.owner_id,
                'timestamp': int(time.time() * 1000),
                'thought_type': thought_type,
                'thought': message,
                'progress': self._progress,
                'current_step': self._current_step,
                'workflow_name': self._workflow_name,
                'workflow_id': self._workflow_id,
                'status': payload.get('status', 'in_progress'),
                # TTL: 7일 후 자동 삭제
                'ttl': int(time.time()) + (7 * 24 * 60 * 60),
            }
            
            # 비동기로 저장 (메인 플로우 블로킹 방지)
            try:
                self.task_service.task_events_table.put_item(Item=event_item)
            except ClientError as e:
                # 테이블이 없는 경우 무시 (선택적 기능)
                if 'ResourceNotFoundException' not in str(e):
                    raise
                    
        except Exception as e:
            # 영속화 실패해도 메인 플로우는 계속 진행
            self._logger.warning(f"Failed to persist event: {e}")

    def report_progress(self, percentage: int, step_name: str = "") -> None:
        """진행률 업데이트"""
        self.report_thought(
            f"{step_name} 처리 중..." if step_name else "작업 진행 중...",
            thought_type="progress",
            progress=percentage,
            current_step=step_name,
            is_important=False,  # 진행률은 중요 이벤트가 아님
        )

    def report_decision(self, question: str, context: str = "") -> None:
        """의사결정 요청 보고"""
        message = f"결정이 필요합니다: {question}"
        if context:
            message += f" ({context})"
        self.report_thought(message, thought_type="decision", is_important=True)

    def report_success(self, message: str) -> None:
        """성공 보고"""
        self.report_thought(message, thought_type="success", progress=100, is_important=True)

    def report_error(self, error: str) -> None:
        """에러 보고 (사용자 친화적으로 변환)"""
        from src.models.task_context import get_friendly_error_message
        friendly_message, _ = get_friendly_error_message(error)
        self.report_thought(friendly_message, thought_type="error", is_important=True)
        self._logger.error(f"Original error: {error}")

    def report_warning(self, message: str) -> None:
        """경고 보고"""
        self.report_thought(message, thought_type="warning", is_important=True)
