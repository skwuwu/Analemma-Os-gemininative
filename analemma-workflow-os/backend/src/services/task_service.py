# -*- coding: utf-8 -*-
"""
Task Service

Business logic service for Task Manager UI.
Converts technical execution logs into business-friendly Task information.

[v2.0] Improvements:
- Efficient task_id lookup based on GSI (removed FilterExpression)
- Critical event DynamoDB persistence (prevents State Drift)
- S3 Presigned URL support (large artifacts)
- Enhanced timestamp validation
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

# Import business metrics calculation module
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
        # Fallback: Return empty dictionary if module not found
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
        
        [v2.1] Executions 테이블에서 직접 조회로 변경
        - 완료된 워크플로우도 히스토리에 표시
        - OwnerIdStartDateIndex GSI 사용 (최신순 정렬)
        
        Args:
            owner_id: 사용자 ID
            status_filter: 상태 필터 (pending_approval, in_progress, completed 등)
            limit: 최대 조회 개수
            include_completed: 완료된 Task 포함 여부
            
        Returns:
            Task 요약 정보 목록
        """
        try:
            # [v2.1] Executions 테이블에서 직접 조회
            owner_index = os.environ.get('OWNER_INDEX', 'OwnerIdStartDateIndex')
            
            query_func = partial(
                self.execution_table.query,
                IndexName=owner_index,
                KeyConditionExpression="ownerId = :oid",
                ExpressionAttributeValues={
                    ":oid": owner_id
                },
                ScanIndexForward=False,  # 최신순
                Limit=limit * 2  # 필터링 후 충분한 결과를 위해 2배 조회
            )
            response = await asyncio.get_event_loop().run_in_executor(None, query_func)
            
            items = response.get('Items', [])
            tasks = []
            
            for item in items:
                task = self._convert_execution_to_task(item)
                if task:
                    # 필터 적용
                    if status_filter:
                        if task.get('status') != status_filter:
                            continue
                    if not include_completed and task.get('status') == 'completed':
                        continue
                    
                    tasks.append(task)
                    
                    # limit 도달하면 중단
                    if len(tasks) >= limit:
                        break
            
            return tasks
            
        except ClientError as e:
            logger.error(f"Failed to get tasks from executions table: {e}")
            # Fallback: notification 테이블에서 조회 (진행 중인 작업만)
            try:
                query_func = partial(
                    self.notification_table.query,
                    KeyConditionExpression="ownerId = :oid",
                    ExpressionAttributeValues={
                        ":oid": owner_id
                    },
                    ScanIndexForward=False,
                    Limit=limit
                )
                response = await asyncio.get_event_loop().run_in_executor(None, query_func)
                items = response.get('Items', [])
                tasks = []
                
                for item in items:
                    task = self._convert_notification_to_task(item)
                    if task:
                        if status_filter and task.get('status') != status_filter:
                            continue
                        if not include_completed and task.get('status') == 'completed':
                            continue
                        tasks.append(task)
                
                return tasks[:limit]
            except Exception as fallback_error:
                logger.error(f"Fallback query also failed: {fallback_error}")
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
                # [FIX] Notification 테이블에 없으면 Executions 테이블에서 직접 조회
                logger.info(f"Task {task_id[:8]}... not found in notifications, checking executions table")
                execution_task = await self._get_task_from_executions_table(task_id, owner_id, include_technical_logs)
                if execution_task:
                    return execution_task
                
                # 마지막으로 이벤트 스토어에서 조회 시도
                return await self._get_task_from_event_store(task_id, owner_id, include_technical_logs)
            
            # 가장 최신 항목 사용
            latest = items[0]
            task = self._convert_notification_to_task(latest, detailed=True)
            
            return task
            
        except ClientError as e:
            logger.error(f"Failed to get task detail: {e}")
            return None

    async def get_task_outcomes(self, task_id: str, owner_id: str) -> Optional[Dict[str, Any]]:
        """
        Task 결과물(Outcomes) 조회
        
        완성된 아티팩트 목록과 축약된 히스토리를 반환합니다.
        """
        task_detail = await self.get_task_detail(task_id, owner_id)
        if not task_detail:
            return None
            
        artifacts = task_detail.get('artifacts', [])
        
        # 축약 히스토리 생성
        thought_history = task_detail.get('thought_history', [])
        summary = f"{len(thought_history)}개의 사고 과정을 거쳐 {task_detail.get('status')} 되었습니다."
        
        collapsed_history = {
            'summary': summary,
            'node_count': len(set(t.get('node_id') for t in thought_history if t.get('node_id'))),
            'llm_call_count': len([t for t in thought_history if t.get('thought_type') == 'thinking']),
            'total_duration_seconds': None,
            'key_decisions': [t.get('message') for t in thought_history if t.get('thought_type') == 'decision'][:3],
            'full_trace_available': True
        }
        
        return {
            'task_id': task_id,
            'task_title': task_detail.get('task_summary'),
            'status': task_detail.get('status'),
            'outcomes': artifacts,
            'collapsed_history': collapsed_history,
            'correction_applied': False,
            'last_updated': task_detail.get('updated_at')
        }

    async def get_task_metrics(self, task_id: str, owner_id: str) -> Optional[Dict[str, Any]]:
        """
        Task 메트릭 조회 (Bento Grid용)
        """
        task_detail = await self.get_task_detail(task_id, owner_id)
        if not task_detail:
            return None
            
        progress = task_detail.get('progress_percentage', 0)
        status = task_detail.get('status', 'unknown')
        
        # Confidence Score
        confidence_score = task_detail.get('confidence_score', 0)
        if confidence_score == 0 and status == 'completed':
            confidence_score = 95
            
        # Autonomy Rate
        autonomy_rate = task_detail.get('autonomy_rate', 100)
        
        # Intervention History
        intervention_history = task_detail.get('intervention_history', {
            'count': 0, 
            'summary': '정상 실행됨',
            'positive_count': 0,
            'negative_count': 0,
            'history': []
        })
        
        return {
            'display': {
                'title': task_detail.get('task_summary'),
                'status_color': self._get_status_color(status),
                'eta_text': 'Completed' if status == 'completed' else 'Calculating...',
                'status': status,
                'status_label': task_detail.get('current_step_name', status)
            },
            'grid_items': {
                'progress': {
                    'value': progress,
                    'label': 'Overall Progress',
                    'sub_text': f"{progress}% complete"
                },
                'confidence': {
                    'value': confidence_score,
                    'level': 'High' if confidence_score > 80 else 'Medium' if confidence_score > 50 else 'Low',
                    'breakdown': {
                        'reflection': 90,
                        'schema': 85,
                        'alignment': 95
                    }
                },
                'autonomy': {
                    'value': autonomy_rate,
                    'display': f"{autonomy_rate}% Autonomous"
                },
                'intervention': intervention_history
            },
            'last_updated': task_detail.get('updated_at')
        }

    def _get_status_color(self, status: str) -> str:
        colors = {
            'completed': 'green',
            'in_progress': 'blue',
            'failed': 'red',
            'pending_approval': 'yellow',
            'queued': 'gray'
        }
        return colors.get(status, 'gray')

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

    async def _get_task_from_executions_table(
        self,
        task_id: str,
        owner_id: str,
        include_technical_logs: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Executions 테이블에서 직접 태스크 정보 조회
        
        진행 중인 작업이 notification 테이블에 없는 경우 사용
        멀티 테넌트 격리를 위해 자신의 owner_id로만 조회
        """
        try:
            logger.info(f"[ExecutionsTable] Querying for task_id={task_id[:8]}..., owner_id={owner_id[:8]}...")
            
            # Executions 테이블에서 조회 (PK: ownerId, SK: executionArn)
            # executionArn 구조: arn:aws:states:region:account:execution:stateMachineName:executionName
            # executionName이 task_id와 일치해야 함
            
            # 🔒 보안: 멀티 테넌트 격리를 위해 자신의 owner_id로만 조회
            # 시스템 실행은 별도 처리하지 않음 (격리 위반 방지)
            get_func = partial(
                self.execution_table.query,
                KeyConditionExpression="ownerId = :oid",
                FilterExpression="executionArn LIKE :arn_pattern",
                ExpressionAttributeValues={
                    ":oid": owner_id,
                    ":arn_pattern": f"%:{task_id}"  # executionArn 끝이 :task_id로 끝나는 패턴
                },
                Limit=10  # 여러 개 가져와서 확인
            )
            
            response = await asyncio.get_event_loop().run_in_executor(None, get_func)
            items = response.get('Items', [])
            
            logger.info(f"[ExecutionsTable] Found {len(items)} items for owner_id={owner_id[:8]}...")
            
            if not items:
                logger.warning(f"Task {task_id[:8]}... not found in executions table for owner {owner_id[:8]}...")
                return None
            
            # 정확한 매칭 확인 (executionArn의 마지막 부분이 task_id와 일치하는지)
            matching_items = []
            for item in items:
                execution_arn = item.get('executionArn', '')
                if execution_arn.endswith(f':{task_id}'):
                    execution_name = execution_arn.split(':')[-1]
                    if execution_name == task_id:
                        matching_items.append(item)
            
            if not matching_items:
                logger.warning(f"No exact executionArn match found for task_id={task_id[:8]}...")
                return None
            
            # 가장 최신 항목 사용 (startDate 기준)
            execution = max(matching_items, key=lambda x: x.get('startDate', ''))
            logger.info(f"Found task {task_id[:8]}... in executions table, status={execution.get('status')}")
            
            # Execution을 Task 형식으로 변환
            task = self._convert_execution_to_task(execution, detailed=True)
            
            if include_technical_logs and task:
                task['technical_logs'] = self._extract_technical_logs_from_execution(execution)
            
            return task
            
        except ClientError as e:
            logger.error(f"Failed to get task from executions table: {e}")
            return None

    def _convert_execution_to_task(
        self,
        execution: Dict[str, Any],
        detailed: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Execution 데이터를 Task 형식으로 변환
        
        [v2.1] Executions 테이블 항목을 Task Manager UI 형식으로 변환
        
        Args:
            execution: DynamoDB execution 항목
            detailed: 상세 정보 포함 여부
            
        Returns:
            Task 정보 딕셔너리
        """
        try:
            execution_arn = execution.get('executionArn', '')
            execution_id = execution_arn.split(':')[-1] if execution_arn else execution.get('execution_id', '')
            
            technical_status = execution.get('status', 'UNKNOWN')
            task_status = convert_technical_status(technical_status)
            
            # step_function_state에서 추가 정보 추출
            sfn_state = execution.get('step_function_state', {})
            
            # 진행률 계산
            progress = 100 if task_status == TaskStatus.COMPLETED else 0
            if task_status == TaskStatus.IN_PROGRESS:
                # initial_input에서 총 세그먼트 수 추출 시도
                initial_input = execution.get('initial_input', {})
                if isinstance(initial_input, str):
                    try:
                        initial_input = json.loads(initial_input)
                    except:
                        initial_input = {}
                
                current_segment = sfn_state.get('current_segment', 0)
                total_segments = initial_input.get('total_segments', 1)
                progress = int((current_segment / max(total_segments, 1)) * 100)
            
            # 기본 Task 정보
            task = {
                "task_id": execution_id,
                "task_summary": execution.get('name', '') or self._generate_task_summary_from_execution(execution),
                "agent_name": "AI Assistant",
                "status": task_status.value,
                "progress_percentage": progress,
                "current_step_name": sfn_state.get('current_step', ''),
                "current_thought": self._generate_thought_from_status(task_status, technical_status),
                "is_interruption": technical_status in ['PAUSED', 'PAUSED_FOR_HITP'],
                "started_at": self._format_timestamp(execution.get('startDate') or execution.get('created_at')),
                "updated_at": self._format_timestamp(execution.get('stopDate') or execution.get('updated_at')),
                "workflow_name": execution.get('workflow_name', ''),
                "workflow_id": execution.get('workflowId', ''),
            }
            
            # 에러 정보
            if task_status == TaskStatus.FAILED:
                error = execution.get('error', '')
                if error:
                    message, suggestion = get_friendly_error_message(str(error))
                    task['error_message'] = message
                    task['error_suggestion'] = suggestion
            
            # 상세 정보
            if detailed:
                task['artifacts'] = []
                task['thought_history'] = []
                
                # final_result에서 아티팩트 추출
                final_result = execution.get('final_result', {})
                if isinstance(final_result, str):
                    try:
                        final_result = json.loads(final_result)
                    except:
                        pass
                
                if isinstance(final_result, dict):
                    task['artifacts'] = self._extract_artifacts(final_result)
            
            return task
            
        except Exception as e:
            logger.error(f"Failed to convert execution to task: {e}")
            return None

    def _generate_task_summary_from_execution(self, execution: Dict[str, Any]) -> str:
        """Execution 데이터로부터 Task 요약 생성"""
        workflow_id = execution.get('workflowId', '')
        if workflow_id:
            return f"워크플로우 {workflow_id[:8]}... 실행"
        
        execution_arn = execution.get('executionArn', '')
        if execution_arn:
            exec_id = execution_arn.split(':')[-1]
            return f"실행 {exec_id[:8]}..."
        
        return "AI 작업"

    def _generate_thought_from_status(self, task_status: TaskStatus, technical_status: str) -> str:
        """상태에 따른 에이전트 사고 메시지 생성"""
        if technical_status in ['PAUSED', 'PAUSED_FOR_HITP']:
            return "사용자의 승인을 기다리고 있습니다."
        
        if task_status == TaskStatus.IN_PROGRESS:
            return "작업을 처리하고 있습니다..."
        
        if task_status == TaskStatus.COMPLETED:
            return "작업이 완료되었습니다."
        
        if task_status == TaskStatus.FAILED:
            return "작업 중 문제가 발생했습니다."
        
        if task_status == TaskStatus.QUEUED:
            return "작업을 준비하고 있습니다..."
        
        return "대기 중입니다."

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

    async def generate_execution_summary(
        self,
        task_id: str,
        owner_id: str,
        summary_type: str = "business"
    ) -> Dict[str, Any]:
        """
        Gemini를 사용하여 실행 로그를 요약
        
        [v3.0] LLM 기반 로그 요약 기능
        - Context Caching으로 비용 최적화
        - DynamoDB 캐싱으로 중복 호출 방지
        
        Args:
            task_id: 실행 ID
            owner_id: 사용자 ID
            summary_type: 요약 타입 (business | technical | full)
        
        Returns:
            {
                "summary": "요약 텍스트",
                "key_insights": ["인사이트1", "인사이트2"],
                "recommendations": ["권장사항1", "권장사항2"],
                "token_usage": {...},
                "generation_time_ms": 1234,
                "cached": false
            }
        """
        # 1. 캐시된 요약 확인
        cache_key = f"summary#{owner_id}#{task_id}#{summary_type}"
        cached_summary = await self._get_cached_summary(cache_key)
        if cached_summary:
            logger.info(f"Returning cached summary for {task_id[:8]}... (type={summary_type})")
            cached_summary["cached"] = True
            return cached_summary
        
        # 2. Task 데이터 로드
        task_detail = await self.get_task_detail(task_id, owner_id, include_technical_logs=False)
        if not task_detail:
            raise ValueError(f"Task not found: {task_id}")
        
        # state_history 추출
        state_history = []
        payload = task_detail.get('payload', {})
        if payload:
            state_history = payload.get('state_history', [])
            if not state_history:
                step_state = payload.get('step_function_state', {})
                state_history = step_state.get('state_history', [])
        
        if not state_history:
            return {
                "summary": "실행 이력이 없습니다.",
                "key_insights": [],
                "recommendations": [],
                "cached": False
            }
        
        # 3. 프롬프트 생성
        prompt = self._build_summary_prompt(state_history, summary_type, task_detail)
        
        # 4. Gemini 호출
        from src.services.llm.gemini_service import GeminiService, GeminiConfig, GeminiModel
        
        gemini = GeminiService(config=GeminiConfig(
            model=GeminiModel.GEMINI_2_0_FLASH,
            temperature=0.3,
            max_output_tokens=2048
        ))
        
        start_time = time.time()
        
        # Context Caching: state_history를 캐싱하여 비용 절감
        context_to_cache = None
        if len(state_history) > 30:  # 충분히 큰 경우만 캐싱
            context_to_cache = self._serialize_history_for_cache(state_history)
        
        response = gemini.invoke_model(
            user_prompt=prompt,
            system_instruction="당신은 워크플로우 실행 로그를 분석하는 전문가입니다. 간결하고 명확하게 요약하세요.",
            response_schema={
                "type": "object",
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "전체 실행 요약 (3-5문장)"
                    },
                    "key_insights": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "주요 인사이트 (3-5개)"
                    },
                    "recommendations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "개선 권장사항 (0-3개, 없으면 빈 배열)"
                    }
                },
                "required": ["summary", "key_insights", "recommendations"]
            },
            context_to_cache=context_to_cache
        )
        
        generation_time_ms = int((time.time() - start_time) * 1000)
        
        # 5. 응답 파싱
        result = response.get("parsed", {})
        result["token_usage"] = response.get("metadata", {}).get("token_usage", {})
        result["generation_time_ms"] = generation_time_ms
        result["model_used"] = "gemini-2.0-flash"
        result["cached"] = False
        
        # 6. 결과 캐싱 (24시간 TTL)
        await self._cache_summary(cache_key, result, ttl_hours=24)
        
        logger.info(
            f"Generated summary for {task_id[:8]}... (type={summary_type}, "
            f"tokens={result['token_usage'].get('total_tokens', 0)}, "
            f"time={generation_time_ms}ms)"
        )
        
        return result

    def _build_summary_prompt(
        self,
        state_history: List[Dict[str, Any]],
        summary_type: str,
        task_detail: Dict[str, Any]
    ) -> str:
        """요약 프롬프트 생성"""
        
        # state_history 샘플링 (너무 길면 최근 N개만)
        MAX_HISTORY_ENTRIES = 50
        sampled_history = state_history[-MAX_HISTORY_ENTRIES:] if len(state_history) > MAX_HISTORY_ENTRIES else state_history
        
        history_text = "\n".join([
            f"[{i+1}] {entry.get('state_name', 'unknown')} - {entry.get('status', 'N/A')} "
            f"(duration: {entry.get('duration', 0)}ms)"
            for i, entry in enumerate(sampled_history)
        ])
        
        if summary_type == "business":
            prompt = f"""
다음은 '{task_detail.get('task_summary', 'AI 작업')}'의 실행 로그입니다.

**현재 상태:** {task_detail.get('status')}
**진행률:** {task_detail.get('progress_percentage')}%
**실행 단계:** (최근 {len(sampled_history)}개)
{history_text}

**비즈니스 관점에서 다음을 분석하세요:**
1. 이 작업이 무엇을 달성했는지 요약 (기술 용어 최소화)
2. 핵심 성과 및 인사이트 (3-5개)
3. 비즈니스 가치와 개선 권장사항 (있으면 1-3개, 없으면 빈 배열)
"""
        
        elif summary_type == "technical":
            token_usage = task_detail.get('token_usage', {})
            prompt = f"""
다음은 워크플로우 실행의 기술 로그입니다.

**Execution ID:** {task_detail.get('task_id')}
**Status:** {task_detail.get('status')}
**Workflow:** {task_detail.get('workflow_name')}
**State History:** (최근 {len(sampled_history)}개)
{history_text}

**Token Usage:** {json.dumps(token_usage) if token_usage else 'N/A'}

**기술 관점에서 다음을 분석하세요:**
1. 실행 흐름 요약 (병목 구간, 재시도, 에러 포함)
2. 성능 메트릭 인사이트 (latency, token usage, 비용 추정)
3. 최적화 가능 영역 (있으면 1-3개, 없으면 빈 배열)
"""
        
        else:  # full
            prompt = f"""
다음은 워크플로우 '{task_detail.get('workflow_name')}'의 전체 실행 로그입니다.

**기본 정보:**
- Execution ID: {task_detail.get('task_id')}
- Status: {task_detail.get('status')}
- Progress: {task_detail.get('progress_percentage')}%
- Started: {task_detail.get('started_at')}
- Updated: {task_detail.get('updated_at')}

**실행 히스토리:** (최근 {len(sampled_history)}개)
{history_text}

**종합 분석:**
1. 전체 실행 흐름 요약 (비즈니스 + 기술)
2. 핵심 인사이트 (3-5개)
3. 종합 개선 제안 (있으면 1-3개, 없으면 빈 배열)
"""
        
        return prompt

    def _serialize_history_for_cache(self, state_history: List[Dict[str, Any]]) -> str:
        """state_history를 캐싱용 문자열로 직렬화"""
        # 중요 필드만 추출하여 크기 최소화
        simplified = [
            {
                "name": entry.get('state_name', 'unknown'),
                "status": entry.get('status', 'N/A'),
                "duration": entry.get('duration', 0)
            }
            for entry in state_history
        ]
        return json.dumps(simplified, ensure_ascii=False)

    async def _get_cached_summary(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """DynamoDB에서 캐시된 요약 조회"""
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                partial(
                    self.task_events_table.get_item,
                    Key={'task_id': cache_key, 'event_id': 'SUMMARY_CACHE'}
                )
            )
            
            item = response.get('Item')
            if not item:
                return None
            
            # TTL 확인
            ttl = item.get('ttl', 0)
            if ttl < int(time.time()):
                return None
            
            # 캐시된 데이터 반환
            cached_data = item.get('cached_summary')
            if cached_data:
                return json.loads(cached_data) if isinstance(cached_data, str) else cached_data
            
            return None
            
        except ClientError as e:
            if 'ResourceNotFoundException' in str(e):
                # 테이블 없으면 무시
                return None
            logger.warning(f"Failed to get cached summary: {e}")
            return None
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
            return None

    async def _cache_summary(self, cache_key: str, summary_data: Dict[str, Any], ttl_hours: int = 24) -> None:
        """DynamoDB에 요약 결과 캐싱"""
        try:
            cache_item = {
                'task_id': cache_key,
                'event_id': 'SUMMARY_CACHE',
                'cached_summary': json.dumps(summary_data, ensure_ascii=False),
                'ttl': int(time.time()) + (ttl_hours * 60 * 60),
                'cached_at': int(time.time() * 1000)
            }
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                partial(self.task_events_table.put_item, Item=cache_item)
            )
            
            logger.debug(f"Cached summary: {cache_key} (TTL: {ttl_hours}h)")
            
        except ClientError as e:
            if 'ResourceNotFoundException' not in str(e):
                logger.warning(f"Failed to cache summary: {e}")
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
