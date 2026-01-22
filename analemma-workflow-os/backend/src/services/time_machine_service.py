# -*- coding: utf-8 -*-
"""
Time Machine Service - Cognitive Rollback with Gemini 3

워크플로우 실행의 롤백 및 분기 관리를 위한 서비스입니다.
기존 execution 데이터를 활용하여 Time Machine 기능을 제공합니다.

[Gemini 3 통합]
- 추론 기반 롤백 추천 (Cognitive Rollback): 단순 휴리스틱이 아닌 Gemini의 분석
- 자동 보정 (Auto-Fix): 롤백 후 같은 실수 반복 방지를 위한 지침 수정
- 원인 분석 (Root Cause Analysis): 에러 로그와 state_snapshot 심층 분석
"""

import os
import logging
import json
import asyncio
import uuid
import hashlib
from functools import partial
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

try:
    from src.services.checkpoint_service import CheckpointService
    from src.common.aws_clients import get_dynamodb_resource, get_stepfunctions_client
except ImportError:
    from src.checkpoint_service import CheckpointService
    from src.common.aws_clients import get_dynamodb_resource, get_stepfunctions_client

# Gemini 서비스 import
try:
    from src.services.llm.gemini_service import get_gemini_pro_service, GeminiService
    GEMINI_AVAILABLE = True
except ImportError:
    get_gemini_pro_service = None
    GeminiService = None
    GEMINI_AVAILABLE = False

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# 환경 변수 - 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
# ═══════════════════════════════════════════════════════════════════════════════
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', '')
BRANCHES_TABLE = os.environ.get('WORKFLOW_BRANCHES_TABLE', 'WorkflowBranchesTable')
ENABLE_COGNITIVE_ROLLBACK = os.environ.get('ENABLE_COGNITIVE_ROLLBACK', 'true').lower() == 'true'

# GSI 이름 (template.yaml GlobalSecondaryIndexes.IndexName과 일치)
ROOT_THREAD_GSI = os.environ.get('ROOT_THREAD_INDEX', 'root-thread-index')


class TimeMachineService:
    """
    Time Machine 서비스 - Cognitive Rollback with Gemini 3
    
    기존 execution 데이터를 활용하여 롤백 및 분기 관리 기능을 제공합니다.
    
    [핵심 기능]
    1. 분기형 실행 모델 (Branching Model): Git 스타일 브랜치 생성
    2. 지능형 추천 시스템: Gemini 3 기반 롤백 지점 분석
    3. 영향도 분석 (Impact Analysis): 변수 변화량, 리스크 미리보기
    4. 자동 보정 (Auto-Fix): 롤백 후 지침 자동 수정
    """
    
    def __init__(
        self,
        checkpoint_service: Optional[CheckpointService] = None,
        executions_table: Optional[str] = None,
        gemini_service: Optional['GeminiService'] = None
    ):
        """
        Args:
            checkpoint_service: CheckpointService 인스턴스
            executions_table: 실행 테이블 이름
            gemini_service: Gemini 서비스 (Cognitive Rollback용)
        """
        self.checkpoint_service = checkpoint_service or CheckpointService()
        self.executions_table_name = executions_table or os.environ.get(
            'EXECUTION_TABLE', 'executions'
        )
        self._executions_table = None
        self._branches_table = None
        self._sfn_client = None
        
        # Gemini 서비스 초기화 (Cognitive Rollback)
        if gemini_service:
            self.gemini_service = gemini_service
        elif GEMINI_AVAILABLE and get_gemini_pro_service:
            self.gemini_service = get_gemini_pro_service()
        else:
            self.gemini_service = None
            logger.warning("Gemini service not available - falling back to heuristic rollback")
    
    @property
    def executions_table(self):
        """지연 초기화된 실행 테이블"""
        if self._executions_table is None:
            dynamodb_resource = get_dynamodb_resource()
            self._executions_table = dynamodb_resource.Table(self.executions_table_name)
        return self._executions_table
    
    @property
    def branches_table(self):
        """지연 초기화된 브랜치 테이블"""
        if self._branches_table is None:
            dynamodb_resource = get_dynamodb_resource()
            self._branches_table = dynamodb_resource.Table(BRANCHES_TABLE)
        return self._branches_table
    
    @property
    def sfn_client(self):
        """지연 초기화된 Step Functions 클라이언트"""
        if self._sfn_client is None:
            self._sfn_client = get_stepfunctions_client()
        return self._sfn_client

    async def preview_rollback(
        self,
        rollback_request: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        롤백 미리보기 - Compare UI 데이터 포함
        
        [데모 시각화 포인트]
        현재의 에러 난 상태와 롤백할 과거의 성공적인 상태를
        좌우로 나란히 비교할 수 있는 데이터를 제공합니다.
        
        Args:
            rollback_request: 롤백 요청 정보
            
        Returns:
            롤백 미리보기 결과 (Compare UI 데이터 포함)
        """
        try:
            thread_id = rollback_request.get('thread_id')
            target_checkpoint_id = rollback_request.get('target_checkpoint_id')
            
            if not thread_id or not target_checkpoint_id:
                raise ValueError("thread_id and target_checkpoint_id are required")
            
            # 대상 체크포인트 조회
            target_checkpoint = await self.checkpoint_service.get_checkpoint_detail(
                thread_id, target_checkpoint_id
            )
            
            if not target_checkpoint:
                raise ValueError("Target checkpoint not found")
            
            # 현재 상태 조회
            current_checkpoints = await self.checkpoint_service.list_checkpoints(
                thread_id, limit=1
            )
            current_checkpoint = current_checkpoints[0] if current_checkpoints else None
            
            # ═══════════════════════════════════════════════════════════════════
            # Compare UI 데이터 생성 (좌우 비교 뷰)
            # ═══════════════════════════════════════════════════════════════════
            compare_data = self._generate_compare_data(
                current_checkpoint, target_checkpoint
            )
            
            # Gemini 분석 (선택적)
            cognitive_recommendation = None
            if self.gemini_service and ENABLE_COGNITIVE_ROLLBACK:
                suggestions = await self._cognitive_rollback_analysis(
                    thread_id=thread_id,
                    checkpoints=[target_checkpoint],
                    error_context=rollback_request.get('error_context')
                )
                if suggestions:
                    cognitive_recommendation = suggestions[0]
            
            preview = {
                "rollback_id": str(uuid.uuid4()),
                "thread_id": thread_id,
                "target_checkpoint_id": target_checkpoint_id,
                "target_timestamp": target_checkpoint.get('created_at'),
                "target_node_id": target_checkpoint.get('node_id'),
                
                # 영향도 분석
                "estimated_impact": self._estimate_rollback_impact(
                    target_checkpoint, current_checkpoint
                ),
                "affected_nodes": self._get_affected_nodes(thread_id, target_checkpoint_id),
                "data_changes": self._preview_data_changes(target_checkpoint, current_checkpoint),
                
                # ═══════════════════════════════════════════════════════════════
                # Compare UI 데이터 (해커톤 데모 핵심)
                # ═══════════════════════════════════════════════════════════════
                "compare": compare_data,
                
                # Gemini 인지 분석
                "cognitive_recommendation": cognitive_recommendation,
                
                # 경고 및 확인
                "warnings": self._generate_rollback_warnings(rollback_request),
                "requires_confirmation": True,
                "estimated_duration_seconds": 30,
            }
            
            return preview
            
        except Exception as e:
            logger.error(f"Failed to preview rollback: {e}")
            raise
    
    def _generate_compare_data(
        self,
        current_checkpoint: Optional[Dict[str, Any]],
        target_checkpoint: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare UI용 좌우 비교 데이터 생성
        
        [데모 시각화]
        "Gemini가 이 시점으로 돌아가라고 제안합니다" 팝업과 함께
        현재 상태 vs 과거 성공 상태를 나란히 표시
        """
        current_state = current_checkpoint.get('state_snapshot', {}) if current_checkpoint else {}
        target_state = target_checkpoint.get('state_snapshot', {})
        
        # 변수 차이 분석
        all_keys = set(current_state.keys()) | set(target_state.keys())
        variable_diffs = []
        
        for key in all_keys:
            current_val = current_state.get(key)
            target_val = target_state.get(key)
            
            if current_val != target_val:
                variable_diffs.append({
                    "variable": key,
                    "current_value": self._truncate_value(current_val),
                    "target_value": self._truncate_value(target_val),
                    "change_type": self._get_change_type(current_val, target_val)
                })
        
        return {
            "left": {
                "title": "🔴 현재 상태 (에러)",
                "timestamp": current_checkpoint.get('created_at') if current_checkpoint else None,
                "node_id": current_checkpoint.get('node_id') if current_checkpoint else None,
                "status": current_checkpoint.get('status') if current_checkpoint else 'UNKNOWN',
                "variable_count": len(current_state)
            },
            "right": {
                "title": "🟢 롤백 대상 (성공)",
                "timestamp": target_checkpoint.get('created_at'),
                "node_id": target_checkpoint.get('node_id'),
                "status": target_checkpoint.get('status'),
                "variable_count": len(target_state)
            },
            "diffs": variable_diffs[:20],  # 상위 20개 차이만
            "diff_count": len(variable_diffs),
            "rollback_recommendation": f"Gemini가 '{target_checkpoint.get('node_id')}' 시점으로 돌아가라고 제안합니다"
        }
    
    def _truncate_value(self, value: Any, max_length: int = 100) -> str:
        """값을 UI 표시용으로 축약"""
        if value is None:
            return "null"
        str_val = str(value)
        if len(str_val) > max_length:
            return str_val[:max_length] + "..."
        return str_val
    
    def _get_change_type(self, current: Any, target: Any) -> str:
        """변경 타입 분류"""
        if current is None and target is not None:
            return "added_in_target"
        elif current is not None and target is None:
            return "removed_in_target"
        else:
            return "modified"

    async def execute_rollback(
        self,
        rollback_request: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        롤백 실행 및 분기 생성 - 실제 Step Functions 트리거
        
        [핵심 로직]
        1. 대상 체크포인트의 state_snapshot 조회
        2. 새 브랜치 ID 생성 (Git 스타일)
        3. 부모-자식 관계를 DynamoDB에 기록 (Workflow Lineage)
        4. Step Functions 실행 트리거 (state_snapshot 주입)
        5. (선택) Auto-Fix: Gemini가 지침 자동 수정
        
        Args:
            rollback_request: 롤백 요청 정보
            
        Returns:
            생성된 분기 정보 및 실행 ARN
        """
        try:
            thread_id = rollback_request.get('thread_id')
            target_checkpoint_id = rollback_request.get('target_checkpoint_id')
            branch_name = rollback_request.get('branch_name', f"rollback-{int(datetime.now().timestamp())}")
            enable_auto_fix = rollback_request.get('enable_auto_fix', True)
            dry_run = rollback_request.get('dry_run', False)
            
            if not thread_id or not target_checkpoint_id:
                raise ValueError("thread_id and target_checkpoint_id are required")
            
            # 대상 체크포인트 조회 (S3 오프로딩된 상태도 투명하게 로드)
            target_checkpoint = await self.checkpoint_service.get_checkpoint_detail(
                thread_id, target_checkpoint_id
            )
            
            if not target_checkpoint:
                raise ValueError("Target checkpoint not found")
            
            # ═══════════════════════════════════════════════════════════════════
            # S3 State Offloading 확인 (투명성 보장)
            # CheckpointService.get_checkpoint_detail()이 자동으로 S3 로드 처리
            # ═══════════════════════════════════════════════════════════════════
            state_snapshot = target_checkpoint.get('state_snapshot', {})
            if target_checkpoint.get('metadata', {}).get('is_s3_offloaded') and not state_snapshot:
                logger.warning(
                    f"S3 state loading may have failed for checkpoint {target_checkpoint_id}. "
                    f"S3 path: {target_checkpoint.get('state_s3_path')}"
                )
            
            # ═══════════════════════════════════════════════════════════════════
            # 1. 분기 ID 생성 (Git 스타일 해시)
            # ═══════════════════════════════════════════════════════════════════
            branch_id = self._generate_branch_id(thread_id, target_checkpoint_id)
            new_thread_id = f"{thread_id}_branch_{branch_id[:8]}"
            
            # ═══════════════════════════════════════════════════════════════════
            # 2. Gemini Auto-Fix: 롤백 이유를 분석하여 지침 자동 수정
            # ═══════════════════════════════════════════════════════════════════
            auto_fix_result = None
            if enable_auto_fix and self.gemini_service and ENABLE_COGNITIVE_ROLLBACK:
                auto_fix_result = await self._generate_auto_fix_instructions(
                    thread_id=thread_id,
                    target_checkpoint=target_checkpoint,
                    rollback_request=rollback_request
                )
            
            # ═══════════════════════════════════════════════════════════════════
            # 3. 부모-자식 관계 DynamoDB에 기록 (Workflow Lineage)
            # ═══════════════════════════════════════════════════════════════════
            root_thread_id = thread_id.split('_branch_')[0]
            branch_record = {
                "branch_id": branch_id,
                "parent_thread_id": thread_id,
                "new_thread_id": new_thread_id,
                "branch_name": branch_name,
                "rollback_checkpoint_id": target_checkpoint_id,
                "rollback_node_id": target_checkpoint.get('node_id'),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "created_by": rollback_request.get('user_id', 'system'),
                "status": "pending",
                "auto_fix_applied": auto_fix_result is not None,
                "auto_fix_instructions": auto_fix_result.get('modified_instructions') if auto_fix_result else None,
                # GSI용 최상위 속성 (중첩 속성은 GSI 키로 사용 불가)
                "root_thread_id": root_thread_id,
                "lineage": {
                    "parent_branch": thread_id.split('_branch_')[-1] if '_branch_' in thread_id else 'main',
                    "depth": thread_id.count('_branch_') + 1,
                    "root_thread_id": root_thread_id
                }
            }
            
            if not dry_run:
                await self._save_branch_record(branch_record)
            
            # ═══════════════════════════════════════════════════════════════════
            # 4. Step Functions 실행 트리거
            # ═══════════════════════════════════════════════════════════════════
            execution_arn = None
            if not dry_run and STATE_MACHINE_ARN:
                state_snapshot = target_checkpoint.get('state_snapshot', {})
                
                # Auto-Fix 지침 주입
                if auto_fix_result and auto_fix_result.get('modified_instructions'):
                    state_snapshot['_auto_fix_instructions'] = auto_fix_result['modified_instructions']
                    state_snapshot['_rollback_context'] = {
                        'original_error': auto_fix_result.get('root_cause'),
                        'fix_strategy': auto_fix_result.get('fix_strategy')
                    }
                
                execution_arn = await self._trigger_step_function_execution(
                    new_thread_id=new_thread_id,
                    state_snapshot=state_snapshot,
                    branch_record=branch_record
                )
                
                # 상태 업데이트
                branch_record['status'] = 'running'
                branch_record['execution_arn'] = execution_arn
                await self._update_branch_status(branch_id, 'running', execution_arn)
            
            logger.info(f"Rollback branch {branch_id} created from checkpoint {target_checkpoint_id}")
            
            return {
                **branch_record,
                "execution_arn": execution_arn,
                "dry_run": dry_run,
                "cognitive_analysis": auto_fix_result
            }
            
        except Exception as e:
            logger.error(f"Failed to execute rollback: {e}")
            raise
    
    async def _trigger_step_function_execution(
        self,
        new_thread_id: str,
        state_snapshot: Dict[str, Any],
        branch_record: Dict[str, Any]
    ) -> str:
        """
        Step Functions 실행 트리거
        
        체크포인트의 state_snapshot을 입력으로 주입하여
        해당 시점부터 실행을 재개합니다.
        """
        try:
            execution_input = {
                "thread_id": new_thread_id,
                "restored_state": state_snapshot,
                "is_rollback": True,
                "rollback_metadata": {
                    "original_thread_id": branch_record['parent_thread_id'],
                    "checkpoint_id": branch_record['rollback_checkpoint_id'],
                    "branch_id": branch_record['branch_id'],
                    "auto_fix_applied": branch_record.get('auto_fix_applied', False)
                }
            }
            
            # Step Functions 실행 이름 규칙: 80자 이내, 영숫자/하이픈/언더스코어만
            # 타임스탬프로 중복 실행 방지 (동일 이름 재사용 불가)
            import re
            safe_thread_id = re.sub(r'[^a-zA-Z0-9_-]', '_', new_thread_id)[:40]
            execution_name = f"rollback-{safe_thread_id}-{int(datetime.now().timestamp())}"
            
            response = self.sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps(execution_input, default=str)
            )
            logger.debug(f"SFN execution name: {execution_name}")
            
            execution_arn = response['executionArn']
            logger.info(f"Step Functions execution started: {execution_arn}")
            return execution_arn
            
        except ClientError as e:
            logger.error(f"Failed to start Step Functions execution: {e}")
            raise
    
    async def _save_branch_record(self, branch_record: Dict[str, Any]) -> None:
        """브랜치 기록을 DynamoDB에 저장"""
        try:
            self.branches_table.put_item(Item=branch_record)
            logger.debug(f"Branch record saved: {branch_record['branch_id']}")
        except ClientError as e:
            logger.error(f"Failed to save branch record: {e}")
            raise
    
    async def _update_branch_status(
        self, 
        branch_id: str, 
        status: str, 
        execution_arn: Optional[str] = None
    ) -> None:
        """브랜치 상태 업데이트"""
        try:
            update_expr = "SET #status = :status, updated_at = :updated_at"
            expr_values = {
                ':status': status,
                ':updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            if execution_arn:
                update_expr += ", execution_arn = :arn"
                expr_values[':arn'] = execution_arn
            
            self.branches_table.update_item(
                Key={'branch_id': branch_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues=expr_values
            )
        except ClientError as e:
            logger.warning(f"Failed to update branch status: {e}")
    
    def _generate_branch_id(self, thread_id: str, checkpoint_id: str) -> str:
        """Git 스타일 브랜치 ID 생성"""
        content = f"{thread_id}:{checkpoint_id}:{datetime.now().isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]
    
    async def _query_branches_by_root(self, root_thread_id: str) -> List[Dict[str, Any]]:
        """
        루트 스레드 ID로 모든 관련 브랜치 조회
        
        [GSI 우선, Scan 폴백]
        1. GSI(root-thread-index)가 있으면 Query 사용 (O(1))
        2. GSI가 없으면 FilterExpression으로 Scan (O(n), 개발환경용)
        
        Args:
            root_thread_id: 루트 스레드 ID
            
        Returns:
            브랜치 목록
        """
        try:
            # 방법 1: GSI Query (프로덕션 권장)
            response = self.branches_table.query(
                IndexName=ROOT_THREAD_GSI,
                KeyConditionExpression='root_thread_id = :root',
                ExpressionAttributeValues={':root': root_thread_id}
            )
            logger.debug(f"GSI query returned {len(response.get('Items', []))} branches")
            return response.get('Items', [])
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            
            if error_code == 'ValidationException':
                # GSI가 존재하지 않음 - Scan으로 폴백
                logger.warning(
                    f"GSI '{ROOT_THREAD_GSI}' not found. "
                    f"Falling back to Scan. Consider creating the GSI for production."
                )
                return await self._scan_branches_by_root(root_thread_id)
            else:
                logger.error(f"Failed to query branches: {e}")
                return []
    
    async def _scan_branches_by_root(self, root_thread_id: str) -> List[Dict[str, Any]]:
        """
        GSI 없을 때 Scan으로 폴백 (개발 환경용)
        
        ⚠️ 프로덕션에서는 GSI 생성 권장 (비용/성능)
        """
        try:
            from boto3.dynamodb.conditions import Attr
            
            response = self.branches_table.scan(
                FilterExpression=Attr('lineage.root_thread_id').eq(root_thread_id),
                Limit=100  # 과도한 스캔 방지
            )
            logger.debug(f"Scan returned {len(response.get('Items', []))} branches")
            return response.get('Items', [])
            
        except Exception as e:
            logger.error(f"Failed to scan branches: {e}")
            return []

    async def get_branch_history(
        self,
        thread_id: str
    ) -> List[Dict[str, Any]]:
        """
        분기 히스토리 조회 - Workflow Lineage 시각화
        
        DynamoDB에서 부모-자식 관계를 조회하여
        워크플로우 가계도(Workflow Lineage)를 구성합니다.
        
        [GSI 요구사항]
        - IndexName: root-thread-index
        - KeySchema: root_thread_id (HASH)
        - template.yaml에 정의 필수
        
        Args:
            thread_id: 실행 스레드 ID
            
        Returns:
            분기 목록 (트리 구조)
        """
        try:
            # 루트 스레드 ID 추출
            root_thread_id = thread_id.split('_branch_')[0]
            
            # DynamoDB에서 관련 브랜치 조회
            branches = await self._query_branches_by_root(root_thread_id)
            
            # 메인 브랜치 추가
            result = [
                {
                    "branch_id": "main",
                    "branch_name": "main",
                    "created_at": "2024-01-01T00:00:00Z",
                    "status": "active",
                    "is_main": True,
                    "thread_id": root_thread_id,
                    "depth": 0,
                    "children": []
                }
            ]
            
            # 분기 트리 구성
            for branch in branches:
                result.append({
                    "branch_id": branch.get('branch_id'),
                    "branch_name": branch.get('branch_name'),
                    "created_at": branch.get('created_at'),
                    "status": branch.get('status'),
                    "is_main": False,
                    "thread_id": branch.get('new_thread_id'),
                    "parent_thread_id": branch.get('parent_thread_id'),
                    "depth": branch.get('lineage', {}).get('depth', 1),
                    "rollback_checkpoint_id": branch.get('rollback_checkpoint_id'),
                    "auto_fix_applied": branch.get('auto_fix_applied', False),
                    "execution_arn": branch.get('execution_arn')
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get branch history: {e}")
            return []

    async def get_rollback_suggestions(
        self,
        thread_id: str,
        error_context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        롤백 추천 지점 분석 - Gemini 3 Cognitive Rollback
        
        [Gemini 3 통합]
        단순 휴리스틱이 아닌, Gemini가 에러 로그와 state_snapshot을 분석하여
        "어느 시점으로 돌아가야 에러의 원인이 된 변수를 수정할 수 있을지" 판단합니다.
        
        Args:
            thread_id: 실행 스레드 ID
            error_context: 현재 에러 정보 (선택)
            
        Returns:
            롤백 추천 목록 (Gemini 분석 결과 포함)
        """
        try:
            # 체크포인트 목록 조회
            checkpoints = await self.checkpoint_service.list_checkpoints(thread_id, limit=20)
            
            if not checkpoints:
                return []
            
            # ═══════════════════════════════════════════════════════════════════
            # Gemini 3 Cognitive Rollback 분석
            # ═══════════════════════════════════════════════════════════════════
            if self.gemini_service and ENABLE_COGNITIVE_ROLLBACK:
                suggestions = await self._cognitive_rollback_analysis(
                    thread_id=thread_id,
                    checkpoints=checkpoints,
                    error_context=error_context
                )
                if suggestions:
                    return suggestions
            
            # Fallback: 휴리스틱 기반 추천
            return self._heuristic_rollback_suggestions(checkpoints)
            
        except Exception as e:
            logger.error(f"Failed to get rollback suggestions: {e}")
            return []
    
    def _truncate_cognitive_context(
        self,
        checkpoints: List[Dict[str, Any]],
        max_checkpoints: int = 10
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Gemini의 Token Limit을 넘지 않도록 체크포인트 컨텍스트를 전략적으로 축소
        
        [Hyper Stress Guard]
        - 20개 초과 시 최신 10개로 제한
        - 상태 스냅샷에서 변화가 없는 키는 제외하고 의미 있는 차이(Diff)만 추출
        
        Args:
            checkpoints: 체크포인트 목록
            max_checkpoints: 최대 유지할 체크포인트 수
            
        Returns:
            (축소된 체크포인트 목록, 축소 여부)
        """
        was_truncated = False
        
        if len(checkpoints) > 20:
            logger.warning(
                f"Cognitive Overload detected: {len(checkpoints)} checkpoints. "
                f"Trimming to latest {max_checkpoints}."
            )
            checkpoints = checkpoints[-max_checkpoints:]
            was_truncated = True
        
        # 상태 스냅샷 Diff 추출 (인접 체크포인트 간 변화만 유지)
        optimized_checkpoints = []
        prev_state_keys = set()
        
        for cp in checkpoints:
            current_state = cp.get('state_snapshot', {})
            current_keys = set(current_state.keys())
            
            # 새로 추가되거나 변경된 키만 추출
            diff_keys = current_keys - prev_state_keys
            if diff_keys or cp.get('status') in ['FAILED', 'ERROR']:
                # 중요 체크포인트는 유지
                optimized_checkpoints.append(cp)
            
            prev_state_keys = current_keys
        
        # 최소 3개는 유지 (시작, 중간, 끝)
        if len(optimized_checkpoints) < 3 and checkpoints:
            optimized_checkpoints = [
                checkpoints[0],
                checkpoints[len(checkpoints) // 2],
                checkpoints[-1]
            ]
        
        return optimized_checkpoints, was_truncated
    
    async def _cognitive_rollback_analysis(
        self,
        thread_id: str,
        checkpoints: List[Dict[str, Any]],
        error_context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Gemini 3 기반 인지적 롤백 분석
        
        Gemini에게 에러 로그와 체크포인트 히스토리를 제공하여
        최적의 복구 지점을 분석하게 합니다.
        
        [해커톤 차별화 포인트]
        "단순히 전 단계로 돌아가세요"가 아니라
        "3단계 전에서 API 응답값이 잘못 파싱되었습니다. 
         해당 시점으로 돌아가 파싱 로직을 수정하는 것을 추천합니다"
        """
        try:
            # 🛡️ Cognitive Overload Guard - 체크포인트 컨텍스트 축소
            checkpoints, was_truncated = self._truncate_cognitive_context(checkpoints)
            if was_truncated:
                logger.info(f"Context truncated for cognitive analysis. Proceeding with {len(checkpoints)} checkpoints.")
            
            # 체크포인트 요약 생성
            checkpoint_summaries = []
            for i, cp in enumerate(checkpoints):
                summary = {
                    "index": i,
                    "checkpoint_id": cp.get('checkpoint_id'),
                    "node_id": cp.get('node_id'),
                    "status": cp.get('status'),
                    "timestamp": cp.get('created_at'),
                    "state_keys": list(cp.get('state_snapshot', {}).keys())[:10],
                    "has_error": cp.get('status') in ['FAILED', 'ERROR']
                }
                checkpoint_summaries.append(summary)
            
            # Gemini 분석 프롬프트
            analysis_prompt = f"""당신은 워크플로우 디버깅 전문가입니다. 
에러가 발생한 워크플로우의 체크포인트 히스토리를 분석하여 
최적의 롤백 지점을 추천해주세요.

## 현재 에러 상황
{json.dumps(error_context, ensure_ascii=False, indent=2) if error_context else "에러 정보 없음 - 일반 분석 모드"}

## 체크포인트 히스토리 (최신 → 과거 순)
{json.dumps(checkpoint_summaries, ensure_ascii=False, indent=2)}

## 분석 요청
1. 에러의 근본 원인(Root Cause)을 추론하세요.
2. 어느 체크포인트로 돌아가야 문제를 해결할 수 있는지 분석하세요.
3. 각 추천 지점에 대해 구체적인 수정 전략을 제시하세요.

## 응답 형식 (JSON)
```json
{{
  "root_cause_analysis": "에러 원인 분석...",
  "suggestions": [
    {{
      "checkpoint_index": 3,
      "checkpoint_id": "...",
      "confidence": 0.85,
      "reason": "3단계 전에서 API 응답값이 잘못 파싱되었습니다...",
      "fix_strategy": "파싱 로직에서 null 체크를 추가하세요...",
      "estimated_impact": "low|medium|high",
      "affected_variables": ["var1", "var2"]
    }}
  ]
}}
```

JSON만 응답하세요."""

            # Gemini 호출
            response = self.gemini_service.invoke_model(
                user_prompt=analysis_prompt,
                max_output_tokens=2048,
                temperature=0.3  # 분석은 낮은 온도로
            )
            
            # JSON 파싱
            response_text = response.get('text', '')
            # JSON 블록 추출
            if '```json' in response_text:
                json_str = response_text.split('```json')[1].split('```')[0]
            elif '```' in response_text:
                json_str = response_text.split('```')[1].split('```')[0]
            else:
                json_str = response_text
            
            analysis_result = json.loads(json_str.strip())
            
            # 추천 목록 변환
            suggestions = []
            for sug in analysis_result.get('suggestions', []):
                idx = sug.get('checkpoint_index', 0)
                if idx < len(checkpoints):
                    checkpoint = checkpoints[idx]
                    suggestions.append({
                        "checkpoint_id": checkpoint.get('checkpoint_id'),
                        "timestamp": checkpoint.get('created_at'),
                        "node_id": checkpoint.get('node_id'),
                        "reason": sug.get('reason'),
                        "confidence": sug.get('confidence', 0.7),
                        "estimated_impact": sug.get('estimated_impact', 'medium'),
                        "fix_strategy": sug.get('fix_strategy'),
                        "affected_variables": sug.get('affected_variables', []),
                        "tags": self._generate_rollback_tags(checkpoint),
                        "cognitive_analysis": True,  # Gemini 분석 표시
                        "root_cause": analysis_result.get('root_cause_analysis')
                    })
            
            logger.info(f"Cognitive rollback analysis completed with {len(suggestions)} suggestions")
            return suggestions[:5]
            
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse Gemini response: {e}")
            return []
        except Exception as e:
            logger.error(f"Cognitive rollback analysis failed: {e}")
            return []
    
    async def _generate_auto_fix_instructions(
        self,
        thread_id: str,
        target_checkpoint: Dict[str, Any],
        rollback_request: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Gemini Auto-Fix: 롤백 후 같은 실수 반복 방지를 위한 지침 자동 수정
        
        [Instruction Distiller 연동]
        롤백 행위 자체를 "부정적 피드백"으로 간주하고
        다음 실행에서는 같은 실수를 반복하지 않도록 지침을 조정합니다.
        """
        if not self.gemini_service:
            return None
        
        try:
            state_snapshot = target_checkpoint.get('state_snapshot', {})
            rollback_reason = rollback_request.get('reason', 'User requested rollback')
            
            auto_fix_prompt = f"""당신은 워크플로우 최적화 전문가입니다.
사용자가 롤백을 요청했습니다. 이는 워크플로우 실행에 문제가 있었다는 부정적 피드백입니다.

## 롤백 컨텍스트
- 롤백 이유: {rollback_reason}
- 롤백 대상 노드: {target_checkpoint.get('node_id')}
- 상태 스냅샷 키: {list(state_snapshot.keys())[:10]}

## 작업 요청
1. 롤백이 필요했던 근본 원인을 추론하세요.
2. 다음 실행에서 같은 문제가 반복되지 않도록 수정된 지침을 생성하세요.
3. 새로운 가드레일이나 검증 단계를 제안하세요.

## 응답 형식 (JSON)
```json
{{
  "root_cause": "추론된 근본 원인...",
  "fix_strategy": "수정 전략...",
  "modified_instructions": "다음 실행 시 적용할 수정된 지침...",
  "new_guardrails": ["추가 검증 1", "추가 검증 2"],
  "confidence": 0.8
}}
```

JSON만 응답하세요."""

            response = self.gemini_service.invoke_model(
                user_prompt=auto_fix_prompt,
                max_output_tokens=1024,
                temperature=0.4
            )
            
            response_text = response.get('text', '')
            if '```json' in response_text:
                json_str = response_text.split('```json')[1].split('```')[0]
            else:
                json_str = response_text
            
            auto_fix_result = json.loads(json_str.strip())
            logger.info(f"Auto-fix instructions generated for rollback")
            return auto_fix_result
            
        except Exception as e:
            logger.warning(f"Auto-fix generation failed: {e}")
            return None
    
    def _heuristic_rollback_suggestions(
        self,
        checkpoints: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """휴리스틱 기반 롤백 추천 (Gemini 불가 시 폴백)"""
        suggestions = []
        
        for checkpoint in checkpoints:
            score = self._calculate_rollback_score(checkpoint)
            
            if score > 0.5:
                suggestion = {
                    "checkpoint_id": checkpoint.get('checkpoint_id'),
                    "timestamp": checkpoint.get('created_at'),
                    "node_id": checkpoint.get('node_id'),
                    "reason": self._generate_rollback_reason(checkpoint),
                    "confidence": score,
                    "estimated_impact": "low" if score < 0.7 else "medium",
                    "tags": self._generate_rollback_tags(checkpoint),
                    "cognitive_analysis": False  # 휴리스틱 표시
                }
                suggestions.append(suggestion)
        
        suggestions.sort(key=lambda x: x['confidence'], reverse=True)
        return suggestions[:5]

    def _estimate_rollback_impact(
        self,
        target_checkpoint: Dict[str, Any],
        current_checkpoint: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """롤백 영향도 추정 - 상세 분석"""
        if not current_checkpoint:
            return {"level": "unknown", "details": "현재 상태를 알 수 없습니다."}
        
        target_state = target_checkpoint.get('state_snapshot', {})
        current_state = current_checkpoint.get('state_snapshot', {})
        
        # 변경된 변수 수 계산
        changed_vars = 0
        for key in set(target_state.keys()) | set(current_state.keys()):
            if target_state.get(key) != current_state.get(key):
                changed_vars += 1
        
        # 시간 차이 계산
        try:
            target_time = datetime.fromisoformat(target_checkpoint.get('created_at', '').replace('Z', '+00:00'))
            current_time = datetime.fromisoformat(current_checkpoint.get('created_at', '').replace('Z', '+00:00'))
            time_diff_seconds = abs((current_time - target_time).total_seconds())
        except Exception:
            time_diff_seconds = 0
        
        # 영향도 레벨 결정
        if changed_vars < 3 and time_diff_seconds < 60:
            level = "low"
            description = "최소한의 변경으로 안전하게 롤백할 수 있습니다."
        elif changed_vars < 10 and time_diff_seconds < 300:
            level = "medium"
            description = "일부 상태 변경이 있지만 관리 가능한 수준입니다."
        else:
            level = "high"
            description = "많은 상태 변경이 있습니다. 신중한 검토가 필요합니다."
        
        return {
            "level": level,
            "description": description,
            "changed_variables": changed_vars,
            "time_difference_seconds": time_diff_seconds,
            "steps_back": self._calculate_steps_back(target_checkpoint, current_checkpoint)
        }
    
    def _calculate_steps_back(
        self,
        target_checkpoint: Dict[str, Any],
        current_checkpoint: Optional[Dict[str, Any]]
    ) -> int:
        """롤백할 단계 수 계산"""
        # 간단한 구현: 시간 기반 추정
        if not current_checkpoint:
            return 0
        
        try:
            target_idx = target_checkpoint.get('sequence_number', 0)
            current_idx = current_checkpoint.get('sequence_number', 0)
            return abs(current_idx - target_idx)
        except Exception:
            return 1

    def _get_affected_nodes(
        self,
        thread_id: str,
        target_checkpoint_id: str
    ) -> List[Dict[str, str]]:
        """영향받는 노드 목록 계산 - 상세 정보 포함"""
        # TODO: 실제로는 체크포인트 이후 실행된 노드들을 분석해야 함
        return [
            {
                "node_id": "node_after_checkpoint",
                "type": "unknown",
                "will_be_reverted": True
            }
        ]

    def _preview_data_changes(
        self,
        target_checkpoint: Dict[str, Any],
        current_checkpoint: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """데이터 변경 미리보기 - 상세 분석"""
        target_state = target_checkpoint.get('state_snapshot', {})
        current_state = current_checkpoint.get('state_snapshot', {}) if current_checkpoint else {}
        
        # 복원될 변수
        restored = []
        removed = []
        modified = []
        
        all_keys = set(target_state.keys()) | set(current_state.keys())
        
        for key in all_keys:
            in_target = key in target_state
            in_current = key in current_state
            
            if in_target and not in_current:
                restored.append(key)
            elif not in_target and in_current:
                removed.append(key)
            elif target_state.get(key) != current_state.get(key):
                modified.append(key)
        
        return {
            "restored_variables": restored[:10],
            "removed_variables": removed[:10],
            "modified_variables": modified[:10],
            "total_restored": len(restored),
            "total_removed": len(removed),
            "total_modified": len(modified),
            "estimated_size_kb": len(json.dumps(target_state, default=str)) / 1024 if target_state else 0,
        }

    def _generate_rollback_warnings(
        self,
        rollback_request: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """롤백 경고 생성 - 구조화된 형태"""
        warnings = []
        
        # 기본 정보
        if not STATE_MACHINE_ARN:
            warnings.append({
                "level": "info",
                "message": "STATE_MACHINE_ARN이 설정되지 않아 시뮬레이션 모드로 동작합니다."
            })
        
        # 조건부 경고
        if rollback_request.get('force', False):
            warnings.append({
                "level": "warning",
                "message": "강제 롤백이 활성화되었습니다. 모든 검증을 건너뜁니다."
            })
        
        if not rollback_request.get('enable_auto_fix', True):
            warnings.append({
                "level": "info",
                "message": "Auto-Fix가 비활성화되었습니다. 동일한 문제가 재발할 수 있습니다."
            })
        
        return warnings

    def _calculate_rollback_score(
        self,
        checkpoint: Dict[str, Any]
    ) -> float:
        """롤백 추천 점수 계산"""
        score = 0.5  # 기본 점수
        
        # 상태 기반 점수 조정
        status = checkpoint.get('status', '')
        if status in ['COMPLETED', 'SUCCEEDED']:
            score += 0.3
        elif status in ['FAILED', 'ERROR']:
            score += 0.4
        
        # 노드 ID 기반 점수 조정
        node_id = checkpoint.get('node_id', '')
        if node_id and len(node_id) > 0:
            score += 0.1
        
        return min(score, 1.0)

    def _generate_rollback_reason(
        self,
        checkpoint: Dict[str, Any]
    ) -> str:
        """롤백 추천 이유 생성"""
        status = checkpoint.get('status', '')
        node_id = checkpoint.get('node_id', 'Unknown')
        
        if status in ['COMPLETED', 'SUCCEEDED']:
            return f"'{node_id}' 단계가 성공적으로 완료된 안정적인 지점입니다."
        elif status in ['FAILED', 'ERROR']:
            return f"'{node_id}' 단계에서 오류가 발생하기 직전의 지점입니다."
        else:
            return f"'{node_id}' 단계 실행 지점입니다."

    def _generate_rollback_tags(
        self,
        checkpoint: Dict[str, Any]
    ) -> List[str]:
        """롤백 태그 생성"""
        tags = []
        
        status = checkpoint.get('status', '')
        if status in ['COMPLETED', 'SUCCEEDED']:
            tags.append('stable')
        elif status in ['FAILED', 'ERROR']:
            tags.append('error-point')
        
        node_id = checkpoint.get('node_id', '')
        if 'llm' in node_id.lower():
            tags.append('llm-node')
        elif 'api' in node_id.lower():
            tags.append('api-node')
        
        return tags