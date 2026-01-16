"""
백엔드 전체에서 사용되는 상수 및 설정값 통합 관리
매직 넘버 제거 및 중앙화된 설정 관리

사용법:
    from src.common.constants import TTLConfig, QuotaLimits, ModelPricing
    
    # TTL 설정
    ttl = int(time.time()) + TTLConfig.WEBSOCKET_CONNECTION
    
    # 쿼터 제한
    limit = QuotaLimits.get_workflow_limit(subscription_plan, stage_name)
"""

import os
from decimal import Decimal
from typing import Dict, Any, Optional
from enum import Enum


class TTLConfig:
    """TTL (Time To Live) 관련 상수"""
    
    # WebSocket 연결 TTL (2시간)
    WEBSOCKET_CONNECTION = 7200
    
    # Task Token TTL (1일)
    TASK_TOKEN_DEFAULT = 86400
    
    # Pending Notification TTL (30일)
    PENDING_NOTIFICATION = 2592000
    
    # Execution Record TTL (90일)
    EXECUTION_RECORD = 90 * 24 * 3600
    
    # Pricing Cache TTL (1시간)
    PRICING_CACHE = 3600


class QuotaLimits:
    """사용량 제한 관련 상수"""
    
    # 무료 티어 제한
    FREE_TIER_DEV = 10000
    FREE_TIER_PROD = 50
    
    # 프리미엄 티어 제한
    PREMIUM_TIER = 10**9
    
    # 샘플링 제한
    USAGE_COLLECTION_SAMPLE_SIZE = 50
    USAGE_COLLECTION_MAX_DEPTH = 10
    
    # 출력 크기 제한 (1MB)
    MAX_OUTPUT_SIZE_BYTES = 1024 * 1024
    
    @classmethod
    def get_workflow_limit(cls, subscription_plan: str, stage_name: str) -> int:
        """구독 플랜과 스테이지에 따른 워크플로우 제한 반환"""
        if subscription_plan == 'free':
            return cls.FREE_TIER_DEV if stage_name == 'dev' else cls.FREE_TIER_PROD
        else:
            return cls.PREMIUM_TIER


class ModelPricing:
    """LLM 모델 가격 정보 (기본값, Parameter Store에서 오버라이드 가능)"""
    
    DEFAULT_MODELS = {
        "gpt-4": {
            "input_per_1k": Decimal("0.03"),
            "output_per_1k": Decimal("0.06")
        },
        "gpt-4-turbo": {
            "input_per_1k": Decimal("0.01"),
            "output_per_1k": Decimal("0.03")
        },
        "gpt-3.5-turbo": {
            "input_per_1k": Decimal("0.002"),
            "output_per_1k": Decimal("0.002")
        },
        "claude-3": {
            "input_per_1k": Decimal("0.015"),
            "output_per_1k": Decimal("0.075")
        },
        "claude-2": {
            "input_per_1k": Decimal("0.008"),
            "output_per_1k": Decimal("0.024")
        },
        "gemini-pro": {
            "input_per_1k": Decimal("0.001"),
            "output_per_1k": Decimal("0.002")
        }
    }
    
    # 기본 모델 (알 수 없는 모델일 때 사용)
    DEFAULT_MODEL = "gpt-3.5-turbo"
    
    # 토큰당 비용 계산 기준
    TOKENS_PER_THOUSAND = Decimal("1000")
    
    # 비용 반올림 정밀도 (마이크로센트 단위)
    COST_PRECISION = Decimal("0.000001")


class LLMModels:
    """
    LLM 모델 ID 상수 통합
    
    Usage:
        from src.common.constants import LLMModels
        
        model_id = LLMModels.CLAUDE_3_HAIKU
    """
    
    # AWS Bedrock - Claude 모델
    CLAUDE_3_HAIKU = os.getenv("HAIKU_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
    CLAUDE_3_SONNET = os.getenv("SONNET_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0")
    CLAUDE_3_OPUS = os.getenv("OPUS_MODEL_ID", "anthropic.claude-3-opus-20240229-v1:0")
    
    # Google Gemini 모델
    GEMINI_2_0_FLASH = os.getenv("GEMINI_FLASH_2_MODEL_ID", "gemini-2.0-flash")
    GEMINI_1_5_PRO = os.getenv("GEMINI_PRO_MODEL_ID", "gemini-1.5-pro-latest")
    GEMINI_1_5_FLASH = os.getenv("GEMINI_FLASH_MODEL_ID", "gemini-1.5-flash")
    GEMINI_1_5_FLASH_8B = os.getenv("GEMINI_FLASH_8B_MODEL_ID", "gemini-1.5-flash-8b")
    
    # 기본 모델 별칭
    DEFAULT_ANALYSIS = CLAUDE_3_HAIKU  # 빠른 분석용
    DEFAULT_REASONING = GEMINI_1_5_PRO  # 복잡한 추론용
    DEFAULT_REALTIME = GEMINI_1_5_FLASH  # 실시간 협업용


class HTTPStatusCodes:
    """HTTP 상태 코드 상수"""
    
    # 성공
    OK = 200
    CREATED = 201
    
    # 클라이언트 에러
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    CONFLICT = 409
    TOO_MANY_REQUESTS = 429
    
    # 서버 에러
    INTERNAL_SERVER_ERROR = 500
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503


class RetryConfig:
    """재시도 관련 설정"""
    
    # 기본 재시도 간격 (초)
    DEFAULT_RETRY_AFTER = 5
    
    # DynamoDB 쓰로틀링 재시도 간격
    DYNAMODB_THROTTLE_RETRY = 5
    DYNAMODB_THROUGHPUT_RETRY = 10
    
    # S3 재시도 간격
    S3_SLOWDOWN_RETRY = 5
    
    # LLM API 재시도 간격
    LLM_RATE_LIMIT_RETRY = 60


class WorkflowConfig:
    """워크플로우 관련 설정"""
    
    # 워크플로우 ID 해시 길이
    WORKFLOW_ID_HASH_LENGTH = 32
    
    # 워크플로우 이름 솔트
    WORKFLOW_NAME_SALT = "analemma_workflow_v1"
    
    # S3 상태 오프로드 임계값 (기본 250KB)
    DEFAULT_INLINE_THRESHOLD = 250000
    
    # 메시지 윈도우 크기
    DEFAULT_MESSAGES_WINDOW = 20


class LoggingConfig:
    """로깅 관련 설정"""
    
    # 기본 로그 레벨
    DEFAULT_LOG_LEVEL = "INFO"
    
    # 디버그 로그 최대 길이
    DEBUG_LOG_MAX_LENGTH = 2000
    
    # 서비스 이름
    DEFAULT_SERVICE_NAME = "analemma-backend"


class SecurityConfig:
    """보안 관련 설정"""
    
    # JWT 클레임 키
    OWNER_ID_CLAIM = "sub"
    
    # API Gateway 정책 버전
    POLICY_VERSION = "2012-10-17"
    
    # WebSocket 인증 쿼리 파라미터
    WEBSOCKET_TOKEN_PARAM = "token"


class EnvironmentVariables:
    """환경 변수 키 상수"""
    
    # 테이블 이름
    WORKFLOWS_TABLE = "WORKFLOWS_TABLE"
    EXECUTIONS_TABLE = "EXECUTIONS_TABLE"
    USERS_TABLE = "USERS_TABLE"
    TASK_TOKENS_TABLE = "TASK_TOKENS_TABLE_NAME"
    IDEMPOTENCY_TABLE = "IDEMPOTENCY_TABLE"
    WEBSOCKET_CONNECTIONS_TABLE = "WEBSOCKET_CONNECTIONS_TABLE"
    USER_USAGE_TABLE = "USER_USAGE_TABLE"
    
    # S3 버킷
    SKELETON_S3_BUCKET = "SKELETON_S3_BUCKET"
    
    # Step Functions
    WORKFLOW_ORCHESTRATOR_ARN = "WORKFLOW_ORCHESTRATOR_ARN"
    
    # WebSocket
    WEBSOCKET_ENDPOINT_URL = "WEBSOCKET_ENDPOINT_URL"
    
    # 설정
    MOCK_MODE = "MOCK_MODE"
    LOG_LEVEL = "LOG_LEVEL"
    STAGE_NAME = "STAGE_NAME"
    
    # API 키 (Secrets Manager 참조)
    OPENAI_API_KEY = "OPENAI_API_KEY"
    ANTHROPIC_API_KEY = "ANTHROPIC_API_KEY"
    GOOGLE_API_KEY = "GOOGLE_API_KEY"
    
    # 가격 설정
    PRICING_CONFIG_PARAM = "PRICING_CONFIG_PARAM"
    
    # TTL 설정
    TASK_TOKEN_TTL_SECONDS = "TASK_TOKEN_TTL_SECONDS"
    RETENTION_DAYS = "RETENTION_DAYS"


class DynamoDBConfig:
    """DynamoDB 관련 설정
    
    🚨 [Critical] 모든 기본값은 template.yaml의 실제 리소스명과 일치해야 함
    - 테이블명: TableName 속성값 (예: Workflows-v3-${StageName})의 논리 이름 (WorkflowsTableV3)
    - GSI명: template.yaml의 IndexName 속성값과 정확히 일치
    """
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 테이블 이름 (환경변수에서 가져옴)
    # 기본값: template.yaml !Ref 리소스 논리 이름과 동일한 형식
    # ═══════════════════════════════════════════════════════════════════════════
    WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
    EXECUTIONS_TABLE = os.environ.get('EXECUTIONS_TABLE', 'ExecutionsTableV3')
    PENDING_NOTIFICATIONS_TABLE = os.environ.get('PENDING_NOTIFICATIONS_TABLE', 'PendingNotificationsTableV3')
    # 🚨 [Critical Fix] 환경변수 통일: TASK_TOKENS_TABLE_NAME을 우선 사용 (template.yaml과 일치)
    TASK_TOKENS_TABLE = os.environ.get('TASK_TOKENS_TABLE_NAME', os.environ.get('TASK_TOKENS_TABLE', 'TaskTokensTableV3'))
    WEBSOCKET_CONNECTIONS_TABLE = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE', 'WebsocketConnectionsTableV3')
    USERS_TABLE = os.environ.get('USERS_TABLE', 'UsersTableV3')
    IDEMPOTENCY_TABLE = os.environ.get('IDEMPOTENCY_TABLE', 'IdempotencyTableV3')
    USER_USAGE_TABLE = os.environ.get('USER_USAGE_TABLE', 'UserUsageTableV3')
    BEDROCK_JOB_TABLE = os.environ.get('BEDROCK_JOB_TABLE', 'BedrockJobTableV3')
    CHECKPOINTS_TABLE = os.environ.get('CHECKPOINTS_TABLE', 'CheckpointsTableV3')
    SKILLS_TABLE = os.environ.get('SKILLS_TABLE', 'SkillsTableV3')
    CORRECTION_LOGS_TABLE = os.environ.get('CORRECTION_LOGS_TABLE', 'CorrectionLogsTable')
    DISTILLED_INSTRUCTIONS_TABLE = os.environ.get('DISTILLED_INSTRUCTIONS_TABLE', 'DistilledInstructionsTable')
    WORKFLOW_BRANCHES_TABLE = os.environ.get('WORKFLOW_BRANCHES_TABLE', 'WorkflowBranchesTable')
    CONFIRMATION_TOKENS_TABLE = os.environ.get('CONFIRMATION_TOKENS_TABLE', 'ConfirmationTokensTable')
    NODE_STATS_TABLE = os.environ.get('NODE_STATS_TABLE', 'NodeStatsTable')
    TASK_EVENTS_TABLE = os.environ.get('TASK_EVENTS_TABLE', 'TaskEventsTable')
    
    # ═══════════════════════════════════════════════════════════════════════════
    # GSI 이름 (template.yaml GlobalSecondaryIndexes.IndexName과 정확히 일치)
    # ⚠️ V2 접미사 제거: template.yaml에는 V2 없음
    # ═══════════════════════════════════════════════════════════════════════════
    # WorkflowsTableV3 GSI
    # 🚨 [Critical Fix] dynamo_db_set.py와 일치시킴 (V2 접미사 사용)
    OWNER_ID_NAME_INDEX = os.environ.get('OWNER_ID_NAME_INDEX', 'OwnerIdNameIndexV2')
    SCHEDULED_WORKFLOWS_INDEX = os.environ.get('SCHEDULED_WORKFLOWS_INDEX', 'ScheduledWorkflowsIndex')
    
    # ExecutionsTableV3 GSI
    OWNER_ID_START_DATE_INDEX = os.environ.get('OWNER_ID_START_DATE_INDEX', 'OwnerIdStartDateIndex')
    OWNER_ID_STATUS_INDEX = os.environ.get('OWNER_ID_STATUS_INDEX', 'OwnerIdStatusIndex')
    NOTIFICATIONS_INDEX = os.environ.get('NOTIFICATIONS_INDEX', 'NotificationsIndex')
    
    # WebsocketConnectionsTableV3 GSI
    WEBSOCKET_OWNER_ID_GSI = os.environ.get('WEBSOCKET_OWNER_ID_GSI', 'OwnerIdConnectionIndex')
    
    # TaskTokensTableV3 / PendingNotificationsTableV3 GSI
    EXECUTION_ID_INDEX = os.environ.get('EXECUTION_ID_INDEX', 'ExecutionIdIndex')
    
    # CheckpointsTableV3 GSI
    TIME_INDEX = os.environ.get('TIME_INDEX', 'TimeIndex')
    
    # SkillsTableV3 GSI
    OWNER_ID_INDEX = os.environ.get('OWNER_ID_INDEX', 'OwnerIdIndex')
    CATEGORY_INDEX = os.environ.get('CATEGORY_INDEX', 'CategoryIndex')
    VISIBILITY_INDEX = os.environ.get('VISIBILITY_INDEX', 'VisibilityIndex')
    
    # CorrectionLogsTable GSI
    TASK_CATEGORY_INDEX = os.environ.get('TASK_CATEGORY_INDEX', 'task-category-index-v2')
    USER_RECENT_INDEX = os.environ.get('USER_RECENT_INDEX', 'user-recent-index-v2')
    
    # WorkflowBranchesTable GSI
    ROOT_THREAD_INDEX = os.environ.get('ROOT_THREAD_INDEX', 'root-thread-index')
    
    # ConfirmationTokensTable GSI
    USER_ID_INDEX = os.environ.get('USER_ID_INDEX', 'UserIdIndex')
    
    # TaskEventsTable GSI
    OWNER_ID_TIMESTAMP_INDEX = os.environ.get('OWNER_ID_TIMESTAMP_INDEX', 'OwnerIdTimestampIndex')
    
    # 배치 크기
    BATCH_WRITE_SIZE = 25
    
    # 쿼리 제한
    DEFAULT_QUERY_LIMIT = 100
    MAX_QUERY_LIMIT = 100


def get_env_var(key: str, default: Any = None, required: bool = False) -> Any:
    """
    환경 변수를 안전하게 가져오는 헬퍼 함수
    
    Args:
        key: 환경 변수 키
        default: 기본값
        required: 필수 여부
    
    Returns:
        환경 변수 값 또는 기본값
    
    Raises:
        ValueError: required=True인데 환경 변수가 없을 때
    """
    value = os.environ.get(key, default)
    
    if required and value is None:
        raise ValueError(f"Required environment variable '{key}' is not set")
    
    return value


def get_table_name(table_key: str) -> str:
    """
    테이블 이름을 환경 변수에서 가져오는 헬퍼 함수
    
    Args:
        table_key: 환경 변수 키 (예: "WORKFLOWS_TABLE")
    
    Returns:
        테이블 이름
    
    Raises:
        ValueError: 테이블 이름이 설정되지 않았을 때
    """
    return get_env_var(table_key, required=True)


def is_mock_mode() -> bool:
    """MOCK_MODE 환경 변수 확인"""
    return get_env_var(EnvironmentVariables.MOCK_MODE, "false").lower() in {"true", "1", "yes", "on"}


def get_stage_name() -> str:
    """스테이지 이름 반환"""
    return get_env_var(EnvironmentVariables.STAGE_NAME, "dev")


def get_log_level() -> str:
    """로그 레벨 반환"""
    return get_env_var(EnvironmentVariables.LOG_LEVEL, LoggingConfig.DEFAULT_LOG_LEVEL)


def get_inline_threshold() -> int:
    """S3 오프로드 임계값 반환"""
    try:
        return int(get_env_var("STREAM_INLINE_THRESHOLD_BYTES", WorkflowConfig.DEFAULT_INLINE_THRESHOLD))
    except (ValueError, TypeError):
        return WorkflowConfig.DEFAULT_INLINE_THRESHOLD


def get_messages_window() -> int:
    """메시지 윈도우 크기 반환"""
    try:
        return int(get_env_var("MESSAGES_WINDOW", WorkflowConfig.DEFAULT_MESSAGES_WINDOW))
    except (ValueError, TypeError):
        return WorkflowConfig.DEFAULT_MESSAGES_WINDOW