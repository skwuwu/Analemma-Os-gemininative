"""
Skill Models - Data structures for the Skills feature.

[v2.1] 개선사항:
1. TypedDict → Pydantic 전환 (런타임 검증 강화)
2. SemVer 버전 관리 + version_constraint 지원
3. AWSPermission 구조체 + IAM Policy 생성 헬퍼

Skills are reusable, modular units of functionality that can be:
- Injected into workflow context at runtime
- Referenced by skill_executor nodes
- Composed hierarchically (skills can depend on other skills)

Architecture follows the design in the Skills integration plan:
- Each skill contains tool_definitions, system_instructions, and dependencies
- Skills are loaded via Context Hydration and stored in WorkflowState
- skill_executor nodes reference skills by ID and execute them
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Dict, Any, Optional, Set, Literal, Union
from datetime import datetime, timezone
from enum import Enum
import re
import uuid


# =============================================================================
# [v2.1] SemVer Version Management
# =============================================================================

class VersionConstraint(str, Enum):
    """
    버전 제약 조건 타입.
    
    NPM/Cargo 스타일의 SemVer 제약을 지원.
    """
    EXACT = "exact"         # "1.0.0" - 정확히 이 버전
    CARET = "caret"         # "^1.0.0" - 1.x.x 호환 (minor/patch 업그레이드 허용)
    TILDE = "tilde"         # "~1.0.0" - 1.0.x 호환 (patch 업그레이드만 허용)
    RANGE = "range"         # ">=1.0.0 <2.0.0" - 범위 지정
    LATEST = "latest"       # 항상 최신 버전 (WARNING: 위험)


class SemVer(BaseModel):
    """
    Semantic Version 파서.
    
    Format: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
    """
    major: int = Field(ge=0)
    minor: int = Field(ge=0)
    patch: int = Field(ge=0)
    prerelease: Optional[str] = None
    build: Optional[str] = None
    
    @classmethod
    def parse(cls, version_str: str) -> "SemVer":
        """Parse a version string into SemVer components."""
        # Remove 'v' prefix if present
        version_str = version_str.lstrip('v')
        
        # Regex for SemVer
        pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$'
        match = re.match(pattern, version_str)
        
        if not match:
            raise ValueError(f"Invalid semantic version: {version_str}")
        
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
            prerelease=match.group(4),
            build=match.group(5)
        )
    
    def __str__(self) -> str:
        version = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            version += f"-{self.prerelease}"
        if self.build:
            version += f"+{self.build}"
        return version
    
    def is_compatible_with(self, constraint: str, constraint_type: VersionConstraint) -> bool:
        """
        Check if this version satisfies the given constraint.
        
        Examples:
        - "1.2.3" with "^1.0.0" (CARET) -> True (same major)
        - "2.0.0" with "^1.0.0" (CARET) -> False (different major)
        - "1.0.5" with "~1.0.0" (TILDE) -> True (same major.minor)
        """
        if constraint_type == VersionConstraint.LATEST:
            return True
        
        try:
            target = SemVer.parse(constraint.lstrip('^~'))
        except ValueError:
            return False
        
        if constraint_type == VersionConstraint.EXACT:
            return (self.major == target.major and 
                    self.minor == target.minor and 
                    self.patch == target.patch)
        
        elif constraint_type == VersionConstraint.CARET:
            # ^1.0.0: 1.x.x (major must match, minor/patch can be higher)
            if target.major == 0:
                # ^0.x.y: 0.x.y (for 0.x, minor must also match)
                return (self.major == 0 and 
                        self.minor == target.minor and 
                        self.patch >= target.patch)
            return (self.major == target.major and
                    (self.minor > target.minor or 
                     (self.minor == target.minor and self.patch >= target.patch)))
        
        elif constraint_type == VersionConstraint.TILDE:
            # ~1.0.0: 1.0.x (major.minor must match)
            return (self.major == target.major and 
                    self.minor == target.minor and 
                    self.patch >= target.patch)
        
        return False


# =============================================================================
# [v2.1] AWS Permission Mapping (IAM Integration)
# =============================================================================

class AWSService(str, Enum):
    """지원되는 AWS 서비스 목록."""
    S3 = "s3"
    DYNAMODB = "dynamodb"
    LAMBDA = "lambda"
    SQS = "sqs"
    SNS = "sns"
    SECRETS_MANAGER = "secretsmanager"
    STEP_FUNCTIONS = "states"
    BEDROCK = "bedrock"
    SES = "ses"


class AWSPermission(BaseModel):
    """
    AWS IAM 권한 정의.
    
    런타임에 이 권한이 IAM Policy로 변환됨.
    
    Example:
    {
        "service": "s3",
        "actions": ["GetObject", "PutObject"],
        "resources": ["arn:aws:s3:::my-bucket/*"],
        "conditions": {"StringEquals": {"s3:x-amz-acl": "private"}}
    }
    """
    service: AWSService
    actions: List[str] = Field(min_length=1)
    resources: List[str] = Field(
        default_factory=lambda: ["*"],
        description="ARN patterns. Use '*' for all resources (not recommended for production)"
    )
    conditions: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description="IAM Condition block"
    )
    
    @field_validator('actions')
    @classmethod
    def validate_actions(cls, v: List[str]) -> List[str]:
        """Validate action format."""
        for action in v:
            # Actions should be PascalCase (e.g., GetObject, PutItem)
            if not re.match(r'^[A-Z][a-zA-Z0-9]*$', action) and action != '*':
                raise ValueError(f"Invalid action format: {action}")
        return v
    
    def to_iam_statement(self) -> Dict[str, Any]:
        """
        Convert to IAM Policy Statement format.
        
        Returns a dict that can be added to an IAM Policy's Statement array.
        """
        # Build action ARNs
        action_prefix = self.service.value
        actions = [f"{action_prefix}:{action}" for action in self.actions]
        
        statement = {
            "Effect": "Allow",
            "Action": actions,
            "Resource": self.resources
        }
        
        if self.conditions:
            statement["Condition"] = self.conditions
        
        return statement


def generate_iam_policy(permissions: List[AWSPermission], skill_id: str) -> Dict[str, Any]:
    """
    [v2.1] 스킬의 권한 목록에서 IAM Policy 생성.
    
    이 정책은 Lambda 실행 역할에 동적으로 연결되거나,
    STS AssumeRole로 임시 자격 증명을 획득할 때 사용됨.
    
    Args:
        permissions: AWSPermission 목록
        skill_id: 스킬 ID (정책 이름/설명용)
    
    Returns:
        IAM Policy document (JSON serializable)
    """
    statements = [perm.to_iam_statement() for perm in permissions]
    
    return {
        "Version": "2012-10-17",
        "Statement": statements,
        # 메타데이터 (실제 IAM에서는 무시됨)
        "_metadata": {
            "skill_id": skill_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "description": f"Auto-generated policy for skill: {skill_id}"
        }
    }


# =============================================================================
# Core Skill Schema (Pydantic)
# =============================================================================

class ToolDefinition(BaseModel):
    """Definition of a tool that a skill can use."""
    name: str = Field(..., min_length=1, max_length=100)
    description: str = Field(default="", max_length=500)
    parameters: Dict[str, Any] = Field(default_factory=dict, description="JSON Schema for parameters")
    required_api_keys: List[str] = Field(default_factory=list)
    handler_type: Literal["llm_chat", "api_call", "operator", "python", "javascript"] = Field(...)
    handler_config: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        extra = "forbid"  # 알 수 없는 필드 거부


class SkillDependency(BaseModel):
    """
    Reference to a dependent skill.
    
    [v2.1] 버전 제약 조건 추가로 Dependency Hell 방지.
    """
    skill_id: str = Field(..., min_length=1)
    
    # [v2.1] 버전 관리 강화
    version_constraint: str = Field(
        default="^1.0.0",
        description="SemVer constraint (^, ~, exact). Avoid 'latest' in production."
    )
    constraint_type: VersionConstraint = Field(
        default=VersionConstraint.CARET,
        description="How to interpret version_constraint"
    )
    
    # 런타임에 고정된 실제 버전 (Lock 파일 역할)
    locked_version: Optional[str] = Field(
        default=None,
        description="Resolved immutable version at deploy time"
    )
    
    alias: Optional[str] = Field(
        default=None,
        description="Optional alias for namespacing in context"
    )
    
    @model_validator(mode='after')
    def warn_latest(self) -> "SkillDependency":
        """latest 사용 시 경고."""
        if self.constraint_type == VersionConstraint.LATEST:
            import logging
            logging.getLogger(__name__).warning(
                f"Skill dependency '{self.skill_id}' uses 'latest' version. "
                "This may cause unexpected breaking changes in production."
            )
        return self


class SkillSchema(BaseModel):
    """
    Complete Skill definition stored in DynamoDB.
    
    [v2.1] Pydantic으로 전환 - 런타임 검증 강화.
    
    Primary Key: skillId (HASH) + version (RANGE)
    GSI: OwnerIdIndex for user-scoped queries
    """
    # Primary identifiers
    skill_id: str = Field(..., min_length=1, max_length=100)
    version: str = Field(..., pattern=r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?$')
    
    # Ownership & access
    owner_id: str = Field(..., min_length=1)
    visibility: Literal["private", "public", "organization"] = Field(default="private")
    
    # Metadata
    name: str = Field(..., min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="general", max_length=50)
    tags: List[str] = Field(default_factory=list, max_length=20)
    
    # Core skill content
    tool_definitions: List[ToolDefinition] = Field(default_factory=list)
    system_instructions: str = Field(default="", max_length=10000)
    
    # Dependencies
    dependencies: List[SkillDependency] = Field(default_factory=list)
    
    # [v2.1] 구조화된 AWS 권한
    aws_permissions: List[AWSPermission] = Field(
        default_factory=list,
        description="Required AWS permissions (generates IAM policy)"
    )
    
    # Legacy field (deprecated, use aws_permissions)
    required_api_keys: List[str] = Field(default_factory=list)
    required_permissions: List[str] = Field(
        default_factory=list,
        description="DEPRECATED: Use aws_permissions instead"
    )
    
    # Execution hints
    timeout_seconds: int = Field(default=300, ge=1, le=900)
    retry_config: Dict[str, Any] = Field(
        default_factory=lambda: {"max_retries": 3, "backoff_multiplier": 2}
    )
    
    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Status
    status: Literal["active", "deprecated", "archived"] = Field(default="active")
    
    # [v2.1] Subgraph support
    skill_type: Literal["tool_based", "subgraph_based"] = Field(default="tool_based")
    subgraph_config: Optional[Dict[str, Any]] = Field(default=None)
    input_schema: Optional[Dict[str, Any]] = Field(default=None)
    output_schema: Optional[Dict[str, Any]] = Field(default=None)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat().replace("+00:00", "Z")
        }
    
    @model_validator(mode='after')
    def validate_skill_type(self) -> "SkillSchema":
        """스킬 타입에 따른 필드 검증."""
        if self.skill_type == "subgraph_based":
            if not self.subgraph_config:
                raise ValueError("subgraph_config is required for skill_type='subgraph_based'")
            if self.tool_definitions:
                raise ValueError("tool_definitions should be empty for subgraph_based skills")
        return self
    
    def generate_iam_policy(self) -> Dict[str, Any]:
        """이 스킬에 필요한 IAM 정책 생성."""
        return generate_iam_policy(self.aws_permissions, self.skill_id)
    
    def get_parsed_version(self) -> SemVer:
        """Parse the skill's version as SemVer."""
        return SemVer.parse(self.version)


# =============================================================================
# Runtime Skill Context (hydrated into WorkflowState)
# =============================================================================

class HydratedSkill(BaseModel):
    """
    Skill data after Context Hydration - ready for execution.
    This is what gets stored in WorkflowState.active_skills
    """
    skill_id: str
    version: str
    name: str
    tool_definitions: List[ToolDefinition] = Field(default_factory=list)
    system_instructions: str = Field(default="")
    
    # Flattened dependencies (already resolved)
    resolved_dependencies: Dict[str, "HydratedSkill"] = Field(default_factory=dict)
    
    # [v2.1] 런타임 IAM 정책 (STS 토큰 획득용)
    runtime_policy: Optional[Dict[str, Any]] = Field(default=None)


class SkillExecutionLog(BaseModel):
    """Log entry for skill execution tracking."""
    skill_id: str
    node_id: str
    tool_name: str
    input_params: Dict[str, Any] = Field(default_factory=dict)
    output: Any = None
    execution_time_ms: int = Field(ge=0)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # [v2.1] 추가 메트릭
    memory_used_mb: Optional[float] = None
    error: Optional[str] = None


# =============================================================================
# Skill Node Configuration (in workflow JSON)
# =============================================================================

class SkillExecutorNodeConfig(BaseModel):
    """
    Configuration for a skill_executor node in workflow JSON.
    """
    skill_ref: str = Field(..., min_length=1)
    skill_version: Optional[str] = Field(default=None, description="Specific version or None for latest locked")
    tool_call: str = Field(..., min_length=1)
    input_mapping: Dict[str, str] = Field(default_factory=dict)
    output_key: str = Field(..., min_length=1)
    error_handling: Literal["fail", "skip", "retry"] = Field(default="fail")


# =============================================================================
# Sub-graph Abstraction Schema
# =============================================================================

class SchemaField(BaseModel):
    """Schema definition for input/output fields."""
    type: Literal["string", "number", "integer", "boolean", "object", "array", "any"] = Field(...)
    description: str = Field(default="")
    required: bool = Field(default=False)
    default: Any = None


class SubgraphMetadata(BaseModel):
    """UI metadata for subgraph visualization."""
    name: str = Field(..., max_length=100)
    description: str = Field(default="", max_length=500)
    icon: str = Field(default="📦")
    color: str = Field(default="#6366f1", pattern=r'^#[0-9a-fA-F]{6}$')
    collapsed: bool = Field(default=True)


class SubgraphDefinition(BaseModel):
    """
    Definition of a sub-graph (nested workflow).
    """
    nodes: List[Dict[str, Any]] = Field(..., min_length=1)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    subgraphs: Optional[Dict[str, "SubgraphDefinition"]] = Field(default=None)
    input_schema: Dict[str, SchemaField] = Field(default_factory=dict)
    output_schema: Dict[str, SchemaField] = Field(default_factory=dict)
    metadata: SubgraphMetadata = Field(default_factory=lambda: SubgraphMetadata(name="Subgraph"))
    
    @model_validator(mode='after')
    def validate_nodes(self) -> "SubgraphDefinition":
        """노드 ID 중복 검사."""
        node_ids = set()
        for node in self.nodes:
            node_id = node.get("id")
            if not node_id:
                raise ValueError("All nodes must have an 'id' field")
            if node_id in node_ids:
                raise ValueError(f"Duplicate node id: {node_id}")
            node_ids.add(node_id)
        return self


class SubgraphNodeConfig(BaseModel):
    """Configuration for a subgraph node in workflow JSON."""
    subgraph_ref: Optional[str] = None
    subgraph_inline: Optional[SubgraphDefinition] = None
    skill_ref: Optional[str] = None
    input_mapping: Dict[str, str] = Field(default_factory=dict)
    output_mapping: Dict[str, str] = Field(default_factory=dict)
    metadata: SubgraphMetadata = Field(default_factory=lambda: SubgraphMetadata(name="Subgraph"))
    timeout_seconds: int = Field(default=300, ge=1, le=900)
    error_handling: Literal["fail", "skip", "isolate"] = Field(default="fail")
    
    @model_validator(mode='after')
    def validate_ref(self) -> "SubgraphNodeConfig":
        """최소 하나의 참조 필요."""
        refs = [self.subgraph_ref, self.subgraph_inline, self.skill_ref]
        if not any(refs):
            raise ValueError("One of subgraph_ref, subgraph_inline, or skill_ref is required")
        if sum(1 for r in refs if r) > 1:
            raise ValueError("Only one of subgraph_ref, subgraph_inline, or skill_ref should be set")
        return self


class ExtendedWorkflowConfig(BaseModel):
    """Extended workflow configuration with sub-graph support."""
    nodes: List[Dict[str, Any]] = Field(..., min_length=1)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    subgraphs: Dict[str, SubgraphDefinition] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Helper Functions
# =============================================================================

def create_skill_id(name: str) -> str:
    """Generate a skill ID from a name."""
    normalized = name.lower().replace(" ", "-").replace("_", "-")
    normalized = re.sub(r'[^a-z0-9-]', '', normalized)
    short_uuid = str(uuid.uuid4())[:8]
    return f"{normalized}-{short_uuid}"


def create_default_skill(
    name: str,
    owner_id: str,
    description: str = "",
    tool_definitions: Optional[List[ToolDefinition]] = None,
    system_instructions: str = "",
) -> SkillSchema:
    """Create a new skill with default values."""
    return SkillSchema(
        skill_id=create_skill_id(name),
        version="1.0.0",
        owner_id=owner_id,
        name=name,
        description=description,
        tool_definitions=tool_definitions or [],
        system_instructions=system_instructions,
    )


def lock_dependencies(
    skill: SkillSchema,
    version_resolver: callable
) -> SkillSchema:
    """
    [v2.1] 의존성 버전 고정 (Lock).
    
    배포 시점에 모든 의존성을 특정 버전으로 고정하여
    Dependency Hell 방지.
    
    Args:
        skill: 원본 스킬
        version_resolver: (skill_id, constraint) -> resolved_version 함수
    
    Returns:
        locked_version이 채워진 스킬
    """
    locked_deps = []
    
    for dep in skill.dependencies:
        resolved = version_resolver(dep.skill_id, dep.version_constraint)
        locked_dep = dep.model_copy(update={"locked_version": resolved})
        locked_deps.append(locked_dep)
    
    return skill.model_copy(update={"dependencies": locked_deps})


# Pydantic forward reference resolution
HydratedSkill.model_rebuild()
SubgraphDefinition.model_rebuild()
