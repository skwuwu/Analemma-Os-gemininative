"""
Skills API Lambda Handler - CRUD operations for Skills.

[v2.1] Improvements:
1. Gemini semantic skill search (GET /skills/search?q=)
2. Breaking Change detection and warning
3. Global event loop + safe_run_async pattern
4. Code consistency (same pattern as other handlers)

Endpoints:
- POST   /skills           - Create a new skill
- GET    /skills           - List skills for authenticated user
- GET    /skills/{id}      - Get a specific skill
- PUT    /skills/{id}      - Update a skill
- DELETE /skills/{id}      - Delete a skill
- GET    /skills/public    - List public/shared skills (marketplace)
- GET    /skills/search    - [v2.1] Gemini semantic skill search

All endpoints require authentication via JWT (Cognito).
"""

import json
import os
import logging
import asyncio
from typing import Optional, Dict, Any, Tuple, List
from concurrent.futures import ThreadPoolExecutor

# Import repository
try:
    from src.services.skill_repository import SkillRepository, get_skill_repository
    from src.models.skill_models import validate_skill, create_default_skill
except ImportError:
    # Fallback for local testing
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from src.services.skill_repository import SkillRepository, get_skill_repository
    from src.models.skill_models import validate_skill, create_default_skill

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =============================================================================
# [v2.1] 전역 이벤트 루프 관리 (Warm Start 최적화)
# =============================================================================
_global_loop: Optional[asyncio.AbstractEventLoop] = None
_executor: Optional[ThreadPoolExecutor] = None

def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """[v2.1] 전역 이벤트 루프 재사용."""
    global _global_loop
    
    if _global_loop is not None:
        try:
            if not _global_loop.is_closed():
                return _global_loop
        except Exception:
            pass
    
    try:
        _global_loop = asyncio.get_running_loop()
    except RuntimeError:
        _global_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_global_loop)
    
    return _global_loop

def safe_run_async(coro):
    """[v2.1] 안전한 비동기 실행 래퍼."""
    global _executor
    
    try:
        loop = asyncio.get_running_loop()
        if _executor is None:
            _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="async_runner")
        import concurrent.futures
        future = _executor.submit(lambda: asyncio.run(coro))
        return future.result(timeout=60)
    except RuntimeError:
        loop = get_or_create_event_loop()
        return loop.run_until_complete(coro)

# =============================================================================
# [v2.1] Gemini 시만틱 스킬 검색
# =============================================================================
_gemini_model = None

def _get_gemini_model():
    """Lazy initialization of Gemini model."""
    global _gemini_model
    if _gemini_model is None:
        try:
            import google.generativeai as genai
            api_key = os.environ.get('GEMINI_API_KEY') or os.environ.get('GOOGLE_API_KEY')
            if api_key:
                genai.configure(api_key=api_key)
                _gemini_model = genai.GenerativeModel('gemini-1.5-flash')
                logger.info("Gemini 1.5 Flash model initialized for skill search")
            else:
                logger.warning("No Gemini API key found, semantic search disabled")
        except Exception as e:
            logger.warning(f"Failed to initialize Gemini model: {e}")
    return _gemini_model

# =============================================================================
# [v2.1] Breaking Change 감지 상수
# =============================================================================
# 이 필드들이 변경되면 기존 워크플로우에 영향을 줄 수 있음
BREAKING_CHANGE_FIELDS = {
    'tool_definitions',      # 툴 시그니처 변경
    'input_schema',          # 입력 스키마 변경
    'output_schema',         # 출력 스키마 변경
    'subgraph_config',       # 서브그래프 구조 변경
    'required_permissions',  # 권한 요구사항 변경
}
logger.setLevel(logging.INFO)

# Import centralized auth utility
from src.common.auth_utils import extract_owner_id_from_event


def _response(status_code: int, body: Any, headers: Dict = None) -> Dict:
    """Build Lambda proxy response."""
    resp = {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            **(headers or {})
        },
        'body': json.dumps(body, ensure_ascii=False) if body else ''
    }
    return resp


def _parse_body(event: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Parse request body, returning (parsed_body, error_message)."""
    raw_body = event.get('body')
    if not raw_body:
        return {}, None
    
    try:
        parsed = json.loads(raw_body)
        if not isinstance(parsed, dict):
            return None, 'Request body must be a JSON object'
        return parsed, None
    except json.JSONDecodeError as e:
        return None, f'Invalid JSON: {str(e)}'


def lambda_handler(event, context):
    """Main Lambda handler for Skills API."""
    # Handle CORS preflight
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    if http_method == 'OPTIONS':
        return _response(200, None)
    
    # Extract path and method
    path = event.get('path') or event.get('rawPath', '')
    path_params = event.get('pathParameters') or {}
    skill_id = path_params.get('id')
    
    logger.info("Skills API: %s %s (skill_id=%s)", http_method, path, skill_id)
    
    # Get authenticated user
    owner_id = extract_owner_id_from_event(event)
    if not owner_id:
        return _response(401, {'error': 'Authentication required'})
    
    # Get repository
    try:
        repo = get_skill_repository()
    except Exception as e:
        logger.exception("Failed to initialize SkillRepository")
        return _response(500, {'error': 'Service initialization failed'})
    
    # Route to appropriate handler
    try:
        # Check for special routes first
        if path.endswith('/public'):
            return handle_list_public(repo, event)
        
        # [v2.1] 시만틱 검색 라우트
        if path.endswith('/search'):
            return handle_semantic_search(repo, owner_id, event)
        
        if skill_id:
            # Single skill operations
            if http_method == 'GET':
                return handle_get_skill(repo, owner_id, skill_id, event)
            elif http_method == 'PUT':
                return handle_update_skill(repo, owner_id, skill_id, event)
            elif http_method == 'DELETE':
                return handle_delete_skill(repo, owner_id, skill_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
        else:
            # Collection operations
            if http_method == 'GET':
                return handle_list_skills(repo, owner_id, event)
            elif http_method == 'POST':
                return handle_create_skill(repo, owner_id, event)
            else:
                return _response(405, {'error': f'Method {http_method} not allowed'})
                
    except Exception as e:
        logger.exception("Skills API error")
        return _response(500, {'error': str(e)})


def handle_create_skill(repo: SkillRepository, owner_id: str, event: Dict) -> Dict:
    """
    Create a new skill.
    
    Supports two skill types:
    - tool_based (default): Uses tool_definitions for discrete tools
    - subgraph_based: Contains a complete subgraph workflow (for canvas abstractions)
    """
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # Validate required fields
    if not body.get('name'):
        return _response(400, {'error': 'name is required'})
    
    # Determine skill type
    skill_type = body.get('skill_type', 'tool_based')
    
    if skill_type == 'subgraph_based':
        # Create subgraph-based skill (from src.canvas abstraction)
        skill_data = _create_subgraph_skill_data(body, owner_id)
    else:
        # Create tool-based skill (default)
        skill_data = create_default_skill(
            name=body['name'],
            owner_id=owner_id,
            description=body.get('description', ''),
            tool_definitions=body.get('tool_definitions'),
            system_instructions=body.get('system_instructions', '')
        )
    
    # Override with any additional fields from src.body
    for key in ['category', 'tags', 'dependencies', 'required_api_keys', 
                'timeout_seconds', 'visibility']:
        if key in body:
            skill_data[key] = body[key]
    
    # Validate
    errors = validate_skill(skill_data)
    if errors:
        return _response(400, {'error': 'Validation failed', 'details': errors})
    
    # Create in DynamoDB
    try:
        created = repo.create_skill(skill_data)
        logger.info("Created skill: %s (type=%s, owner=%s)", 
                   created['skill_id'], skill_type, owner_id)
        return _response(201, created)
    except ValueError as e:
        return _response(409, {'error': str(e)})


def _create_subgraph_skill_data(body: Dict, owner_id: str) -> Dict:
    """
    Create skill data for a subgraph-based skill.
    
    This is called when users abstract canvas nodes into a reusable skill.
    """
    from src.models.skill_models import create_skill_id
    from datetime import datetime, timezone
    
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # Validate subgraph_config is provided
    subgraph_config = body.get('subgraph_config')
    if not subgraph_config:
        raise ValueError("subgraph_config is required for skill_type='subgraph_based'")
    
    return {
        'skill_id': create_skill_id(body['name']),
        'version': body.get('version', '1.0.0'),
        'owner_id': owner_id,
        'visibility': body.get('visibility', 'private'),
        'name': body['name'],
        'description': body.get('description', ''),
        'category': body.get('category', 'custom'),
        'tags': body.get('tags', []),
        
        # Subgraph-specific fields
        'skill_type': 'subgraph_based',
        'subgraph_config': subgraph_config,
        'input_schema': body.get('input_schema', {}),
        'output_schema': body.get('output_schema', {}),
        
        # Empty tool_definitions (not used for subgraph skills)
        'tool_definitions': [],
        'system_instructions': '',
        
        # Defaults
        'dependencies': body.get('dependencies', []),
        'required_api_keys': body.get('required_api_keys', []),
        'required_permissions': body.get('required_permissions', []),
        'timeout_seconds': body.get('timeout_seconds', 300),
        'retry_config': body.get('retry_config', {'max_retries': 3, 'backoff_multiplier': 2}),
        'created_at': now,
        'updated_at': now,
        'status': 'active',
    }


def handle_get_skill(repo: SkillRepository, owner_id: str, skill_id: str, event: Dict) -> Dict:
    """Get a specific skill by ID."""
    # Check for version query param
    query_params = event.get('queryStringParameters') or {}
    version = query_params.get('version')
    
    if version:
        skill = repo.get_skill(skill_id, version)
    else:
        skill = repo.get_latest_skill(skill_id)
    
    if not skill:
        return _response(404, {'error': 'Skill not found'})
    
    # Check access: owner or public visibility
    if skill.get('owner_id') != owner_id and skill.get('visibility') != 'public':
        return _response(403, {'error': 'Access denied'})
    
    return _response(200, skill)


def handle_list_skills(repo: SkillRepository, owner_id: str, event: Dict) -> Dict:
    """List skills for the authenticated user."""
    query_params = event.get('queryStringParameters') or {}
    
    # Safe parsing of limit with validation
    try:
        limit = int(query_params.get('limit', 50))
        limit = max(1, min(limit, 100))  # Clamp to 1-100 range
    except (TypeError, ValueError):
        limit = 50
    
    status_filter = query_params.get('status')
    
    # Parse pagination key
    last_key = None
    if query_params.get('nextToken'):
        try:
            last_key = json.loads(query_params['nextToken'])
        except json.JSONDecodeError:
            pass
    
    result = repo.list_skills_by_owner(
        owner_id=owner_id,
        limit=limit,
        last_key=last_key,
        status_filter=status_filter
    )
    
    response_body = {
        'items': result['items'],
        'count': len(result['items'])
    }
    
    if result.get('last_key'):
        response_body['nextToken'] = json.dumps(result['last_key'])
    
    return _response(200, response_body)


def handle_update_skill(repo: SkillRepository, owner_id: str, skill_id: str, event: Dict) -> Dict:
    """
    Update an existing skill.
    
    [v2.1] Breaking Change 감지:
    - tool_definitions, input/output_schema 등 중요 필드 변경 시 경고
    - 기존 워크플로우 호환성 정보 반환
    
    [v2.2] Strict Mode:
    - severity=high 변경 시 force_update=true 필수
    - 엔터프라이즈 급 변경 관리
    """
    body, error = _parse_body(event)
    if error:
        return _response(400, {'error': error})
    
    # [v2.2] Strict Mode 파라미터
    query_params = event.get('queryStringParameters') or {}
    force_update = query_params.get('force_update', '').lower() in ('true', '1', 'yes')
    strict_mode = os.environ.get('SKILL_UPDATE_STRICT_MODE', 'true').lower() in ('true', '1', 'yes')
    
    # Get version from src.body or query params
    version = body.get('version') or query_params.get('version')
    
    if not version:
        # Get latest version
        existing = repo.get_latest_skill(skill_id)
        if not existing:
            return _response(404, {'error': 'Skill not found'})
        version = existing['version']
    else:
        existing = repo.get_skill(skill_id, version)
        if not existing:
            return _response(404, {'error': 'Skill not found'})
    
    # Fields that can be updated
    allowed_updates = [
        'name', 'description', 'category', 'tags',
        'tool_definitions', 'system_instructions',
        'input_schema', 'output_schema', 'subgraph_config',
        'dependencies', 'required_api_keys', 'required_permissions',
        'timeout_seconds', 'retry_config', 'visibility', 'status'
    ]
    
    updates = {k: v for k, v in body.items() if k in allowed_updates}
    
    if not updates:
        return _response(400, {'error': 'No valid fields to update'})
    
    # [v2.1] Breaking Change 감지
    breaking_changes = _detect_breaking_changes(existing, updates)
    
    # [v2.2] Strict Mode: high severity 변경 시 force_update 필수
    if strict_mode and breaking_changes and not force_update:
        high_severity_changes = {
            field: info for field, info in breaking_changes.items()
            if info.get('severity') == 'high'
        }
        
        if high_severity_changes:
            return _response(409, {
                'error': 'Breaking changes require explicit confirmation',
                'error_code': 'BREAKING_CHANGE_BLOCKED',
                'breaking_changes': high_severity_changes,
                'message': (
                    'This update contains high-severity breaking changes that may '
                    'break existing workflows. Add ?force_update=true to proceed.'
                ),
                'action': 'Add query parameter: ?force_update=true',
                'affected_fields': list(high_severity_changes.keys())
            })
    
    try:
        updated = repo.update_skill(
            skill_id=skill_id,
            version=version,
            updates=updates,
            owner_id=owner_id  # Validates ownership
        )
        
        logger.info(
            "Updated skill: %s (owner=%s, breaking_changes=%s, force_update=%s)", 
            skill_id, owner_id, list(breaking_changes.keys()), force_update
        )
        
        # [v2.1] Breaking Change 경고 포함 (medium severity는 경고만)
        response_body = updated
        if breaking_changes:
            response_body = {
                **updated,
                '_warnings': {
                    'breaking_changes': breaking_changes,
                    'message': (
                        'This update may affect existing workflows. '
                        'Consider incrementing version or testing existing workflows.'
                    ),
                    'recommendation': 'Consider incrementing version or testing existing workflows',
                    'force_update_used': force_update
                }
            }
        
        return _response(200, response_body)
    except ValueError as e:
        return _response(404, {'error': str(e)})


def _detect_breaking_changes(existing: Dict, updates: Dict) -> Dict[str, Dict]:
    """
    [v2.1] Breaking Change 감지.
    
    중요 필드의 변경사항을 분석하여 잠재적 호환성 문제 경고.
    """
    breaking_changes = {}
    
    for field in BREAKING_CHANGE_FIELDS:
        if field not in updates:
            continue
        
        old_value = existing.get(field)
        new_value = updates[field]
        
        # 값이 다르면 breaking change
        if old_value != new_value:
            change_info = {
                'field': field,
                'severity': 'high' if field in ('tool_definitions', 'subgraph_config') else 'medium'
            }
            
            # 툴 정의 변경 상세 분석
            if field == 'tool_definitions':
                change_info['details'] = _analyze_tool_changes(old_value or [], new_value or [])
            
            # 스키마 변경 상세 분석
            elif field in ('input_schema', 'output_schema'):
                change_info['details'] = _analyze_schema_changes(old_value or {}, new_value or {})
            
            breaking_changes[field] = change_info
    
    return breaking_changes


def _analyze_tool_changes(old_tools: List, new_tools: List) -> Dict:
    """Tool definitions 변경 분석."""
    old_names = {t.get('name') for t in old_tools if isinstance(t, dict)}
    new_names = {t.get('name') for t in new_tools if isinstance(t, dict)}
    
    return {
        'removed_tools': list(old_names - new_names),
        'added_tools': list(new_names - old_names),
        'potentially_modified': list(old_names & new_names)
    }


def _analyze_schema_changes(old_schema: Dict, new_schema: Dict) -> Dict:
    """입출력 스키마 변경 분석."""
    old_props = set((old_schema.get('properties') or {}).keys())
    new_props = set((new_schema.get('properties') or {}).keys())
    
    old_required = set(old_schema.get('required') or [])
    new_required = set(new_schema.get('required') or [])
    
    return {
        'removed_properties': list(old_props - new_props),
        'added_properties': list(new_props - old_props),
        'new_required_fields': list(new_required - old_required)
    }


def handle_delete_skill(repo: SkillRepository, owner_id: str, skill_id: str, event: Dict) -> Dict:
    """Delete a skill."""
    query_params = event.get('queryStringParameters') or {}
    version = query_params.get('version')
    
    if not version:
        # Get latest version
        existing = repo.get_latest_skill(skill_id)
        if not existing:
            return _response(404, {'error': 'Skill not found'})
        version = existing['version']
    
    success = repo.delete_skill(
        skill_id=skill_id,
        version=version,
        owner_id=owner_id  # Validates ownership
    )
    
    if success:
        logger.info("Deleted skill: %s:%s (owner=%s)", skill_id, version, owner_id)
        return _response(204, None)
    else:
        return _response(404, {'error': 'Skill not found or access denied'})


def handle_list_public(repo: SkillRepository, event: Dict) -> Dict:
    """
    List public/shared skills (marketplace).
    
    Uses VisibilityIndex GSI for efficient querying of public skills.
    Supports filtering by category and pagination.
    """
    query_params = event.get('queryStringParameters') or {}
    
    category = query_params.get('category')
    
    # Safe parsing of limit with validation
    try:
        limit = int(query_params.get('limit', 50))
        limit = max(1, min(limit, 100))  # Clamp to 1-100 range
    except (TypeError, ValueError):
        limit = 50
    
    sort_order = query_params.get('sort', 'desc')  # newest first by default
    
    # Parse pagination key
    last_key = None
    if query_params.get('nextToken'):
        try:
            last_key = json.loads(query_params['nextToken'])
        except json.JSONDecodeError:
            pass
    
    # Use the new GSI-based method
    result = repo.list_public_skills(
        limit=limit,
        last_key=last_key,
        category_filter=category,
        sort_order=sort_order
    )
    
    response_body = {
        'items': result['items'],
        'count': len(result['items'])
    }
    
    if result.get('last_key'):
        response_body['nextToken'] = json.dumps(result['last_key'])
    
    return _response(200, response_body)


# =============================================================================
# [v2.1] Gemini 시맨틱 스킬 검색
# [v2.2] Vector DB RAG 검색 로드맵
# =============================================================================

# Search mode: 'context' (current) or 'rag' (production)
# - context: All skills loaded into Gemini context (PoC, <1000 skills)
# - rag: Vector DB similarity search + Gemini reranking (production, unlimited)
SKILL_SEARCH_MODE = os.environ.get('SKILL_SEARCH_MODE', 'context')


async def _search_skills_vector_db(
    query: str,
    owner_id: str,
    scope: str,
    top_k: int = 5
) -> Optional[List[Dict]]:
    """
    [v2.2] Vector DB 기반 RAG 검색 (Production 로드맵).
    
    현재는 인터페이스만 정의, 실제 구현은 VectorSyncService 연동 필요.
    
    Architecture:
    1. Query embedding via Gemini/Vertex AI
    2. Similarity search in Pinecone/Milvus
    3. Top-K retrieval + metadata filtering
    4. Optional Gemini reranking for precision
    
    Benefits over context injection:
    - O(log n) vs O(n) search complexity
    - No token limit concerns
    - Sub-100ms latency at scale
    """
    try:
        # TODO: Production implementation
        # from src.services.vector_sync_service import VectorSyncService
        # vector_service = VectorSyncService()
        # 
        # # 1. Generate query embedding
        # query_embedding = await vector_service.embed_text(query)
        # 
        # # 2. Search with scope filter
        # filter_conditions = {}
        # if scope == 'owned':
        #     filter_conditions['owner_id'] = owner_id
        # elif scope == 'public':
        #     filter_conditions['visibility'] = 'public'
        # 
        # # 3. Retrieve similar skills
        # results = await vector_service.search_skills(
        #     embedding=query_embedding,
        #     top_k=top_k * 2,  # Over-fetch for reranking
        #     filters=filter_conditions
        # )
        # 
        # # 4. Optional: Gemini reranking
        # return await _rerank_with_gemini(query, results, top_k)
        
        logger.debug("Vector DB search not yet implemented, falling back to context mode")
        return None
        
    except Exception as e:
        logger.warning(f"Vector DB search failed: {e}")
        return None


async def _search_skills_with_gemini(
    query: str, 
    skills: List[Dict],
    top_k: int = 5
) -> List[Dict]:
    """
    [v2.1] Gemini를 사용한 시맨틱 스킬 검색.
    
    사용자의 자연어 쿼리를 이해하고, 가장 적합한 스킬을 반환.
    """
    model = _get_gemini_model()
    if not model:
        # Gemini 없으면 단순 키워드 매칭으로 폴백
        return _fallback_keyword_search(query, skills, top_k)
    
    # 스킬 요약 생성 (Gemini 컨텍스트용)
    skills_context = []
    for i, skill in enumerate(skills[:50]):  # 최대 50개만 고려
        skills_context.append({
            'index': i,
            'name': skill.get('name', ''),
            'description': skill.get('description', ''),
            'category': skill.get('category', ''),
            'tags': skill.get('tags', []),
            'skill_type': skill.get('skill_type', 'tool_based')
        })
    
    prompt = f"""You are a skill matching assistant for an AI workflow system.

User Query: "{query}"

Available Skills:
{json.dumps(skills_context, indent=2, ensure_ascii=False)}

Analyze the user's intent and return the indices of the most relevant skills.
Consider:
- Semantic meaning (not just keyword matching)
- User's likely intent and use case
- Skill capabilities and descriptions

Respond in JSON format only:
{{
  "matches": [
    {{"index": 0, "relevance_score": 0.95, "reason": "brief reason"}},
    ...
  ],
  "query_interpretation": "what the user is looking for"
}}

Return up to {top_k} most relevant matches, sorted by relevance."""

    try:
        response = await asyncio.to_thread(
            lambda: model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.1,
                    "max_output_tokens": 500,
                    "response_mime_type": "application/json"
                }
            )
        )
        
        result = json.loads(response.text)
        matches = result.get('matches', [])
        
        # 결과 조합
        matched_skills = []
        for match in matches[:top_k]:
            idx = match.get('index', -1)
            if 0 <= idx < len(skills):
                skill = skills[idx].copy()
                skill['_search_meta'] = {
                    'relevance_score': match.get('relevance_score', 0),
                    'reason': match.get('reason', ''),
                    'query_interpretation': result.get('query_interpretation', '')
                }
                matched_skills.append(skill)
        
        logger.info(f"Gemini semantic search: query='{query}', found={len(matched_skills)}")
        return matched_skills
        
    except Exception as e:
        logger.warning(f"Gemini search failed, falling back to keyword: {e}")
        return _fallback_keyword_search(query, skills, top_k)


def _fallback_keyword_search(query: str, skills: List[Dict], top_k: int) -> List[Dict]:
    """Gemini 실패 시 키워드 기반 폴백 검색."""
    query_lower = query.lower()
    query_words = set(query_lower.split())
    
    scored_skills = []
    for skill in skills:
        score = 0
        
        # 이름 매칭
        name = (skill.get('name') or '').lower()
        if query_lower in name:
            score += 10
        score += sum(2 for word in query_words if word in name)
        
        # 설명 매칭
        desc = (skill.get('description') or '').lower()
        score += sum(1 for word in query_words if word in desc)
        
        # 태그 매칭
        tags = [t.lower() for t in (skill.get('tags') or [])]
        score += sum(3 for word in query_words if word in tags)
        
        if score > 0:
            skill_copy = skill.copy()
            skill_copy['_search_meta'] = {
                'relevance_score': min(score / 20, 1.0),
                'reason': 'keyword_match',
                'query_interpretation': query
            }
            scored_skills.append((score, skill_copy))
    
    # 점수 내림차순 정렬
    scored_skills.sort(key=lambda x: x[0], reverse=True)
    return [s[1] for s in scored_skills[:top_k]]


def handle_semantic_search(repo: SkillRepository, owner_id: str, event: Dict) -> Dict:
    """
    [v2.1] Gemini 시맨틱 스킬 검색.
    [v2.2] Vector DB RAG 모드 지원.
    
    GET /skills/search?q=<query>&scope=all|owned|public&mode=context|rag
    
    Search Modes:
    - context (default): Load all skills into Gemini context
      - Best for: PoC, <1000 skills, highest accuracy
      - Latency: 1-3s depending on skill count
    
    - rag (production roadmap): Vector DB similarity search
      - Best for: Production, unlimited skills
      - Latency: <100ms at scale
    
    Examples:
    - GET /skills/search?q=send email tool
    - GET /skills/search?q=PDF analyzer&scope=public
    - GET /skills/search?q=customer data processor&mode=rag
    """
    query_params = event.get('queryStringParameters') or {}
    
    query = query_params.get('q') or query_params.get('query', '')
    if not query:
        return _response(400, {'error': 'Query parameter "q" is required'})
    
    scope = query_params.get('scope', 'all')  # all, owned, public
    search_mode = query_params.get('mode') or SKILL_SEARCH_MODE
    
    try:
        top_k = int(query_params.get('limit', 5))
        top_k = max(1, min(top_k, 20))
    except (TypeError, ValueError):
        top_k = 5
    
    # [v2.2] RAG 모드 시도
    if search_mode == 'rag':
        rag_results = safe_run_async(_search_skills_vector_db(query, owner_id, scope, top_k))
        if rag_results is not None:
            return _response(200, {
                'items': rag_results,
                'count': len(rag_results),
                'query': query,
                'scope': scope,
                'search_method': 'vector_db_rag'
            })
        # RAG 실패 시 context 모드로 폴백
        logger.info("RAG search unavailable, falling back to context mode")
    
    # Context 모드: 모든 스킬 로드
    all_skills = []
    
    if scope in ('all', 'owned'):
        owned_result = repo.list_skills_by_owner(owner_id, limit=100)
        all_skills.extend(owned_result.get('items', []))
    
    if scope in ('all', 'public'):
        public_result = repo.list_public_skills(limit=100)
        # 중복 제거
        existing_ids = {s['skill_id'] for s in all_skills}
        for skill in public_result.get('items', []):
            if skill['skill_id'] not in existing_ids:
                all_skills.append(skill)
    
    if not all_skills:
        return _response(200, {
            'items': [],
            'count': 0,
            'query': query,
            'message': 'No skills available to search'
        })
    
    # 스킬 수 경고 (production에서는 RAG 권장)
    if len(all_skills) > 100:
        logger.warning(
            f"Large skill set ({len(all_skills)} skills) in context mode. "
            f"Consider switching to RAG mode for better performance."
        )
    
    # [v2.1] Gemini 시맨틱 검색 실행
    matched_skills = safe_run_async(_search_skills_with_gemini(query, all_skills, top_k))
    
    return _response(200, {
        'items': matched_skills,
        'count': len(matched_skills),
        'query': query,
        'scope': scope,
        'search_method': 'gemini_semantic' if _get_gemini_model() else 'keyword_fallback',
        'total_skills_searched': len(all_skills),
        '_hints': {
            'rag_available': False,  # TODO: Set to True when implemented
            'recommended_mode': 'rag' if len(all_skills) > 100 else 'context'
        }
    })
