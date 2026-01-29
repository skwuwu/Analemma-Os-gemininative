import json
import re
import time
import os
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

# Import model provider function for LLM node validation
try:
    from src.common.model_router import get_model_provider, ModelProvider
except ImportError:
    get_model_provider = None
    ModelProvider = None

# Avoid importing boto3.dynamodb.conditions at module import time because
# test environments use a lightweight boto3 shim (repo-level boto3.py) or
# inject fake dynamodb modules via sys.modules. Import Key lazily where
# needed and provide a small fallback if it's unavailable.
import logging
import hashlib

# 공통 모듈에서 유틸리티 가져오기
try:
    from src.common.http_utils import get_cors_headers
    from src.common.json_utils import DecimalEncoder
    from src.common.constants import DynamoDBConfig
    _USE_COMMON_UTILS = True
except ImportError:
    _USE_COMMON_UTILS = False

# Pre-compilation: 저장 시점에 파티셔닝 수행
try:
    from src.services.workflow.partition_service import partition_workflow_advanced
    _HAS_PARTITION = True
except ImportError:
    try:
        from src.services.workflow.partition_service import partition_workflow_advanced
        _HAS_PARTITION = True
    except ImportError:
        _HAS_PARTITION = False
        partition_workflow_advanced = None


def _nl_to_structured_condition(nl: str):
    """Very small heuristic converter from src.natural-language to structured condition.

    This is intentionally conservative: it only recognizes a few common
    patterns (grammar/문법, error/오류, no/없음, continue/계속) and maps them
    to a boolean state key `syntax_ok`. If no pattern matches, return None.
    """
    if not isinstance(nl, str):
        return None
    s = nl.lower()
    # grammar-related phrases
    if "grammar" in s or "문법" in s or "syntax" in s:
        # look for negation (no grammar errors)
        if any(token in s for token in ("no", "없", "없음", "없다면", "없으면")):
            return {"lhs": "syntax_ok", "op": "equals", "rhs": True}
        # default to check that syntax_ok is True
        return {"lhs": "syntax_ok", "op": "equals", "rhs": True}
    # error count phrases
    if "error" in s or "오류" in s or "mistake" in s:
        # if sentence says 'no errors' -> syntax_ok True
        if any(token in s for token in ("no", "없", "없음", "없다면", "없으면")):
            return {"lhs": "errors_count", "op": "equals", "rhs": 0}
        return None
    # fallback: not recognized
    return None



def convert_conditions_in_config(cfg: dict) -> dict:
    """Walk edges in cfg and convert natural-language while conditions to structured dicts.

    This mutates and returns a deep-copied config where possible conversions
    have been applied. Conversion is purely heuristic and non-fatal; if no
    mapping is found the original condition is preserved.
    """
    if not isinstance(cfg, dict):
        return cfg
    import copy
    out = copy.deepcopy(cfg)
    edges = out.get("edges", [])
    for e in edges:
        if e.get("type") == "while":
            cond = e.get("condition")
            # only handle simple string natural-language forms
            if isinstance(cond, str):
                # If it's an identifier-like string, skip
                if re.match(r"^[\w\.]+$", cond):
                    continue
                structured = _nl_to_structured_condition(cond)
                if structured is not None:
                    e["condition"] = structured
    out["edges"] = edges
    return out

# Use environment variable for table name for flexibility
# 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')
SKELETON_S3_BUCKET = os.environ.get('SKELETON_S3_BUCKET')
SKELETON_S3_PREFIX = os.environ.get('SKELETON_S3_PREFIX', '')
APP_VERSION = os.environ.get('APP_VERSION')

# Module-level resource caching for performance
_dynamodb = None
_table = None

def get_table():
    global _dynamodb, _table
    if _table is None:
        if _dynamodb is None:
            _dynamodb = boto3.resource('dynamodb')
        _table = _dynamodb.Table(WORKFLOWS_TABLE)
    return _table

logger = logging.getLogger("save_workflow")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# Use common CORS headers or fallback
if _USE_COMMON_UTILS:
    JSON_HEADERS = get_cors_headers()
else:
    JSON_HEADERS = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": os.environ.get("CLOUDFRONT_DOMAIN", "*"),
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Credentials": "true"
    }


def _response(status: int, payload: dict) -> dict:
    # Use Decimal-aware serializer to avoid TypeError when DynamoDB numeric types appear
    if _USE_COMMON_UTILS:
        return {'statusCode': status, 'headers': JSON_HEADERS, 'body': json.dumps(payload, cls=DecimalEncoder)}
    else:
        def _json_default(value):
            if isinstance(value, Decimal):
                if value % 1 == 0:
                    return int(value)
                return float(value)
            raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
        return {'statusCode': status, 'headers': JSON_HEADERS, 'body': json.dumps(payload, default=_json_default)}


def _validate_workflow_config(cfg):
    """Validate workflow configuration structure and LLM node parameters."""
    if not isinstance(cfg, dict):
        return None, 'Config must be a dictionary'
    
    nodes = cfg.get('nodes', [])
    for n in nodes:
        if not isinstance(n, dict):
            continue
        # Accept both legacy 'llm_chat' and frontend 'aiModel' as LLM nodes.
        if n.get('type') in ('llm_chat', 'aiModel'):
            pc = n.get('prompt_content')
            # prompt_content is optional: when present it must be a string,
            # but empty/missing prompt is allowed and will be handled at runtime.
            if pc is not None and not isinstance(pc, str):
                return None, 'llm/aiModel prompt_content must be a string when provided'

            temp = n.get('temperature')
            if temp is not None:
                try:
                    tval = float(temp)
                    if not (0.0 <= tval <= 1.0):
                        return None, 'temperature must be between 0 and 1'
                except Exception:
                    return None, 'temperature must be a number between 0 and 1'
            
            max_toks = n.get('max_tokens')
            if max_toks is not None:
                try:
                    mval = int(max_toks)
                    if mval <= 0:
                        return None, 'max_tokens must be a positive integer'
                except Exception:
                    return None, 'max_tokens must be a positive integer'
                    
            max_iters = n.get('max_iterations')
            if max_iters is not None:
                try:
                    iv = int(max_iters)
                    if iv <= 0:
                        return None, 'max_iterations must be a positive integer'
                except Exception:
                    return None, 'max_iterations must be a positive integer'
            
            # Auto-correct provider based on model for LLM nodes
            model = n.get('model')
            if model and get_model_provider:
                try:
                    correct_provider = get_model_provider(model).value.lower()
                    current_provider = n.get('provider')
                    if current_provider != correct_provider:
                        n['provider'] = correct_provider
                        logger.info(f"Auto-corrected provider for LLM node {n.get('id', 'unknown')}: {current_provider} -> {correct_provider} (based on model: {model})")
                except Exception as e:
                    logger.warning(f"Failed to auto-correct provider for model {model}: {e}")
    
    return cfg, None

def _process_config_conversion(cfg, body):
    """Process configuration conversion with safety checks."""
    try:
        from copy import deepcopy
        converted_cfg = convert_conditions_in_config(deepcopy(cfg))
        
        warnings = []
        remaining = []
        for idx, e in enumerate(converted_cfg.get('edges', [])):
            if e.get('type') == 'while':
                cond = e.get('condition')
                if isinstance(cond, str) and not re.match(r"^[\w\.]+$", cond):
                    remaining.append((idx, e))
        
        convert_with_llm = False
        try:
            if isinstance(body.get('convert_with_llm'), bool):
                convert_with_llm = body.get('convert_with_llm')
            else:
                convert_with_llm = os.environ.get('ALLOW_SAVE_LLM_CONVERT', 'false').lower() == 'true'
        except Exception:
            convert_with_llm = False
        
        if remaining and convert_with_llm:
            for idx, edge in remaining:
                cond_text = edge.get('condition')
                converted = _nl_to_structured_condition(cond_text)
                if converted is not None:
                    converted_cfg['edges'][idx]['condition'] = converted
                else:
                    warnings.append(f"Edge at index {idx} with natural-language condition could not be converted safely")
        elif remaining and not convert_with_llm:
            for idx, _ in remaining:
                warnings.append(f"Edge at index {idx} contains a natural-language condition and was not converted; set convert_with_llm to true to attempt a single LLM conversion at save time")
        
        return converted_cfg, warnings
    except Exception:
        return cfg, []

def _convert_floats_to_decimals(obj):
    """Recursively convert float values to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_floats_to_decimals(v) for v in obj]
    return obj

def lambda_handler(event, context):
    # Handle OPTIONS request for CORS
    if event.get('httpMethod') == 'OPTIONS' or event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return _response(200, {})
    
    try:
        body = event.get('body')
        # 페이로드 2.0 대응: body가 dict, str, bytes 모두 안전하게 파싱
        if isinstance(body, bytes):
            body = body.decode('utf-8')
        if isinstance(body, dict):
            pass
        else:
            body = json.loads(body or '{}')

        # Support PUT /workflows/{id} where API Gateway supplies pathParameters.id
        # If present, prefer the path parameter as the authoritative workflowId.
        try:
            path_id = None
            if isinstance(event, dict):
                path_id = (event.get('pathParameters') or {}).get('id')
            if path_id:
                body['workflowId'] = path_id
                is_update = True  # PUT 요청: 업데이트
            else:
                is_update = False  # POST 요청: 생성
        except Exception:
            # non-fatal; continue with existing body
            is_update = False
            pass

        # Authoritative ownerId MUST come from src.the authenticated JWT's `sub` claim.
        # Do NOT trust ownerId provided in the request body or query params.
        try:
            jwt_claims = (event.get('requestContext', {})
                         .get('authorizer', {})
                         .get('jwt', {})
                         .get('claims', {}))
            owner_id = jwt_claims.get('sub') if isinstance(jwt_claims, dict) else None
        except Exception:
            owner_id = None

        if not owner_id:
            # Enforce authentication: ownerId is required and must come from src.JWT
            return _response(401, {'error': 'Unauthorized: missing owner identity'})

        # Overwrite any ownerId in the body with the JWT-derived ownerId
        body['ownerId'] = owner_id
        
        # Generate workflowId if not provided (for POST requests)
        if not body.get('workflowId'):
            import uuid
            body['workflowId'] = str(uuid.uuid4())


        # 필수 필드 체크 (ownerId는 JWT에서 이미 설정됨 or provided in body)
        # Note: is_scheduled and next_run_time are OPTIONAL. They are required
        # only when the workflow config contains a time trigger block. If no
        # time trigger is present we default is_scheduled to false and remove
        # any next_run_time to avoid accidental indexing.
        required_fields = ['workflowId', 'config', 'ownerId']
        for field in required_fields:
            if field not in body:
                return _response(400, {'error': f'Missing required field: {field}'})

        # Basic validation for LLM node fields when config is provided as dict
        cfg = body.get('config')
        if cfg is not None:
            if not isinstance(cfg, dict):
                return _response(400, {'error': 'config must be a dictionary'})
            try:
                # [개선] 자연어 처리 로직 제거: 저장 단계에서 변환하지 않고 프론트엔드나 비동기 Builder로 위임
                # cfg, warnings = _process_config_conversion(cfg, body)
                # body['config'] = cfg
                # if warnings:
                #     body['_conversion_warnings'] = warnings
                
                # Validate configuration
                validated_cfg, error_msg = _validate_workflow_config(cfg)
                if error_msg:
                    return _response(400, {'error': error_msg})
                
                # [보안 강화] 프로덕션 환경에서는 임의 코드 저장 원천 차단
                if os.environ.get('APP_ENV') == 'production':
                    for node in cfg.get('nodes', []):
                        if 'code' in node.get('config', {}):
                            return _response(400, {'error': 'Code injection is not allowed in production'})
                    
            except Exception:
                return _response(400, {'error': 'Invalid workflow config format'})

            # Ensure config is stored as a JSON string to keep DynamoDB attribute types stable
            # Persist allow_runtime_llm_eval at top-level of the saved config so
            # runtime behavior can respect the designer's intent without requiring
            # additional runtime flags.
            cfg_obj = body.get('config')
            try:
                allow_runtime_flag = False
                if isinstance(body.get('allow_runtime_llm_eval'), bool):
                    allow_runtime_flag = body.get('allow_runtime_llm_eval')
                else:
                    allow_runtime_flag = os.environ.get('ALLOW_RUNTIME_LLM_EVAL', 'false').lower() == 'true'
                if isinstance(cfg_obj, dict):
                    cfg_obj['allow_runtime_llm_eval'] = bool(allow_runtime_flag)
                    # Normalize frontend authored node types:
                    # - If a node is authored as an operator but marked as a
                    #   frontend trigger (sets._frontend_type == 'trigger'),
                    #   persist it as a `trigger` node so the runtime/scheduler
                    #   treats it accordingly.
                    try:
                        nodes_list = cfg_obj.get('nodes', [])
                        if isinstance(nodes_list, list):
                            for nd in nodes_list:
                                if not isinstance(nd, dict):
                                    continue
                                sets = nd.get('sets') or {}
                                if isinstance(sets, dict) and sets.get('_frontend_type') == 'trigger':
                                    nd['type'] = 'trigger'
                    except Exception:
                        # non-fatal; continue
                        pass

                    # Ensure edges have ids for frontend compatibility
                    try:
                        from src.common.workflow_utils import ensure_edge_ids
                        ensure_edge_ids(cfg_obj)
                    except Exception:
                        pass
                    # Preserve the config as a native dict for DynamoDB Map storage
                    try:
                        # store a shallow copy to avoid accidental later mutation
                        from copy import deepcopy as _deepcopy
                        body['config_map'] = _deepcopy(cfg_obj)
                    except Exception:
                        # Fallback: store the dict reference if deepcopy fails
                        body['config_map'] = cfg_obj
                    # Also keep the JSON string representation for backward compatibility
                    body['config'] = json.dumps(cfg_obj)
                else:
                    # if config is not a dict (unlikely here), store flag as separate attribute
                    body['config'] = json.dumps(cfg_obj)
                    body['allow_runtime_llm_eval'] = bool(allow_runtime_flag)
            except Exception:
                body['config'] = json.dumps(body['config'])

        # createdAt, updatedAt 자동 생성
        now = int(time.time())
        body['createdAt'] = body.get('createdAt', now)
        body['updatedAt'] = now

        # Normalize is_scheduled to string because the GSI uses a STRING partition key
        if 'is_scheduled' in body:
            body['is_scheduled'] = str(body['is_scheduled']).lower()
        # Ensure next_run_time is numeric (or leave as None)
        if body.get('next_run_time') is not None:
            try:
                body['next_run_time'] = int(body['next_run_time'])
            except Exception:
                pass

    # Ensure the config attribute is a JSON string and that it contains the
    # allow_runtime_llm_eval flag so runtime behavior can pick it up.
        try:
            cfg_val = body.get('config')
            if isinstance(cfg_val, dict):
                # Ensure flag exists (prefer explicit body field, else env)
                try:
                    flag = bool(body.get('allow_runtime_llm_eval')) if isinstance(body.get('allow_runtime_llm_eval'), bool) else (os.environ.get('ALLOW_RUNTIME_LLM_EVAL', 'false').lower() == 'true')
                except Exception:
                    flag = False
                cfg_val.setdefault('allow_runtime_llm_eval', flag)
                body['config'] = json.dumps(cfg_val)
            else:
                # If config is already a string, attempt to inject the flag into
                # the parsed object and then re-serialize; fallback to leaving it.
                try:
                    parsed = json.loads(cfg_val)
                    try:
                        flag = bool(body.get('allow_runtime_llm_eval')) if isinstance(body.get('allow_runtime_llm_eval'), bool) else (os.environ.get('ALLOW_RUNTIME_LLM_EVAL', 'false').lower() == 'true')
                    except Exception:
                        flag = False
                    if isinstance(parsed, dict):
                        parsed.setdefault('allow_runtime_llm_eval', flag)
                        # Ensure edges have ids even when config came in as a string
                        try:
                            from src.common.workflow_utils import ensure_edge_ids
                            ensure_edge_ids(parsed)
                        except Exception:
                            pass
                        body['config'] = json.dumps(parsed)
                except Exception:
                    # give up and leave body['config'] as-is
                    pass

        except Exception:
            pass

        # Determine whether the workflow config includes a time trigger block.
        # The frontend typically converts triggers into backend 'operator'
        # nodes with sets._frontend_type == 'trigger' and sets.trigger_type == 'time',
        # but older formats or other representations may use node.type == 'trigger'
        # or a blockId of 'time'. Be permissive when detecting.
        def _config_contains_time_trigger(cfg_obj) -> bool:
            try:
                if not isinstance(cfg_obj, dict):
                    return False
                nodes = cfg_obj.get('nodes', []) or []
                for n in nodes:
                    if not isinstance(n, dict):
                        continue
                    # direct trigger node type
                    if n.get('type') == 'trigger':
                        # if trigger node has a subtype, check for 'time'
                        st = (n.get('sets') or {}).get('trigger_type') if isinstance(n.get('sets'), dict) else None
                        if st is None or str(st).strip().lower() == 'time':
                            return True

                    # frontend conversion: triggers often become operator nodes
                    sets = n.get('sets') or {}
                    if isinstance(sets, dict) and sets.get('_frontend_type') == 'trigger':
                        ttype = sets.get('trigger_type')
                        if ttype and str(ttype).strip().lower() == 'time':
                            return True

                    # legacy or BlockLibrary blockId marker
                    if n.get('blockId') == 'time' or (isinstance(n.get('data'), dict) and n.get('data').get('blockId') == 'time'):
                        return True
                return False
            except Exception:
                return False

        # Obtain a parsed config object to inspect. Prefer config_map created
        # earlier; otherwise parse the JSON string if necessary.
        cfg_for_check = None
        try:
            if isinstance(body.get('config_map'), dict):
                cfg_for_check = body.get('config_map')
            else:
                cfg_raw = body.get('config')
                if isinstance(cfg_raw, str):
                    try:
                        parsed = json.loads(cfg_raw)
                        if isinstance(parsed, dict):
                            cfg_for_check = parsed
                    except Exception:
                        cfg_for_check = None
                elif isinstance(cfg_raw, dict):
                    cfg_for_check = cfg_raw
        except Exception:
            cfg_for_check = None

        has_time_trigger = _config_contains_time_trigger(cfg_for_check or {})
        
        # Debug logging to see what's being checked
        logger.info(f"Time trigger detection: has_time_trigger={has_time_trigger}")
        if cfg_for_check:
            logger.info(f"Config nodes count: {len(cfg_for_check.get('nodes', []))}")
            for i, node in enumerate(cfg_for_check.get('nodes', [])[:3]):  # Log first 3 nodes
                logger.info(f"Node {i}: type={node.get('type')}, blockId={node.get('blockId')}, sets={node.get('sets', {}).get('trigger_type') if isinstance(node.get('sets'), dict) else None}")

        # If a time trigger exists, enforce that the workflow is scheduled and
        # that next_run_time is provided. Otherwise default is_scheduled to false
        # and remove next_run_time to avoid unintended indexing.
        if has_time_trigger:
            # coerce body['is_scheduled'] to true (string 'true') if not set
            body['is_scheduled'] = str(True).lower()
            # Auto-generate next_run_time if not provided (use current time + 1 hour as default)
            if 'next_run_time' not in body or body.get('next_run_time') in (None, ''):
                from datetime import datetime, timedelta
                default_next_run = datetime.utcnow() + timedelta(hours=1)
                body['next_run_time'] = default_next_run.isoformat() + 'Z'
                logger.info(f"Auto-generated next_run_time for time trigger: {body['next_run_time']}")
        else:
            # Not scheduled: ensure is_scheduled=false and no next_run_time
            body['is_scheduled'] = str(False).lower()
            if 'next_run_time' in body:
                try:
                    del body['next_run_time']
                except Exception:
                    pass

        # Persist top-level 'name' derived from src.config for efficient querying
        try:
            # If config is a JSON string, try to parse it
            cfg_for_name = None
            if isinstance(body.get('config'), str):
                try:
                    parsed = json.loads(body['config'])
                    if isinstance(parsed, dict):
                        cfg_for_name = parsed
                except Exception:
                    cfg_for_name = None
            elif isinstance(body.get('config'), dict):
                cfg_for_name = body.get('config')

            if cfg_for_name and isinstance(cfg_for_name.get('name'), str):
                body['name'] = cfg_for_name.get('name')
        except Exception:
            # non-fatal: if name extraction fails, continue without top-level name
            pass

        # If no top-level name was provided or extracted, generate a sensible default
        # so that listing endpoints (which surface the `name` field) can show the
        # workflow. Use a short fallback based on workflowId to keep names unique.
        try:
            if not body.get('name') and body.get('workflowId'):
                short_id = str(body.get('workflowId'))[:8]
                body['name'] = f"untitled-{short_id}"
        except Exception:
            pass

        # Get cached table instance
        table = get_table()
        
        # Before storing, fetch existing item (if any) to allow structure
        # comparison so we only rebuild the skeleton when the graph
        # structure changed (nodes/edges). This implements the
        # 'structure vs settings' decision.
        old_item = None
        try:
            # Table now uses composite key (ownerId, workflowId). Fetch by both keys
            resp = table.get_item(Key={'ownerId': body.get('ownerId'), 'workflowId': body['workflowId']})
            old_item = resp.get('Item')
        except Exception:
            # non-fatal: continue as if no old item
            old_item = None

        # Persist the workflow into DynamoDB
        # Enforce uniqueness of (ownerId, name) using the OwnerIdNameIndex GSI.
        # We query the index with Limit=1 and project the workflowId so we can
        # allow updates to the same workflowId while preventing duplicate names
        # for other workflowIds owned by the same owner.
        try:
            if body.get('name') and body.get('ownerId'):
                # Try to use table.query if available (real DDB). In test shims
                # FakeTable may not implement query, so fall back to a scan+filter.
                items = []
                if hasattr(table, 'query'):
                    try:
                        try:
                            from boto3.dynamodb.conditions import Key as DynKey
                        except Exception:
                            # Provide a minimal DynKey fallback for test environments
                            class KeyCond:
                                def __init__(self, data):
                                    self.data = data
                                def __and__(self, other):
                                    return self.data

                            class DynKey:
                                def __init__(self, name):
                                    self.name = name
                                def eq(self, v):
                                    return KeyCond({'name': self.name, 'op': 'eq', 'value': v})

                        q = table.query(
                            IndexName=DynamoDBConfig.OWNER_ID_NAME_INDEX,
                            KeyConditionExpression=DynKey('ownerId').eq(body['ownerId']) & DynKey('name').eq(body['name']),
                            ProjectionExpression='workflowId, ownerId',
                            Limit=1,
                        )
                        items = q.get('Items', [])
                    except Exception:
                        # If the Index doesn't exist or the query fails, raise to return 500
                        raise
                else:
                    # Fallback: scan and filter in test environments
                    try:
                        resp = table.scan()
                        all_items = resp.get('Items', [])
                        for it in all_items:
                            if it.get('ownerId') == body['ownerId'] and it.get('name') == body.get('name'):
                                items.append(it)
                    except Exception:
                        raise
                if items:
                    existing_wfid = items[0].get('workflowId')
                    # If there is an existing item with a different workflowId,
                    # treat as a conflict.
                    if existing_wfid and existing_wfid != body.get('workflowId'):
                        return _response(409, {'error': 'Workflow name already exists for owner', 'conflictWorkflowId': existing_wfid})

        except ClientError as e:
            return _response(500, {'error': str(e)})
        except Exception as e:
            logger.exception('Error checking workflow name uniqueness')
            return _response(500, {'error': 'Internal error during uniqueness check'})

        try:
            # Ensure next_run_time matches GSI expectations before PutItem
            # ScheduledWorkflowsIndex expects next_run_time to be a Number (N).
            # If workflow is not scheduled, remove next_run_time to avoid indexing with NULL.
            is_scheduled_flag = str(body.get('is_scheduled', 'false')).lower() == 'true'
            next_run_time_value = body.get('next_run_time')

            if is_scheduled_flag:
                # For scheduled workflows, next_run_time must be a valid integer timestamp
                try:
                    if next_run_time_value is None:
                        raise ValueError('missing')
                    # coerce to int (if it's a numeric string)
                    body['next_run_time'] = int(next_run_time_value)
                except Exception:
                    logger.error('Invalid or missing next_run_time for scheduled workflow')
                    return _response(400, {'error': 'Scheduled workflow requires a valid numeric next_run_time'})
            else:
                # Not scheduled: remove next_run_time if present so the item won't be indexed
                if 'next_run_time' in body:
                    try:
                        del body['next_run_time']
                    except Exception:
                        pass

            # 🛡️ [Critical Fix] Initialize partition metadata with default values
            # Ensures fields exist even if partitioning is skipped or fails
            if 'llm_segments_count' not in body:
                body['llm_segments_count'] = 0
            if 'hitp_segments_count' not in body:
                body['hitp_segments_count'] = 0
            if 'total_segments' not in body:
                body['total_segments'] = 0

            # [1순위 최적화] Pre-compilation: 저장 시점에 파티셔닝 수행
            # 런타임 InitializeStateData Lambda가 partition_map을 로드만 하도록 변경
            if _HAS_PARTITION and cfg_for_check:
                try:
                    partition_start = time.time()
                    partition_result = partition_workflow_advanced(cfg_for_check)
                    
                    # 🛡️ [Critical Fix] Ensure metadata fields are never None
                    body['partition_map'] = partition_result.get('partition_map', [])
                    body['total_segments'] = partition_result.get('total_segments') or 0
                    body['llm_segments_count'] = partition_result.get('llm_segments') or 0
                    body['hitp_segments_count'] = partition_result.get('hitp_segments') or 0
                    
                    partition_time = time.time() - partition_start
                    logger.info(f"Pre-compiled partition_map: {body['total_segments']} segments "
                               f"(llm={body['llm_segments_count']}, hitp={body['hitp_segments_count']}) in {partition_time:.3f}s")
                except Exception as e:
                    # 파티셔닝 실패 시 워크플로우 저장 자체를 차단 (런타임 오류 방지)
                    logger.error(f"Partition pre-compilation failed: {e}")
                    return _response(400, {'error': f'Workflow partitioning failed: {str(e)}'})

            # [Fix] Convert floats to Decimals for DynamoDB
            body = _convert_floats_to_decimals(body)

            # [Hybrid Storage] Check for Payload Limit (DynamoDB 400KB)
            # If config > 350KB, offload to S3
            try:
                config_str = body.get('config', '')
                # Check utf-8 byte size
                config_size = len(config_str.encode('utf-8')) if isinstance(config_str, str) else 0
                
                # Threshold: 350KB (Buffer 50KB for other fields)
                if config_size > 350 * 1024 and SKELETON_S3_BUCKET:
                    workflow_id = body['workflowId']
                    # Use SKELETON_S3_PREFIX if available, else default to 'workflows/'
                    prefix = SKELETON_S3_PREFIX or 'workflows/'
                    s3_key = f"{prefix.rstrip('/')}/{workflow_id}/config.json"
                    if s3_key.startswith('/'): s3_key = s3_key[1:] # Safety
                    
                    logger.info(f"⬆️ Config size {config_size/1024:.1f}KB exceeds limit. Offloading to S3: s3://{SKELETON_S3_BUCKET}/{s3_key}")
                    
                    s3 = boto3.client('s3')
                    s3.put_object(
                        Bucket=SKELETON_S3_BUCKET,
                        Key=s3_key,
                        Body=config_str,
                        ContentType='application/json'
                    )
                    
                    # Set pointer and clear heavy fields
                    body['config_s3_ref'] = f"s3://{SKELETON_S3_BUCKET}/{s3_key}"
                    body['config_s3_size'] = config_size
                    
                    # Keep minimal metadata if possible, but clear the bulk
                    # We keep 'config' as a small placeholder or empty to avoid schema issues if any
                    # But ideally we remove it or set to empty JSON '{}'
                    body['config'] = '{}' 
                    if 'config_map' in body:
                        del body['config_map'] # Remove the map version too
                    
                    logger.info("✅ Hybrid Storage: S3 Offloading successful")
            except Exception as e:
                logger.error(f"Failed to offload config to S3: {e}")
                # Fallback: Try to save to DynamoDB (might fail if > 400KB)

            # [개선] 경쟁 상태 방지 + IDOR 방지: ConditionExpression으로 원자성/소유권 확보
            put_kwargs = {'Item': body}
            if not is_update:  # POST 요청 (새 워크플로우 생성)
                put_kwargs['ConditionExpression'] = "attribute_not_exists(workflowId)"
            else:  # PUT 요청 (기존 워크플로우 업데이트)
                # IDOR 방지: 소유자만 업데이트 가능하도록 조건부 쓰기
                from boto3.dynamodb.conditions import Attr
                put_kwargs['ConditionExpression'] = Attr('ownerId').eq(body.get('ownerId'))
            
            table.put_item(**put_kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return _response(409, {'error': 'Workflow ID already exists'})
            logger.exception('DynamoDB ClientError when saving workflow')
            return _response(500, {'error': 'Database error occurred'})
        except Exception as e:
            logger.exception('Unexpected error when saving workflow')
            return _response(500, {'error': 'Internal error saving workflow'})

        resp_payload = {'message': 'Workflow saved', 'workflowId': body['workflowId']}
        if 'name' in body:
            resp_payload['name'] = body['name']

        # We intentionally avoid performing heavy build/upload work in the
        # synchronous Save API to keep user-facing latency low. A separate
        # DynamoDB-Stream-triggered Lambda should handle building and
        # uploading skeletons asynchronously. This function therefore only
        # persists the workflow and returns success.
        # RESTful: 201 Created for new resources, 200 OK for updates
        status_code = 201 if not is_update else 200
        return _response(status_code, resp_payload)
    except ClientError as e:
        logger.exception('DynamoDB ClientError in save_workflow outer block')
        return _response(500, {'error': 'Database error occurred'})
    except Exception as e:
        logger.exception('Unexpected error in save_workflow')
        return _response(500, {'error': 'Internal server error'})
