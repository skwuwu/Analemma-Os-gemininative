import os
import json
import logging
import boto3
import base64
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from decimal import Decimal

# 공통 모듈에서 AWS 클라이언트 및 유틸리티 가져오기
try:
    from src.common.aws_clients import get_dynamodb_resource
    from src.common.json_utils import DecimalEncoder
    from src.common.pagination_utils import (
        decode_pagination_token as decode_token,
        encode_pagination_token as encode_token
    )
    dynamodb = get_dynamodb_resource()
    _USE_COMMON_UTILS = True
except ImportError:
    dynamodb = boto3.resource('dynamodb')
    _USE_COMMON_UTILS = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EXEC_TABLE = os.environ.get('EXECUTIONS_TABLE')
# 🚨 [Critical Fix] ExecutionsTableV3는 OwnerIdStartDateIndex GSI 사용
OWNER_INDEX = os.environ.get('OWNER_INDEX', os.environ.get('OWNER_ID_START_DATE_INDEX', 'OwnerIdStartDateIndex'))

if EXEC_TABLE:
    table = dynamodb.Table(EXEC_TABLE)
else:
    table = None

# Fallback: 공통 모듈 import 실패 시에만 로컬 정의
if not _USE_COMMON_UTILS:
    class DecimalEncoder(json.JSONEncoder):
        """DynamoDB Decimal 타입을 JSON 호환되도록 변환 (Fallback)"""
        def default(self, obj):
            if isinstance(obj, Decimal):
                return float(obj) if obj % 1 else int(obj)
            return super(DecimalEncoder, self).default(obj)

    def decode_token(token):
        if not token:
            return None
        try:
            return json.loads(base64.b64decode(token).decode('utf-8'))
        except Exception:
            return None

    def encode_token(obj):
        if not obj:
            return None
        try:
            return base64.b64encode(json.dumps(obj).encode('utf-8')).decode('utf-8')
        except Exception:
            return None


def lambda_handler(event, context):
    """GET /executions
    Expects JWT authorizer with claims at event['requestContext']['authorizer']['jwt']['claims']
    Returns list of executions for the authenticated ownerId using the configured GSI.
    Supports query params: limit (int), nextToken (opaque token returned from src.previous call)
    """
    # 보안 로깅: 민감한 정보 제외하고 필요한 정보만 로깅
    logger.info('ListExecutions called: method=%s, path=%s',
                event.get('requestContext', {}).get('http', {}).get('method'),
                event.get('requestContext', {}).get('http', {}).get('path'))

    if not table:
        logger.error('No EXECUTIONS_TABLE configured')
        return {
            "statusCode": 500,
            
            "body": json.dumps({"error": "Server misconfigured: no executions table"}),
        }

    if not OWNER_INDEX:
        logger.error('No OWNER_INDEX configured')
        return {
            "statusCode": 500,
            
            "body": json.dumps({"error": "Server misconfigured: no owner index"}),
        }

    # Extract ownerId from src.JWT claims
    owner_id = None
    try:
        owner_id = (
            event.get('requestContext', {})
            .get('authorizer', {})
            .get('jwt', {})
            .get('claims', {})
            .get('sub')
        )
    except Exception:
        owner_id = None

    if not owner_id:
        logger.warning('Missing ownerId in authorizer claims')
        return {
            "statusCode": 401,
            
            "body": json.dumps({"error": "Unauthorized"}),
        }

    # Query params with enhanced validation
    qs = event.get('queryStringParameters') or {}
    limit = 20  # 기본값
    
    # limit 파라미터 유효성 검사 강화
    if qs.get('limit'):
        try:
            limit_val = int(qs.get('limit'))
            if limit_val < 1 or limit_val > 100:
                return {
                    "statusCode": 400,
                    
                    "body": json.dumps({"error": "limit must be between 1 and 100"}),
                }
            limit = limit_val
        except (ValueError, TypeError):
            return {
                "statusCode": 400,
                
                "body": json.dumps({"error": "limit must be a valid integer"}),
            }

    next_token = qs.get('nextToken')
    exclusive_start_key = None
    if next_token:
        exclusive_start_key = decode_token(next_token)

    try:
        key_expr = Key('ownerId').eq(owner_id)
        
        # ProjectionExpression 추가: 목록 조회에 필요한 핵심 필드만 선택
        # 대용량 필드(state_data, workflow_config 등) 제외하여 RCU 절약
        # DynamoDB 예약어(status, name, error)에 대한 별칭 매핑
        kwargs = {
            'IndexName': OWNER_INDEX,
            'KeyConditionExpression': key_expr,
            # Include step_function_state so frontend can inspect execution history.
            # We will strip large `state_history` details before returning to client.
            'ProjectionExpression': 'executionArn, ownerId, workflowId, #s, startDate, stopDate, #e, #n, final_result, created_at, updated_at, step_function_state, initial_input',
            'ExpressionAttributeNames': {
                '#s': 'status',
                '#n': 'name',
                '#e': 'error'  # error는 예약어이므로 별칭 사용 필수
            },
            'Limit': limit,
            'ScanIndexForward': False,
        }
        
        if exclusive_start_key:
            kwargs['ExclusiveStartKey'] = exclusive_start_key

        logger.info(f'Querying executions for owner={owner_id}, limit={limit}, index={OWNER_INDEX}')
        resp = table.query(**kwargs)
        items = resp.get('Items', [])
        lek = resp.get('LastEvaluatedKey')
        out_token = encode_token(lek) if lek else None

        # final_result 필드 JSON 파싱 (문자열인 경우)
        for item in items:
            if 'final_result' in item and isinstance(item['final_result'], str):
                try:
                    item['final_result'] = json.loads(item['final_result'])
                except (json.JSONDecodeError, TypeError):
                    # 파싱 실패 시 문자열 그대로 유지
                    pass

            # Strip detailed state_history from src.step_function_state for list responses
            if 'step_function_state' in item and isinstance(item['step_function_state'], dict):
                try:
                    # Remove large or sensitive `state_history` keys while keeping the snapshot
                    sfs = item['step_function_state']
                    if isinstance(sfs, dict):
                        sfs.pop('state_history', None)
                        # Also remove nested occurrences
                        for k in ('state_data', 'current_state'):
                            if isinstance(sfs.get(k), dict):
                                sfs[k].pop('state_history', None)
                        
                        # Inject initial_input if input is missing
                        if not sfs.get('input') and item.get('initial_input'):
                            sfs['input'] = item.get('initial_input')

                    item['step_function_state'] = sfs
                except Exception:
                    # non-fatal: if stripping fails, remove the field to avoid huge payloads
                    item.pop('step_function_state', None)

            # Security/UX: remove sensitive identifiers before returning to frontend
            for fld in ('ownerId', 'workflowId'):
                if fld in item:
                    item.pop(fld, None)

        body = {
            'executions': items,  # 프론트엔드 호환성을 위해 items -> executions로 변경
            'nextToken': out_token,
        }
        
        # DecimalEncoder를 사용하여 숫자는 숫자 그대로 JSON 변환
        return {
            'statusCode': 200,
            'body': json.dumps(body, cls=DecimalEncoder),
        }

    except ClientError as e:
        logger.exception('DynamoDB query failed: %s', e)
        return {
            "statusCode": 500,
            
            "body": json.dumps({"error": "Internal error"}),
        }
    except Exception as e:
        logger.exception('Unhandled error: %s', e)
        return {
            "statusCode": 500,
            
            "body": json.dumps({"error": "Internal error"}),
        }
