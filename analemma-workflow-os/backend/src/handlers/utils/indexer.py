import os
import json
import logging
import time
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Import common utility modules (relative path imports not available in Lambda environment)
try:
    from src.common.websocket_utils import get_connections_for_owner
except ImportError:
    def get_connections_for_owner(owner_id):
        return []


# Resource initialization (Global scope for Warm Start)
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

# Environment variables
EXEC_TABLE = os.environ.get('EXECUTIONS_TABLE')
# 🚨 [Critical Fix] ExecutionsTableV3 uses OwnerIdStartDateIndex GSI
OWNER_INDEX = os.environ.get('OWNER_INDEX', os.environ.get('OWNER_ID_START_DATE_INDEX', 'OwnerIdStartDateIndex'))
WEBSOCKET_ENDPOINT_URL = os.environ.get('WEBSOCKET_ENDPOINT_URL')
WEBSOCKET_CONNECTIONS_TABLE = os.environ.get('WEBSOCKET_CONNECTIONS_TABLE')
WEBSOCKET_OWNER_ID_GSI = os.environ.get('WEBSOCKET_OWNER_ID_GSI', 'OwnerIdConnectionIndex')

# Table resource initialization
table = dynamodb.Table(EXEC_TABLE) if EXEC_TABLE else None
connections_table = dynamodb.Table(WEBSOCKET_CONNECTIONS_TABLE) if WEBSOCKET_CONNECTIONS_TABLE else None

# API Gateway client caching
apigw_clients = {}

def get_apigw_client(endpoint_url):
    """Factory function for client reuse"""
    if endpoint_url not in apigw_clients:
        # Correct endpoint_url protocol
        if not endpoint_url.startswith("https://") and not endpoint_url.startswith("http://"):
            formatted_url = "https://" + endpoint_url
        else:
            formatted_url = endpoint_url
            
        apigw_clients[endpoint_url] = boto3.client(
            'apigatewaymanagementapi', 
            endpoint_url=formatted_url.rstrip('/')
        )
    return apigw_clients[endpoint_url]

def safe_json_load(s):
    if s is None:
        return None
    if isinstance(s, str):
        try:
            return json.loads(s)
        except (json.JSONDecodeError, ValueError):
            return s
    return s

    return s

def _convert_floats_to_decimals(obj):
    """
    Recursively converts float values in a dictionary or list to Decimal.
    DynamoDB does not support float types; they must be converted to Decimal.
    """
    from decimal import Decimal
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_floats_to_decimals(i) for i in obj]
    return obj

def upsert_execution(item):
    if not table:
        logger.error('No Executions table defined')
        return
    
    if 'ownerId' not in item or 'executionArn' not in item:
        logger.error("Missing Key attributes (ownerId, executionArn) for upsert")
        return

    try:
        key = {
            'ownerId': item['ownerId'],
            'executionArn': item['executionArn']
        }
        
        update_expr_parts = []
        expr_attr_names = {}
        expr_attr_values = {}
        
        for k, v in item.items():
            if k in ['ownerId', 'executionArn']: continue
            
            attr_name = f"#{k}"
            attr_val = f":{k}"
            
            update_expr_parts.append(f"{attr_name} = {attr_val}")
            expr_attr_names[attr_name] = k
            expr_attr_values[attr_val] = v
            
        if not update_expr_parts:
            return
            
        update_expr = "SET " + ", ".join(update_expr_parts)
        
        table.update_item(
            Key=key,
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values
        )
        logger.info('Upserted execution %s', item.get('executionArn'))
    except ClientError as e:
        logger.exception('DynamoDB update_item failed: %s', e)

def handler_from_stepfunctions_event(event):
    detail = event.get('detail', {})
    execution_arn = detail.get('executionArn')
    status = detail.get('status')
    start_date = detail.get('startDate')
    stop_date = detail.get('stopDate')

    owner_id = None
    workflow_id = None
    output = None

    if execution_arn:
        try:
            # DescribeExecution incurs API call costs but is essential for obtaining original Input/Output
            desc = stepfunctions.describe_execution(executionArn=execution_arn)
            input_raw = desc.get('input')
            input_obj = safe_json_load(input_raw) or {}
            
            owner_id = (
                input_obj.get('ownerId')
                or input_obj.get('user_id')
                or input_obj.get('owner')
            )
            workflow_id = input_obj.get('workflowId')
            
            output_raw = desc.get('output')
            output = safe_json_load(output_raw)
            
            if not start_date: start_date = desc.get('startDate')
            if not stop_date: stop_date = desc.get('stopDate')
            if not status: status = desc.get('status')
            
        except ClientError as e:
            logger.warning('DescribeExecution failed for %s: %s', execution_arn, e)

    if not execution_arn:
        # Fallback logic for custom events
        owner_id = owner_id or detail.get('ownerId')
        workflow_id = workflow_id or detail.get('workflowId')
        output = output or detail.get('output')

    def iso_or_str(v):
        try:
            if hasattr(v, 'isoformat'):
                return v.isoformat()
        except Exception:
            pass
        return str(v) if v is not None else None

    return {
        'executionArn': execution_arn,
        'ownerId': owner_id,
        'workflowId': workflow_id,
        'status': status,
        'startDate': iso_or_str(start_date),
        'stopDate': iso_or_str(stop_date),
        'output': output
    }

def lambda_handler(event, context):
    logger.info('Indexer received event: %s', json.dumps(event, default=str))
    try:
        normalized = handler_from_stepfunctions_event(event)
        exec_arn = normalized.get('executionArn')
        owner_id = normalized.get('ownerId')
        status = normalized.get('status')
        stop_date = normalized.get('stopDate')
        
        if not exec_arn:
            logger.warning('Event ignored: no executionArn')
            return {'statusCode': 202, 'body': 'Ignored'}

        current_time = int(time.time())

        # Compose DynamoDB save item
        item = {
            'executionArn': exec_arn,
            'status': status or 'UNKNOWN',
            'updatedAt': current_time
        }
        
        # Add optional fields
        for field in ['ownerId', 'workflowId', 'startDate', 'stopDate', 'output']:
            val = normalized.get(field)
            if val is not None:
                # Try JSON parsing if output is string
                if field == 'output' and isinstance(val, str):
                    item[field] = safe_json_load(val)
                else:
                    item[field] = val

        # [Critical Fix] Notification (Sparse Index) creation logic
        # "notificationTime" should only be assigned when "work is completely finished (Terminal State)".
        TERMINAL_STATUSES = ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']
        
        if status in TERMINAL_STATUSES:
            # Notification time is stopDate or current time
            item['notificationTime'] = stop_date if stop_date else str(current_time)
        
        # DynamoDB Upsert (partial update)
        # [FIX] Convert floats to Decimals for DynamoDB compatibility
        item = _convert_floats_to_decimals(item)
        upsert_execution(item)

        # --- WebSocket Push (status change notification) ---
        if WEBSOCKET_ENDPOINT_URL and connections_table and WEBSOCKET_OWNER_ID_GSI and owner_id:
            apigw_management = get_apigw_client(WEBSOCKET_ENDPOINT_URL)
            
            websocket_payload = {
                'type': 'workflow_status',
                'payload': {
                    'status': status,
                    'startDate': normalized.get('startDate'),
                    'stopDate': stop_date,
                    'timestamp': current_time,
                    'hasOutput': normalized.get('output') is not None
                }
            }

            # Payload Size Check (32KB Limit)
            payload_str = json.dumps(websocket_payload, ensure_ascii=False)
            if len(payload_str.encode('utf-8')) > 32 * 1024:
                logger.warning("WebSocket payload too large, truncating output flag")
                websocket_payload['payload'].pop('hasOutput', None)
                websocket_payload['payload']['outputTruncated'] = True
                payload_str = json.dumps(websocket_payload, ensure_ascii=False)

            payload_bytes = payload_str.encode('utf-8')
            connection_ids = get_connections_for_owner(owner_id)
            
            for conn_id in connection_ids:
                try:
                    apigw_management.post_to_connection(ConnectionId=conn_id, Data=payload_bytes)
                except ClientError as e:
                    if e.response['Error']['Code'] == 'GoneException':
                        _delete_connection(connections_table, conn_id) # Helper function needed or implement directly
                    else:
                        logger.warning(f"Failed to push to {conn_id}: {e}")
                except Exception as e:
                    logger.warning(f"Unexpected error pushing to {conn_id}: {e}")

        return {'statusCode': 200, 'body': 'Indexed'}

    except Exception as e:
        logger.exception('Indexer handler error: %s', e)
        return {'statusCode': 500, 'body': str(e)}

# Helper for GoneException cleanup
def _delete_connection(table_resource, connection_id):
    try:
        table_resource.delete_item(Key={'connectionId': connection_id})
    except Exception:
        pass
