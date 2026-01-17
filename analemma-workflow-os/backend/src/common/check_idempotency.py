import os
import boto3
import logging
from botocore.exceptions import ClientError

# Import AWS client from common module
try:
    from src.common.aws_clients import get_dynamodb_resource
    dynamodb = get_dynamodb_resource()
except ImportError:
    dynamodb = boto3.resource('dynamodb')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

IDEMPOTENCY_TABLE = os.environ.get('IDEMPOTENCY_TABLE')

def lambda_handler(event, context):
    """
    Checks for existing execution to prevent duplicates.
    """
    idempotency_key = event.get('idempotency_key')
    
    if not idempotency_key:
        return {
            "existing_execution_arn": None,
            "existing_execution_status": None
        }

    if not IDEMPOTENCY_TABLE:
        logger.error("IDEMPOTENCY_TABLE not configured")
        raise RuntimeError("Server configuration error: IDEMPOTENCY_TABLE missing")

    table = dynamodb.Table(IDEMPOTENCY_TABLE)

    try:
        # Query by idempotency key (based on partition key)
        # If using sort key (segment), need a key representing 'entire workflow' (e.g., segment_to_run=-1 or meta)
        # Here we assume idempotency_key is the PK.
        response = table.get_item(Key={'idempotency_key': idempotency_key})
        item = response.get('Item')

        if item:
            logger.info(f"Duplicate execution found: {idempotency_key}")
            return {
                "existing_execution_arn": item.get('executionArn'),
                "existing_execution_status": item.get('status')
            }
        
        return {
            "existing_execution_arn": None,
            "existing_execution_status": None
        }

    except Exception as e:
        logger.error(f"Idempotency check failed: {e}")
        # Even if DB error occurs, workflow shouldn't die, can skip duplicate check by returning False
        raise e
