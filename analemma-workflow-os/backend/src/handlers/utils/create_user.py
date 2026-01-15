import os
import time
import logging
import boto3
from botocore.exceptions import ClientError
try:
    from src.common import statebag
except Exception:
    from src.common import statebag

# 공통 모듈에서 AWS 클라이언트 가져오기
try:
    from src.common.aws_clients import get_dynamodb_resource
    _dynamodb = get_dynamodb_resource()
except ImportError:
    _dynamodb = boto3.resource('dynamodb')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 🚨 [Critical Fix] 기본값을 template.yaml과 일치시킴
USERS_TABLE = os.environ.get('USERS_TABLE', 'UsersTableV3')


def lambda_handler(event, context):
    """Cognito PostConfirmation trigger to create a user record in DynamoDB.

    Expects Cognito event shape for PostConfirmation: event['request']['userAttributes']
    Contains at least 'sub' (UUID) and optionally 'email'.

    Returns the original event (required by Cognito triggers).
    """
    # Normalize state-bag inputs if present
    event = statebag.normalize_event(event)

    try:
        attrs = (event.get('request') or {}).get('userAttributes') or {}
        user_id = attrs.get('sub')
        email = attrs.get('email')

        if not user_id:
            logger.error('PostConfirmation event missing userAttributes.sub: %s', event)
            return event

        table = _dynamodb.Table(USERS_TABLE)

        item = {
            'userId': user_id,
            'email': email if email else None,
            'subscription_plan': 'free',
            # monthly_run_count and last_run_month are managed by WorkflowRepository::consume_run
            'monthly_run_count': 0,
            'last_run_month': '1970-01',
            'createdAt': int(time.time())
        }

        # Put item only if it doesn't already exist to make this idempotent.
        try:
            table.put_item(
                Item={k: v for k, v in item.items() if v is not None},
                ConditionExpression='attribute_not_exists(userId)'
            )
            logger.info('Created user record for userId=%s', user_id)
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code == 'ConditionalCheckFailedException':
                # Item already exists — that's fine (idempotent)
                logger.info('User record already exists for userId=%s', user_id)
            else:
                logger.exception('Failed to put user item for %s', user_id)
                # Do not raise: allow Cognito flow to continue; surface error via logs

    except Exception:
        logger.exception('Unexpected error in PostConfirmation create_user')

    # Cognito expects the original event returned for PostConfirmation triggers
    return event
