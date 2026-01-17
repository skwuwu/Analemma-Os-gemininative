"""
Recover missing Users table records from src.Cognito User Pool.

This Lambda iterates Cognito users (paginated) and ensures a minimal
DynamoDB record exists for each Cognito user (idempotent).

Configuration (environment variables):
  COGNITO_USER_POOL_ID  - Cognito User Pool id (required)
  USERS_TABLE           - DynamoDB table name for users (default: 'Users')
  AWS_REGION            - AWS region (optional; boto3 will use default if not set)
  DRY_RUN               - if set to 'true' do not write to DynamoDB, only report (optional)

Recommended IAM permissions for the Lambda role (least privilege):
  - cognito-idp:ListUsers (on the target User Pool)
  - dynamodb:GetItem
  - dynamodb:PutItem

This file is safe to run repeatedly. It uses conditional PutItem to
avoid overwriting existing records and logs summary metrics.
"""

import os
import time
import logging
import boto3
from botocore.exceptions import ClientError

# Import AWS clients from common module
try:
    from src.common.aws_clients import get_dynamodb_resource
    _dynamodb_from_common = get_dynamodb_resource()
except ImportError:
    _dynamodb_from_common = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


COGNITO_USER_POOL_ID = os.environ.get('COGNITO_USER_POOL_ID')
USERS_TABLE = os.environ.get('USERS_TABLE', 'Users')
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'

_cognito = None


def get_cognito_client():
    global _cognito
    if _cognito is None:
        _cognito = boto3.client('cognito-idp')
    return _cognito


def _attrs_to_dict(attrs):
    """Convert Cognito Attributes list to dict{ name: value }"""
    out = {}
    for a in attrs or []:
        name = a.get('Name')
        value = a.get('Value')
        if name:
            out[name] = value
    return out


def ensure_user_record(table, user_id, email=None):
    """Ensure a minimal user record exists in DynamoDB.

    Uses conditional PutItem for idempotency. Returns True if created,
    False if already existed.
    """
    now = int(time.time())
    item = {
        'userId': user_id,
        'subscription_plan': 'free',
        'monthly_run_count': 0,
        'last_run_month': '1970-01',
        'createdAt': now,
    }
    if email:
        item['email'] = email

    if DRY_RUN:
        logger.info("DRY RUN: would create user %s (email=%s)", user_id, email)
        return True

    try:
        table.put_item(
            Item={k: v for k, v in item.items() if v is not None},
            ConditionExpression='attribute_not_exists(userId)'
        )
        logger.info("Created user record for userId=%s", user_id)
        return True
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == 'ConditionalCheckFailedException':
            # already exists
            return False
        logger.exception("Failed to create user %s: %s", user_id, str(e))
        raise


def lambda_handler(event, context):
    """Lambda entrypoint. If invoked directly, it will run a single
    pass over all users in the configured Cognito User Pool.
    """
    if not COGNITO_USER_POOL_ID:
        logger.error("COGNITO_USER_POOL_ID environment variable is required")
        # Fail fast so CloudWatch clearly shows a configuration error
        raise ValueError('Missing COGNITO_USER_POOL_ID environment variable')

    cognito = get_cognito_client()
    ddb = get_dynamodb_resource()
    table = ddb.Table(USERS_TABLE)

    created = 0
    skipped = 0
    errors = 0
    processed = 0

    pagination_token = None
    page = 0
    try:
        while True:
            page += 1
            if pagination_token:
                resp = cognito.list_users(UserPoolId=COGNITO_USER_POOL_ID, PaginationToken=pagination_token, Limit=60)
            else:
                resp = cognito.list_users(UserPoolId=COGNITO_USER_POOL_ID, Limit=60)

            users = resp.get('Users', [])
            logger.info("Processing page %d: %d users", page, len(users))

            for u in users:
                processed += 1
                attrs = _attrs_to_dict(u.get('Attributes'))
                sub = attrs.get('sub')
                email = attrs.get('email')
                if not sub:
                    logger.warning("Skipping Cognito user without 'sub' attribute: %s", u)
                    skipped += 1
                    continue

                try:
                    # Directly attempt the conditional put. This avoids an
                    # extra GetItem call per user and relies on the
                    # ConditionExpression for idempotency.
                    created_flag = ensure_user_record(table, sub, email=email)
                    if created_flag:
                        created += 1
                    else:
                        # conditional put found existing item
                        skipped += 1
                except Exception:
                    errors += 1

            pagination_token = resp.get('PaginationToken') or resp.get('NextToken')
            if not pagination_token:
                break

        logger.info("Recovery run complete: processed=%d created=%d skipped=%d errors=%d", processed, created, skipped, errors)
        return {
            'statusCode': 200,
            'body': {
                'processed': processed,
                'created': created,
                'skipped': skipped,
                'errors': errors
            }
        }

    except Exception as exc:
        # Log and re-raise so Lambda records an error state (and invokes
        # built-in retry/alerting as configured). Returning a 200-like
        # dict would hide operational failures.
        logger.exception("Recovery run failed: %s", str(exc))
        raise
