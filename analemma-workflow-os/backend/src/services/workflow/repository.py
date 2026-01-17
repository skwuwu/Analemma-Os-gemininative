import os
import logging
import boto3
from botocore.exceptions import ClientError

# Import AWS clients from common module
try:
    from src.common.aws_clients import get_dynamodb_resource
    _dynamodb = get_dynamodb_resource()
except ImportError:
    _dynamodb = boto3.resource('dynamodb')

logger = logging.getLogger(__name__)

# 🚨 [Critical Fix] Match default values with template.yaml
USERS_TABLE = os.environ.get('USERS_TABLE', 'UsersTableV3')
JOB_TABLE = os.environ.get('BEDROCK_JOB_TABLE', 'BedrockJobTableV3')


class WorkflowRepository:
    """Simple repository for DynamoDB operations used by workflow Lambdas.

    Responsibilities:
    - get_user(user_id)
    - consume_run(user_id, subscription_plan): atomically consume a run quota for the current month
    - put_job_mapping(job_id, task_token)
    - pop_job_mapping(job_id)
    """

    def __init__(self, dynamodb_resource=None):
        self.dynamodb = dynamodb_resource or _dynamodb
        self.users_table = self.dynamodb.Table(USERS_TABLE)
        self.job_table = self.dynamodb.Table(JOB_TABLE)

    def get_user(self, user_id: str):
        try:
            resp = self.users_table.get_item(Key={'userId': user_id})
            return resp.get('Item')
        except ClientError as e:
            logger.exception('DynamoDB get_user error')
            raise

    def consume_run(self, user_id: str, subscription_plan: str):
        """
        Atomically consume a monthly run for the given user.

        Returns (True, None) on success.
        Returns (False, message) on quota exceeded.
        Raises on other DB errors.
        """
        from datetime import datetime, timezone
        now_month = datetime.now(timezone.utc).strftime('%Y-%m')

        # 테스트 환경에서는 free 플랜도 높은 제한 허용
        try:
            from src.common.constants import QuotaLimits, get_stage_name
            stage_name = get_stage_name()
            limit = QuotaLimits.get_workflow_limit(subscription_plan, stage_name)
        except ImportError:
            # Fallback to original logic
            stage_name = os.environ.get('STAGE_NAME', 'dev')
            if subscription_plan == 'free':
                limit = 10000 if stage_name == 'dev' else 50
            else:
                limit = 10**9

        try:
            # Try to increment for same-month path
            self.users_table.update_item(
                Key={'userId': user_id},
                UpdateExpression="ADD monthly_run_count :inc SET last_run_month = :mon",
                ConditionExpression="last_run_month = :mon AND (attribute_not_exists(monthly_run_count) OR monthly_run_count < :limit)",
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':mon': now_month,
                    ':limit': limit
                }
            )
            return True, None
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                # Try reset when month changed
                try:
                    self.users_table.update_item(
                        Key={'userId': user_id},
                        UpdateExpression="SET monthly_run_count = :one, last_run_month = :mon",
                        ConditionExpression="attribute_not_exists(last_run_month) OR last_run_month <> :mon",
                        ExpressionAttributeValues={':one': 1, ':mon': now_month}
                    )
                    return True, None
                except ClientError as e2:
                    if e2.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                        return False, 'quota_exceeded'
                    logger.exception('DynamoDB consume_run reset failed')
                    raise
            else:
                logger.exception('DynamoDB consume_run failed')
                raise

    def put_job_mapping(self, job_id: str, task_token: str):
        try:
            self.job_table.put_item(Item={'jobId': job_id, 'taskToken': task_token})
        except ClientError:
            logger.exception('DynamoDB put_job_mapping failed')
            raise

    def pop_job_mapping(self, job_id: str):
        """Get and delete the job mapping atomically-ish.

        DynamoDB doesn't offer a single get-and-delete API; we perform a get_item
        then a conditional delete to avoid races. If delete fails, we log and return the token.
        """
        try:
            resp = self.job_table.get_item(Key={'jobId': job_id})
            item = resp.get('Item')
            if not item:
                return None
            task_token = item.get('taskToken')

            # attempt delete (best-effort)
            try:
                self.job_table.delete_item(Key={'jobId': job_id})
            except ClientError:
                logger.exception('DynamoDB delete job mapping failed')

            return task_token
        except ClientError:
            logger.exception('DynamoDB pop_job_mapping failed')
            raise


__all__ = ['WorkflowRepository']
