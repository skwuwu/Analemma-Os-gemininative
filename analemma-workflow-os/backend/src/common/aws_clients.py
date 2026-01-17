"""
AWS client common module
Centralized management of Boto3 clients for Cold Start optimization and improved reusability

Usage:
    from src.common.aws_clients import get_dynamodb_resource, get_s3_client, get_stepfunctions_client
"""

import os
import logging
import boto3
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Global client cache (Lambda Cold Start optimization)
_dynamodb_resource: Optional[Any] = None
_s3_client: Optional[Any] = None
_stepfunctions_client: Optional[Any] = None
_ssm_client: Optional[Any] = None
_ecs_client: Optional[Any] = None
_lambda_client: Optional[Any] = None
_kinesis_client: Optional[Any] = None


def get_dynamodb_resource():
    """
    DynamoDB resource singleton
    
    Returns:
        boto3.resource('dynamodb')
    """
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource('dynamodb')
        logger.debug("DynamoDB resource initialized")
    return _dynamodb_resource


def get_dynamodb_table(table_name: Optional[str] = None, env_var: Optional[str] = None):
    """
    Get DynamoDB table object
    
    Args:
        table_name: Directly specified table name
        env_var: Environment variable key to get table name
        
    Returns:
        DynamoDB Table object or None
    """
    if not table_name and env_var:
        table_name = os.environ.get(env_var)
    
    if not table_name:
        logger.warning(f"Table name not provided (env_var: {env_var})")
        return None
    
    return get_dynamodb_resource().Table(table_name)


def get_s3_client():
    """
    S3 client singleton
    
    Returns:
        boto3.client('s3')
    """
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
        logger.debug("S3 client initialized")
    return _s3_client


def get_stepfunctions_client():
    """
    Step Functions client singleton
    
    Returns:
        boto3.client('stepfunctions')
    """
    global _stepfunctions_client
    if _stepfunctions_client is None:
        _stepfunctions_client = boto3.client('stepfunctions')
        logger.debug("Step Functions client initialized")
    return _stepfunctions_client


def get_ssm_client():
    """
    SSM Parameter Store client singleton
    
    Returns:
        boto3.client('ssm')
    """
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client('ssm')
        logger.debug("SSM client initialized")
    return _ssm_client


def get_ecs_client():
    """
    ECS client singleton
    
    Returns:
        boto3.client('ecs')
    """
    global _ecs_client
    if _ecs_client is None:
        _ecs_client = boto3.client('ecs')
        logger.debug("ECS client initialized")
    return _ecs_client


def get_lambda_client():
    """
    Lambda client singleton
    
    Returns:
        boto3.client('lambda')
    """
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client('lambda')
        logger.debug("Lambda client initialized")
    return _lambda_client


def get_kinesis_client():
    """
    Kinesis client singleton
    
    Returns:
        boto3.client('kinesis')
    """
    global _kinesis_client
    if _kinesis_client is None:
        _kinesis_client = boto3.client('kinesis')
        logger.debug("Kinesis client initialized")
    return _kinesis_client


# Secrets Manager client (used in secrets_utils.py)
_secrets_client: Optional[Any] = None


def get_secrets_client():
    """
    Secrets Manager client singleton
    
    Returns:
        boto3.client('secretsmanager')
    """
    global _secrets_client
    if _secrets_client is None:
        region = os.environ.get("AWS_REGION", "us-east-1")
        _secrets_client = boto3.client('secretsmanager', region_name=region)
        logger.debug("Secrets Manager client initialized")
    return _secrets_client


# Bedrock client
_bedrock_client: Optional[Any] = None


def get_bedrock_client():
    """
    Bedrock Runtime client singleton
    
    Returns:
        boto3.client('bedrock-runtime')
    """
    global _bedrock_client
    if _bedrock_client is None:
        from botocore.config import Config
        region = os.environ.get("AWS_REGION", "us-east-1")
        config = Config(
            retries={"max_attempts": 3, "mode": "standard"},
            read_timeout=int(os.environ.get("BEDROCK_READ_TIMEOUT_SECONDS", "60")),
            connect_timeout=int(os.environ.get("BEDROCK_CONNECT_TIMEOUT_SECONDS", "5")),
        )
        _bedrock_client = boto3.client('bedrock-runtime', region_name=region, config=config)
        logger.debug("Bedrock client initialized")
    return _bedrock_client
