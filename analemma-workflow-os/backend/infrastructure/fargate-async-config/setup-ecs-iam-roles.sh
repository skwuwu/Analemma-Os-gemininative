#!/bin/bash

# Script to create IAM roles for ECS Async Worker
# Run: ./scripts/setup-ecs-iam-roles.sh

set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"

echo "🔐 Starting ECS Async Worker IAM role setup..."
echo "📊 Account ID: ${ACCOUNT_ID}"
echo "🌏 Region: ${REGION}"

# 1. Create ECS Task Execution Role
echo "📋 Creating ECS Task Execution Role..."

EXECUTION_TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'

# Create Execution Role
aws iam create-role \
    --role-name ecsAsyncLLMExecutionRole \
    --assume-role-policy-document "$EXECUTION_TRUST_POLICY" \
    --description "ECS Task Execution Role for Async LLM Worker" \
    --tags Key=Purpose,Value=AsyncLLMWorker Key=Type,Value=ExecutionRole \
    2>/dev/null || echo "⚠️ ecsAsyncLLMExecutionRole already exists"

# Attach policy to Execution Role
aws iam put-role-policy \
    --role-name ecsAsyncLLMExecutionRole \
    --policy-name AsyncLLMExecutionPolicy \
    --policy-document file://ecs-execution-role-policy.json

echo "✅ ECS Task Execution Role setup completed"

# 2. Create ECS Task Role
echo "📋 Creating ECS Task Role..."

TASK_TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'

# Create Task Role
aws iam create-role \
    --role-name ecsAsyncLLMTaskRole \
    --assume-role-policy-document "$TASK_TRUST_POLICY" \
    --description "ECS Task Role for Async LLM Worker Application Logic" \
    --tags Key=Purpose,Value=AsyncLLMWorker Key=Type,Value=TaskRole \
    2>/dev/null || echo "⚠️ ecsAsyncLLMTaskRole already exists"

# Attach policy to Task Role
aws iam put-role-policy \
    --role-name ecsAsyncLLMTaskRole \
    --policy-name AsyncLLMTaskPolicy \
    --policy-document file://ecs-task-role-policy.json

echo "✅ ECS Task Role setup completed"

# 3. Create API key in Secrets Manager (example)
echo "🔑 Secrets Manager secret creation guide..."
echo ""
echo "Save the API keys to Secrets Manager with the following commands:"
echo ""
echo "aws secretsmanager create-secret \\"
echo "    --name openai-api-key \\"
echo "    --description 'OpenAI API Key for Async LLM Worker' \\"
echo "    --secret-string 'sk-proj-your-openai-key-here'"
echo ""
echo "aws secretsmanager create-secret \\"
echo "    --name anthropic-api-key \\"
echo "    --description 'Anthropic API Key for Async LLM Worker' \\"
echo "    --secret-string 'sk-ant-your-anthropic-key-here'"
echo ""
echo "aws secretsmanager create-secret \\"
echo "    --name google-api-key \\"
echo "    --description 'Google API Key for Async LLM Worker' \\"
echo "    --secret-string 'your-google-api-key-here'"
echo ""

# 4. Output role ARNs
EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/ecsAsyncLLMExecutionRole"
TASK_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/ecsAsyncLLMTaskRole"

echo "🎉 IAM role setup completed!"
echo ""
echo "📋 Created roles:"
echo "   Execution Role: ${EXECUTION_ROLE_ARN}"
echo "   Task Role: ${TASK_ROLE_ARN}"
echo ""
echo "⚠️ Task Definition file has already been updated."
echo "   Next step: Save the API keys to Secrets Manager."