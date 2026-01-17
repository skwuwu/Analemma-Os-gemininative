#!/bin/bash

# ECS Task Definition dynamic update script
# Usage: ./scripts/update-task-definition.sh <IMAGE_TAG>

set -e

# Parameter validation
if [ -z "$1" ]; then
    echo "❌ Usage: $0 <IMAGE_TAG>"
    echo "Example: $0 abc123def456"
    exit 1
fi

IMAGE_TAG="$1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"
TASK_DEFINITION_FILE="async-llm-worker-task-definition.json"
TASK_FAMILY="async-llm-worker"

echo "🚀 Starting ECS Task Definition update..."
echo "📦 Image tag: ${IMAGE_TAG}"
echo "🏷️ Account ID: ${ACCOUNT_ID}"

# Replace image tag in Task Definition file
TEMP_FILE=$(mktemp)
sed "s/\${IMAGE_TAG:-latest}/${IMAGE_TAG}/g" "${TASK_DEFINITION_FILE}" > "${TEMP_FILE}"

echo "📋 Registering Task Definition..."

# Register Task Definition
TASK_DEF_ARN=$(aws ecs register-task-definition \
    --cli-input-json "file://${TEMP_FILE}" \
    --region "${REGION}" \
    --query 'taskDefinition.taskDefinitionArn' \
    --output text)

if [ -z "$TASK_DEF_ARN" ]; then
    echo "❌ Task Definition registration failed"
    rm "${TEMP_FILE}"
    exit 1
fi

echo "✅ Task Definition registration completed: ${TASK_DEF_ARN}"

# Clean up temporary file
rm "${TEMP_FILE}"

# GitHub Actions output (optional)
if [ -n "$GITHUB_OUTPUT" ]; then
    echo "task-definition-arn=${TASK_DEF_ARN}" >> "$GITHUB_OUTPUT"
fi

echo "🎉 Task Definition update completed!"
echo "📝 New ARN: ${TASK_DEF_ARN}"