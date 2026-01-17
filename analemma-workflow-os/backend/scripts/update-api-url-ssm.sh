#!/bin/bash
set -e

STACK_NAME="$1"  # Receive stack name as first argument
AWS_REGION="$2"  # Receive region as second argument

# Set PARAM_NAME based on environment
if [[ "$STACK_NAME" == *"prod"* ]]; then
    PARAM_NAME="/my-app/prod/api-url"
else
    PARAM_NAME="/my-app/dev/api-url"
fi

#!/bin/bash
set -e

STACK_NAME="$1"  # Receive stack name as first argument
AWS_REGION="$2"  # Receive region as second argument

# Set PARAM_NAME based on environment (always fixed to dev)
API_PARAM_NAME="/my-app/dev/api-url"
WS_PARAM_NAME="/my-app/dev/websocket-url"

# Extract ApiEndpoint and WebsocketApiUrl values from CloudFormation Outputs
API_URL=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='ApiEndpoint'].OutputValue" --output text)
WS_URL=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='WebsocketApiUrl'].OutputValue" --output text)

# Save latest API URL to SSM Parameter Store
aws ssm put-parameter --name "$API_PARAM_NAME" --value "$API_URL" --type String --overwrite --region "$AWS_REGION"
aws ssm put-parameter --name "$WS_PARAM_NAME" --value "$WS_URL" --type String --overwrite --region "$AWS_REGION"

echo "✅ Latest API Gateway URL has been saved to SSM Parameter Store: $API_URL"
echo "✅ Latest WebSocket URL has been saved to SSM Parameter Store: $WS_URL"
