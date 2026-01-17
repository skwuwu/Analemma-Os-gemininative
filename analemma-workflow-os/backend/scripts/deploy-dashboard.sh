#!/bin/bash
# CloudWatch dashboard deployment script
# Usage: ./deploy-dashboard.sh [stage] [region]

set -e

STAGE="${1:-dev}"
REGION="${2:-us-east-1}"
STACK_NAME="analemma-workflow-${STAGE}"
DASHBOARD_NAME="Analemma-Operations-${STAGE}"

echo "🚀 Deploying CloudWatch Dashboard: ${DASHBOARD_NAME}"

# Get resource ARN from CloudFormation stack
echo "📦 Fetching resource ARNs from CloudFormation stack..."

# Get State Machine ARN
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='WorkflowStateMachineArn'].OutputValue" \
    --output text 2>/dev/null || echo "")

if [ -z "$STATE_MACHINE_ARN" ]; then
    echo "⚠️ Warning: Could not find WorkflowStateMachineArn, using placeholder"
    STATE_MACHINE_ARN="arn:aws:states:${REGION}:*:stateMachine:*"
fi

# Replace dashboard JSON template variables
DASHBOARD_BODY=$(cat infrastructure/cloudwatch-dashboard.json | \
    sed "s/\${AWS::Region}/${REGION}/g" | \
    sed "s|\${WorkflowStateMachineArn}|${STATE_MACHINE_ARN}|g")

# Create/Update dashboard
echo "📊 Creating/Updating dashboard..."
aws cloudwatch put-dashboard \
    --dashboard-name "${DASHBOARD_NAME}" \
    --dashboard-body "${DASHBOARD_BODY}" \
    --region "${REGION}"

echo "✅ Dashboard deployed successfully!"
echo ""
echo "📈 View dashboard at:"
echo "   https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${DASHBOARD_NAME}"
