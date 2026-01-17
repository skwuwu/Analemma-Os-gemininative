#!/bin/bash
set -e

TABLE_NAME=$1
STREAM_ARN=$2
REGION=$3

echo "Enabling Kinesis streaming destination for table '$TABLE_NAME' to stream '$STREAM_ARN'..."

# Check if already enabled (optional but for logging)
EXISTING=$(aws dynamodb describe-kinesis-streaming-destination \
    --table-name "$TABLE_NAME" \
    --region "$REGION" \
    --query "KinesisDataStreamDestinations[?StreamArn=='$STREAM_ARN'].DestinationStatus" \
    --output text)

if [ "$EXISTING" == "ACTIVE" ]; then
    echo "✅ Kinesis streaming is already ACTIVE for this stream."
    exit 0
fi

if [ "$EXISTING" == "ENABLING" ]; then
    echo "⏳ Kinesis streaming is currently ENABLING. Waiting..."
    # 여기서 wait 로직을 추가할 수도 있지만, 보통은 그냥 넘어가도 됨
    exit 0
fi

# 활성화 명령 실행
# 이미 활성화된 경우 ResourceInUseException이 발생할 수 있으므로 || true로 에러 무시하거나
# 위에서 체크했으므로 시도.
# aws cli는 이미 설정된 경우 에러를 뱉을 수 있음.

aws dynamodb enable-kinesis-streaming-destination \
    --table-name "$TABLE_NAME" \
    --stream-arn "$STREAM_ARN" \
    --region "$REGION" || echo "⚠️ Command failed or already enabled. Checking status..."

# 최종 확인
FINAL_STATUS=$(aws dynamodb describe-kinesis-streaming-destination \
    --table-name "$TABLE_NAME" \
    --region "$REGION" \
    --query "KinesisDataStreamDestinations[?StreamArn=='$STREAM_ARN'].DestinationStatus" \
    --output text)

echo "Current Kinesis Destination Status: $FINAL_STATUS"
