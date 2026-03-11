#!/bin/bash

ENDPOINT=http://localhost:4566

# Create S3 bucket
aws --endpoint-url=$ENDPOINT s3 mb s3://movies

# Create SQS queue
aws --endpoint-url=$ENDPOINT sqs create-queue --queue-name upload-events
QUEUE_URL=$(aws --endpoint-url=$ENDPOINT sqs get-queue-url \
  --queue-name upload-events --query QueueUrl --output text)

# Get queue ARN
QUEUE_ARN=$(aws --endpoint-url=$ENDPOINT sqs get-queue-attributes \
  --queue-url $QUEUE_URL --attribute-name QueueArn \
  --query Attributes.QueueArn \
  --output text)

# Apply CORS
aws --endpoint-url=$ENDPOINT s3api put-bucket-cors \
  --bucket movies \
  --cors-configuration file://cors.json

# Replace placeholder in notification.json (macOS compatible)
sed -i '' "s|__QUEUE_ARN__|$QUEUE_ARN|g" notification.json

# Apply bucket notifications
aws --endpoint-url=$ENDPOINT s3api put-bucket-notification-configuration \
  --bucket movies \
  --notification-configuration file://notification.json