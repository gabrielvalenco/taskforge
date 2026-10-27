#!/bin/sh
# Executado pelo LocalStack quando fica pronto. Reproduz localmente o que o Terraform cria na AWS:
# fila principal + DLQ + redrive policy.
set -eu

DLQ_URL=$(awslocal sqs create-queue --queue-name taskforge-jobs-dlq --query QueueUrl --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

cat > /tmp/main-queue-attributes.json <<EOF
{
  "VisibilityTimeout": "360",
  "ReceiveMessageWaitTimeSeconds": "20",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}"
}
EOF

awslocal sqs create-queue --queue-name taskforge-jobs --attributes file:///tmp/main-queue-attributes.json
echo "filas taskforge-jobs e taskforge-jobs-dlq criadas"
