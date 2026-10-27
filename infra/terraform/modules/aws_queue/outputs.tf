output "queue_url" {
  description = "Valor para TASKFORGE_SQS_QUEUE_URL."
  value       = aws_sqs_queue.main.url
}

output "queue_arn" {
  value = aws_sqs_queue.main.arn
}

output "dlq_url" {
  description = "Valor para TASKFORGE_SQS_DLQ_URL."
  value       = aws_sqs_queue.dlq.url
}

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "producer_policy_arn" {
  description = "Anexe à role de quem chama o dispatcher (API, CLI)."
  value       = aws_iam_policy.producer.arn
}

output "worker_policy_arn" {
  description = "Anexe à role dos workers."
  value       = aws_iam_policy.worker.arn
}
