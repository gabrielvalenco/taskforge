output "queue_url" {
  value = module.jobs_queue.queue_url
}

output "dlq_url" {
  value = module.jobs_queue.dlq_url
}

output "producer_policy_arn" {
  value = module.jobs_queue.producer_policy_arn
}

output "worker_policy_arn" {
  value = module.jobs_queue.worker_policy_arn
}

output "taskforge_env" {
  description = "Variáveis de ambiente para os workers e produtores."
  value = {
    TASKFORGE_QUEUE_BACKEND = "sqs"
    TASKFORGE_SQS_QUEUE_URL = module.jobs_queue.queue_url
    TASKFORGE_SQS_DLQ_URL   = module.jobs_queue.dlq_url
    TASKFORGE_AWS_REGION    = var.region
  }
}
