variable "name" {
  description = "Nome da fila principal. A DLQ recebe o sufixo -dlq."
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]{1,70}$", var.name))
    error_message = "O nome deve ter 1 a 70 caracteres: letras, números, hífen ou underscore."
  }
}

variable "visibility_timeout_seconds" {
  description = "Tempo que uma mensagem fica invisível após ser recebida. Deve ser MAIOR que TASKFORGE_JOB_TIMEOUT_SECONDS, senão a mensagem volta à fila enquanto o job ainda roda."
  type        = number
  default     = 360

  validation {
    condition     = var.visibility_timeout_seconds >= 0 && var.visibility_timeout_seconds <= 43200
    error_message = "O visibility timeout do SQS vai de 0 a 43200 segundos (12 h)."
  }
}

variable "max_receive_count" {
  description = "Entregas antes de a mensagem ser movida para a DLQ (redrive policy)."
  type        = number
  default     = 5

  validation {
    condition     = var.max_receive_count >= 1 && var.max_receive_count <= 1000
    error_message = "max_receive_count deve estar entre 1 e 1000."
  }
}

variable "message_retention_seconds" {
  description = "Retenção da fila principal (padrão: 4 dias)."
  type        = number
  default     = 345600
}

variable "dlq_retention_seconds" {
  description = "Retenção da DLQ (padrão: 14 dias, o máximo). Dá tempo de investigar e reprocessar."
  type        = number
  default     = 1209600
}

variable "kms_key_arn" {
  description = "ARN de uma CMK para criptografar as filas. Se nulo, usa SSE-SQS (chave gerenciada pela AWS, sem custo)."
  type        = string
  default     = null
}

variable "alarm_topic_arns" {
  description = "Tópicos SNS notificados quando um alarme dispara ou volta ao normal."
  type        = list(string)
  default     = []
}

variable "dlq_alarm_threshold" {
  description = "O alarme dispara quando a DLQ tem MAIS mensagens que este valor."
  type        = number
  default     = 0
}

variable "oldest_message_age_threshold_seconds" {
  description = "O alarme dispara se a mensagem mais antiga da fila principal passar deste tempo (sintoma de workers parados ou lentos)."
  type        = number
  default     = 900
}

variable "tags" {
  description = "Tags aplicadas a todos os recursos."
  type        = map(string)
  default     = {}
}
