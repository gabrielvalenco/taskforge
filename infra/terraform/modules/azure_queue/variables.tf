variable "namespace_name" {
  description = "Nome do namespace do Service Bus (único globalmente, 6 a 50 caracteres)."
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9-]{4,48}[a-zA-Z0-9]$", var.namespace_name))
    error_message = "Use 6 a 50 caracteres: letras, números e hífen; começa com letra e não termina em hífen."
  }
}

variable "queue_name" {
  description = "Nome da fila (valor para TASKFORGE_SERVICEBUS_QUEUE_NAME)."
  type        = string
  default     = "taskforge-jobs"
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku" {
  description = "Standard atende a maioria dos casos. Premium dá isolamento, VNet e mensagens maiores."
  type        = string
  default     = "Standard"

  validation {
    condition     = contains(["Standard", "Premium"], var.sku)
    error_message = "A duplicate detection e o agendamento exigem Standard ou Premium (Basic não suporta)."
  }
}

variable "premium_capacity" {
  description = "Messaging units (1, 2, 4, 8 ou 16). Usado só quando sku = Premium."
  type        = number
  default     = 1
}

variable "max_delivery_count" {
  description = "Entregas antes de o Service Bus mover a mensagem para a dead-letter sub-queue."
  type        = number
  default     = 5

  validation {
    condition     = var.max_delivery_count >= 1 && var.max_delivery_count <= 2000
    error_message = "max_delivery_count deve estar entre 1 e 2000."
  }
}

variable "lock_duration_seconds" {
  description = "Tempo de lock (peek-lock). Máximo do Service Bus: 300 s. Para jobs mais longos, o receiver precisa renovar o lock."
  type        = number
  default     = 300

  validation {
    condition     = var.lock_duration_seconds >= 5 && var.lock_duration_seconds <= 300
    error_message = "O lock duration do Service Bus vai de 5 a 300 segundos."
  }
}

variable "message_ttl_days" {
  description = "Tempo de vida das mensagens antes de expirarem (e irem para a DLQ)."
  type        = number
  default     = 14
}

variable "duplicate_detection_window" {
  description = "Janela ISO 8601 de detecção de duplicatas (MessageId = id do job)."
  type        = string
  default     = "PT10M"
}

variable "local_auth_enabled" {
  description = "Permite connection string/SAS. Em produção, prefira false e use Entra ID (RBAC) com managed identity."
  type        = bool
  default     = true
}

variable "worker_principal_ids" {
  description = "Object IDs (managed identities, service principals) que consomem a fila."
  type        = list(string)
  default     = []
}

variable "producer_principal_ids" {
  description = "Object IDs que só publicam jobs."
  type        = list(string)
  default     = []
}

variable "dlq_alarm_threshold" {
  description = "O alerta dispara quando há MAIS mensagens mortas que este valor."
  type        = number
  default     = 0
}

variable "action_group_ids" {
  description = "Action groups notificados pelo alerta de dead-letter."
  type        = list(string)
  default     = []
}

variable "tags" {
  type    = map(string)
  default = {}
}
