variable "subscription_id" {
  description = "ID da assinatura Azure (obrigatório no azurerm 4.x)."
  type        = string
}

variable "location" {
  type    = string
  default = "brazilsouth"
}

variable "project" {
  type    = string
  default = "taskforge"
}

variable "environment" {
  type = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment deve ser dev, staging ou prod."
  }
}

variable "max_delivery_count" {
  type    = number
  default = 5
}

variable "local_auth_enabled" {
  description = "false desliga connection string/SAS e força Entra ID (recomendado em prod)."
  type        = bool
  default     = true
}

variable "worker_principal_ids" {
  type    = list(string)
  default = []
}

variable "producer_principal_ids" {
  type    = list(string)
  default = []
}

variable "alert_email" {
  type    = string
  default = null
}
