variable "region" {
  type    = string
  default = "us-east-1"
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

variable "visibility_timeout_seconds" {
  description = "Precisa ser maior que TASKFORGE_JOB_TIMEOUT_SECONDS (padrão 300)."
  type        = number
  default     = 360
}

variable "max_receive_count" {
  type    = number
  default = 5
}

variable "kms_key_arn" {
  type    = string
  default = null
}

variable "alert_email" {
  description = "E-mail que recebe os alarmes (precisa confirmar a inscrição). Nulo = sem e-mail."
  type        = string
  default     = null
}
