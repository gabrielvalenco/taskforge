terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # Backend parcial: o bucket/tabela vêm de `terraform init -backend-config=backend.hcl`
  # (veja backend.hcl.example). Em CI, use `terraform init -backend=false` para validar.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

locals {
  name = "${var.project}-${var.environment}-jobs"

  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Atenção: alarmes do CloudWatch NÃO conseguem publicar em tópicos SNS criptografados com a
# chave gerenciada `alias/aws/sns`. Se precisar de criptografia aqui, use uma CMK própria
# com política que permita o serviço cloudwatch.amazonaws.com.
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alert_email == null ? 0 : 1

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

module "jobs_queue" {
  source = "../../modules/aws_queue"

  name                       = local.name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  max_receive_count          = var.max_receive_count
  kms_key_arn                = var.kms_key_arn
  alarm_topic_arns           = [aws_sns_topic.alerts.arn]
}
