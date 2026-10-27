terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.11"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Backend parcial: veja backend.hcl.example. Em CI, `terraform init -backend=false`.
  backend "azurerm" {}
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
}

locals {
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# O nome do namespace precisa ser único no mundo todo: um sufixo aleatório estável evita colisão.
resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project}-${var.environment}"
  location = var.location
  tags     = local.tags
}

resource "azurerm_monitor_action_group" "alerts" {
  name                = "ag-${var.project}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  short_name          = substr("${var.project}${var.environment}", 0, 12)
  tags                = local.tags

  dynamic "email_receiver" {
    for_each = var.alert_email == null ? [] : [var.alert_email]
    content {
      name          = "oncall"
      email_address = email_receiver.value
    }
  }
}

module "jobs_queue" {
  source = "../../modules/azure_queue"

  namespace_name      = "sb-${var.project}-${var.environment}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  max_delivery_count     = var.max_delivery_count
  local_auth_enabled     = var.local_auth_enabled
  worker_principal_ids   = var.worker_principal_ids
  producer_principal_ids = var.producer_principal_ids
  action_group_ids       = [azurerm_monitor_action_group.alerts.id]
  tags                   = local.tags
}
