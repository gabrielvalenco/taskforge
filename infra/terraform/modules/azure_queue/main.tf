resource "azurerm_servicebus_namespace" "this" {
  name                = var.namespace_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = var.sku
  capacity            = var.sku == "Premium" ? var.premium_capacity : 0
  minimum_tls_version = "1.2"
  local_auth_enabled  = var.local_auth_enabled
  tags                = var.tags
}

resource "azurerm_servicebus_queue" "jobs" {
  name         = var.queue_name
  namespace_id = azurerm_servicebus_namespace.this.id

  max_delivery_count  = var.max_delivery_count
  lock_duration       = format("PT%dS", var.lock_duration_seconds)
  default_message_ttl = format("P%dD", var.message_ttl_days)

  # Mensagens expiradas vão para a DLQ em vez de sumirem silenciosamente.
  dead_lettering_on_message_expiration = true

  # O dispatcher publica com MessageId = id do job: reenvios (retry de publicação) são
  # descartados pelo broker dentro desta janela.
  requires_duplicate_detection            = true
  duplicate_detection_history_time_window = var.duplicate_detection_window
}

# --- Acesso por SAS (desligue com local_auth_enabled = false e use RBAC) --------------------

resource "azurerm_servicebus_queue_authorization_rule" "worker" {
  count = var.local_auth_enabled ? 1 : 0

  name     = "taskforge-worker"
  queue_id = azurerm_servicebus_queue.jobs.id

  listen = true
  send   = true # `send` é necessário para o retry com atraso (agenda uma cópia da mensagem)
  manage = false
}

# --- Acesso por RBAC (Entra ID / managed identity): recomendado ----------------------------

resource "azurerm_role_assignment" "worker_receiver" {
  for_each = toset(var.worker_principal_ids)

  scope                = azurerm_servicebus_queue.jobs.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "worker_sender" {
  for_each = toset(var.worker_principal_ids)

  scope                = azurerm_servicebus_queue.jobs.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "producer_sender" {
  for_each = toset(var.producer_principal_ids)

  scope                = azurerm_servicebus_queue.jobs.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = each.value
}

# --- Alerta: mensagens na dead-letter sub-queue ---------------------------------------------

resource "azurerm_monitor_metric_alert" "dead_letters" {
  name                = "${var.queue_name}-dead-letters"
  resource_group_name = var.resource_group_name
  scopes              = [azurerm_servicebus_namespace.this.id]
  description         = "Há jobs na dead-letter queue de ${var.queue_name}: investigue e reprocesse."
  severity            = 2
  frequency           = "PT1M"
  window_size         = "PT5M"
  tags                = var.tags

  criteria {
    metric_namespace = "Microsoft.ServiceBus/namespaces"
    metric_name      = "DeadletteredMessages"
    aggregation      = "Maximum"
    operator         = "GreaterThan"
    threshold        = var.dlq_alarm_threshold

    dimension {
      name     = "EntityName"
      operator = "Include"
      values   = [azurerm_servicebus_queue.jobs.name]
    }
  }

  dynamic "action" {
    for_each = toset(var.action_group_ids)
    content {
      action_group_id = action.value
    }
  }
}
