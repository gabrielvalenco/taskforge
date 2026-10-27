output "namespace_id" {
  value = azurerm_servicebus_namespace.this.id
}

output "namespace_fqdn" {
  description = "Endpoint para autenticação via Entra ID (fully_qualified_namespace)."
  value       = "${azurerm_servicebus_namespace.this.name}.servicebus.windows.net"
}

output "queue_name" {
  description = "Valor para TASKFORGE_SERVICEBUS_QUEUE_NAME."
  value       = azurerm_servicebus_queue.jobs.name
}

output "worker_connection_string" {
  description = "Valor para TASKFORGE_SERVICEBUS_CONNECTION_STRING. Nulo quando local_auth_enabled = false."
  value       = one(azurerm_servicebus_queue_authorization_rule.worker[*].primary_connection_string)
  sensitive   = true
}
