output "namespace_fqdn" {
  value = module.jobs_queue.namespace_fqdn
}

output "queue_name" {
  value = module.jobs_queue.queue_name
}

output "worker_connection_string" {
  description = "Nulo quando local_auth_enabled = false. Guarde em um Key Vault, não em texto puro."
  value       = module.jobs_queue.worker_connection_string
  sensitive   = true
}
