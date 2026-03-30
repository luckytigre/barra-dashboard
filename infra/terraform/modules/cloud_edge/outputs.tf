output "load_balancer_ip" {
  description = "Global load balancer IPv4 address when the edge is enabled."
  value       = try(google_compute_global_address.cloud_app[0].address, null)
}

output "load_balancer_dns_records" {
  description = "Cloudflare DNS record metadata when the edge is enabled."
  value = var.enabled ? {
    frontend = {
      hostname  = var.hostnames.frontend
      record_id = cloudflare_dns_record.frontend[0].id
    }
    serve = {
      hostname  = var.hostnames.serve
      record_id = cloudflare_dns_record.serve[0].id
    }
    control = {
      hostname  = var.hostnames.control
      record_id = cloudflare_dns_record.control[0].id
    }
  } : null
}

output "load_balancer_host_routing" {
  description = "Host-based routing contract when the edge is enabled."
  value       = var.enabled ? var.hostnames : null
}
