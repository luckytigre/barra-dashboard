variable "enabled" {
  description = "Whether the custom-domain edge resources should exist."
  type        = bool
}

variable "project_id" {
  description = "GCP project id for edge resources."
  type        = string
}

variable "region" {
  description = "Primary region for serverless NEGs."
  type        = string
}

variable "name_prefix" {
  description = "Shared name prefix for edge resources."
  type        = string
}

variable "cloudflare_zone_name" {
  description = "Cloudflare zone name for public DNS."
  type        = string
}

variable "cloudflare_proxied" {
  description = "Whether Cloudflare should proxy the DNS records."
  type        = bool
}

variable "hostnames" {
  description = "Custom-domain hostnames for the edge."
  type = object({
    frontend = string
    serve    = string
    control  = string
  })
}

variable "service_names" {
  description = "Cloud Run service names backing the edge."
  type = object({
    frontend = string
    serve    = string
    control  = string
  })
}
