variable "atlas_project_id" {
  description = "Existing Atlas project ID to create the benchmark clusters in."
  type        = string
}

variable "cluster_name_prefix" {
  description = <<-EOT
    Prefix for the four cluster names this creates (e.g. "<prefix>-m0",
    "<prefix>-flex", "<prefix>-m10", "<prefix>-m30"). Defaults to something
    that won't collide with clusters you may have already created manually
    (e.g. "benchmark-m0") -- change this if you want Terraform to manage
    those instead, and `terraform import` them first (see README).
  EOT
  type        = string
  default     = "tf-bench"
}

variable "region_name" {
  description = "Atlas region name (AWS region format, e.g. US_EAST_1, EU_WEST_1)."
  type        = string
  default     = "US_EAST_1"
}

variable "backing_provider_name" {
  description = "Cloud backing the M0/Flex shared-tier clusters."
  type        = string
  default     = "AWS"
}

variable "m10_node_count" {
  type    = number
  default = 3
}

variable "m30_node_count" {
  type    = number
  default = 3
}
