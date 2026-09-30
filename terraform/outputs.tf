# Paste these into .env (as M0_MONGO_URI, FLEX_MONGO_URI, etc.) after
# `terraform apply` -- and add a real DB user's credentials into the URI,
# since Atlas doesn't return one with a password embedded.
output "m0_connection_string" {
  value     = mongodbatlas_advanced_cluster.m0.connection_strings.standard_srv
  sensitive = true
}

output "flex_connection_string" {
  value     = mongodbatlas_advanced_cluster.flex.connection_strings.standard_srv
  sensitive = true
}

output "m10_connection_string" {
  value     = mongodbatlas_advanced_cluster.m10.connection_strings.standard_srv
  sensitive = true
}

output "m30_connection_string" {
  value     = mongodbatlas_advanced_cluster.m30.connection_strings.standard_srv
  sensitive = true
}

output "cluster_names" {
  value = {
    m0   = mongodbatlas_advanced_cluster.m0.name
    flex = mongodbatlas_advanced_cluster.flex.name
    m10  = mongodbatlas_advanced_cluster.m10.name
    m30  = mongodbatlas_advanced_cluster.m30.name
  }
}
