# The four tiers this suite compares, as real Atlas clusters.
# `mongodbatlas_advanced_cluster` handles all of them uniformly -- tenant
# (M0), Flex, and dedicated (M10/M30) just differ by provider_name and
# whether electable_specs is set. See:
# https://registry.terraform.io/providers/mongodb/mongodbatlas/latest/docs/resources/advanced_cluster

resource "mongodbatlas_advanced_cluster" "m0" {
  project_id   = var.atlas_project_id
  name         = "${var.cluster_name_prefix}-m0"
  cluster_type = "REPLICASET"

  replication_specs = [
    {
      region_configs = [
        {
          provider_name         = "TENANT"
          backing_provider_name = var.backing_provider_name
          region_name           = var.region_name
          priority              = 7
          electable_specs = {
            instance_size = "M0"
          }
        }
      ]
    }
  ]
}

resource "mongodbatlas_advanced_cluster" "flex" {
  project_id   = var.atlas_project_id
  name         = "${var.cluster_name_prefix}-flex"
  cluster_type = "REPLICASET"

  replication_specs = [
    {
      region_configs = [
        {
          provider_name         = "FLEX"
          backing_provider_name = var.backing_provider_name
          region_name           = var.region_name
          priority              = 7
        }
      ]
    }
  ]
}

resource "mongodbatlas_advanced_cluster" "m10" {
  project_id   = var.atlas_project_id
  name         = "${var.cluster_name_prefix}-m10"
  cluster_type = "REPLICASET"

  replication_specs = [
    {
      region_configs = [
        {
          provider_name = "AWS"
          region_name   = var.region_name
          priority      = 7
          electable_specs = {
            instance_size = "M10"
            node_count    = var.m10_node_count
          }
        }
      ]
    }
  ]
}

resource "mongodbatlas_advanced_cluster" "m30" {
  project_id   = var.atlas_project_id
  name         = "${var.cluster_name_prefix}-m30"
  cluster_type = "REPLICASET"

  replication_specs = [
    {
      region_configs = [
        {
          provider_name = "AWS"
          region_name   = var.region_name
          priority      = 7
          electable_specs = {
            instance_size = "M30"
            node_count    = var.m30_node_count
          }
        }
      ]
    }
  ]
}
