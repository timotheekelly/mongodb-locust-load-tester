terraform {
  required_version = ">= 1.5"
  required_providers {
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "~> 2.0"
    }
  }
}

# Auth: set MONGODB_ATLAS_PUBLIC_API_KEY / MONGODB_ATLAS_PRIVATE_API_KEY in
# your shell (Programmatic API Key auth) -- no credentials belong in this
# repo. See https://registry.terraform.io/providers/mongodb/mongodbatlas/latest/docs/guides/provider-configuration
provider "mongodbatlas" {}
