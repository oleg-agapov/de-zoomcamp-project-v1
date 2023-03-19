terraform {
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

resource "google_storage_bucket" "data_lake" {
  name     = var.gcs_bucket_name
  location = var.gcp_region

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }

  force_destroy = true
}

resource "google_storage_bucket" "dataproc" {
  name     = "dataproc-de-zoomcamp-2023"
  location = var.gcp_region

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "bigquery_dataset" {
  dataset_id = var.bigquery_dataset
  project    = var.gcp_project_id
  location   = var.gcp_region
}

resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.gcp_project_id
  location      = var.gcp_region
  repository_id = "docker-repo"
  description   = "Docker repository for images"
  format        = "DOCKER"
}
