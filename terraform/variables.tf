variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = "de-zoomcamp-2023-project"
}

variable "gcp_region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type        = string
  default     = "europe-west6"
}

variable "gcp_credentials_path" {
  description = "Path to GCP credentials file"
  type        = string
  default     = "/Users/oleg/.google/de-zoomcamp-project.json"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
  type        = string
  default     = "de-zoomcamp-2023-project"
}

variable "bigquery_dataset" {
  description = "BigQuery Dataset for raw data"
  type        = string
  default     = "raw_data"
}
