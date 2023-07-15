locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "southamerica-east1"
  type        = string
}

variable "bucket_name" {
  description = "The name of GCS storage. Must be unique."
  default     = ""
}

variable "storage_class" {
  description = "Storage class type for the bucket."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type        = string
  default     = "ny_trips"
}
