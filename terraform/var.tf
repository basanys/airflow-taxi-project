locals {
  data_lake_bucket = "zoomcamp_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "zoomcamp-361206"
}

variable "region" {
  description = "region of GCP resource"
  default = "europe-west3"
}

variable "bq_dataset" {
  description = "A unique ID for BQ dataset"
  default = "trips_data_all"
}