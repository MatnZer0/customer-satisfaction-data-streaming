variable "credentials" {
  description = "My Credentials"
  default     = "~/.google/credentials/google_credentials_project.json"
}

variable "project" {
  description = "GCP Project ID"
  #Update the below with your GCP project ID
  default     = "<YOUR PROJECT ID>"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "<YOUR UNIQUE BUCKET NAME>"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "customer_survey_dataset"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}