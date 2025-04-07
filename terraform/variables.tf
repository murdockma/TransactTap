variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "personal_finance"
}

variable "owner_email" {
  description = "Email of the dataset owner"
  type        = string
}
