provider "google" {
  region = "us-central1"
  project = "eighth-duality-457819-r4"
}

terraform {
  required_version = ">= 0.14"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  backend "gcs" {}
}

#--------------------------- Cloud Storage

resource "google_storage_bucket" "ai_storage" {
  name     = "anomaly-detection-${terraform.workspace}"
  location = "us-central1"
  lifecycle {
    prevent_destroy = false
  }
  force_destroy = true
  public_access_prevention = "enforced"
}   
