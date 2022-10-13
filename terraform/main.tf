terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}


provider "aws" {
  shared_config_files      = ["${var.aws_config}"]
  shared_credentials_files = ["${var.aws_credentials}"]
  region = "${var.aws_region}"
}


# S3 Bucket
resource "aws_s3_bucket" "my-s3-bucket" {
  bucket = var.bucket_name

  acl = var.acl

  versioning {
    enabled = var.versioning
  }
  
  tags = var.tags
  
  force_destroy = true
}


# AWS Redshift
# resource "aws_redshift_cluster" "vc-cluster-redshift" {
#   cluster_identifier  = "vc-redshift-cluster"
#   database_name       = "mydb"
#   master_username     = "cuonghtv"
#   master_password     = "RedShift2022^^"
#   node_type           = "dc2.large"
#   cluster_type        = "single-node"
#   skip_final_snapshot = "true"
# }