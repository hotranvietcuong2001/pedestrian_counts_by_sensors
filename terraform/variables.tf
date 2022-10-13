variable "aws_region" {
  description = "The AWS region to use to create resources."
  default     = "us-east-2"
}

variable "aws_credentials" {
  description = "The AWS credentials file path"
  default = "~/.aws/credentials" 
}

variable "aws_config" {
  description = "The AWS config file path"
  default = "~/.aws/config" 
}

variable "bucket_name" {
  type        = string
  description = "(required since we are not using 'bucket') Creates a unique bucket name beginning with the specified prefix. Conflicts with bucket."
  default     = "vc-s3bucket-pedestrian-sensor"
}

variable "tags" {
  type        = map
  description = "(Optional) A mapping of tags to assign to the bucket."
  default     = {
    environment = "DEV"
    terraform   = "true"
  }
}

variable "versioning" {
  type        = bool
  description = "(Optional) A state of versioning."
  default     = true
}

variable "acl" {
  type        = string
  description = " Defaults to private "
  default     = "private"
}