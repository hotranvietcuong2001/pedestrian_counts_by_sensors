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
	description = "Creates a unique bucket name."
	default     = "vc-pedestrian-sensor-s3bucket"
}

variable "redshift_cluster_identifier" {
	type        = string
	description = "Creates a unique Redshift cluster name"
	default     = "vc-pedestrian-sensor-redshift-cluster"
}


variable "kinesis_stream_name" {
	type        = string
	description = "Creates a unique Kinesis Stream name"
	default     = "vc-pedestrian-sensor-kinesis-stream"
}

variable "lambda_function_name" {
	type        = string
	description = "Creates a unique Lambda Function name for Trigger Data Streaming"
	default     = "vc-pedestrian-sensor-lambda-function"
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