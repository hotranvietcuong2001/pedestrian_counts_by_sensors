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
	default     = "vc_pedestrian_sensor_s3_bucket"
}

variable "redshift_cluster_identifier" {
	type        = string
	description = "Creates a unique Redshift cluster name"
	default     = "vc_pedestrian_sensor_redshift_cluster"
}


variable "kinesis_stream_name" {
	type        = string
	description = "Creates a unique Kinesis Stream name"
	default     = "vc_pedestrian_sensor_kinesis_stream"
}

variable "lambda_function_name" {
	type        = string
	description = "Creates a unique Lambda Function name for Trigger Data Streaming"
	default     = "vc_pedestrian_sensor_lambda_function"
}

variable "lambda_function_path" {
	type        = string
	description = "Path of Lambda Function"
	default     = "../../streaming_data/streaming_data_kinesis.py"
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