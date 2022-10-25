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

provider "aws"{
	shared_config_files      = ["${var.aws_config}"]
	shared_credentials_files = ["${var.aws_credentials}"]
	region = "${var.aws_region}"
}



# # S3 Bucket
# resource "aws_s3_bucket" "pedestrian_sensor_s3_bucket" {
# 	bucket = var.bucket_name
# 	acl = var.acl

# 	versioning {
# 		enabled = var.versioning
# 	}

# 	tags = var.tags
  
# 	force_destroy = true
# }


# AWS Redshift
# resource "aws_redshift_cluster" "pedestrian_sensor_redshift" {
# 	cluster_identifier  = var.redshift_cluster_identifier
# 	database_name       = "pedestrian_sensor"
# 	master_username     = "cuonghtv"
# 	master_password     = "Redshift2022^^"
# 	node_type           = "dc2.large"
# 	cluster_type        = "single-node"
# 	skip_final_snapshot = "true"
# }

# AWS Kinesis
# resource "aws_kinesis_stream" "pedestrian_sensor_kinesis_stream" {
# 	name             = var.kinesis_stream_name
# 	retention_period = 24
# 	shard_level_metrics = [
# 		"IncomingBytes",
# 		"OutgoingBytes",
# 	]
# 	stream_mode_details {
# 		stream_mode = "ON_DEMAND"
# 	}
# 	tags = {
# 		Environment = "test"
# 	}
# }

# resource "aws_iam_role" "pedestrain_sensor_trigger_lambda" {
#     name = "pedestrain_sensor_trigger_lambda_role"
#     assume_role_policy = <<EOF
# 	{
# 		"Version": "2012-10-17",
#   		"Statement": [{
#     		"Action": "sts:AssumeRole",
#     		"Principal": {
# 				"Service": "lambda.amazonaws.com"},
#     	"Effect": "Allow",
#     	"Sid": ""
#     }]}
# EOF
# }


# resource "aws_iam_role_policy_attachment" "basic-exec-role" {
# 	role       = "${aws_iam_role.pedestrain_sensor_trigger_lambda.name}"
# 	policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
# }


resource "aws_lambda_function" "pedestrain_sensor_trigger_lambda" {
    filename = "../../streaming_data/trigger_data_lambda.py"
    function_name = "trigger_data_lambda"
    role = "arn:aws:iam::666243375423:role/DataCamp_Lambda_Kinesis_Role"
    handler = "lambda_handler.handler"
    runtime = "python3.9"
}