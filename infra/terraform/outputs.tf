output "s3_bucket_name" {
	description = "Name of s3 bucket for this project"
	value = aws_s3_bucket.pedestrian_sensor_s3_bucket.bucket
}

output "redshift_cluster_name" {
	description = "Name of redshift cluster for this project"
	value = aws_redshift_cluster.pedestrian_sensor_redshift.cluster_identifier
}

output "kinesis_stream_name" {
	description = "Name of kinesis stream for this project"
	value = aws_kinesis_stream.pedestrian_sensor_kinesis_stream.name
}

output "lambda_function_name" {
	description = "Name of Lambda to trigger data from Kinesis Stream for this project"
	value = aws_lambda_function.pedestrain_sensor_trigger_lambda.function_name
}