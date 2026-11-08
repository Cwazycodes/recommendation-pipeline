output "api_url" {
  description = "GET this for the latest trending list."
  value       = "${aws_apigatewayv2_api.trending.api_endpoint}/trending"
}

output "table_name" {
  value = aws_dynamodb_table.trending.name
}

output "function_name" {
  value = aws_lambda_function.api.function_name
}

output "log_group" {
  value = aws_cloudwatch_log_group.api.name
}

output "writer_role_arn" {
  description = "The role_arn for the writer's AWS profile."
  value       = aws_iam_role.writer.arn
}
