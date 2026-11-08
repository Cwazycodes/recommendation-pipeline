locals {
  function_name = "${var.name}-api"
}

# Created here rather than by Lambda on first use, so it has a retention period
# and make destroy deletes it.
resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "api" {
  name = local.function_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Read only: the one Query the handler makes (latest_window), and its own logs.
# No logs:CreateLogGroup, so Lambda cannot recreate the group without its
# retention.
resource "aws_iam_role_policy" "api" {
  name = "read-trending-table"
  role = aws_iam_role.api.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadTrendingTable"
        Effect   = "Allow"
        Action   = "dynamodb:Query"
        Resource = aws_dynamodb_table.trending.arn
      },
      {
        Sid      = "WriteOwnLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.api.arn}:*"
      },
    ]
  })
}

resource "aws_lambda_function" "api" {
  function_name = local.function_name
  role          = aws_iam_role.api.arn
  runtime       = "python${local.python_version}"
  architectures = ["x86_64"]
  handler       = "realtime_recommendation_pipeline.api.handler.handler"

  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)

  # The handler's DynamoDB client gives up within about 6 s (api/handler.py).
  memory_size = 256
  timeout     = 10

  # No DYNAMODB_ENDPOINT_URL, so the handler uses DynamoDB in AWS.
  environment {
    variables = {
      DYNAMODB_TABLE         = aws_dynamodb_table.trending.name
      DYNAMODB_PARTITION_KEY = var.partition_key
      LOG_LEVEL              = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.api, aws_iam_role_policy.api]
}
