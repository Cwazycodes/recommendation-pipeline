resource "aws_apigatewayv2_api" "trending" {
  name          = var.name
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.trending.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

# The only route: anything else gets API Gateway's own 404.
resource "aws_apigatewayv2_route" "get_trending" {
  api_id    = aws_apigatewayv2_api.trending.id
  route_key = "GET /trending"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.trending.id
  name        = "$default"
  auto_deploy = true

  # The URL is public, so the request rate is capped: at 1 a second, at most
  # about 2.6 million requests a month reach the Lambda, which keeps the
  # Lambda, DynamoDB and log costs to a few dollars (docs/design.md).
  default_route_settings {
    throttling_rate_limit  = var.api_rate_limit
    throttling_burst_limit = var.api_burst_limit
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowHttpApiGetTrending"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.trending.execution_arn}/*/GET/trending"
}
