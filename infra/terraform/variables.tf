variable "region" {
  description = "The one region the stack runs in."
  type        = string
  default     = "eu-west-2"
}

variable "name" {
  description = "Prefix for the Lambda, HTTP API and IAM role names."
  type        = string
  default     = "recommendation-pipeline"
}

variable "partition_key" {
  description = "The list the API serves: DYNAMODB_PARTITION_KEY, as in .env.example."
  type        = string
  default     = "TRENDING#1h"
}

variable "lambda_zip" {
  description = "The API's deployment package, built by make package-lambda."
  type        = string
  default     = "../../build/lambda.zip"
}

variable "api_rate_limit" {
  description = "Requests a second the API serves on average; the rest get 429 Too Many Requests."
  type        = number
  default     = 1
}

variable "api_burst_limit" {
  description = "Requests the API serves at once before api_rate_limit applies."
  type        = number
  default     = 5
}

variable "log_retention_days" {
  description = "How long CloudWatch keeps the API's logs."
  type        = number
  default     = 7
}

locals {
  # The table's one definition, shared with make db-init (docs/design.md,
  # "Serving table").
  table = jsondecode(file("${path.module}/../dynamodb/trending-table.json"))

  # The project's Python version (.python-version) is the Lambda runtime's,
  # and make package-lambda installs wheels for the same version.
  python_version = trimspace(file("${path.module}/../../.python-version"))
}
