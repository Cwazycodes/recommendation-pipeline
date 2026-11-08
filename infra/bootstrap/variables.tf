variable "region" {
  description = "The main stack's region."
  type        = string
  default     = "eu-west-2"
}

variable "name" {
  description = "The main stack's name prefix (infra/terraform), which the deploy role is scoped to."
  type        = string
  default     = "recommendation-pipeline"
}

variable "github_subject" {
  description = "The repository as its OIDC tokens' sub claim names it: gh api repos/OWNER/NAME/actions/oidc/customization/sub --jq .sub_claim_prefix"
  type        = string
  # Immutable form, with the owner's and the repository's IDs: a repository
  # deleted and created again under the same name would have a new ID.
  default = "repo:Cwazycodes@170112433/recommendation-pipeline@1427492276"
}

variable "deploy_branch" {
  description = "The only branch the deploy workflow may run from."
  type        = string
  default     = "main"
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # Bucket names are global, so this one includes the account ID. make works
  # out the same name for terraform init (Makefile, TF_INIT).
  state_bucket = "${var.name}-tfstate-${local.account_id}"

  # The main stack's resources, by the names it gives them.
  table_arn     = "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${jsondecode(file("${path.module}/../dynamodb/trending-table.json")).TableName}"
  function_arn  = "arn:aws:lambda:${var.region}:${local.account_id}:function:${var.name}-api"
  log_group_arn = "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/${var.name}-api"
  api_role_arn  = "arn:aws:iam::${local.account_id}:role/${var.name}-api"
  writer_arn    = "arn:aws:iam::${local.account_id}:role/${var.name}-writer"
}
