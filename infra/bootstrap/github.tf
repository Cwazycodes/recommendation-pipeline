# AWS trusts the short-lived tokens GitHub Actions issues, so GitHub holds no
# AWS keys. AWS checks GitHub's certificate itself, so no thumbprint is needed.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

# Only runs on the deploy branch of this repository can assume the role. Their
# token's sub claim is <github_subject>:ref:refs/heads/main; pull requests
# (<github_subject>:pull_request), other branches, tags and environments have
# other subjects.
resource "aws_iam_role" "deploy" {
  name = "${var.name}-github-deploy"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "${var.github_subject}:ref:refs/heads/${var.deploy_branch}"
        }
      }
    }]
  })
}

# What make deploy and make destroy touch, and nothing else: the main stack's
# resources by name, and its state. This role, the identity provider and the
# bucket are out of its reach.
resource "aws_iam_role_policy" "deploy" {
  name = "deploy-${var.name}"
  role = aws_iam_role.deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListState"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.state.arn
      },
      {
        # The state file and its lock file.
        Sid      = "ReadAndWriteState"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.state.arn}/${var.name}/*"
      },
      {
        Sid      = "TrendingTable"
        Effect   = "Allow"
        Action   = "dynamodb:*"
        Resource = local.table_arn
      },
      {
        Sid      = "ApiFunction"
        Effect   = "Allow"
        Action   = "lambda:*"
        Resource = [local.function_arn, "${local.function_arn}:*"]
      },
      {
        Sid      = "ApiLogGroup"
        Effect   = "Allow"
        Action   = "logs:*"
        Resource = [local.log_group_arn, "${local.log_group_arn}:*"]
      },
      {
        # Terraform finds the log group by listing groups.
        Sid      = "FindLogGroups"
        Effect   = "Allow"
        Action   = "logs:DescribeLogGroups"
        Resource = "arn:aws:logs:${var.region}:${local.account_id}:log-group:*"
      },
      {
        # An HTTP API's ID is only known once it exists, so this covers the
        # region's HTTP APIs. Creating a stage with tags is also checked as
        # TagResource.
        Sid    = "HttpApi"
        Effect = "Allow"
        Action = [
          "apigateway:GET",
          "apigateway:POST",
          "apigateway:PUT",
          "apigateway:PATCH",
          "apigateway:DELETE",
          "apigateway:TagResource",
        ]
        Resource = [
          "arn:aws:apigateway:${var.region}::/apis",
          "arn:aws:apigateway:${var.region}::/apis/*",
          "arn:aws:apigateway:${var.region}::/tags/*",
        ]
      },
      {
        Sid    = "StackRoles"
        Effect = "Allow"
        Action = [
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:GetRole",
          "iam:UpdateRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:ListRoleTags",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
        ]
        Resource = [local.api_role_arn, local.writer_arn]
      },
      {
        Sid       = "PassApiRoleToLambda"
        Effect    = "Allow"
        Action    = "iam:PassRole"
        Resource  = local.api_role_arn
        Condition = { StringEquals = { "iam:PassedToService" = "lambda.amazonaws.com" } }
      },
    ]
  })
}
