data "aws_caller_identity" "current" {}

# The results writer runs outside AWS, as a role it assumes from the deployer's
# profile (docs/design.md, "Deployment"). The trust policy lets the account's
# own principals assume it, if their IAM permissions also allow that.
resource "aws_iam_role" "writer" {
  name = "${var.name}-writer"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Write only: the writer's one call is PutItem (put_window).
resource "aws_iam_role_policy" "writer" {
  name = "write-trending-table"
  role = aws_iam_role.writer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "WriteTrendingWindows"
      Effect   = "Allow"
      Action   = "dynamodb:PutItem"
      Resource = aws_dynamodb_table.trending.arn
    }]
  })
}
