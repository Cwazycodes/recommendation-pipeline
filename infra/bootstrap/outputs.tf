output "deploy_role_arn" {
  description = "Set this as the repository's AWS_DEPLOY_ROLE_ARN variable."
  value       = aws_iam_role.deploy.arn
}

output "state_bucket" {
  value = aws_s3_bucket.state.bucket
}
