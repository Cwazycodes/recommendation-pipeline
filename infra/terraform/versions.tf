# The AWS side of the pipeline: the trending table, the read API (Lambda behind
# an HTTP API), its logs, and the IAM roles for the API and the results writer.
# make deploy creates it and make destroy deletes it (docs/design.md,
# "Deployment").
#
# State is in S3, in the bucket make bootstrap creates (infra/bootstrap), so a
# deploy from GitHub Actions and one from a laptop share it, and S3 lock files
# stop two from running at once. make passes the bucket's name, which includes
# the account ID.

terraform {
  required_version = ">= 1.17, < 2.0"

  backend "s3" {
    key          = "recommendation-pipeline/terraform.tfstate"
    region       = "eu-west-2"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "recommendation-pipeline"
      ManagedBy = "terraform"
    }
  }
}
