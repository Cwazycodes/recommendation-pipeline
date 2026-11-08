# The AWS side of the pipeline: the trending table, the read API (Lambda behind
# an HTTP API), its logs, and the IAM roles for the API and the results writer.
# make deploy creates it and make destroy deletes it (docs/design.md,
# "Deployment").
#
# State is local (terraform.tfstate here, which git ignores): one person
# deploys this stack, so there is nothing to share or lock against.

terraform {
  required_version = ">= 1.17, < 2.0"

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
