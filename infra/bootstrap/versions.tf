# What the main stack (infra/terraform) needs before GitHub Actions can deploy
# it: an S3 bucket for its state, GitHub's OIDC identity provider, and the role
# the deploy workflow assumes. make bootstrap applies it once, with your own
# credentials (docs/design.md, "Deployment").
#
# Its own state is local (terraform.tfstate here, which git ignores): it rarely
# changes, and everything in it could be imported again if the file were lost.

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
