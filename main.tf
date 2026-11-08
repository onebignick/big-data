terraform {
  required_version = ">= 1.10" # use_lockfile on the S3 backend needs 1.10+
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Shared state: private, versioned bucket (created outside Terraform). Credentials come from the
  # usual AWS chain (AWS_PROFILE / default profile), so each teammate uses their own profile.
  backend "s3" {
    bucket       = "bda8-aml-tfstate-129712841435"
    key          = "bda8-aml/terraform.tfstate"
    region       = "ap-southeast-1"
    use_lockfile = true
  }
}

provider "aws" {
  profile = var.aws_profile
  region  = var.region

  default_tags {
    tags = {
      project = "bda8-aml"
      owner   = "onebignick"
    }
  }
}

data "aws_caller_identity" "me" {}

locals {
  name       = "bda8-aml"
  account_id = data.aws_caller_identity.me.account_id
  bucket     = "${local.name}-lake-${local.account_id}"
}

# Refuse to run against any account other than the agreed one.
resource "terraform_data" "account_guard" {
  lifecycle {
    precondition {
      condition     = local.account_id == var.expected_account_id
      error_message = "AWS profile points at the wrong account."
    }
  }
}

# Untagged provider for the budget: the IAM policy allows budgets:ModifyBudget but not budgets:TagResource.
provider "aws" {
  alias   = "untagged"
  profile = var.aws_profile
  region  = var.region
}
