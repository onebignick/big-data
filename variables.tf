variable "aws_profile" {
  type    = string
  default = "default"
}

variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "expected_account_id" {
  type    = string
  default = "129712841435"
}

variable "budget_usd" {
  description = "Monthly account budget; alerts at 50/80/100% of actual spend."
  type        = number
  default     = 50
}

variable "alert_email" {
  description = "Budget alert recipient."
  type        = string
}

variable "athena_scan_limit_bytes" {
  description = "Per-query scan cutoff for the Athena workgroup."
  type        = number
  default     = 10737418240 # 10 GiB
}
