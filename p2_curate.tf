# P2: Glue ETL job (clean + features -> curated Parquet) and the curated crawler.

variable "dataset" {
  type    = string
  default = "hi-small"
}

variable "train_end" {
  description = "Last day (inclusive) of the train split. HI-Small: normal traffic stops after 09-10."
  type        = string
  default     = "2022-09-07"
}

variable "test_end" {
  description = "Last day (inclusive) of the test split; later days are the laundering-only tail."
  type        = string
  default     = "2022-09-10"
}

resource "aws_s3_object" "curate_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "code/glue/curate.py"
  source = "${path.module}/jobs/curate.py"
  etag   = filemd5("${path.module}/jobs/curate.py")
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${local.name}"
  retention_in_days = 14
}

resource "aws_glue_job" "curate" {
  name              = "${local.name}-curate"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.curate_script.key}"
  }

  default_arguments = {
    "--bucket"                           = aws_s3_bucket.lake.bucket
    "--dataset"                          = var.dataset
    "--train_end"                        = var.train_end
    "--test_end"                         = var.test_end
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue.name
    "--TempDir"                          = "s3://${aws_s3_bucket.lake.bucket}/tmp/glue/"
  }
}

resource "aws_glue_crawler" "curated" {
  name          = "${local.name}-curated"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.aml.name
  table_prefix  = "curated_"

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/curated/${var.dataset}/transactions/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version  = 1.0
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })
}

output "curate_job" {
  value = aws_glue_job.curate.name
}
