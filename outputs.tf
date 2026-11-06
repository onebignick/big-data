output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.aml.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.aml.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}
