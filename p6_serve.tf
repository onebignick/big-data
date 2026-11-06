# P6: catalog the result tables (crawled after each run) for Athena / Streamlit.
resource "aws_glue_crawler" "results" {
  name          = "${local.name}-results"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.aml.name
  table_prefix  = "res_"

  dynamic "s3_target" {
    for_each = ["bq1/metrics", "bq1/feature_importance", "bq1/test_scores", "bq2/pattern_frequency",
    "bq2/account_flags", "bq2/rules", "stream_scored", "flags", "bq1_stream/rates"]
    content {
      path = "s3://${aws_s3_bucket.lake.bucket}/results/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DELETE_FROM_DATABASE"
  }
}
