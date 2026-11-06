# P5: Kafka (Amazon MSK) ingest, Lambda replay producer, SNS alert topic.
#
# The producer plays the bank's core system: it sends the RAW Kaggle CSV rows, unchanged,
# to the Kafka topic in event-time order. Everything downstream of Kafka cleans and
# featurises those raw rows itself; the stream reads nothing pre-processed from the lake.
#   Kafka --> Spark (EMR) --> BQ1 running rate tables      (results/bq1_stream/)
#                         --> BQ2 scoring + flags + SNS     (results/stream_scored/, results/flags/)
#                         --> raw archive into the lake     (landing/<dataset>/stream/dt=...)

variable "kafka_topic" {
  type    = string
  default = "aml-raw-txn"
}

data "aws_subnet" "kafka" {
  for_each          = toset(["${var.region}a", "${var.region}b"])
  vpc_id            = data.aws_vpc.default.id
  availability_zone = each.value
  default_for_az    = true
}

resource "aws_security_group" "kafka" {
  name        = "${local.name}-kafka"
  description = "MSK brokers: Kafka plaintext from inside the default VPC only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Kafka plaintext from the VPC (EMR, replay Lambda)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "replay" {
  name        = "${local.name}-replay"
  description = "Replay Lambda: outbound only"
  vpc_id      = data.aws_vpc.default.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# The VPC Lambda has no internet route; reach S3 through a (free) gateway endpoint.
data "aws_route_table" "default_main" {
  vpc_id = data.aws_vpc.default.id
  filter {
    name   = "association.main"
    values = ["true"]
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [data.aws_route_table.default_main.id]
}

resource "aws_msk_configuration" "aml" {
  name              = "${local.name}-kafka"
  kafka_versions    = ["3.9.x"]
  server_properties = <<-EOT
    auto.create.topics.enable=false
    num.partitions=3
    default.replication.factor=2
    min.insync.replicas=1
    log.retention.hours=24
  EOT
}

resource "aws_msk_cluster" "aml" {
  cluster_name           = "${local.name}-kafka"
  kafka_version          = "3.9.x"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = [for s in data.aws_subnet.kafka : s.id]
    security_groups = [aws_security_group.kafka.id]
    storage_info {
      ebs_storage_info {
        volume_size = 10
      }
    }
  }

  client_authentication {
    unauthenticated = true
  }
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }
  configuration_info {
    arn      = aws_msk_configuration.aml.arn
    revision = aws_msk_configuration.aml.latest_revision
  }
}

resource "aws_sns_topic" "flags" {
  name = "${local.name}-flags"
}

resource "aws_sns_topic_subscription" "flags_email" {
  topic_arn = aws_sns_topic.flags.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "replay" {
  name               = "${local.name}-replay"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy_attachment" "replay_vpc" {
  role       = aws_iam_role.replay.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "replay" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/landing/*"]
  }
}

resource "aws_iam_role_policy" "replay" {
  name   = "replay-access"
  role   = aws_iam_role.replay.id
  policy = data.aws_iam_policy_document.replay.json
}

# Built by bin/build-lambda.sh (vendors kafka-python next to replay.py).
data "archive_file" "replay" {
  type        = "zip"
  source_dir  = "${path.module}/.build/replay-pkg"
  output_path = "${path.module}/.build/replay.zip"
}

resource "aws_cloudwatch_log_group" "replay" {
  name              = "/aws/lambda/${local.name}-replay"
  retention_in_days = 14
}

resource "aws_lambda_function" "replay" {
  function_name    = "${local.name}-replay"
  role             = aws_iam_role.replay.arn
  runtime          = "python3.12"
  handler          = "replay.handler"
  filename         = data.archive_file.replay.output_path
  source_code_hash = data.archive_file.replay.output_base64sha256
  memory_size      = 3008 # holds one raw replay day (~480k CSV lines) to sort by event time
  timeout          = 900
  depends_on       = [aws_cloudwatch_log_group.replay, aws_iam_role_policy_attachment.replay_vpc]

  vpc_config {
    subnet_ids         = [for s in data.aws_subnet.kafka : s.id]
    security_group_ids = [aws_security_group.replay.id]
  }

  environment {
    variables = {
      BUCKET    = aws_s3_bucket.lake.bucket
      RAW_KEY   = "landing/${var.dataset}/transactions/HI-Small_Trans.csv"
      BOOTSTRAP = aws_msk_cluster.aml.bootstrap_brokers
      TOPIC     = var.kafka_topic
    }
  }
}

# Raw rows archived by the stream (landing zone): one CSV line per row, partitioned by dt.
resource "aws_glue_catalog_table" "landing_stream_raw" {
  name          = "landing_stream_raw"
  database_name = aws_glue_catalog_database.aml.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    "projection.enabled"        = "true"
    "projection.dt.type"        = "date"
    "projection.dt.format"      = "yyyy-MM-dd"
    "projection.dt.range"       = "2022-09-01,2022-09-30"
    "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/landing/${var.dataset}/stream/dt=$${dt}/"
  }
  partition_keys {
    name = "dt"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/landing/${var.dataset}/stream/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
    }
    dynamic "columns" {
      for_each = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
      "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
      content {
        name = columns.value
        type = "string"
      }
    }
  }
}

output "kafka_bootstrap" {
  value = aws_msk_cluster.aml.bootstrap_brokers
}

output "kafka_topic" {
  value = var.kafka_topic
}

output "sns_topic" {
  value = aws_sns_topic.flags.arn
}
