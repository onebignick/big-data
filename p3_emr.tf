# P3-P5: EMR roles. Clusters are transient (bin/emr-run.sh): created per run, terminate
# after their steps, so there is no cluster resource in Terraform state.

data "aws_iam_policy_document" "emr_service_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_service" {
  name               = "${local.name}-emr-service"
  assume_role_policy = data.aws_iam_policy_document.emr_service_trust.json
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_ec2" {
  name               = "${local.name}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

# Cluster nodes: read the lake, write only results/models/checkpoints/logs/tmp,
# write the raw stream archive, publish to the one topic. No managed EMR EC2 policy (it grants s3:* on *).
data "aws_iam_policy_document" "emr_ec2" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [for p in ["results", "models", "checkpoints", "logs", "tmp", "landing/${var.dataset}/stream"] : "${aws_s3_bucket.lake.arn}/${p}*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.region}.elasticmapreduce", "arn:aws:s3:::${var.region}.elasticmapreduce/*"]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.flags.arn]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData", "ec2:DescribeTags"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_ec2" {
  name   = "lake-stream-access"
  role   = aws_iam_role.emr_ec2.id
  policy = data.aws_iam_policy_document.emr_ec2.json
}

resource "aws_iam_instance_profile" "emr_ec2" {
  provider = aws.untagged # policy allows TagRole but not TagInstanceProfile
  name     = "${local.name}-emr-ec2"
  role     = aws_iam_role.emr_ec2.name
}

data "aws_subnet" "emr" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = "${var.region}a"
  default_for_az    = true
}

data "aws_vpc" "default" {
  default = true
}

output "emr_subnet" {
  value = data.aws_subnet.emr.id
}

# Spark job scripts (curate.py is uploaded separately for Glue).
resource "aws_s3_object" "spark_jobs" {
  for_each = setsubtract(fileset("${path.module}/jobs", "*.py"), ["curate.py"])
  bucket   = aws_s3_bucket.lake.id
  key      = "code/spark/${each.value}"
  source   = "${path.module}/jobs/${each.value}"
  etag     = filemd5("${path.module}/jobs/${each.value}")
}

resource "aws_s3_object" "bootstrap" {
  bucket = aws_s3_bucket.lake.id
  key    = "code/bootstrap/python-deps.sh"
  source = "${path.module}/bootstrap/python-deps.sh"
  etag   = filemd5("${path.module}/bootstrap/python-deps.sh")
}
