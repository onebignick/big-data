# P6 hosting: the Streamlit dashboard on one small EC2 instance, read-only over Athena.
# Open http://<dashboard_url>; a shared password (output dashboard_password) gates the page.
# Changing app/streamlit_app.py replaces the instance on the next apply (user_data hash).

variable "dashboard_cidrs" {
  description = "Who may reach the dashboard port. Narrow to the group's IPs if known."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "dashboard_instance_type" {
  type    = string
  default = "t3.small"
}

resource "random_password" "dashboard" {
  length  = 20
  special = false
}

resource "aws_s3_object" "app" {
  bucket = aws_s3_bucket.lake.id
  key    = "code/app/streamlit_app.py"
  source = "${path.module}/app/streamlit_app.py"
  etag   = filemd5("${path.module}/app/streamlit_app.py")
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
  filter {
    name   = "state"
    values = ["available"]
  }
}

resource "aws_iam_role" "dashboard" {
  name               = "${local.name}-dashboard"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

data "aws_iam_policy_document" "dashboard" {
  statement {
    actions = ["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution",
    "athena:GetQueryResults", "athena:GetWorkGroup"]
    resources = [aws_athena_workgroup.aml.arn]
  }
  statement {
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
    "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition"]
    resources = [
      "arn:aws:glue:${var.region}:${var.expected_account_id}:catalog",
      aws_glue_catalog_database.aml.arn,
      "arn:aws:glue:${var.region}:${var.expected_account_id}:table/${aws_glue_catalog_database.aml.name}/*",
    ]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = [for p in ["curated", "results", "landing", "athena-results", "code/app"] : "${aws_s3_bucket.lake.arn}/${p}/*"]
  }
  statement {
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/athena-results/*"]
  }
}

resource "aws_iam_role_policy" "dashboard" {
  name   = "athena-read"
  role   = aws_iam_role.dashboard.id
  policy = data.aws_iam_policy_document.dashboard.json
}

resource "aws_iam_instance_profile" "dashboard" {
  provider = aws.untagged
  name     = "${local.name}-dashboard"
  role     = aws_iam_role.dashboard.name
}

resource "aws_security_group" "dashboard" {
  name        = "${local.name}-dashboard"
  description = "Streamlit dashboard port"
  vpc_id      = data.aws_vpc.default.id
  ingress {
    description = "Streamlit"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = var.dashboard_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "dashboard" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.dashboard_instance_type
  subnet_id                   = data.aws_subnet.emr.id
  vpc_security_group_ids      = [aws_security_group.dashboard.id]
  iam_instance_profile        = aws_iam_instance_profile.dashboard.name
  associate_public_ip_address = true
  user_data_replace_on_change = true

  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_type = "gp3"
    volume_size = 10
    encrypted   = true
  }

  user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail
    # app ${aws_s3_object.app.etag}
    dnf install -y python3.12 python3.12-pip
    useradd -r -m -d /opt/dash dash || true
    python3.12 -m venv /opt/dash/venv
    /opt/dash/venv/bin/pip install --quiet "streamlit==1.66.1" "PyAthena==4.2.1" "altair==6.3.0" "pandas==3.1.1" boto3
    cat > /etc/dash.env <<'ENV'
    APP_PASSWORD=${random_password.dashboard.result}
    AWS_DEFAULT_REGION=${var.region}
    ENV
    chmod 600 /etc/dash.env
    cat > /etc/systemd/system/dash.service <<'UNIT'
    [Unit]
    Description=BDA8 AML Streamlit dashboard
    After=network-online.target
    [Service]
    User=dash
    WorkingDirectory=/opt/dash
    EnvironmentFile=/etc/dash.env
    ExecStartPre=/usr/bin/aws s3 cp s3://${aws_s3_bucket.lake.bucket}/code/app/streamlit_app.py /opt/dash/streamlit_app.py
    ExecStart=/opt/dash/venv/bin/streamlit run /opt/dash/streamlit_app.py --server.headless true --server.port 8501 --server.address 0.0.0.0 --browser.gatherUsageStats false
    Restart=always
    [Install]
    WantedBy=multi-user.target
    UNIT
    systemctl daemon-reload
    systemctl enable --now dash
  EOT

  tags = {
    Name = "${local.name}-dashboard"
  }
}

output "dashboard_url" {
  value = "http://${aws_instance.dashboard.public_ip}:8501"
}

output "dashboard_password" {
  value     = random_password.dashboard.result
  sensitive = true
}
