terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
}

# AWS S3 bucket to store data 
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true
}

# Enable bucket versioning
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake_bucket.id # Reference the S3 bucket created above

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access to prevent data leaks
resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id # Reference the S3 bucket created above

  block_public_acls = true
  block_public_policy = true
  ignore_public_acls  = true
  restrict_public_buckets = true
}

# Delete old buckets after 30 days
resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_rules" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  rule {
    id = "lifecycle_delete_after_30_days"
    status = "Enabled"

    expiration {
      days = 30
    }
    filter {
      prefix = ""
    }
  }
}

# Insert a dataset
resource "aws_glue_catalog_database" "dataset" {
  name = var.glue_database_name
}

# 1. Glue Crawler: IAM role for Glue Crawler
resource "aws_iam_role" "glue_crawler_role" {
  name = "ladot_parking_glue_crawler_role"

  # Standard configs
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "://amazonaws.com"
        }
      }
    ]
  })
}

# 2. Glue Crawler: Attach the AWSGlueServiceRole policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 3. Glue Crawler: Create a custom inline policy allowing Glue to access S3 bucket
resource "aws_iam_role_policy" "glue_s3_access_policy" {
  name = "ladot_parking_glue_s3_access"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake_bucket.arn,
          "${aws_s3_bucket.data_lake_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Create a Glue Crawler
resource "aws_glue_crawler" "parking_metrics_crawler" {
  database_name = aws_glue_catalog_database.dataset.name
  name = "ladot_parking_metrics_crawler"
  role = aws_iam_role.glue_crawler_role.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake_bucket.bucket}"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}