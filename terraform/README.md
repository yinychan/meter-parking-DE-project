## Terraform Contents

- [Infrastructure as Code (IaC) with Terraform](#infrastructure-as-code-iac-with-terraform)
- [Getting Started with AWS](#getting-started-with-aws)
- [Adding terraform to your current codepsaces session:](#adding-terraform-to-your-current-codepsaces-session)
- [Connecting AWS with Terraform using variables](#connecting-aws-with-terraform-using-variables)
  - [IAM users](#iam-users)
  - [Generate an Access Key Pair](#generate-an-access-key-pair)
  - [Creating Terraform Configuration Files](#creating-terraform-configuration-files)
- [In summary](#in-summary)

## Infrastructure as Code (IaC) with Terraform

Infrastructure as Code is the practice of provisioning, managing, and updating your cloud infrastructure of networks, servers, and databases using configuration files rather than through a graphic user interface. For my exercise here, I will be using Terraform as my IaC tool to provision AWS S3 and AWS Glue. Here are the distinct features of setting up your data workflow through infrastructure as code: 

- Human-readable configuration files
- Which you can version, reuse, and share (easy collaboration)
- Consistent workflow to provision and manage all of your infrastructure (keeps track of infrastructure)
- You can writing into your configuration to ensure resources are removed so you do not continue to be charged for them

With that in mind, lets get started.

## Getting Started with AWS
The starting point assumes you already have an AWS account. If not, create one.

- [Terraform with AWS official provider docs](https://registry.terraform.io/providers/hashicorp/aws/latest){:target="_blank" class="external"}
- Add terraform extension to your VS Code (search for terraform and Install the one from HashiCorp Terraform)

## Installing Terraform in Codepsaces

```
# Installation
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform

# Then, check that it's there
terraform --version
```

## Connecting AWS with Terraform

To start, we need to create an AWS user for our Terraform runner. In your AWS dashboard, search for IAM. The page url would look something like this `*your-location.console.aws.amazon.com/iam/home/*`

### IAM users
1. From the "Identity and Access Management (IAM)" page, navigate to "IAM users" from the links on the left
2. Click "Create User" button on the top right corner
3. Specify user details page
    - User name: terraform-runner
    - Leave this option _unchecked_: "Provide user access to the AWS Management Console - optional"
    - Click "Next"
4. On the set permissions page, in "Permissions options" > select "Attach policies directly"
5. Permissions policies will show up. Search for and add the following: + AmazonS3FullAccess, + AWSGlueConsoleFullAccess, + AmazonAthenaFullAccess, + AmazonEC2FullAccess
6. Click "Next" > Review page > Click "Create user"

### Generate an Access Key Pair
We need to generate your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to use throughout all the exercises in this end-to-end data pipeline. Once you have created your terraform-runner user, you should be back at the "IAM users" page. From there:

1. Click on `terraform-runner` from your user list to open its settings.
2. Click on the "Security credentials" tab > Look for Access keys > Click "Create access key"
3. Select "Command Line Interface (CLI)" > Confirmation: I understand the above recommendation and want to proceed to create an access key. (check this box)
4. Click "Next", then > Description tag value: Enter "Data engineering with terraform"
5. Store the keys in your terraform project folder (e.g. /terraform/.env or /terraform/terraform-runner_accessKeys.csv)
    - Option A: Downlod .csv
    - Option B: create a new .env file in your terraform folder, and Copy / Paste keys:

```
AWS_ACCESS_KEY=key_random_string_copy_pasted
AWS_SECRET_ACCESS_KEY=secret_key_random_string_copy_pasted
```

Make sure you .gitignore your AWS credentials first, so you don't accidentally push them to github:

```
# In .gitignore, add:

*.csv
.terraform/
terraform.tfstate*
```

### Creating Terraform Configuration Files
In your terminal, navigate to your terraform project directory `/terraform`, add a `variables.tf` file. 

__1. *variables.tf*__

Think of this file as the schema behind your terraform configuration.

```
cd terraform
touch variables.tf
```

This is where you define your inputs for AWS services:

```
# In variables.tf file:

variable "aws_region" {
  description = "Region for AWS resources (N. California)"
  type        = string
  default     = "us-west-1"
}

variable "s3_bucket_name" {
  description = "AWS S3 Bucket Name (Data Lake)"
  type        = string
}

variable "glue_database_name" {
  description = "AWS Glue Catalog Database Name"
  type        = string
}
```

Create a `terraform.tfvars` file to assign concrete values to your previously established input variables. 

This separates your deployment data from structural infastructure code so you can reuse the same `*.tf` files across different deployment environments. 

__2. *terraform.tfvars*__

Think of this file as the data to your schema in `variables.tf`

```
# In your terminal
touch terraform.tfvars
```

```
# In terraform.tfvars

aws_region = "us-west-1" # change this to the region closest to you
s3_bucket_name = "ladot-meter-parking-de-project-aws-data-lake" # change this to a bucket name matching your project
glue_database_name = "la_meter_parking_data" # change this db name to match your db
```

Create your `main.tf`.

__3. *main.tf*__

This is where we will be implementing the main Terraform configurations with AWS as the provider

```
# In your terminal
touch main.tf
```

You can reference the provider example code in [Terraform docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs){:target="_blank" class="external"} for your `main.tf`.

```
# In main.tf

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
```

__4. export YOUR_KEYS__

Run the AWS export command through your terminal

```
export AWS_ACCESS_KEY_ID="your_access_key_id_from_csv"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key_from_csv"
export AWS_DEFAULT_REGION="us-west-1"
```

__5. init__

Then, run the initialization command to download the AWS provider plugin.

```
terraform init
```

From the output, you should see text that includes `Terraform has been successfully initialized!`

The AWS provider plugin acts as the translator that turns generic Terraform code into specific AWS API calls.

__6a. AWS S3 Bucket__

Create the AWS S3 bucket through Terraform. This block provisions the actual storage space where your raw data files will go.

In your `main.tf` file, add 

```
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true
}
```

`var.s3_bucket_name` should match the variable declared in your `variables.tf` file.

`force_destroy = true` lets Terraform clear out the data and the bucket instantly without you manually deleting millions of rows of data first. We have this setting for this Terraform exercise purposes only.

__6b. Enable bucket versioning__

The following block of code tells AWS to retain older variables of an object if a file with the exact same name is uploaded or modified. If your data pipeline overwrites a clean dataset with corrupted data, it allows you to roll back to the previous clean version directly inside S3.

In your `main.tf` file, add 

```
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake_bucket.id # Reference the S3 bucket created above

  versioning_configuration {
    status = "Enabled"
  }
}
```

__6b. Prevent data leaks__

This block of code isolates our S3 bucket from the public internet to prevent cloud data leaks.

In your `main.tf` file, add 

```
resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  block_public_acls = true
  block_public_policy = true
  ignore_public_acls  = true
  restrict_public_buckets = true
}
```

__6c. Clean up old buckets__

We clean up old buckets by setting a 30-day lifecycle. This block of code will instruct automatic deletion of data files after 30 days. For the purposes of this exercise, we don't need to store data for long.

```
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
```

__7. AWS Glue__

Add a dataset to AWS Glue

```
resource "aws_glue_catalog_database" "dataset" {
  name = var.glue_database_name
}
```

We also need to set up an IAM role for AWS Glue Crawler (which we later run in our workflow orchestration DAG).

```

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
```

Attach the AWS managed service policy for `AWSGlueServiceRole`. It "allows access to related services including EC2, S3, and Cloudwatch Logs".

Where it says `glue_service` is your name for the role policy

```
resource "aws_iam_role_policy_attachment" "glue_service" {
  role = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
```

Attach your custom policy:

```
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
```

Create your Glue Crawler. Where it says `parking_metrics_crawler` will be your custom name for this crawler. You'll need to pass that in to the `GlueCrawlerOperator` within [your DAG](/orchestration/README.md#aws-glue-crawler).

```
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
```

## In summary

Terraform gives us flexibility to say what resources we'd like to create through infrastructure as code.

Through each step of your `main.tf` set up, you can preview your configuration by running `terraform plan` in your terminal:

```
terraform plan
```

Once you're ready, you can deploy with

```
terraform apply
```

(There will be a prompt for you to say `yes`).

You can clean up with (since this is a development exercise)

```
terraform destroy
```

## Back to main

Excellent, you can [continue back at the main project](../README.md).