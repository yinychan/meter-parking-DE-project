
## Getting Started with AWS
The starting point assumes you already have an AWS account. If not, create one.
- [Terraform with AWS official provider docs](https://registry.terraform.io/providers/hashicorp/aws/latest)

- Add terraform extension to your VS Code (search for terraform and Install the one from HashiCorp Terraform)

## Infrastructure as Code (IaC) with Terraform

- Human-readable configuration files
- Can version, reuse, and share (easy collaboration)
- Consistent workflow to provision and manage all of your infrastructure (keeps track of infrastructure)
- Ensures resources are removed. So you do not continue to be charged for them

## Adding terraform to your current codepsaces session:

```
# Installation
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform

# Then, check that it's there
terraform --version
```

## Connecting AWS with Terraform using variables

### IAM users 
1. Create User button
2. Username: terraform-runner
    - Provide user access to the AWS Management Console - optional (leave unchecked)
    - Next
3. Set permissions > Permissions options > select Attach policies directly
4. Permissions policies (add) + AmazonS3FullAccess, + AWSGlueConsoleFullAccess, + AmazonAthenaFullAccess, + AmazonEC2FullAccess
5. Next > Review > Create user

### Generate an Access Key Pair
1. Click on `terraform-runner` from your user list to open its settings.
2. Click on the "Security credentials" tab > Look for Access keys > Click "Create access key"
3. Select "Command Line Interface (CLI)" > Confirmation: I understand the above recommendation and want to proceed to create an access key. (check this box)
4. Next > Description tag value: Enter "Data engineering with terraform"
5. Store the keys in your terraform project folder (e.g. /terraform/.env or /terraform/terraform-runner_accessKeys.csv)
- Option A: Downlod .csv
- Option B: create a new .env file in your terraform folder, and Copy / Paste keys:
```
AWS_ACCESS_KEY=key_random_string_copy_pasted
AWS_SECRET_ACCESS_KEY=secret_key_random_string_copy_pasted
```
6. Make sure you .gitignore your AWS credentials first, so you don't accidentally push them to github:
```
# In .gitignore, add:

*.csv
.terraform/
terraform.tfstate*
```

### Creating Terraform Configuration Files
1. In your terminal, navigate to your terraform project directory `/terraform`, add a `variables.tf` file. Think of this file as the schema behind your terraform configuration.

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

2. Create a `terraform.tfvars` file to assign concrete values to your previously established input variables. This separates your deployment data from structural infastructure code so you can reuse the same `*.tf` files across different deployment environments. Think of this file as the data to your schema in `variables.tf`

```
# In your terminal
touch terraform.tfvars
```

```
# In terraform.tfvars

aws_region         = "us-west-1" # change this to the region closest to you
s3_bucket_name     = "meter-parking-DE-project-aws-data-lake-yinychan" # change this to a bucket name matching your project
glue_database_name = "la_meter_parking_data" # change this db name to match your db
```

3. Create your `main.tf` where we will be implementing the main terraform integration with AWS

```
# In your terminal
touch main.tf
```

You can reference the provider example code in [terraform docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)for your `main.tf`.

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

4. Run the AWS export command through your terminal
```
export AWS_ACCESS_KEY_ID="your_access_key_id_from_csv"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key_from_csv"
export AWS_DEFAULT_REGION="us-west-1"
```

Then, run the initialization command to download the AWS provider plugin.

```
terraform init
```

From the output, you should see text that includes `Terraform has been successfully initialized!`

The AWS provider plugin acts as the translator that turns generic Terraform code into specific AWS API calls.

5. Create the AWS S3 bucket through Terraform. This block provisions the actual storage space where your raw data files will go.

In your `main.tf` file, add 

```
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true
}
```

`var.s3_bucket_name` should match the variable declared in your `variables.tf` file.

`force_destroy = true` lets Terraform clear out the data and the bucket instantly without you manually deleting millions of rows of data first. We have this setting for this Terraform exercise purposes only.

6. Enable bucket versioning. This block of code tells AWS to retain older variables of an object if a file with the exact same name is uploaded or modified. If your data pipeline overwrites a clean dataset with corrupted data, it allows you to roll back to the previous clean version directly inside S3.

In your `main.tf` file, add 

```
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake_bucket.id # Reference the S3 bucket created above

  versioning_configuration {
    status = "Enabled"
  }
}
```

7. Prevent data leaks. This block of code isolates our S3 bucket from the public internet to prevent cloud data leaks.

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

8. Clean up old buckets (set to 30 days). This block of code will instruct automatic deletion of data files after 30 days. For the purposes of this exercise, we don't need to store data for long.

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

9. Add a dataset to AWS Glue

```
resource "aws_glue_catalog_database" "dataset" {
  name = var.glue_database_name
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