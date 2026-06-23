## Data Warehouse with AWS

Data warehousing is the practice of consolidating raw data from multiple different company or ogranizational systems into a single, highly structured and centralized database optimized specifically for running data queries, generating business reports, and populating dashboards.

In this exercise, the bulk of our work is done within Snowflake and AWS, so we only have a `README.md` file here. There are no other file assets.

## Contents

- [Concepts & Definitions](#concepts--definitions)
    - [Database Systems](#database-systems)
        - [Online Transactional Processing (OLTP)](#online-transactional-processing-oltp)
        - [Online Analytical Processing (OLAP)](#online-analytical-processing-olap)
    - [Staging Tables](#staging-tables)
    - [Data Normalization](#data-normalization)
    - [Data Denormalization](#data-denormalization)
        - [Fact Tables](#fact-tables)
        - [Dimension Tables](#dimension-tables)
        - [Star Schema](#star-schema)

- [Snowflake Architecture](#snowflake-architecture)
    - [Secure Cloud Authentication](#secure-cloud-authentication)

## Concepts & Definitions

### Database Systems

Before diving in to data warehousing, we need to understand the distinction between 2 types of database systems: Online Transactional Processing (known as OLTP) and Online Analytical Processing (known as OLAP).

#### Online Transactional Processing (OLTP)

OLTP database systems focus on real-time database operations, typically used in business operational contexts. They are built to handle rapid, day-to-day transactions such as making payments, appointment setting, or user account management. It deals with INSERT, UPDATE, and DELETE.

#### Online Analytical Processing (OLAP)

OLAP database systems focus on fast, complex, bulk analytical data querying. It typically transforms data extracted from OLTP systems, aggregating them for data warehousing. It is columnar-based, meaning it only reads specific columns needed for a specific query instead of reading entire rows. OLAPs will be what we're working with for data warehousing and data analytics. We will be dealing with SELECT, JOIN, and aggregation methods including `grouping sets`, `rollups`, and `cross-tabulation`.

### Staging Tables

Before we move onto core data warehouse concepts, we need to at least touch on how we got here. Our data orchestration might have been our most extensive section so far, but it ended with a [complete extract and load onto AWS S3](/orchestration/README.md#completed-extract-and-load) and 3 AWS Glue database tables containing raw Parquet files extracted from source data. 

In a typical data warehouse process, you would break down data from staging tables and normalize them into schemas that match your analytical query requirements. After the normalization process, you would denomralize them by joining the smaller normalized tables into a larger table so users don't have to write complex joins to access the exact data they need. 

However, because Socrata streams all fields as raw strings, we inject an interim processing layer: **Apache Spark**. Spark acts as our heavy compute engine to clean data, apply strict data types, and pre-calculate surrogate keys. Once processed by Spark, the strongly-typed data lands in an optimized S3 folder before loading into our target analytical engine: **Snowflake**.

### Data Normalization

Normalization breaks a single, messy table down into multiple, smaller tables to eliminate redundant data. These smaller tables are also called Dimension Tables. In data warehousing, we start here by taking staging tables and breaking them down into dimension tables. Dimension tables contrast our transaction tables in that it organizes and stores only information we need for data analysis.

While Amazon Redshift and Amazon Athena are the traditional choices in an AWS workflow, we will be working with **Snowflake** as our Cloud Data Platform along side local **Apache Spark** compute clusers.

Snowflake is a moder, cloud-native analytical data platform. It separates compute from storage in the cloud, allowing both to scale independently and dynamically.

### Data Denormalization

When we denormalize our data, we take our dimension tables and join them back to a central table containing only the columns essential to our analysis criteria. It allows us to optimize our queries so we are not running joins on multiple, separate table taken from our source transactional datasets.

#### Fact Tables

In the process of denormalization, the joining of a central table creates our Fact Table, which stores measurable, numerical data such as sales amounts, quantities, or temperatures. It also requires `foreign_key`s used to link to dimension tables for streamlined querying.

#### Dimension Tables

We created dimension tables from our data normalization process. In contrast to fact tables, they contain human-readable attributes that describe who, what, where, and when details.

#### Star Schema

The star schema is the resulting, foundation data warehouse design where there is a central fact table, surrounded by dimension tables. Visually, this creates a star-like structure.

## Snowflake Architecture

We will configure Snowflake to pull our strongly-typed data straight from our S3 data lake via secure integrations, bypassing the need to manage database users or static passwords.

We will:
- Write the SQL statements inside Snowflake to create your virtual warehouse
- Establish our database schemas
- Configure the passwordless AWS S3 storage integration

The following steps assume a Snowflake account already exists.

### Secure Cloud Authentication

We will create an AWS IAM Cross-Account Role to link Snowflake and S3 without using passwords. This allows Snowflake to assume a secure identity to read our S3 bucket without hardcoded keys.

You can follow Snowflake's official documentation here to [Configure a Snowflake storage integration to access Amazon S3](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration){:target="_blank" class="external"}. I will only touch out concepts you should make note of. Otherwise, the Snowflake documentation will have all the details you need for this section.

__Step 1: Configure access permissions for the S3 bucket__

Once you get to number 8 in "Create an IAM policy", your bucket name should be the same as the bucket name created in [Terraform: Creating Terraform Configuration Files](/terraform/README.md#creating-terraform-configuration-files). We also want to set a prefix to separate our cleaned data (from future steps using Apache Spark). We can use the "Medallion Architecture" and label the Spark output as `silver`.

`<bucket>` -> ladot-meter-parking-de-project-aws-data-lake
`<prefix> -> silver

Going with the "Alternative policy: Load from a read-only S3 bucket" instruction option, your JSON should look like this:

```
{
 "Version": "2012-10-17",
 "Statement": [
     {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:GetObjectVersion"
         ],
         "Resource": "arn:aws:s3:::ladot-meter-parking-de-project-aws-data-lake/silver/*"
     },
     {
         "Effect": "Allow",
         "Action": [
             "s3:ListBucket",
             "s3:GetBucketLocation"
         ],
         "Resource": "arn:aws:s3:::ladot-meter-parking-de-project-aws-data-lake",
         "Condition": {
             "StringLike": {
                 "s3:prefix": [
                     "silver/*"
                 ]
             }
         }
     }
 ]
}
```

**Note** the `URL` pointing to a `/silver/` directory that doesn't exist yet. We will create this in our Apache Spark Batch Script section in [Batch and Analytics Engineering](/batch/README.md#apache-spark-batch-script).

__Step 2: Create the IAM role in AWS__

Once you have successfully completed this step, copy and paste your AWS role arn somewhere safe. It should look something like `arn:aws:iam::123456789012:role/snowflake_role`

__Step 3: Create a cloud storage integration in Snowflake__

The Snowflake documentation doesn't specific where you're running the `CREATE STORAGE INTEGRATION` SQL they suggested. You can go to your Snowflake dashboard, hover over "Projects" on the left navigation, and click on "Workspaces". Under "My Workspace", click "+ Add new" and select "SQL file". I named mine `configure_s3.sql`, but you can name what makes most sense to you.

Once complete with the remainder of the instructions, you will see a success message similar to "Integration S3_LADOT_LAKE_INTEGRATION successfully created".

Remember your `<integration_name>` for the next step.

__Step 4: Retrieve the AWS IAM user for your Snowflake account__

You can use the same `configure_s3.sql` file to run the `DESCRIBE` command. Before running the script though, check the following:

- There's a semicolon `;` at the end of each SQL statement.
- Only the command you want to run is selected. You don't want to run the `CREATE STORAGE` SQL again.

Make your own separate note of the following values:

```
STORAGE_AWS_IAM_USER_ARN=arn:aws:iam::123456789012:user/1abc0000-s
STORAGE_AWS_EXTERNAL_ID=copy_your_external_id_here
```

__Step 5: Grant the IAM user permissions to access bucket objects__

We replace the temporary values that we created in Step 2 earlier using what we noted for `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.

__Step 6: Create an external stage__

If you are operating under the `ACCOUNTADMIN` role, you can skip Snowflake's note on `GRANT CREATE STAGE` and `GRANT USAGE` commands.

Snowflake's instructions here assume we already have `mydb`, `public`, and `my_csv_format` from our Snowflake account, which we don't. So we'll need to create them along with this step. We also have to remember we're dealing with Parquet files on the AWS side.

```
CREATE DATABASE IF NOT EXISTS ladot_warehouse;
CREATE SCHEMA IF NOT EXISTS ladot_warehouse.core;

USE DATABASE ladot_warehouse;
USE SCHEMA core;

CREATE FILE FORMAT parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

CREATE STAGE silver_stage
  STORAGE_INTEGRATION = s3_ladot_lake_integration
  URL = 's3://ladot-meter-parking-de-project-aws-data-lake/silver/'
  FILE_FORMAT = parquet_format;
```

**Note** the `URL` pointing to a `/silver/` directory that doesn't exist yet. We will create this in our Apache Spark Batch Script section in [Batch Processing & Analytics Engineering](/batch/README.md#apache-spark-batch-script).

Once you see "Stage area SILVER_STAGE successfully created.", you are done with this Data Warehouse exercise!

## Back to main

Excellent, you can [continue back at the main project](../README.md).