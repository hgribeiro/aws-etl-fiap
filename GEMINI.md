# Project Overview

This project implements a serverless ETL (Extract, Transform, Load) pipeline on AWS for processing financial market data from the B3 stock exchange. The entire infrastructure is provisioned using Terraform, and the data processing logic is written in Python for AWS Glue and AWS Lambda.

The pipeline is designed with a decoupled architecture:
1.  An **AWS Glue job (`b3_collector.py`)** is triggered on a schedule. It extracts stock data using the `yfinance` library and saves it in Parquet format to an S3 bucket in a `raw/` directory.
2.  The arrival of new data in the `raw/` directory triggers an **AWS Lambda function (`glue_starter_lambda_function.py`)**.
3.  The Lambda function starts a second **AWS Glue job (`b3_transform.py`)**, which reads the raw data, applies business transformations, and saves the cleaned, enriched data to a `refined/` directory in the same S3 bucket.
4.  The transformed data is cataloged in the **AWS Glue Data Catalog**, making it available for querying via **Amazon Athena**.

The infrastructure is managed as code using Terraform, with separate modules for S3, IAM, Glue, and Lambda. The project appears to be configured for local development against a LocalStack environment.

## Building and Running

### Dependencies

The project's Python dependencies are listed in `requirements.txt`. To install them, run:

```bash
pip install -r requirements.txt
```

### Infrastructure

The AWS infrastructure is managed by Terraform. The configuration is located in the `infra/` directory. The `infra/iam/providers.tf` file is configured to use LocalStack, suggesting a local development setup.

To provision the infrastructure locally (assuming you have LocalStack and Terraform installed):

```bash
cd infra/s3
terraform init
terraform apply

cd ../iam
terraform init
terraform apply

# (Apply for other terraform modules)
```

### Running the Pipeline

1.  **Deployment**: Deploy the Glue scripts and Lambda function to your AWS (or LocalStack) environment. The Terraform scripts should handle the creation of the necessary resources.
2.  **Execution**: The pipeline is designed to be triggered automatically. The initial data extraction (`b3_collector.py`) is scheduled (likely via EventBridge, as per the ADR), and subsequent steps are triggered by S3 events.

## Development Conventions

*   **ETL Logic**: The core ETL logic is implemented in Python using PySpark within AWS Glue jobs. There is a clear separation between the extraction (`b3_collector.py`) and transformation (`b3_transform.py`) steps.
*   **Infrastructure as Code**: All AWS resources are defined and managed using Terraform. The configuration is modular, with different components of the infrastructure separated into their own directories.
*   **Data Format**: Data is stored in the S3 bucket in Parquet format, which is a columnar storage format optimized for analytics.
*   **Partitioning**: The data in S3 is partitioned by date and, in the `refined` layer, by stock ticker. This is a best practice for optimizing query performance in Athena.
