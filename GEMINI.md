## Project Overview

This project implements a serverless ETL (Extract, Transform, Load) pipeline on AWS to process financial market data. It extracts daily stock data using `yfinance`, stores it in S3, transforms it with AWS Glue, and makes it available for querying via Amazon Athena. The entire infrastructure is defined as code using Terraform.

The pipeline is orchestrated as follows:

1.  A daily scheduled EventBridge rule triggers an AWS Glue job to extract data.
2.  The extraction job (`glue_extractor_job.py`) fetches data using `yfinance` and saves it as Parquet files in an S3 bucket under the `raw/` prefix.
3.  The arrival of new data in the `raw/` directory triggers a Lambda function (`glue_starter_lambda_function.py`).
4.  This Lambda function starts a second AWS Glue job (`glue_transformer_job.py`).
5.  The transformation job enriches the data, performs calculations, and saves the results in the `refined/` prefix of the S3 bucket.
6.  The AWS Glue Data Catalog is updated, allowing the transformed data to be queried using Amazon Athena.

## Building and Running

### Prerequisites

*   Docker
*   Docker Compose
*   An AWS account with credentials configured for Terraform

### Local Development

This project uses LocalStack to simulate AWS services locally.

1.  **Start LocalStack:**
    ```bash
    docker-compose up
    ```
2.  **Run the ETL process:**
    The `etl.py` script can be used for local testing.
    ```bash
    python src/etl.py
    ```
### AWS Deployment

The infrastructure is deployed using Terraform.

1.  **Initialize Terraform:**
    ```bash
    cd infra
    terraform init
    ```
2.  **Apply Terraform:**
    ```bash
    terraform apply
    ```
## Development Conventions

*   **Infrastructure:** All infrastructure is managed as code using Terraform, with modules for each service (`s3`, `iam`, etc.).
*   **ETL Scripts:** ETL logic is implemented in Python using PySpark for AWS Glue jobs.
*   **Data Format:** Data is stored in Parquet format in S3, partitioned by date and ticker symbol.
*   **Containerization:** The project includes a `Dockerfile` for containerizing the Python application and a `docker-compose.yml` file for running LocalStack.
*   **Dependencies:** Python dependencies are managed in `requirements.txt`.
