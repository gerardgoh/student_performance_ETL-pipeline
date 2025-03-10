# Student Performance ETL Pipeline

A data engineering pipeline that processes student performance data from Kaggle, performs ETL operations, and uploads the results to Amazon S3, orchestrated with Apache Airflow.

## Project Overview

This project builds an end-to-end data pipeline that:
1. Extracts student performance data from a CSV source
2. Validates data quality and consistency
3. Transforms the data with feature engineering and preprocessing
4. Loads the processed data locally and to Amazon S3
5. Orchestrates the entire workflow using Apache Airflow

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│             │    │             │    │             │    │             │
│  Extract    │───►│  Validate   │───►│  Transform  │───►│  Load       │
│  (CSV)      │    │  Data       │    │  (Features) │    │ (Local/S3)  │
│             │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
        │                                                       ▲
        │                                                       │
        │                ┌────────────────────────┐            │
        └───────────────►│                        │────────────┘
                         │  Airflow Orchestration │
                         │                        │
                         └────────────────────────┘
```

## Components

### 1. ETL Module (`student_etl.py`)
- **Extract**: Reads data from CSV files
- **Validate**: Performs data quality checks including missing values, duplicates, and range validation
- **Transform**: Creates derived features (total score, average score, performance categories)
- **Load**: Saves processed data locally and to Amazon S3

### 2. S3 Operations (`s3_operations.py`)
- Functions for uploading to and reading from Amazon S3 buckets
- Handles serialization and error handling

### 3. Airflow DAG (`student_performance_dag.py`)
- Defines the workflow for daily data processing
- Manages task dependencies and execution
- Handles data passing between tasks with XCom

## Data Transformation

The pipeline performs these key transformations:
- Column renaming for consistency
- Creation of total and average score columns
- Performance categorization (Excellent, Good, Satisfactory, Needs Improvement, Failing)
- Data validation at multiple stages

## Setup Instructions

### Prerequisites
- Python 3.8+
- Apache Airflow 2.0+
- AWS account with S3 access
- Pandas, NumPy, Matplotlib, Seaborn

### Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/student-performance-pipeline.git
cd student-performance-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```
This will store your AWS credentials in `~/.aws/credentials` which can be used by the pipeline.

4. Set up Airflow:
```bash
# Initialize the Airflow database
airflow db init

# Create a user
airflow users create \
    --username admin \
    --firstname YourFirstName \
    --lastname YourLastName \
    --role Admin \
    --email your.email@example.com \
    --password yourpassword

# Start the webserver
airflow webserver -p 8080

# In another terminal, start the scheduler
airflow scheduler
```

5. Copy your DAG files to the Airflow DAGs folder:
```bash
cp student_performance_dag.py ~/airflow/dags/
```

6. Configure AWS credentials for Airflow:
   
   There are three ways to set up AWS credentials for the Airflow S3 tasks:

   **Option 1: Direct credentials in DAG** (for testing only)
   ```python
   # In your load_s3_task function
   ACCESS_KEY = 'your_access_key'
   SECRET_KEY = 'your_secret_key'
   REGION = 'your_region'  # e.g., 'us-east-1'
   
   s3_client = boto3.client(
       's3',
       aws_access_key_id=ACCESS_KEY,
       aws_secret_access_key=SECRET_KEY,
       region_name=REGION
   )
   ```

   **Option 2: Use Airflow Connections** (recommended for production)
   ```bash
   # Add AWS connection to Airflow
   airflow connections add 'aws_default' \
       --conn-type 'aws' \
       --conn-login 'your_access_key' \
       --conn-password 'your_secret_key' \
       --conn-extra '{"region_name": "your_region"}'
   ```

   **Option 3: Use AWS Configuration from ~/.aws/credentials**
   ```python
   # In your load_s3_task function
   from botocore.config import Config
   
   boto3_config = Config(
       region_name='your_region',
       signature_version='s3v4'
   )
   
   s3_client = boto3.client('s3', config=boto3_config)
   ```

## Usage

### Running the Pipeline Manually

```python
from student_etl import run_pipeline

run_pipeline(
    input_path="data/StudentsPerformance.csv",
    output_path="data/student_performance_processed.csv",
    visualize=True
)
```

### Using Airflow

1. Access the Airflow UI at http://localhost:8080
2. Enable the `student_performance_pipeline` DAG
3. Trigger the DAG manually or wait for the scheduled execution

## Project Structure

```
student-performance-pipeline/
├── data/
│   ├── StudentsPerformance.csv
│   └── student_performance_processed.csv
├── student_etl.py
├── s3_operations.py
├── student_performance_dag.py
├── requirements.txt
└── README.md
```

## Configuration

Edit the following variables in `student_performance_dag.py` to match your environment:

```python
input_path = '/path/to/your/StudentsPerformance.csv'
output_path = '/path/to/your/student_performance_processed.csv'
bucket_name = 'your-s3-bucket-name'
s3_key = 'data/student_performance.csv'
```

### S3 Bucket Configuration

Ensure your S3 bucket has the proper permissions:

1. Your AWS user needs to have permission to write to the bucket
2. Make sure your bucket policy allows PutObject actions
3. To verify access, try a simple upload using the AWS CLI:
   ```bash
   echo "test" > test.txt
   aws s3 cp test.txt s3://studentperformance-gerard/test.txt
   ```

### Troubleshooting S3 Upload Issues

If you encounter issues with S3 uploads in the pipeline:

1. Check your AWS credentials are correct
2. Verify region configuration matches your bucket's region
3. Check for "SignatureDoesNotMatch" errors which indicate credential mismatches
4. Try the S3 task independently:
   ```bash
   airflow tasks test student_performance_pipeline load_s3 2025-03-10
   ```
5. Review logs for specific AWS errors

## Data Pipeline Workflow

The pipeline follows this progression:

1. **Extract**: Reads student performance data from the Kaggle CSV file
2. **Transform**: 
   - Converts column names (spaces to underscores)
   - Calculates total and average scores
   - Categorizes student performance (Excellent, Good, Satisfactory, etc.)
3. **Validate**: 
   - Checks for missing values
   - Identifies duplicate records
   - Validates score ranges (0-100)
4. **Load**: 
   - Saves processed data to a local CSV file
   - Uploads the data to Amazon S3 for cloud storage

### Airflow Tasks and Dependencies

```
Extract → Transform → Validate → Load(Local)
                              → Load(S3)
```

All tasks use XCom to pass data references between them, with fallback mechanisms if the previous task data isn't available.

## Future Improvements

- Add data quality monitoring dashboards
- Implement alerting for pipeline failures
- Add support for incremental data loading
- Integrate with other data sources (databases, APIs)
- Implement CI/CD for pipeline updates
- Improve error handling with retry logic
- Add data schema validation
- Implement data lineage tracking
- Create visualization dashboards for student performance metrics
- Automate reporting and insights generation

## License

[MIT License](LICENSE)
