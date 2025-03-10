import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
from s3_operations import upload_to_s3
import boto3

# Configure visualization
plt.style.use('seaborn-v0_8-whitegrid')
sns.set(font_scale=1.2)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def extract_data(source_path):
    """Extract data from CSV source file."""
    log(f"Extracting data from {source_path}")
    try:
        df = pd.read_csv(source_path)
        log(f"Successfully extracted {len(df)} records")
        return df
    except Exception as e:
        log(f"Error extracting data: {e}")
        raise

def validate_data(df):
    """Perform data quality checks."""
    log("Starting data validation")
    validation_results = {}
    all_passed = True
    
    # Check for missing values
    missing_values = df.isnull().sum().sum()
    validation_results['missing_values'] = missing_values
    if missing_values > 0:
        all_passed = False
    log(f"Missing values: {missing_values}")
    
    # Check for duplicate rows
    duplicates = df.duplicated().sum()
    validation_results['duplicates'] = duplicates
    if duplicates > 0:
        all_passed = False
    log(f"Duplicate rows: {duplicates}")
    
    # Validate score ranges (0-100)
    score_columns = ['math score', 'reading score', 'writing score']
    for col in score_columns:
        if col in df.columns:
            invalid_scores = ((df[col] < 0) | (df[col] > 100)).sum()
            validation_results[f'invalid_{col.replace(" ", "_")}'] = invalid_scores
            if invalid_scores > 0:
                all_passed = False
            log(f"Invalid {col} values: {invalid_scores}")
    
    validation_results['validation_passed'] = all_passed
    return validation_results

def transform_data(df):
    """Transform the dataset with necessary preprocessing steps."""
    log("Starting data transformation")
    
    # Create a copy to avoid modifying the original
    transformed_df = df.copy()
    
    # 1. Rename columns for consistency (remove spaces)
    transformed_df.columns = [col.replace(' ', "_") for col in transformed_df.columns]
    
    # 2. Create a total score column
    transformed_df['total_score'] = transformed_df['math_score'] + \
                                  transformed_df['reading_score'] + \
                                  transformed_df['writing_score']
    
    # 3. Create an average score column
    transformed_df['average_score'] = transformed_df['total_score'] / 3
    
    # 4. Create a performance category
    conditions = [
        (transformed_df['average_score'] >= 90),
        (transformed_df['average_score'] >= 75) & (transformed_df['average_score'] < 90),
        (transformed_df['average_score'] >= 65) & (transformed_df['average_score'] < 75),
        (transformed_df['average_score'] >= 50) & (transformed_df['average_score'] < 65),
        (transformed_df['average_score'] < 50)
    ]
    choices = ['Excellent', 'Good', 'Satisfactory', 'Needs Improvement', 'Failing']
    transformed_df['performance_category'] = np.select(conditions, choices, default='Unknown')
    
    log(f"Transformation complete. Shape: {transformed_df.shape}")
    return transformed_df

def visualize_data(df):
    """Create visualizations of the transformed data."""
    log("Creating visualizations")
    plt.figure(figsize=(12, 6))
    sns.countplot(data=df, x='performance_category', 
                  order=['Excellent', 'Good', 'Satisfactory', 'Needs Improvement', 'Failing'])
    plt.title('Distribution of Performance Categories')
    plt.xlabel('Performance Category')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def load_data(df, local_path):
    # Save locally
    df.to_csv(local_path, index=False)
    
    bucket_name = "studentperformance-gerard" 
    object_key = "data/student_performance.csv"
    s3_path = upload_to_s3(df, bucket_name, object_key)
    print(f"Data available in cloud at: {s3_path}")
    
    return local_path

def run_pipeline(input_path, output_path, visualize=True, to_s3=False, bucket_name=None, object_key=None):
    """Run the complete ETL pipeline."""
    log("Starting ETL pipeline")
    
    # Extract
    df = extract_data(input_path)
    
    # Validate
    validation_results = validate_data(df)
    if not validation_results['validation_passed']:
        log("Validation failed. See results above.")
        return validation_results
    
    # Transform
    transformed_df = transform_data(df)
    
    # Visualize (optional)
    if visualize:
        visualize_data(transformed_df)
    
    # Load
    load_data(transformed_df, output_path)
    

if __name__ == "__main__":
    input_path = "data/StudentsPerformance.csv"
    output_path = "data/student_performance_processed.csv"
    
    run_pipeline(input_path, output_path)

