import boto3
import stack
import functions
import glue
from botocore.client import ClientError

DATA_BUCKET = "data-bucket-063"
DATA_BASE_NAME = "parquet_database"
STACK_NAME = "week2"
BUCKET_NAME = "target-bucket-063"
CRAWLER_NAME = "parquet_crawler"
JOB_NAME = "csv_parquet_job"
TEMPLATE_URL = "https://data-bucket-063.s3.ap-south-1.amazonaws.com/template.yaml"

parameter = {
    "data_bucket": DATA_BUCKET,
    "database_name": DATA_BASE_NAME,
    "stack_name": STACK_NAME,
    "bucket_name": BUCKET_NAME,
    "crawler_name": CRAWLER_NAME,
    "job_name": JOB_NAME,
    "template_url": TEMPLATE_URL
}


# uploading templates and job file to S3

def upload_template_python_script():
    s3 = boto3.resource('s3')
    try:
        s3.create_bucket(Bucket=DATA_BUCKET, CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print("Data Bucket Already Created")
        else:
            print(ce)
    except Exception as e:
        print(e)
        exit()
    functions.upload_file_folder(DATA_BUCKET, "Template")
    functions.upload_object(DATA_BUCKET, 'job.py', 'job_scripts/job.py')


if __name__ == "__main__":
    upload_template_python_script()
    Stack = stack.Stack(parameter)
    status = Stack.create_update_stack()

    # uploading the sample files to S3

    if status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
        functions.upload_object(BUCKET_NAME, 'Sample Data/sample1.csv', 'csv/sample1.csv')
        functions.upload_object(BUCKET_NAME, 'Sample Data/sample2.csv', 'csv/sample2.csv')
    else:
        print("stack creation failed")
        exit()
    
    # job - reads csv from s3 and convert it to parquet
    # crawler - crawl parquet file in s3
    
    Glue = glue.Glue(JOB_NAME, CRAWLER_NAME)
    Glue.start_job()
    Glue.start_crawler()
    print("table created with parquet file")
