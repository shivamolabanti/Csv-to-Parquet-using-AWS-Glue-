import boto3
import stack
import functions
import glue
from botocore.client import ClientError

DATA_BUCKET = "data-bucket-063"
DATABASE_NAME = "parquet_database"
STACK_NAME = "week2"
BUCKET_NAME = "target-bucket-063"
CRAWLER_NAME = "parquet_crawler"
JOB_NAME = "csv_parquet_job"
TEMPLATE_URL = "https://data-bucket-063.s3.ap-south-1.amazonaws.com/template.yaml"
REGION = "ap-south-1"


# uploading templates and job file to S3

def upload_template_python_script():
    s3 = boto3.resource('s3', region_name=REGION)
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
    Stack = stack.Stack(STACK_NAME, TEMPLATE_URL, DATABASE_NAME, BUCKET_NAME, CRAWLER_NAME, JOB_NAME, DATA_BUCKET)
    Stack.create_update_stack()

    # uploading the sample files to S3

    functions.upload_object(BUCKET_NAME, 'Sample Data/sample1.csv', 'csv/sample1.csv')
    functions.upload_object(BUCKET_NAME, 'Sample Data/sample2.csv', 'csv/sample2.csv')

    # job - reads csv from s3 and convert it to parquet
    # crawler - crawl parquet file in s3

    Glue = glue.Glue(JOB_NAME, CRAWLER_NAME)
    Glue.start_job()
    Glue.start_crawler()
    print("table created with parquet file")
