import boto3
import stack
import functions
import glue
from botocore.client import ClientError

REGION = "ap-south-1"
DATA_BUCKET = "data-bucket-063"
DATABASE_NAME = "parquet_database"
STACK_NAME = "week2"
BUCKET_NAME = "target-bucket-063"
CRAWLER_NAME = "parquet_crawler"
JOB_NAME = "csv_parquet_job"
TEMPLATE_URL = "https://" + DATA_BUCKET + ".s3." + REGION + ".amazonaws.com/template.yaml"


# uploading templates and job file to S3

def upload_template_job_script():
    s3_client = boto3.client('s3', region_name=REGION)
    try:
        s3_client.create_bucket(Bucket=DATA_BUCKET, CreateBucketConfiguration={'LocationConstraint': REGION})
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print("Data Bucket Already Created")
        else:
            print(ce)
    except Exception as e:
        print(e)
        exit()
    functions_ob = functions.Functions(REGION)
    functions_ob.upload_file_folder(DATA_BUCKET, "Template")
    functions_ob.upload_object(DATA_BUCKET, 'job.py', 'job_scripts/job.py')


if __name__ == "__main__":
    upload_template_job_script()
    Stack = stack.Stack(STACK_NAME, TEMPLATE_URL, DATABASE_NAME, BUCKET_NAME, CRAWLER_NAME, JOB_NAME, DATA_BUCKET)
    if Stack.create_update_stack() == 0:

        # uploading the sample files to S3

        functions_obj = functions.Functions(REGION)
        functions_obj.upload_object(BUCKET_NAME, 'Sample Data/sample1.csv', 'csv/sample1.csv')
        functions_obj.upload_object(BUCKET_NAME, 'Sample Data/sample2.csv', 'csv/sample2.csv')

        # job - reads csv from s3 and convert it to parquet
        # crawler - crawl parquet file in s3

        Glue = glue.Glue(JOB_NAME, CRAWLER_NAME, REGION)
        Glue.start_job()
        Glue.start_crawler()
        print("table created with parquet file")
