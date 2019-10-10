import boto3
from botocore.client import ClientError
import time
from stack import create_update_stack
import functions as function

parameter = {
    "data-bucket": "data-suddu",
    "stack-name": "week2",
    "s3-bucket": "crawlertarget",
    "CSVCrawler": "CSVCrawler",
    "ParquetCrawler": "ParquetCrawler",
    "job-name": "job",
    "template_url": "https://data-suds3.ap-south-1.amazonaws.com/template.yaml"
}
s3 = boto3.resource('s3')
try:
    s3.create_bucket(Bucket=parameter["data-bucket"], CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
except ClientError:
    print("Data Bucket Already Created")

# uploading templates and job file to S3

function.upload_file_folder(parameter["data-bucket"], "Template")
function.upload_object(parameter["data-bucket"], 'job.py', 'job_scripts/job.py')
status = create_update_stack(parameter)

# uploading the sample files to S3

if status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
    function.upload_object(parameter["s3-bucket"], 'Sample Data/sample1.csv', 'csv/sample1.csv')
    function.upload_object(parameter["s3-bucket"], 'Sample Data/sample2.csv', 'csv/sample2.csv')
    function.upload_object(parameter["s3-bucket"], 'Sample Data/new1.csv', 'csv/new1.csv')

# csv crawler

client_glue = boto3.client('glue')
client_glue.start_crawler(Name=parameter["CSVCrawler"])
print("csv crawler started")
while function.crawler_status(parameter["CSVCrawler"]) != 'READY':
    time.sleep(2)
print("csv crawler completed")

# job - converting sample to parquet file and saving it to S3

print("job started")
s3 = boto3.resource('s3')
bucket = s3.Bucket(parameter["s3-bucket"])
bucket.objects.filter(Prefix="job-output/").delete()
response = client_glue.start_job_run(JobName=parameter["job-name"])
while function.job_status(parameter["job-name"], response['JobRunId']) == 'STARTING' or\
        function.job_status(parameter["job-name"], response['JobRunId']) == 'RUNNING' or \
        function.job_status(parameter["job-name"], response['JobRunId']) == 'STOPPING':
    time.sleep(2)
if function.job_status(parameter["job-name"], response['JobRunId']) == 'FAILED':
    print("job failed")
    exit()
print("job completed")

# parquet crawler - crawling all parquet file

client_glue.start_crawler(Name=parameter["ParquetCrawler"])
print("parquet crawler started")
while function.crawler_status(parameter["ParquetCrawler"]) != 'READY':
    time.sleep(2)
print("crawler completed")
print("table created with parquet file")
