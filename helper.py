import boto3
from botocore.client import ClientError
import time
import utils as u
param = {
    "data-bucket" : "data-suddu",
    "stack-name": "week2",
    "s3-bucket": "crawlertarget",
    "crawler-1": "CSVCrawler",
    "crawler-2": "ParquetCrawler",
    "job-name": "job"
}
s3 = boto3.resource('s3')
try:
    s3.create_bucket(Bucket=param["data-bucket"], CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
except ClientError:
    print("Data Bucket Already Created")
u.upload_object(param["data-bucket"], 'template.yaml', 'template.yaml')
u.upload_object(param["data-bucket"], 'job.py', 'job_scripts/job.py')
client = boto3.client('cloudformation')
status = u.status_stack(param["stack-name"])
if status == 'ROLLBACK_COMPLETE' or status == 'ROLLBACK_FAILED' or status == 'UPDATE_ROLLBACK_COMPLETE' or status == \
        'DELETE_FAILED':
    u.delete_object(param["s3-bucket"])
    client.delete_stack(StackName=param["stack-name"])
    print("deleting stack")
    while u.status_stack(param["stack-name"]) == 'DELETE_IN_PROGRESS':
        time.sleep(1)
    print("stack deleted")
    u.create_stack(param["stack-name"], 'https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml')
    print("creating stack")
elif status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
    u.update_stack(param["stack-name"], 'https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml')
else:
    u.create_stack(param["stack-name"], 'https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml')
    print("creating stack")
while u.status_stack(param["stack-name"]) == 'CREATE_IN_PROGRESS' or u.status_stack(param["stack-name"]) == 'UPDATE_IN_PROGRESS' \
        or u.status_stack(param["stack-name"]) == 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS':
    time.sleep(1)
status = u.status_stack(param["stack-name"])
print("stack created")
if status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
    u.upload_object(param["s3-bucket"], 'Sample Data/sample1.csv', 'csv/sample1.csv')
    u.upload_object(param["s3-bucket"], 'Sample Data/sample2.csv', 'csv/sample2.csv')
    u.upload_object(param["s3-bucket"], 'Sample Data/new1.csv', 'csv/new1.csv')
client_glue = boto3.client('glue')
client_glue.start_crawler(Name=param["crawler-1"])
print("csv crawler started")
while u.crawler_status(param["crawler-1"]) != 'READY':
    time.sleep(2)
print("job started")
#delteing all object from output folder
s3 = boto3.resource('s3')
bucket = s3.Bucket(param["s3-bucket"])
bucket.objects.filter(Prefix="job-output/").delete()

response = client_glue.start_job_run(JobName=param["job-name"])
while u.job_status(param["job-name"], response['JobRunId']) == 'STARTING' or u.job_status(param["job-name"], response['JobRunId']) == \
        'RUNNING' or u.job_status(param["job-name"], response['JobRunId']) == 'STOPPING':
    time.sleep(2)
if u.job_status(param["job-name"], response['JobRunId']) == 'FAILED':
    print("job failed")
    exit()
print("job completed")
client_glue.start_crawler(Name=param["crawler-2"])
print("parquet crawler started")
while u.crawler_status(param["crawler-2"]) != 'READY':
    time.sleep(2)
print("crawler completed")
print("table created with parquet file")