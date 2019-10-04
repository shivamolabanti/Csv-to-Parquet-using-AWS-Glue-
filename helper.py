import boto3
from botocore.client import ClientError
import zipfile,os
import time
import utils as u
s3 = boto3.resource('s3')
try:
    s3.create_bucket(Bucket='data-suddu', CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
except ClientError:
    print("Data Bucket Already Created")
u.upload_object('data-suddu', 'template.yaml', 'template.yaml')
u.upload_object('data-suddu', 'job.py', 'job_scripts/job.py')
client = boto3.client('cloudformation')
status = u.status_stack('week2')
if status == 'ROLLBACK_COMPLETE' or status == 'ROLLBACK_FAILED':
    u.delete_object('CrawlerTarget')
    client.delete_stack(StackName='week2')
    while u.status_stack('week2') == 'DELETE_IN_PROGRESS':
        time.sleep(1)
    print("stack deleted")
    u.create_stack('week2', 'https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml')
    print("creating stack")
elif status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
    u.update_stack('week2', 'https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml')
    print("updating stack")
else:
    u.create_stack('week2', 'https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml')
    print("creating stack")
while u.status_stack('week2') == 'CREATE_IN_PROGRESS' or u.status_stack('week2') == 'UPDATE_IN_PROGRESS' \
        or u.status_stack('week2') == 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS':
    time.sleep(1)
status = u.status_stack('week2')
print("stack created")
if status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
    u.upload_object('crawlertarget', 'Sample Data/sample1.csv', 'csv/sample1.csv')
    u.upload_object('crawlertarget', 'Sample Data/sample2.csv', 'csv/sample2.csv')
client_glue = boto3.client('glue')
client_glue.start_job_run(
    JobName='cf-job',)
