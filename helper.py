import boto3
from botocore.client import ClientError
import zipfile,os
import time
import stack as s
s3 = boto3.resource('s3')
try:
    s3.create_bucket(Bucket='data-suddu', CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
except ClientError:
    print("Data Bucket Already Created")
s3.Object('data-suddu', 'template.yaml').upload_file(Filename='template.yaml')
client = boto3.client('cloudformation')
try:
    stack = client.describe_stacks(StackName='week2')
    if stack['Stacks'][0]['StackStatus'] == 'ROLLBACK_FAILED' or stack['Stacks'][0]['StackStatus'] == 'ROLLBACK_COMPLETE':
        try:
            bucket = s3.Bucket('CrawlerTarget')
            bucket.objects.all().delete()
        except Exception:
            print("Bucket Deleted")
        response = client.delete_stack(StackName='week2')
        while stack['Stacks'][0]['StackStatus'] == 'DELETE_IN_PROGRESS':
            try:
                stack = client.describe_stacks(StackName='stack')
            except Exception:
                time.sleep(1)
                print("Stack Deleted")
                break
        s.create_stack()
        print("Stack Created")
    else:
        try:
            s.update_stack()
            print("Stack Updated")
        except ClientError:
            print("No updates are to be performed")
except Exception:
    s.create_stack()
    print("Stack Created")
