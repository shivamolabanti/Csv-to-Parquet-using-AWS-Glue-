import boto3
import time
from botocore.client import ClientError

client = boto3.client('cloudformation', region_name='ap-south-1')
client_s3 = boto3.client('s3', region_name='ap-south-1')


class Stack:
    def __init__(self, stack_name, template_url, database_name, bucket_name, crawler_name, job_name, data_bucket):
        self.stack_name = stack_name
        self.template_url = template_url
        self.database_name = database_name
        self.bucket_name = bucket_name
        self.crawler_name = crawler_name
        self.job_name = job_name
        self.data_bucket = data_bucket

    # if stack is in rollback stage then stack get deleted and then it gets created.
    # if stack is in create stage then it gets updated

    def create_update_stack(self):
        status = self.status_stack()
        if status == 'ROLLBACK_COMPLETE' or status == 'ROLLBACK_FAILED' or status == 'UPDATE_ROLLBACK_COMPLETE' or \
                status == 'DELETE_FAILED':
            self.delete_object()
            client.delete_stack(StackName=self.stack_name)
            print("deleting stack")
            while self.status_stack() == 'DELETE_IN_PROGRESS':
                time.sleep(2)
            print("stack deleted")
            self.create_stack()
            print("creating stack")
        elif status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
            self.update_stack()
            print("updating stack")
        else:
            self.create_stack()
            print("creating stack")
        while self.status_stack() == 'CREATE_IN_PROGRESS' or \
                self.status_stack() == 'UPDATE_IN_PROGRESS' or \
                self.status_stack() == 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS':
            time.sleep(2)
        if self.status_stack() == 'CREATE_COMPLETE' or self.status_stack() == 'UPDATE_COMPLETE':
            print("stack created")
            return 0
        else:
            print("stack creation failed")
            return 1

    def create_stack(self):
        try:
            client.create_stack(
                StackName=self.stack_name,
                TemplateURL=self.template_url,
                Capabilities=['CAPABILITY_NAMED_IAM'],
                Parameters=[
                    {
                        'ParameterKey': "DataBaseName",
                        'ParameterValue': self.database_name
                    },
                    {
                        'ParameterKey': "BucketName",
                        'ParameterValue': self.bucket_name
                    },
                    {
                        'ParameterKey': "CrawlerName",
                        'ParameterValue': self.crawler_name
                    },
                    {
                        'ParameterKey': "JobName",
                        'ParameterValue': self.job_name
                    },
                    {
                        'ParameterKey': "DataBucket",
                        'ParameterValue': self.data_bucket
                    }
                ]
            )
        except ClientError as ce:
            print(ce)

    def update_stack(self):
        try:
            client.update_stack(
                StackName=self.stack_name,
                TemplateURL=self.template_url,
                Capabilities=['CAPABILITY_NAMED_IAM'],
                Parameters=[
                    {
                        'ParameterKey': "DataBaseName",
                        'ParameterValue': self.database_name
                    },
                    {
                        'ParameterKey': "BucketName",
                        'ParameterValue': self.bucket_name
                    },
                    {
                        'ParameterKey': "CrawlerName",
                        'ParameterValue': self.crawler_name
                    },
                    {
                        'ParameterKey': "JobName",
                        'ParameterValue': self.job_name
                    },
                    {
                        'ParameterKey': "DataBucket",
                        'ParameterValue': self.data_bucket
                    }
                ]
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ValidationError':
                print("Stack Already Updated")
            else:
                print(ce)

    def status_stack(self):
        try:
            stack = client.describe_stacks(StackName=self.stack_name)
            status = stack['Stacks'][0]['StackStatus']
            return status
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ValidationError':
                print("No stack present")
            else:
                print(ce)

    def delete_object(self):
        try:
            res = client_s3.list_objects(Bucket=self.bucket_name)
            for list_key in res['Contents']:
                client_s3.delete_object(Bucket=self.bucket_name, Key=list_key['key'])
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'NoSuchBucket':
                print(ce)
            else:
                print(ce)
