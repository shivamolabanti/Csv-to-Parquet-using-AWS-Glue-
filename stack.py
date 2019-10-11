import boto3
import time
from botocore.client import ClientError

client = boto3.client('cloudformation')
s3 = boto3.resource('s3')
client_glue = boto3.client('glue')


class Stack:
    def __init__(self, parameter):
        self.parameter = parameter

    # if stack is in rollback stage then stack get deleted and then it gets created.
    # if stack is in create stage then it gets updated

    def create_update_stack(self):
        status = self.status_stack()
        if status == 'ROLLBACK_COMPLETE' or status == 'ROLLBACK_FAILED' or status == 'UPDATE_ROLLBACK_COMPLETE' or \
                status == 'DELETE_FAILED':
            self.delete_object()
            client.delete_stack(StackName=self.parameter["stack_name"])
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
        print("stack created")
        return self.status_stack()

    def create_stack(self):
        try:
            client.create_stack(
                StackName=self.parameter["stack_name"],
                TemplateURL=self.parameter["template_url"],
                Capabilities=['CAPABILITY_NAMED_IAM'],
                Parameters=[
                    {
                        'ParameterKey': "DataBaseName",
                        'ParameterValue': self.parameter["database_name"]
                    },
                    {
                        'ParameterKey': "BucketName",
                        'ParameterValue': self.parameter["bucket_name"]
                    },
                    {
                        'ParameterKey': "CrawlerName",
                        'ParameterValue': self.parameter["crawler_name"]
                    },
                    {
                        'ParameterKey': "JobName",
                        'ParameterValue': self.parameter["job_name"]
                    },
                    {
                        'ParameterKey': "DataBucket",
                        'ParameterValue': self.parameter["data_bucket"]
                    }
                ]
            )
        except ClientError as ce:
            print(ce)
            exit()

    def update_stack(self):
        try:
            client.update_stack(
                StackName=self.parameter["stack_name"],
                TemplateURL=self.parameter["template_url"],
                Capabilities=['CAPABILITY_NAMED_IAM'],
                Parameters=[
                    {
                        'ParameterKey': "DataBaseName",
                        'ParameterValue': self.parameter["database_name"]
                    },
                    {
                        'ParameterKey': "BucketName",
                        'ParameterValue': self.parameter["bucket_name"]
                    },
                    {
                        'ParameterKey': "CrawlerName",
                        'ParameterValue': self.parameter["crawler_name"]
                    },
                    {
                        'ParameterKey': "JobName",
                        'ParameterValue': self.parameter["job_name"]
                    },
                    {
                        'ParameterKey': "DataBucket",
                        'ParameterValue': self.parameter["data_bucket"]
                    }
                ]
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ValidationError':
                print("Stack Already Updated")
            else:
                print(ce)
                exit()

    def status_stack(self):
        try:
            stack = client.describe_stacks(StackName=self.parameter["stack_name"])
            status = stack['Stacks'][0]['StackStatus']
            return status
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ValidationError':
                print("No stack present")
            else:
                print(ce)
                exit()

    def delete_object(self):
        try:
            bucket = s3.Bucket(self.parameter["bucket_name"])
            bucket.objects.all().delete()
        except ClientError as ce:
            print(ce)
            exit()
