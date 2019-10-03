import boto3
client = boto3.client('cloudformation')
def create_stack():
    response = client.create_stack(
        StackName='week2',
        TemplateURL='https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml',
        Capabilities=['CAPABILITY_NAMED_IAM']
    )
def update_stack():
    response = client.update_stack(
        StackName='week2',
        TemplateURL='https://data-suddu.s3.ap-south-1.amazonaws.com/template.yaml',
        Capabilities=['CAPABILITY_NAMED_IAM']
    )