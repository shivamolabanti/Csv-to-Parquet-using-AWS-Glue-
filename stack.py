import boto3
import time

client = boto3.client('cloudformation')
s3 = boto3.resource('s3')
client_glue = boto3.client('glue')


def create_update_stack(parameter):
    status = status_stack(parameter["stack-name"])
    if status == 'ROLLBACK_COMPLETE' or status == 'ROLLBACK_FAILED' or status == 'UPDATE_ROLLBACK_COMPLETE' or \
            status == 'DELETE_FAILED':
        delete_object(parameter["s3-bucket"])
        client.delete_stack(StackName=parameter["stack-name"])
        print("deleting stack")
        while status_stack(parameter["stack-name"]) == 'DELETE_IN_PROGRESS':
            time.sleep(1)
        print("stack deleted")
        create_stack(parameter["stack-name"], parameter["template_url"])
        print("creating stack")
    elif status == 'CREATE_COMPLETE' or status == 'UPDATE_COMPLETE':
        update_stack(parameter["stack-name"], parameter["template_url"])
    else:
        create_stack(parameter["stack-name"], parameter["template_url"])
        print("creating stack")
    while status_stack(parameter["stack-name"]) == 'CREATE_IN_PROGRESS' or \
            status_stack(parameter["stack-name"]) == 'UPDATE_IN_PROGRESS' or \
            status_stack(parameter["stack-name"]) == 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS':
        time.sleep(1)
    print("stack created")
    return status_stack(parameter["stack-name"])


def create_stack(stack_name, template_url):
    response = client.create_stack(
        StackName=stack_name,
        TemplateURL=template_url,
        Capabilities=['CAPABILITY_NAMED_IAM']
    )


def update_stack(stack_name, template_url):
    try:
        print("updating stack")
        response = client.update_stack(
            StackName=stack_name,
            TemplateURL=template_url,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
    except Exception:
        print("stack already updated")


def status_stack(stack_name):
    try:
        stack = client.describe_stacks(StackName=stack_name)
        status = stack['Stacks'][0]['StackStatus']
        return status
    except Exception:
        return "NO_STACK"


def delete_object(bucket_name):
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
    except Exception:
        print("Bucket Not Present")
