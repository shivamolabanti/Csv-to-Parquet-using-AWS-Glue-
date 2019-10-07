import boto3
import zipfile, os

client = boto3.client('cloudformation')
s3 = boto3.resource('s3')
client_glue = boto3.client('glue')


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


def upload_object(bucket_name, filename, location):
    s3.Object(bucket_name, location).upload_file(Filename=filename)


def upload_zip_object(bucket_name, input_filename, output_filename, location):
    zip = zipfile.ZipFile(output_filename, "w")
    zip.write(input_filename, os.path.basename(input_filename))
    zip.close()
    print(output_filename)
    upload_object(bucket_name, output_filename, location)
    os.remove(output_filename)


def crawler_status(crawler_name):
    response = client_glue.get_crawler(Name=crawler_name)
    return response['Crawler']['State']
