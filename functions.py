import os
import zipfile
import boto3

client = boto3.client('cloudformation')
s3 = boto3.resource('s3')
client_glue = boto3.client('glue')


def upload_object(bucket_name, filename, location):
    s3.Object(bucket_name, location).upload_file(Filename=filename)


def upload_file_folder(bucket_name, folder_name):
    for file in os.listdir(folder_name):
        s3.Object(bucket_name, file).upload_file(Filename=folder_name + '/' + file)


def upload_zip_object(bucket_name, input_filename, output_filename, location):
    zip = zipfile.ZipFile(output_filename, "w")
    zip.write(input_filename, os.path.basename(input_filename))
    zip.close()
    print(output_filename)
    upload_object(bucket_name, output_filename, location)
    os.remove(output_filename)