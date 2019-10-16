import os
import zipfile
import boto3

s3 = boto3.client('s3', region_name='ap-south-1')


def upload_object(bucket_name, filename, location):
    s3.upload_file(filename, bucket_name, location)


def upload_file_folder(bucket_name, folder_name):
    for file in os.listdir(folder_name):
        s3.upload_file(folder_name + '/' + file, bucket_name, file)


def upload_zip_object(bucket_name, input_filename, output_filename, location):
    zip = zipfile.ZipFile(output_filename, "w")
    zip.write(input_filename, os.path.basename(input_filename))
    zip.close()
    print(output_filename)
    upload_object(bucket_name, output_filename, location)
    os.remove(output_filename)