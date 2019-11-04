import os
import zipfile
import boto3
import shutil


class Functions:
    def __init__(self, region):
        self.client_s3 = boto3.client('s3', region_name=region)

    def upload_object(self, bucket_name, filename, location):
        self.client_s3.upload_file(filename, bucket_name, location)

    def upload_file_folder(self, bucket_name, folder_name):
        for file in os.listdir(folder_name):
            self.client_s3.upload_file(folder_name + '/' + file, bucket_name, file)

    def upload_zip_object(self, bucket_name, input_filename, output_filename, location):
        zip_file = zipfile.ZipFile(output_filename, "w")
        zip_file.write(input_filename, os.path.basename(input_filename))
        zip_file.close()
        self.upload_object(bucket_name, output_filename, location)
        os.remove(output_filename)

    def upload_zip_folder(self, folder_name, output_file, data_bucket):
        shutil.make_archive(output_file, 'zip', folder_name)
        self.upload_object(data_bucket, output_file + '.zip', output_file + '.zip')
        os.remove(output_file + '.zip')
