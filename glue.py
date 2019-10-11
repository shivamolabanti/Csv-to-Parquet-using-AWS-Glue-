import boto3
import time

s3 = boto3.resource('s3')
client_glue = boto3.client('glue')


class Glue:
    def __init__(self, job_name, crawler_name):
        self.job_name = job_name
        self.crawler_name = crawler_name

    def start_job(self):
        response = client_glue.start_job_run(JobName=self.job_name)
        print("job started")
        while self.job_status(response['JobRunId']) == 'STARTING' or \
                self.job_status(response['JobRunId']) == 'RUNNING' or \
                self.job_status(response['JobRunId']) == 'STOPPING':
            time.sleep(2)
        if self.job_status(response['JobRunId']) == 'FAILED':
            print("job failed")
            exit()
        print("job completed")

    def start_crawler(self):
        print("crawler started")
        client_glue.start_crawler(Name=self.crawler_name)
        print("parquet crawler started")
        while self.crawler_status() != 'READY':
            time.sleep(2)
        print("crawler completed")

    def job_status(self, run_id):
        response = client_glue.get_job_run(JobName=self.job_name, RunId=run_id)
        return response['JobRun']['JobRunState']

    def crawler_status(self):
        response = client_glue.get_crawler(Name=self.crawler_name)
        return response['Crawler']['State']
