# Csv to Parquet using AWS Glue&nbsp;
This project performs a ETL task on CSV files present in a s3 bucket, join the CSV file by common column and convert them to parquet file, write the parquet file to a s3 bucket and convert parquet file to table for running queries.

# Files
- **helper.py** - This file has the main function of the project. Flow of main the function -
    - Definition of all the parameter used in the project.
    - Upload the template and job scripts to s3 bucket.
    - Creating or updating stack.
    - Uploading the sample data to s3.
    - Running the job.
    - Running the crawler.
- **stack.py** - This file contains a Stack class which handles all the stack functions like create, delete and  update a stack using boto3.&nbsp;
- **Glue.py** - This file contains a Glue class which handles all the glue function like stating a job and starting a crawler.
- **function.py** - Comprises of upload functions like upload a folder, file or zip file to s3.
- **template.ymal** - A cloudforamtion template which comprises of configuration of database, job, crawler, s3bucket, glue role.
- **Job.py** - Script for glue job which converts the sample CSV data to spark dataframe and then convert them to parquet files.

# Output
A table is created in Glue Database. SQL query can be run on the table using AWS Athena.

