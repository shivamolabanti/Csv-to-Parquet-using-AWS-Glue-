import sys
from awsglue import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def csv_to_dataframe(location):
    lines = sc.textFile(location)
    rdd = lines.map(lambda line: line.split(','))
    header = rdd.first()
    df = rdd.filter(lambda row: row != header).toDF(header)
    return df


args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

sample1 = csv_to_dataframe("s3://target-bucket-063/csv/sample1.csv")
sample2 = csv_to_dataframe("s3://target-bucket-063/csv/sample2.csv")
final_df = sample1.union(sample2).distinct()
final = DynamicFrame.fromDF(final_df, glueContext, "final_df")
glueContext.write_dynamic_frame.from_options(frame=final,
                                             connection_type="s3",
                                             connection_options={"path": "s3://target-bucket-063/job-output"},
                                             format="parquet")
