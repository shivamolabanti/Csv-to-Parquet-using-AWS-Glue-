import sys
from awsglue import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

sample1 = glueContext.create_dynamic_frame.from_catalog(database="dbcrawler", table_name="sample1_csv")
sample2 = glueContext.create_dynamic_frame.from_catalog(database="dbcrawler", table_name="sample2_csv")
sample1_df = sample1.toDF()
sample2_df = sample2.toDF()
final_df = sample1_df.union(sample2_df).distinct()
final = DynamicFrame.fromDF(final_df, glueContext, "final_df")
glueContext.write_dynamic_frame.from_options(frame=final,
                                             connection_type="s3",
                                             connection_options={"path": "s3://data-suddu/job-output"},
                                             format="parquet")
