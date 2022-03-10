import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3

AWS_REGION = "us-east-1"

sns_client = boto3.client("sns", region_name=AWS_REGION)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#create DynamicFame from S3 parquet files
datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": ['s3://hpy-example/raw/userdata1.parquet',
                            's3://hpy-example/raw/userdata2.parquet']
            },
            format="parquet",
            transformation_ctx="datasource0")

# df1 = datasource0.toDF()


S3_location = "s3://hpy-example/curated"
datasink = glueContext.write_dynamic_frame_from_options(
                                                    frame= datasource0,
                                                    connection_type="s3",
                                                    connection_options={
                                                    "path": S3_location
                                                    },
                                                    format="json",
                                                    transformation_ctx ="datasink")
                
topic_arn = 'arn:aws:sns:us-east-1:491534477676:MyExampleTopic'
message = 'This is a test message on topic.'
subject = 'This is a message subject on topic.'                                    
response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject,
        )['MessageId']

job.commit()