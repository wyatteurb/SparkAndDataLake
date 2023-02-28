import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glueytestb/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1677537070595 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glueytestb/accelerometer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677537070595",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1677537117206 = DynamicFrame.fromDF(
    S3bucket_node1.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DropDuplicates_node1677537117206",
)

# Script generated for node Join
Join_node1677537153994 = Join.apply(
    frame1=AmazonS3_node1677537070595,
    frame2=DropDuplicates_node1677537117206,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1677537153994",
)

# Script generated for node Drop Fields
DropFields_node1677538522814 = DropFields.apply(
    frame=Join_node1677537153994,
    paths=["z", "y", "x"],
    transformation_ctx="DropFields_node1677538522814",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677538522814,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://glueytestb/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
