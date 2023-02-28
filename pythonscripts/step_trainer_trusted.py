import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
        "paths": ["s3://glueytestb/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1677539012961 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glueytestb/customers/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677539012961",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1677539012961,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1677543434117 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1677543434117",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677543434117,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://glueytestb/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
