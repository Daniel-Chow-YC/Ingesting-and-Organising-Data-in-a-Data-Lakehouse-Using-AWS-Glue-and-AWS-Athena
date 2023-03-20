import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

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
        "paths": ["s3://dcyc-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node2 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node2",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node3 = glueContext.getSink(
    path="s3://dcyc-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node3",
)
TrustedCustomerZone_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node3.setFormat("json")
TrustedCustomerZone_node3.writeFrame(PrivacyFilter_node2)
job.commit()
