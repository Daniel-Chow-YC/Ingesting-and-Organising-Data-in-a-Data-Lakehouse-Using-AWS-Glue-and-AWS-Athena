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

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="customer_curated_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1679096733200 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1679096733200",
)

# Script generated for node Renamed serial number for customer curated
Renamedserialnumberforcustomercurated_node1679097718122 = RenameField.apply(
    frame=customer_curated_node1,
    old_name="serialnumber",
    new_name="customer_serialnumber",
    transformation_ctx="Renamedserialnumberforcustomercurated_node1679097718122",
)

# Script generated for node ApplyJoin
ApplyJoin_node2 = Join.apply(
    frame1=Renamedserialnumberforcustomercurated_node1679097718122,
    frame2=step_trainer_landing_node1679096733200,
    keys1=["customer_serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="ApplyJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1679097637218 = DropFields.apply(
    frame=ApplyJoin_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "timestamp",
        "customer_serialnumber",
    ],
    transformation_ctx="DropFields_node1679097637218",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://dcyc-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1679097637218)
job.commit()
