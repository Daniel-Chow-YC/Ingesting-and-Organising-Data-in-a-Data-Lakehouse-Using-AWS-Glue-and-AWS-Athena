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

# Script generated for node accelerometer trusted
accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1679096733200 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1679096733200",
)

# Script generated for node ApplyJoin
ApplyJoin_node2 = Join.apply(
    frame1=accelerometertrusted_node1,
    frame2=step_trainer_trusted_node1679096733200,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="ApplyJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1679097637218 = DropFields.apply(
    frame=ApplyJoin_node2,
    paths=["user", "timestamp"],
    transformation_ctx="DropFields_node1679097637218",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://dcyc-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1679097637218)
job.commit()
