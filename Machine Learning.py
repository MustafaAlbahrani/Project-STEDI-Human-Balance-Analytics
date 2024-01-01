import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1703811284790 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1703811284790",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1703811285457 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1703811285457",
)

# Script generated for node SQL Query
SqlQuery2443 = """
select * from accelerometer_trusted
join step_trainer_trusted
on timestamp = sensorreadingtime;
"""
SQLQuery_node1703811288421 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2443,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1703811285457,
        "step_trainer_trusted": step_trainer_trusted_node1703811284790,
    },
    transformation_ctx="SQLQuery_node1703811288421",
)

# Script generated for node Drop Fields
DropFields_node1704123636812 = DropFields.apply(
    frame=SQLQuery_node1703811288421,
    paths=[
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "lastUpdateDate",
        "registrationDate",
        "birthDay",
        "phone",
        "email",
        "customerName",
        "timestamp",
    ],
    transformation_ctx="DropFields_node1704123636812",
)

# Script generated for node Amazon S3
AmazonS3_node1703811297459 = glueContext.getSink(
    path="s3://udacity-project-data-lake/M_L_Test/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703811297459",
)
AmazonS3_node1703811297459.setCatalogInfo(
    catalogDatabase="database", catalogTableName="Machine Learing"
)
AmazonS3_node1703811297459.setFormat("json")
AmazonS3_node1703811297459.writeFrame(DropFields_node1704123636812)
job.commit()
