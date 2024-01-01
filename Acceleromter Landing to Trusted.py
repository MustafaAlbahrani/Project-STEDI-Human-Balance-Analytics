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

# Script generated for node accelerometer_landing
accelerometer_landing_node1703804766206 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1703804766206",
)

# Script generated for node customer_trusted
customer_trusted_node1703804764513 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1703804764513",
)

# Script generated for node SQL Query
SqlQuery2444 = """
select * from customer_landing
join accelerometer_landing
on email = user;

"""
SQLQuery_node1703804788363 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2444,
    mapping={
        "customer_landing": customer_trusted_node1703804764513,
        "accelerometer_landing": accelerometer_landing_node1703804766206,
    },
    transformation_ctx="SQLQuery_node1703804788363",
)

# Script generated for node Drop Fields
DropFields_node1703804796036 = DropFields.apply(
    frame=SQLQuery_node1703804788363,
    paths=[
        "shareWithPublicAsOfDate",
        "birthDay",
        "phone",
        "customerName",
        "email",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1703804796036",
)

# Script generated for node Amazon S3
AmazonS3_node1703804800196 = glueContext.getSink(
    path="s3://udacity-project-data-lake/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703804800196",
)
AmazonS3_node1703804800196.setCatalogInfo(
    catalogDatabase="database", catalogTableName="accelerometer_landing"
)
AmazonS3_node1703804800196.setFormat("json")
AmazonS3_node1703804800196.writeFrame(DropFields_node1703804796036)
job.commit()
