import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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
accelerometer_landing_node1703807704857 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1703807704857",
)

# Script generated for node customer_trusted
customer_trusted_node1703807702313 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1703807702313",
)

# Script generated for node SQL Query
SqlQuery2402 = """
select * from accelerometer_landing
join customer_trusted
on email = user

"""
SQLQuery_node1703807712055 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2402,
    mapping={
        "accelerometer_landing": accelerometer_landing_node1703807704857,
        "customer_trusted": customer_trusted_node1703807702313,
    },
    transformation_ctx="SQLQuery_node1703807712055",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1703807724386 = DynamicFrame.fromDF(
    SQLQuery_node1703807712055.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1703807724386",
)

# Script generated for node Drop Fields
DropFields_node1703807733818 = DropFields.apply(
    frame=DropDuplicates_node1703807724386,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1703807733818",
)

# Script generated for node Amazon S3
AmazonS3_node1703807741483 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703807733818,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-project-data-lake/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1703807741483",
)

job.commit()
