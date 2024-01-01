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

# Script generated for node step_trainer_landing
step_trainer_landing_node1703806216105 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1703806216105",
)

# Script generated for node customer_trusted
customer_trusted_node1703806218064 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1703806218064",
)

# Script generated for node SQL Query
SqlQuery2780 = """
select * from step_trainer_landing as s
join customer_trusted as c
on s.serialnumber = c.serialnumber;
"""
SQLQuery_node1703806221033 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2780,
    mapping={
        "customer_trusted": customer_trusted_node1703806218064,
        "step_trainer_landing": step_trainer_landing_node1703806216105,
    },
    transformation_ctx="SQLQuery_node1703806221033",
)

# Script generated for node Drop Fields
DropFields_node1703806230359 = DropFields.apply(
    frame=SQLQuery_node1703806221033,
    paths=[],
    transformation_ctx="DropFields_node1703806230359",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1703806516686 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703806230359,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-project-data-lake/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1703806516686",
)

job.commit()
