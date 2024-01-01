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

# Script generated for node customer_landing
customer_landing_node1703802946982 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-data-lake/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1703802946982",
)

# Script generated for node SQL Query
SqlQuery2819 = """
select * from myDataSource
where sharewithresearchasofdate != 0;

"""
SQLQuery_node1703802963083 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2819,
    mapping={"myDataSource": customer_landing_node1703802946982},
    transformation_ctx="SQLQuery_node1703802963083",
)

# Script generated for node Amazon S3
AmazonS3_node1703802969698 = glueContext.getSink(
    path="s3://udacity-project-data-lake/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703802969698",
)
AmazonS3_node1703802969698.setCatalogInfo(
    catalogDatabase="database", catalogTableName="Customer_trusted"
)
AmazonS3_node1703802969698.setFormat("json")
AmazonS3_node1703802969698.writeFrame(SQLQuery_node1703802963083)
job.commit()
