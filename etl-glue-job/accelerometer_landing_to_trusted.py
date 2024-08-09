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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1723177756499 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1723177756499")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1723177753375 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1723177753375")

# Script generated for node Join - Filter Customer Privacy
SqlQuery6676 = '''
select a.user, a.timestamp, a.x, a.y, a.z from cusTrusted c
inner join accLanding a
on c.email = a.user
'''
JoinFilterCustomerPrivacy_node1723177813819 = sparkSqlQuery(glueContext, query = SqlQuery6676, mapping = {"cusTrusted":CustomerTrusted_node1723177756499, "accLanding":AccelerometerLanding_node1723177753375}, transformation_ctx = "JoinFilterCustomerPrivacy_node1723177813819")

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1723177985674 = glueContext.getSink(path="s3://stedi-project-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedZone_node1723177985674")
AccelerometerTrustedZone_node1723177985674.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrustedZone_node1723177985674.setFormat("json")
AccelerometerTrustedZone_node1723177985674.writeFrame(JoinFilterCustomerPrivacy_node1723177813819)
job.commit()