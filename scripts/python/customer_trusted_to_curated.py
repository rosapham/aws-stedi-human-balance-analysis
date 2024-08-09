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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1723183306774 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1723183306774")

# Script generated for node Customer Trusted
CustomerTrusted_node1723183307590 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1723183307590")

# Script generated for node Join Customer Privacy
SqlQuery6655 = '''
select * 
from cusTrusted c
inner join accLanding a
on c.email = a.user

'''
JoinCustomerPrivacy_node1723183354754 = sparkSqlQuery(glueContext, query = SqlQuery6655, mapping = {"cusTrusted":CustomerTrusted_node1723183307590, "accLanding":AccelerometerLanding_node1723183306774}, transformation_ctx = "JoinCustomerPrivacy_node1723183354754")

# Script generated for node Drop Fields and Duplications
SqlQuery6656 = '''
select distinct customerName, email, phone, birthDay, serialNumber,
    registrationDate, lastUpdateDate, shareWithResearchAsOfDate,
    shareWithPublicAsOfDate, shareWithFriendsAsOfDate
from myDataSource

'''
DropFieldsandDuplications_node1723183426774 = sparkSqlQuery(glueContext, query = SqlQuery6656, mapping = {"myDataSource":JoinCustomerPrivacy_node1723183354754}, transformation_ctx = "DropFieldsandDuplications_node1723183426774")

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1723183518234 = glueContext.getSink(path="s3://stedi-project-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedZone_node1723183518234")
CustomerCuratedZone_node1723183518234.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCuratedZone_node1723183518234.setFormat("json")
CustomerCuratedZone_node1723183518234.writeFrame(DropFieldsandDuplications_node1723183426774)
job.commit()