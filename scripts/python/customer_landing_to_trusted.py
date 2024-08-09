import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1723162670434 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingZone_node1723162670434")

# Script generated for node Privacy Filter
PrivacyFilter_node1723163111219 = Filter.apply(frame=CustomerLandingZone_node1723162670434, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1723163111219")

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1723164034090 = glueContext.write_dynamic_frame.from_options(frame=PrivacyFilter_node1723163111219, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project-lake-house/customer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="CustomerTrustedZone_node1723164034090")

job.commit()