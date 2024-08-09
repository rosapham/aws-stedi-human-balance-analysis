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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1723184704468 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1723184704468")

# Script generated for node Customer Curated
CustomerCurated_node1723185916703 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1723185916703")

# Script generated for node Join and Filter Step Trainer Privacy
SqlQuery6498 = '''
select st.sensorReadingTime, st.serialNumber, st.distanceFromObject
from cusCurated cus
inner join stLanding st
on cus.serialNumber = st.serialNumber
'''
JoinandFilterStepTrainerPrivacy_node1723185960679 = sparkSqlQuery(glueContext, query = SqlQuery6498, mapping = {"cusCurated":CustomerCurated_node1723185916703, "stLanding":StepTrainerLanding_node1723184704468}, transformation_ctx = "JoinandFilterStepTrainerPrivacy_node1723185960679")

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1723186103538 = glueContext.getSink(path="s3://stedi-project-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedZone_node1723186103538")
StepTrainerTrustedZone_node1723186103538.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrustedZone_node1723186103538.setFormat("json")
StepTrainerTrustedZone_node1723186103538.writeFrame(JoinandFilterStepTrainerPrivacy_node1723185960679)
job.commit()