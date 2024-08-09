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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1723187315014 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1723187315014")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1723187269362 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1723187269362")

# Script generated for node Aggregated Step Trainer and Accelerometer
SqlQuery7049 = '''
select a.user, a.x, a.y, a.z, s.sensorreadingtime,
    s.serialnumber, s.distancefromobject
from accTrusted a
inner join stTrusted s
on s.sensorReadingTime = a.timestamp

'''
AggregatedStepTrainerandAccelerometer_node1723187337434 = sparkSqlQuery(glueContext, query = SqlQuery7049, mapping = {"accTrusted":AccelerometerTrusted_node1723187315014, "stTrusted":StepTrainerTrusted_node1723187269362}, transformation_ctx = "AggregatedStepTrainerandAccelerometer_node1723187337434")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1723187684619 = glueContext.getSink(path="s3://stedi-project-lake-house/ML/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1723187684619")
MachineLearningCurated_node1723187684619.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1723187684619.setFormat("json")
MachineLearningCurated_node1723187684619.writeFrame(AggregatedStepTrainerandAccelerometer_node1723187337434)
job.commit()