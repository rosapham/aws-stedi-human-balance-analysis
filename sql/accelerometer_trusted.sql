CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-project-lake-house/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing to Trusted', 
  'CreatedByJobRun'='jr_72357686906d49911b25013e7a8e480202d448bdd0cb763e99271282354813b2', 
  'classification'='json')