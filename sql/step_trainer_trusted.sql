CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-project-lake-house/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Landing To Trusted', 
  'CreatedByJobRun'='jr_4a0b33db420ee9f8be3593550b49e829588a0f4226a2adeed6e15b8dace4f5e4', 
  'classification'='json')