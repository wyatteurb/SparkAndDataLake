CREATE EXTERNAL TABLE IF NOT EXISTS `exercise`.`machine_learning_curated` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://glueytestb/step_trainer/curated/'
TBLPROPERTIES ('classification' = 'json');
