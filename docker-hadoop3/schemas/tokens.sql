CREATE EXTERNAL TABLE IF NOT EXISTS tokens (
    address STRING,
    symbol STRING,
    name STRING,
    decimals BIGINT,
    total_supply DECIMAL(38,0)
)
PARTITIONED BY (start_block BIGINT, end_block BIGINT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ',',
    'escape.delim' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3a://etl-data/tokens'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);

ALTER TABLE tokens ADD PARTITION (start_block='0', end_block='100000000') LOCATION 's3a://etl-data/tokens';
