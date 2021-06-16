CREATE EXTERNAL TABLE IF NOT EXISTS contracts (
    address STRING,
    bytecode STRING,
    function_sighashes STRING,
    is_erc20 BOOLEAN,
    is_erc721 BOOLEAN
)
PARTITIONED BY (start_block BIGINT, end_block BIGINT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ',',
    'escape.delim' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3a://etl-data/contracts'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);

ALTER TABLE contracts ADD PARTITION (start_block='0', end_block='100000000') LOCATION 's3a://etl-data/contracts';