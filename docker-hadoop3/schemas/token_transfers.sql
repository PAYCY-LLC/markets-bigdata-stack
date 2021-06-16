CREATE EXTERNAL TABLE IF NOT EXISTS token_transfers (
    token_address STRING,
    from_address STRING,
    to_address STRING,
    value DECIMAL(38,0),
    transaction_hash STRING,
    log_index BIGINT,
    block_number BIGINT
)
PARTITIONED BY (start_block BIGINT, end_block BIGINT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ',',
    'escape.delim' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3a://etl-data/token_transfers'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);

ALTER TABLE token_transfers ADD PARTITION (start_block='0', end_block='100000000') LOCATION 's3a://etl-data/token_transfers';
