CREATE TABLE IF NOT EXISTS logs (
    log_index BIGINT,
    transaction_hash VARCHAR,
    transaction_index BIGINT,
    block_hash VARCHAR,
    block_number BIGINT,
    address VARCHAR,
    data VARCHAR,
    topics VARCHAR,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data-pqt/logs/',
  format = 'parquet',
  partitioned_by = ARRAY['start_block', 'end_block']
);
