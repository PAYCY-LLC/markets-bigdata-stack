CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    decimals BIGINT,
    total_supply DECIMAL(38,0),
    block_number BIGINT,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data-pqt/tokens/',
  format = 'parquet',
  partitioned_by = ARRAY['start_block', 'end_block']
);
