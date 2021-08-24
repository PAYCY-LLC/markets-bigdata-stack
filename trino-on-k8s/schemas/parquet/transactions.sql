CREATE TABLE IF NOT EXISTS transactions (
    hash VARCHAR,
    nonce BIGINT,
    block_hash VARCHAR,
    block_number BIGINT,
    transaction_index BIGINT,
    from_address VARCHAR,
    to_address VARCHAR,
    value DECIMAL(38,0),
    gas BIGINT,
    gas_price BIGINT,
    input VARCHAR,
    block_timestamp BIGINT,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data-pqt/transactions/',
  format = 'parquet',
  partitioned_by = ARRAY['start_block', 'end_block']
);