CREATE TABLE IF NOT EXISTS receipts (
    transaction_hash VARCHAR,
    transaction_index BIGINT,
    block_hash VARCHAR,
    block_number BIGINT,
    cumulative_gas_used BIGINT,
    gas_used BIGINT,
    contract_address VARCHAR,
    root VARCHAR,
    status BIGINT,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data-pqt/receipts/',
  format = 'parquet',
  partitioned_by = ARRAY['start_block', 'end_block']
);
