CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT,
    hash VARCHAR,
    parent_hash VARCHAR,
    nonce VARCHAR,
    sha3_uncles VARCHAR,
    logs_bloom VARCHAR,
    transactions_root VARCHAR,
    state_root VARCHAR,
    receipts_root VARCHAR,
    miner VARCHAR,
    difficulty DECIMAL(38,0),
    total_difficulty DECIMAL(38,0),
    size BIGINT,
    extra_data VARCHAR,
    gas_limit BIGINT,
    gas_used BIGINT,
    timestamp BIGINT,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data/blocks/',
  format = 'TEXTFILE',
  textfile_field_separator = ',',
  skip_header_line_count = 1,
  partitioned_by = ARRAY['start_block', 'end_block']
);