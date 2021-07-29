CREATE TABLE IF NOT EXISTS token_transfers (
    token_address VARCHAR,
    from_address VARCHAR,
    to_address VARCHAR,
    value DECIMAL(38,0),
    transaction_hash VARCHAR,
    log_index BIGINT,
    block_number BIGINT,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data/token_transfers/',
  format = 'TEXTFILE',
  textfile_field_separator = ',',
  skip_header_line_count = 1,
  partitioned_by = ARRAY['start_block', 'end_block']
);
