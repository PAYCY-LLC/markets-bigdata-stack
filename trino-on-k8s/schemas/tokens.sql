CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    decimals BIGINT,
    total_supply DECIMAL(38,0),
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data/tokens/',
  format = 'TEXTFILE',
  textfile_field_separator = ',',
  skip_header_line_count = 1,
  partitioned_by = ARRAY['start_block', 'end_block']
);
