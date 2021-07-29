CREATE TABLE IF NOT EXISTS contracts (
    address VARCHAR,
    bytecode VARCHAR,
    function_sighashes VARCHAR,
    is_erc20 BOOLEAN,
    is_erc721 BOOLEAN,
    start_block VARCHAR,
    end_block VARCHAR
)
WITH (
  external_location = 's3a://etl-data/contracts/',
  format = 'TEXTFILE',
  textfile_field_separator = ',',
  skip_header_line_count = 1,
  partitioned_by = ARRAY['start_block', 'end_block']
);
