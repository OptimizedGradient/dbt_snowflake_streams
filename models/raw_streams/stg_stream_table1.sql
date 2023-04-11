SELECT *
  FROM {{ source('raw_streams', 'stream_table1') }}