#!/bin/bash
cd "${0%/*}"
docker-compose -f ../docker-compose.yml exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000


CREATE EXTERNAL TABLE blocks (sepal_length DECIMAL, sepal_width DECIMAL,
petal_length DECIMAL, petal_width DECIMAL, species STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3a://etl-data/blocks/'
TBLPROPERTIES ("skip.header.line.count"="1");