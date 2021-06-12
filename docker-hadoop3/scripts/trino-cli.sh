#!/bin/bash
cd "${0%/*}"

TRINO_VERSION=358
if [ ! -e ./bin/trino-cli.jar ]; then

     curl -L https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar -o ./bin/trino-cli.jar
fi
java -jar bin/trino-cli.jar --server localhost:8080 --catalog hive --schema default
