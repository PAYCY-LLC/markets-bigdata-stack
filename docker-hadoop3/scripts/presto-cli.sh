#!/bin/bash
cd "${0%/*}"

PRESTO_VERSION=0.240
if [ ! -e ./bin/presto-cli.jar ]; then
     curl -L https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/$PRESTO_VERSION/presto-cli-$PRESTO_VERSION-executable.jar -o ./bin/presto-cli.jar
fi
java -jar bin/presto-cli.jar --server localhost:8080 --catalog hive --schema default
