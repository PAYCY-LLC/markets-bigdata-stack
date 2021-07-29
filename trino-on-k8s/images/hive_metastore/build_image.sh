#!/bin/bash

#set -e

REPONAME=hsdao
TAG=hivemetastore

docker build -t $TAG .

# Tag and push to the public docker repository.
docker tag $TAG $REPONAME/$TAG
docker push $REPONAME/$TAG


# Update configmaps
kubectl create configmap metastore-cfg --dry-run --from-file=metastore-site.xml -o yaml | kubectl apply -f -
