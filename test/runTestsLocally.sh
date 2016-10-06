#!/bin/sh

# Mainly used for running the test locally

echo $"\n>>>Getting/Building the Latest Version of Docker Images"
docker pull benchflow/minio:dev
docker pull cassandra:3.7
docker pull benchflow/data-analyses-scheduler:dev
docker build -f ./test/Dockerfile.ci -t sparktests .
echo $"\n>>>Got/Built the Latest Version of Docker Images"

echo $"\n>>>Starting Minio"
docker run --name minio -e "MINIO_ALIAS=benchflow" -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" -d benchflow/minio:dev
sleep 60
echo $"\n>>>Minio Started"

echo $"\n>>>Copying Minio Data"
docker cp ./test/data/runs/mockProcessEngine minio:/benchflow/runs
docker cp ./test/data/runs/mockStats minio:/benchflow/runs
echo $"\n>>>Minio Data Copied"

echo $"\n>>>Starting Cassandra"
docker run -d --name cassandra cassandra:3.7
sleep 60
docker run -d --name cassandra2 --link cassandra:cassandra cassandra:3.7
sleep 60
echo $"\n>>>Cassandra Started"

echo $"\n>>>Copying and Setting Up Cassandra Schema"
docker cp ./test/data/benchflow.cql cassandra:/
docker exec cassandra cqlsh -f /benchflow.cql
echo $"\n>>>Cassandra Schema Set Up Done"

echo $"\n>>>Starting Tests"
docker run --rm --name spark --link minio:minio --link cassandra:cassandra --entrypoint=/test/runTests.sh sparktests
# docker run -ti --rm --name spark --link minio:minio --link cassandra:cassandra --entrypoint=sh sparktests
echo $"\n>>>Tests Done"

echo $"\n>>>Removing all Test Containers"
docker rm -f -v minio cassandra cassandra2
echo $"\n>>>All Test Containers Removed"