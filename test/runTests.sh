#!/bin/sh

PYSPARK_PYTHON=/usr/bin/python2.7
SPARK_MASTER=local[*]
PYSPARK_CASSANDRA=/pyspark-cassandra-assembly-0.3.5.jar
SUT_TEST_CONFIGURATION_FILE=/test/sut/wfms/camunda/3.5.0-3.7.0/data-transformers.configuration.yml
CAMUNDA_SUT_TEST_CONFIGURATION_FILE=/test/sut/wfms/camunda/3.5.0-3.7.0/data-transformers.configuration.yml
ACTIVITI_SUT_TEST_CONFIGURATION_FILE=/test/sut/wfms/activiti/5.18.0-5.21.0/data-transformers.configuration.yml
CASSANDRA_HOST=cassandra
MINIO_HOST="minio"
MINIO_PORT="9000"
CAMUNDA_TRIAL_ID="camundaZZZZZZMV_ZZZZZZMV"
CAMUNDA_EXPERIMENT_ID="camundaZZZZZZMV"
ACTIVITI_TRIAL_ID="BenchFlow.activitiICPE2017FastTestWorking.4.1"
ACTIVITI_EXPERIMENT_ID="BenchFlow.activitiICPE2017FastTestWorking.4"
CAMUNDA_SUT_NAME="camunda"
HOST_NAME="docker_host"
CONFIG_FILE="data-transformers.configuration.yml"


echo ">>>Starting Python tests"

/usr/bin/python2.7 /test/python/statsTests.py
/usr/bin/python2.7 /test/python/mysqlTests.py
/usr/bin/python2.7 /test/python/propertiesTests.py

echo ">>>Python tests done"

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $SUT_TEST_CONFIGURATION_FILE \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/statsTransformer.py \
'{"cassandra_keyspace":"benchflow", "minio_host": "'$MINIO_HOST'", "minio_port":"'$MINIO_PORT'", "minio_access_key":"AKIAIOSFODNN7EXAMPLE", "minio_secret_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "file_bucket":"runs", "file_path": "mockStats/mock", "sut_name": "'$CAMUNDA_SUT_NAME'", "trial_id": "'$CAMUNDA_TRIAL_ID'", "experiment_id": "'$CAMUNDA_EXPERIMENT_ID'", "container_id": "stats_camunda", "container_name": "stats_camunda", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi
sleep 5

echo ">>>Starting Spark tests"

echo ">>>Starting Camunda mysqlTransformer"

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $CAMUNDA_SUT_TEST_CONFIGURATION_FILE \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/mysqlTransformer.py \
'{"cassandra_keyspace":"benchflow", "minio_host": "'$MINIO_HOST'", "minio_port":"'$MINIO_PORT'", "minio_access_key":"AKIAIOSFODNN7EXAMPLE", "minio_secret_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "file_bucket":"runs", "file_path": "mockProcessEngine/camunda", "config_file": "'$CONFIG_FILE'", "trial_id": "'$CAMUNDA_TRIAL_ID'", "experiment_id": "'$CAMUNDA_EXPERIMENT_ID'", "container_id": "mysql_camunda", "container_name": "mysql_camunda", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi
sleep 5

echo ">>>End Camunda mysqlTransformer"

echo ">>>Starting Activiti mysqlTransformer"

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $ACTIVITI_SUT_TEST_CONFIGURATION_FILE \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/mysqlTransformer.py \
'{"cassandra_keyspace":"benchflow", "minio_host": "'$MINIO_HOST'", "minio_port":"'$MINIO_PORT'", "minio_access_key":"AKIAIOSFODNN7EXAMPLE", "minio_secret_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "file_bucket":"runs", "file_path": "mockProcessEngine/activiti", "config_file": "'$CONFIG_FILE'", "trial_id": "'$ACTIVITI_TRIAL_ID'", "experiment_id": "'$ACTIVITI_EXPERIMENT_ID'", "container_id": "mysql", "container_name": "mysql", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi
sleep 5

echo ">>>End Activiti mysqlTransformer"

echo ">>>Spark tests done"

for SCRIPT in "cassandraTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA \
	--driver-class-path $PYSPARK_CASSANDRA \
	--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
	--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
	/test/spark/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
done
sleep 5

exit 0
