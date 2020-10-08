#!/bin/bash

set -e

cleanup(){
	rm -rf ./tmp.txt
	jobs -p | xargs -r kill
}

trap cleanup EXIT INT TERM

Help()
{
   echo "Run an exemplary Raft cluster."
   echo
   echo "Syntax: example.sh [-p -o]"
   echo "options:"
   echo "-o, --output:          Directory that should be used for temporary files."
   echo "-p, --path:            Path to the Raft project."
   echo
}

while [[ $# -gt 0 ]]; do
case "$1" in
    -o|--output)
    TMP_PATH=$2
    shift 2
    ;;
    -p|--path)
    RAFT_PROJECT_PATH=$2
    shift 2
    ;;
    -h|--help|*)
    Help
    exit 0
    ;;
esac
done

REQUIRED_PARAMS=(${RAFT_PROJECT_PATH} ${TMP_PATH})

for REQUIRED_PARAM in "${REQUIRED_PARAMS[@]}"; do
    if [[ -z ${REQUIRED_PARAM} ]]; then
		Help
		exit 1
    fi
done

# Prepare separate directories for three Raft nodes and spawn them
TMP_PATH=$(readlink -f "${TMP_PATH}")
RAFT_PROJECT_PATH=$(readlink -f "${RAFT_PROJECT_PATH}")
 
rm -rf ${TMP_PATH} && mkdir ${TMP_PATH}
cd ${RAFT_PROJECT_PATH}/cmd/node && go build -o ${TMP_PATH}

cd ${TMP_PATH}

echo "Spawning Raft cluster with 3 nodes."

mkdir ${TMP_PATH}/node1 && cd ${TMP_PATH}/node1
cp ${TMP_PATH}/node ${TMP_PATH}/node1/node
${TMP_PATH}/node1/node --http-endpoint localhost:8000 --cluster-endpoint localhost:9000 --name Node0 --http-endpoints Node1=localhost:8001 \
	--http-endpoints Node2=localhost:8002 --cluster-endpoints Node1=localhost:9001 --cluster-endpoints Node2=localhost:9002 &>./log.txt &
cd - > /dev/null

mkdir ${TMP_PATH}/node2 && cd ${TMP_PATH}/node2
cp ${TMP_PATH}/node ${TMP_PATH}/node2/node
${TMP_PATH}/node2/node --http-endpoint localhost:8001 --cluster-endpoint localhost:9001 --name Node1 --http-endpoints Node0=localhost:8000 \
	--http-endpoints Node2=localhost:8002 --cluster-endpoints Node0=localhost:9000 --cluster-endpoints Node2=localhost:9002 &>./log.txt &
cd - > /dev/null

mkdir ${TMP_PATH}/node3 && cd ${TMP_PATH}/node3
cp ${TMP_PATH}/node ${TMP_PATH}/node3/node
${TMP_PATH}/node3/node --http-endpoint localhost:8002 --cluster-endpoint localhost:9002 --name Node2 --http-endpoints Node1=localhost:8001 \
	--http-endpoints Node0=localhost:8000 --cluster-endpoints Node1=localhost:9001 --cluster-endpoints Node0=localhost:9000 &>./log.txt &
cd - > /dev/null

# Wait fot the cluster to start
for i in {1..10}; do
  echo "Waiting for Raft cluster to start."
  sleep 1
done

# Run the test with CURL - create, get, delete, and get
KEY="key-1"
VALUE=$(echo "value-1" | base64)
JSON_DATA_CREATE='{"Key":"'${KEY}'","Value":"'${VALUE}'"}"'
JSON_DATA='{"Key":"'${KEY}'"}'

echo "Inserting key-value pair into the cluster: ${JSON_DATA_CREATE}."

echo "STEP 1: Creating key-value pair."
RESULT=$(curl -s -o tmp.txt -w "%{http_code}" -L -d ${JSON_DATA_CREATE} -H "Content-Type: application/json" -X POST http://localhost:8000/api/v1/object)
if [[ $RESULT != 200 ]]; then
	echo "Could not create key-data pair due to an error: ${RESULT}."
	cat tmp.txt
	exit 1
fi

sleep 3

echo "STEP 2: Getting key-value pair."
RESULT=$(curl -s -o tmp.txt -w "%{http_code}" -d ${JSON_DATA} -H "Content-Type: application/json" -X GET http://localhost:8000/api/v1/object)
if [[ $RESULT != 200 ]]; then
	echo "Could not get key-data pair due to an error: ${RESULT}."
	cat tmp.txt
	exit 1
else
	RECEIVED_VALUE=$(cat tmp.txt | jq -r '.Value')
	if [[ "${RECEIVED_VALUE}" != "${VALUE}" ]]; then
		echo "Expected value ${VALUE} differs from the received one ${RECEIVED_VALUE}."
		exit 1
	else
		echo "Received correct value after GET: ${RECEIVED_VALUE}."
	fi
fi

sleep 3

echo "STEP 3: Deleting key-value pair."
RESULT=$(curl -s -o tmp.txt -w "%{http_code}" -d ${JSON_DATA} -H "Content-Type: application/json" -X DELETE http://localhost:8000/api/v1/object)
if [[ $RESULT != 200 ]]; then
	echo "Deletion of the key-value pair resulted in an error: ${RESULT}."
	cat tmp.txt
	exit 1
fi

sleep 3

echo "STEP 4: Getting key-value pair after deletion."
RESULT=$(curl -s -o tmp.txt -w "%{http_code}" -d ${JSON_DATA} -H "Content-Type: application/json" -X GET http://localhost:8000/api/v1/object)
if [[ $RESULT != 404 ]]; then
	echo "Getting key-value pair after deletion resulted in an error: ${RESULT}."
	cat tmp.txt
	exit 1
fi

echo "Raft testing successful!"