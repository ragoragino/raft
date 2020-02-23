22.02.2019:
- Go over consensus.go and log.go
- http://cricca.disi.unitn.it/montresor/teaching/ds2/slides/

For spawning local nodes, run executable (cmd/node/main.go) in three different folders, e.g. like this:

mkdir ${TMP_PATH} 
cd ${RAFT_PROJECT_PATH}/cmd/node && go build -o ${TMP_PATH}

// First shell
mkdir ${TMP_PATH}/node1 && cp ${TMP_PATH}/node ${TMP_PATH}/node1/node
${TMP_PATH}/node1/node --http-endpoint localhost:8000 --cluster-endpoint localhost:9000 --name Node0 --http-endpoints Node1:localhost:8001 --http-endpoints Node2:localhost:8002 --cluster-endpoints Node1:localhost:9001 --cluster-endpoints Node2:localhost:9002

// Second shell
mkdir ${TMP_PATH}/node2 && cp ${TMP_PATH}/node ${TMP_PATH}/node2/node
${TMP_PATH}/node2/node --http-endpoint localhost:8001 --cluster-endpoint localhost:9001 --name Node1 --http-endpoints Node0:localhost:8000 --http-endpoints Node2:localhost:8002 --cluster-endpoints Node0:localhost:9000 --cluster-endpoints Node2:localhost:9002

// Third shell
mkdir ${TMP_PATH}/node3 && cp ${TMP_PATH}/node ${TMP_PATH}/node3/node
${TMP_PATH}/node3/node --http-endpoint localhost:8002 --cluster-endpoint localhost:9002 --name Node2 --http-endpoints Node1:localhost:8001 --http-endpoints Node0:localhost:8000 --cluster-endpoints Node1:localhost:9001 --cluster-endpoints Node0:localhost:9000

// Fourth shell. Run a client and test create/get/delete "key-1" (e.g. with Base64 encoded "value-1" as the value)
curl -L -v -d '{"Key":"key-1","Value":"dmFsdWUtMQ=="}' -H "Content-Type: application/json" -X POST http://localhost:8000/create # 200 OK
sleep 2
curl -L -v -d '{"Key":"key-1"}' -H "Content-Type: application/json" -X POST http://localhost:8000/get # 200 OK, receives {"Value":"dmFsdWUtMQ=="}
sleep 2
curl -L -v -d '{"Key":"key-1"}' -H "Content-Type: application/json" -X POST http://localhost:8000/delete # 200 OK
sleep 2
curl -L -v -d '{"Key":"key-1"}' -H "Content-Type: application/json" -X POST http://localhost:8000/get # 400 Not Found