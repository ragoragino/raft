# raft

This project aims to be an examination of the RAFT protocol: "Ongaro, Diego, and John Ousterhout. "In search of an understandable consensus algorithm (extended version)." Retrieved July 20 (2016): 2018."

## Getting Started

I have implemented a distributed key-value datastore with strong consistency provided by Raft protocol. Datastore allows CRUD operations and is accessible via a simple REST API.

### Prerequisites

- Go 1.13

### Running

See [./scripts/example.sh](https://github.com/ragoragino/raft/blob/master/scripts/example.sh) for an exemplary cluster creation and sending of requests (create, get, delete).

### Running the tests

I am using toxiproxy in the tests to simulate network failures. Therefore its executable (toxiproxy-server-linux-amd64 on Linux or toxiproxy-server-linux-amd64.exe on Windows) needs to be present in the project root.

go test ./...

## License

This project is licensed under the MIT License.