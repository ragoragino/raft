22.09.2019:
- validate the code
- think about making it more concurrent, as currently only one request from any type can be pending on a server (because each request needs to access the node's state)
- pass log as param in cluster
- buffer channels