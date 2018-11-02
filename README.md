# ydb

## Usage

### Running servers at local machines

``go run server_main.go <master_adress_and_port> <node_num> <local_port> <node_id>`` 

**Note:** ``<master_adress_and_port>`` should be ``""`` if it's master server.

#### Example: Start 3 Servers locally

```
go run server_main.go "" 3 8180 0
go run server_main.go "localhost:8180" 3 8181 1
go run server_main.go "localhost:8180" 3 8182 2
```

### Running servers use Docker

``docker run boylee1111/ydb_server:1.0 <master_adress_and_port> <node_num> <local_port> <node_id>``

**Note1:** ``<master_adress_and_port>`` should be ``""`` if it's master server.
**Note2:** Only port within range 8000 to 9000 is supported.


#### Example: Start 3 Servers with docker

```
docker run boylee1111/ydb_server:1.0 "" 3 8180 0
docker run boylee1111/ydb_server:1.0 "<master_container_ip>:8180" 3 8181 1
docker run boylee1111/ydb_server:1.0 "<master_container_ip>:8180" 3 8182 2
```