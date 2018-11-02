# ydb

# Usage

``go run server_main.go <master_adress_and_port> <node_num> <local_port> <node_id>`` 

**Note:** ``<master_adress_and_port>`` should be ``""`` if it's master server.

## Example: Start 3 Servers

```
go run server_main.go "" 3 8180 0
go run server_main.go "localhost:8180" 3 8181 1
go run server_main.go "localhost:8180" 3 8182 2
```