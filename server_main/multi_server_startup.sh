#!/usr/bin/env bash

# Make sure the port from 8180 to 8184 are open

go run server_main.go "" 3 8180 0 &
go run server_main.go "localhost:8180" 3 8181 1 &
go run server_main.go "localhost:8180" 3 8182 2 &
go run server_main.go "localhost:8180" 3 8183 3 &
go run server_main.go "localhost:8180" 3 8184 4 &