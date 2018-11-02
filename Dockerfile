FROM golang:1.11.1

# Copy the local package files to the container's workspace.
ADD . /go/src/github.com/boylee1111/ydb
ADD . /go/src/go.etcd.io/bbolt

# Build the outyet command inside the container.
# (You may fetch or manage dependencies here,
# either manually or with a tool like "godep".)
RUN go install github.com/boylee1111/ydb/server_main

ENTRYPOINT ["/go/bin/server_main"]
CMD []

# Document that the service listens on port 8080.
EXPOSE 8000-9000