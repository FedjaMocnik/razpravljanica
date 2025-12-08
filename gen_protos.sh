#!/bin/bash

protoc \
    --go_out=public_pb --go_opt=paths=source_relative \
    --go-grpc_out=public_pb --go-grpc_opt=paths=source_relative \
    -I public_proto \
    public_proto/razpravljalnica.proto
