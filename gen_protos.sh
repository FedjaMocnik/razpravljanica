#!/bin/bash

protoc \
    --go_out=pkgs/public/pb --go_opt=paths=source_relative \
    --go-grpc_out=pkgs/public/pb --go-grpc_opt=paths=source_relative \
    -I pkgs/public/proto \
    pkgs/public/proto/razpravljalnica.proto