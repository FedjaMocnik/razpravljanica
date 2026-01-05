#!/bin/bash

protoc \
    --go_out=pkgs/public/pb --go_opt=paths=source_relative \
    --go-grpc_out=pkgs/public/pb --go-grpc_opt=paths=source_relative \
    -I pkgs/public/proto \
    pkgs/public/proto/razpravljalnica.proto

protoc \
    --go_out=pkgs/private/pb --go_opt=paths=source_relative \
    --go-grpc_out=pkgs/private/pb --go-grpc_opt=paths=source_relative \
    -I pkgs/private/proto \
    pkgs/private/proto/replikacija.proto

protoc \
    --go_out=pkgs/control/pb --go_opt=paths=source_relative \
    --go-grpc_out=pkgs/control/pb --go-grpc_opt=paths=source_relative \
    -I pkgs/control/proto \
    pkgs/private/proto/control.proto
