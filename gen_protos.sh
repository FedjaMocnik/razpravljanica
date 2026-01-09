#!/bin/bash
set -e

mkdir -p pkgs/public/pb pkgs/private/pb pkgs/control/pb

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
  pkgs/control/proto/control.proto
