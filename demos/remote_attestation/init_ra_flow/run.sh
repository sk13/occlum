#!/bin/bash
set -e

GRPC_ADDR="localhost:50051"

echo "Start GRPC server on backgound ..."

pushd occlum_server
occlum run /bin/ratls_kms ${GRPC_ADDR} /kms.json &
popd

sleep 3

echo "Start Flask-TLS restful web portal on backgound ..."

pushd occlum_client
occlum run --config-svn 1234 /bin/rest_api.py &
popd