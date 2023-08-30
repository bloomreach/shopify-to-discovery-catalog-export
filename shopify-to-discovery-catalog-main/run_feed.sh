#!/usr/bin/env bash

# Small helper to run a full feed

# The following environment variables must be present:
#   BR_SHOPIFY_URL=
#   BR_SHOPIFY_PAT=
#   BR_ENVIRONMENT_NAME=(staging|production)
#   BR_ACCOUNT_ID=
#   BR_CATALOG_NAME=
#   BR_API_TOKEN=
#   BR_OUTPUT_DIR=

cd "$(dirname -- "$0")"

mkdir -p ${BR_OUTPUT_DIR}

source ./venv/bin/activate

./venv/bin/python3 src/main.py
