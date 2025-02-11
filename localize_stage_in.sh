#!/usr/bin/env bash
# Uses U-DS stage-in to download Unity inputs the same way they would for the processing DAG
# This can be used to create localized datasets.
# 
# Define the AWS environment variables below to access the MDPS environment

usage() {
    echo "$0 <stac_json> [<download_dir>]"
    exit 1
}

STAGE_IN_CWL="https://raw.githubusercontent.com/unity-sds/unity-data-services/refs/heads/develop/cwl/stage-in-unity/stage-in.cwl"

# unity-app-to-app-client-user-pool-client
CLIENT_ID="7vehllplbone6p4usqgutqun35"

DEFAULT_DOWNLOAD_DIR="./stage_in_download"

# Detect if using Podman 
if [ ! -z "$(which podman)" ]; then
    use_podman_arg="--podman"
fi

stac_json="$1"
if [ -z "$stac_json" ]; then
    echo "ERROR: stac_json not provided"
    usage
    exit 1
fi
shift

download_dir="$1"
if [ -z "$download_dir" ]; then
    echo "WARNINING: download_dir not provided, using: $DEFAULT_DOWNLOAD_DIR"
    download_dir="$DEFAULT_DOWNLOAD_DIR"
fi

cwltool \
$use_podman_arg \
 --no-read-only \
 --preserve-environment AWS_ACCESS_KEY_ID \
 --preserve-environment AWS_SECRET_ACCESS_KEY \
 --preserve-environment AWS_SESSION_TOKEN \
 $STAGE_IN_CWL \
 --unity_client_id "$CLIENT_ID" \
 --stac_json "$stac_json" \
 --download_dir "$download_dir"