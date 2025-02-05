#!/usr/bin/env bash

WORKFLOW_FILENAME=$1

if [ -z "$WORKFLOW_FILENAME" ]; then
    echo "ERROR: Specify a process.cwl file to run"
    exit 1
fi

if [ ! -e $WORKFLOW_FILENAME ]; then
    echo "ERROR: CWL file does not exist: $WORKFLOW_FILENAME"
    exit 1
fi

WORKFLOW_FILENAME=$(realpath $WORKFLOW_FILENAME)

JOB_INP_FILENAME=$2

if [ -z "$JOB_INP_FILENAME" ]; then
    JOB_INP_FILENAME=$(dirname $WORKFLOW_FILENAME)/example_job_input.json
fi

if [ ! -e $JOB_INP_FILENAME ]; then
    echo "ERROR: Copy ${JOB_INP_FILENAME}.template to ${JOB_INP_FILENAME} and edit to include credentials"
    exit 1
fi

SCRIPT_DIR=$(realpath $(dirname $0))
TEST_BASE_DIR=$SCRIPT_DIR/test/$(basename $(dirname $WORKFLOW_FILENAME))
INP_DIR=$TEST_BASE_DIR/in
RUN_DIR=$TEST_BASE_DIR/out

mkdir -p $INP_DIR
mkdir -p $RUN_DIR
cd $RUN_DIR

# Detect if using Podman 
if [ ! -z "$(which podman)" ]; then
    use_podman_arg="--podman"
fi

cwltool \
    $use_podman_arg \
    "$WORKFLOW_FILENAME" "$JOB_INP_FILENAME" \
    |& tee $RUN_DIR/$(basename $WORKFLOW_FILENAME | sed 's/.cwl/.log/')