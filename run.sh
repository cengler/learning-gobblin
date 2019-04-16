#!/bin/bash

home_dir=$(pwd)
echo "HOME: ${home_dir}"

export GOBBLIN_JOB_CONFIG_DIR=${home_dir}/gobblin-data/jobs
export GOBBLIN_WORK_DIR=${home_dir}/gobblin-data/work
export GOBBLIN_LOG_DIR=${home_dir}/gobblin-data/log

gobblin-dist/bin/gobblin.sh run $1