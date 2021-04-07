#!/bin/bash

USER="ubuntu"
PREFIX="test"
OUT_DIR="."

while getopts a:c:i:o:p:u: flag
do
  case "${flag}" in
    a) ADMIN_TOOL=${OPTARG};;
    c) CONFIG=${OPTARG};;
    i) IMAGE=${OPTARG};;
    o) OUT_DIR=${OPTARG};;
    u) USER=${OPTARG};;
    p) PREFIX=${OPTARG};;
  esac
done

if [[ -z $ADMIN_TOOL ]]; then
  echo "ERROR: Use the -a flag to specify the admin tool"
  exit 1
fi

if [[ -z $CONFIG ]]; then
  echo "ERROR: Use the -c flag to specify the config file"
  exit 1
fi


if [[ -z $IMAGE ]]; then
  echo "ERROR: Use the -i flag to specify the Docker image"
  exit 1
fi

BENCHMARK_ARGS="--rate 50000 --workers 8 --num-txns 500000 --sample 1 --seed 0"

set -x

function run_benchmark {
  HOT=$1
  MH=$2
  MP=$3
  TAG=$PREFIX-hot${HOT}mh${MH}mp${MP}
 
  python3 ${ADMIN_TOOL} benchmark ${CONFIG} ${BENCHMARK_ARGS} --image ${IMAGE} -u ${USER} --params "writes=5,records=10,hot=$HOT,mh=$MH,mp=$MP" --tag $TAG
  sleep 5
  python3 ${ADMIN_TOOL} collect_client --out-dir ${OUT_DIR} -u ${USER} ${CONFIG} $TAG
  python3 ${ADMIN_TOOL} collect_server --out-dir ${OUT_DIR} -u ${USER} --image ${IMAGE} ${CONFIG} $TAG
  sleep 5
}

run_benchmark 10000 0 0
run_benchmark 10000 50 0
run_benchmark 10000 100 0
run_benchmark 10000 0 50
run_benchmark 10000 50 50
run_benchmark 10000 100 50
run_benchmark 10000 0 100
run_benchmark 10000 50 100
run_benchmark 10000 100 100
run_benchmark 30 0 0
run_benchmark 30 50 0
run_benchmark 30 100 0
run_benchmark 30 0 50
run_benchmark 30 50 50
run_benchmark 30 100 50
run_benchmark 30 0 100
run_benchmark 30 50 100
run_benchmark 30 100 100
