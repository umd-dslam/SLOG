#!/bin/bash
#
# Usage:
#   -a  Path to the admin tool
#   -c  Path to the config file
#   -i  Docker image name
#   -o  Output directory
#   -u  Username used to SSH to the remote machines
#
# Example:
#
# tools/microbenchmark.sh\
#  -a tools/admin.py\
#  -c ~/configs/slog/slog.conf\
#  -i ctring/slog:rma-master\
#  -o ~/data/slog
#

BENCHMARK_ARGS="--clients 200 --generators 5 --txns 500000 --duration 30 --sample 10"

USER="ubuntu"
PREFIX="slog"
OUT_DIR="."
TRIALS=1

while getopts a:c:i:o:p:u: flag
do
  case "${flag}" in
    a) ADMIN_TOOL=${OPTARG};;
    c) CONFIG=${OPTARG};;
    i) IMAGE=${OPTARG};;
    o) OUT_DIR=${OPTARG};;
    u) USER=${OPTARG};;
    p) PREFIX=${OPTARG};;
    t) TRIALS=${OPTARG};;
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

set -x

function run_benchmark {
  HOT=$1
  MH=$2
  MP=$3
  TAG=${PREFIX}-hot${HOT}mh${MH}mp${MP}
  
  for i in $(seq 1 ${TRIALS})
  do
    if [ $TRIALS -gt 1 ]; then
      NTAG=${TAG}-${i}
    else
      NTAG=${TAG}
    fi
    python3 ${ADMIN_TOOL} benchmark ${CONFIG} ${BENCHMARK_ARGS} --image ${IMAGE} -u ${USER} --params "writes=10,records=10,hot=$HOT,hot_records=2,mh=$MH,mp=$MP" --tag ${NTAG}
    python3 ${ADMIN_TOOL} collect_client --out-dir ${OUT_DIR} -u ${USER} ${CONFIG} ${NTAG}
    # For debug only
    # python3 ${ADMIN_TOOL} collect_server --out-dir ${OUT_DIR} -u ${USER} --image ${IMAGE} ${CONFIG} --tag ${NTAG}
  done
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
