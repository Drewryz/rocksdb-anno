#!/usr/bin/env bash

raw_size=68719476736
test_dir="/home/odin/yangzaorang/test"

function run_value_size {
    export KEY_SIZE=16
    export COMPRESSION_TYPE="none"
    # 20GB
    export CACHE_SIZE=21474836480
    export NUM_THREADS=32
    export CACHE_INDEX_AND_FILTER_BLOCKS=1
    export DURATION=3600
    export ENABLE_BLOB_FILES=$1
    # 1k 4k 16k
    value_sizes=(1024 4096 16384)
    for value_size in ${value_sizes[@]}; do
        cur_time=`date +%Y-%m-%d-%H-%M-%S`
        export DB_DIR="${test_dir}/${cur_time}/data"
        export WAL_DIR="${test_dir}/${cur_time}/data"
        export OUTPUT_DIR="${test_dir}/${cur_time}/output"
        export NUM_KEYS=$( echo "${raw_size}/${value_size}" | bc )
        export VALUE_SIZE=${value_size}
        echo "value_size=${value_size}, num_keys=${NUM_KEYS}, time=${cur_time}, enable_blob_file=${ENABLE_BLOB_FILES}, start"
        mkdir -p "${test_dir}/${cur_time}"
        ./benchmark.sh run_fillrandom,overwrite,readrandom > "${test_dir}/${cur_time}/test.log" 2>&1
        end_time=`date +%Y-%m-%d-%H-%M-%S`
        echo "value_size=${value_size}, num_keys=${NUM_KEYS}, time=${end_time}, enable_blob_file=${ENABLE_BLOB_FILES}, end"
        sleep 5
    done 
}

run_value_size true
run_value_size false
