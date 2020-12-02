#!/bin/bash

if [[ !$EUID -eq 0 ]]; then
	echo "Please run this program with super-user privileges."
	exit
fi

DEV=nvme0n2
ZONE_CAP=$(expr $(sudo zbd report -i /dev/$DEV | grep -oP '(?<=cap )[0-9xa-f]+' | head -1) + 0)
FUZZ=5
BASE_FZ=$(($ZONE_CAP  * (100 - $FUZZ) / 100))
WB_SIZE=$((1 * $BASE_FZ))

TARGET_FZ_BASE=$((1 * $WB_SIZE))
TARGET_FILE_SIZE_MULTIPLIER=2
MAX_BYTES_FOR_LEVEL_BASE=$((1 * $TARGET_FZ_BASE))

echo "ZONE_CAP=$ZONE_CAP bytes"
echo "WB_SIZE=$WB_SIZE bytes"
echo "TARGET_FZ_BASE=$TARGET_FZ_BASE bytes"
echo "TARGET_FILE_SIZE_MULTIPLIER=$TARGET_FILE_SIZE_MULTIPLIER"
echo "MAX_BYTES_FOR_LEVEL_BASE=$MAX_BYTES_FOR_LEVEL_BASE bytes"

NR_KEYS=9000000
#NR_KEYS=100000

echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=$DEV --aux_path=/tmp/zenfs_$DEV --finish_threshold=$FUZZ --force
./db_bench \
    --fs_uri=zenfs://dev:$DEV \
    --key_size=16 \
    --value_size=800 \
    --target_file_size_base=$TARGET_FZ_BASE \
    --write_buffer_size=$WB_SIZE \
    --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE \
    --max_bytes_for_level_multiplier=$TARGET_FILE_SIZE_MULTIPLIER \
    --use_direct_io_for_flush_and_compaction \
    --max_background_jobs=$(nproc) \
    --num=$NR_KEYS \
    --benchmarks=fillrandom,overwrite
