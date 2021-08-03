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
TARGET_FILE_SIZE_MULTIPLIER=1
MAX_BYTES_FOR_LEVEL_BASE=$((1 * $TARGET_FZ_BASE))

echo "ZONE_CAP=$ZONE_CAP bytes"
echo "WB_SIZE=$WB_SIZE bytes"
echo "TARGET_FZ_BASE=$TARGET_FZ_BASE bytes"
echo "TARGET_FILE_SIZE_MULTIPLIER=$TARGET_FILE_SIZE_MULTIPLIER"
echo "MAX_BYTES_FOR_LEVEL_BASE=$MAX_BYTES_FOR_LEVEL_BASE bytes"

# test must be executed based on the 6,000,000
NR_KEYS=6000000
#NR_KEYS=1000000

echo deadline > /sys/class/block/$DEV/queue/scheduler

NR_PROC=$(expr $(nproc) / 2)

rm -rf /tmp/zenfs_$DEV
./zenfs mkfs --zbd=$DEV --aux_path=/tmp/zenfs_$DEV --finish_threshold=$FUZZ --force

echo "Press any key to procede"
read

./db_bench \
    --fs_uri=zenfs://dev:$DEV \
    --key_size=16 \
    --value_size=800 \
    --target_file_size_base=$TARGET_FZ_BASE \
    --write_buffer_size=$WB_SIZE \
    --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE \
    --max_bytes_for_level_multiplier=$TARGET_FILE_SIZE_MULTIPLIER \
    --use_direct_io_for_flush_and_compaction \
    --max_background_jobs=$NR_PROC \
    --num=$NR_KEYS \
    --benchmarks=fillseq,overwrite

ls -t1 *.log  | head -n 1 | xargs -i mv {} {}.${NR_KEYS}.$(date +%s)
# ./db_bench \
#    --fs_uri=zenfs://dev:$DEV \
#   --benchmarks=mixgraph \
#   -use_direct_io_for_flush_and_compaction=true \
#   -use_direct_reads=true \
#   -cache_size=268435456 \
#   -keyrange_dist_a=14.18 \
#   -keyrange_dist_b=-2.917 \
#   -keyrange_dist_c=0.0164 \
#   -keyrange_dist_d=-0.08082 \
#   -keyrange_num=30 \
#   -value_k=0.2615 \
#   -value_sigma=25.45 \
#   -iter_k=2.517 \
#   -iter_sigma=14.236 \
#   -mix_get_ratio=0.85 \
#   -mix_put_ratio=0.14 \
#   -mix_seek_ratio=0.01 \
#   -sine_mix_rate_interval_milliseconds=5000 \
#   -sine_a=1000 \
#   -sine_b=0.000073 \
#   -sine_d=4500 \
#   --perf_level=2 \
#   -reads=420000000 \
#   -num=$NR_KEYS \
#   -key_size=48 \
