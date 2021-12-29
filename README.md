# ZenFS with Garbage Collection

## Overview

This program is based on Western Digital's [zenfs](https://github.com/westerndigitalcorporation/zenfs) project. This project was added the garbage collection and some hot/cold classification logic.

If you want to more specific about this project, please look [this paper](https://www.mdpi.com/2076-3417/11/24/11842)

## How to compile

First, you must install the libzbd based on [this repository](https://github.com/westerndigitalcorporation/libzbd).
You can compile this project by using the below commands.

```bash
make -j$(nproc) clean && DEBUG_LEVEL=1 make -j$(nproc) db_bench zenfs
```

## How to run

You can benchmark this program by using below commands.

```bash
sudo ./run_zenfs_db_bench.sh
```

## Troubleshooting

Unless you follow the installation instruction cannot compile the libzbd, then you must check all packages(e.g. make) for compilation are installed.

If you encounter an error related to the RocksDB, you must clear your build results and follow this [link](https://github.com/facebook/rocksdb/blob/master/INSTALL.md) step by step.
