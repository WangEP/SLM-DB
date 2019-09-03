# SLM-DB: Single-Level Key-Value Store with Persistent Memory

Paper and presentation of SLM-DB presented at USENIX FAST'19 <https://www.usenix.org/conference/fast19/presentation/kaiyrakhmet>

Based on original source of LevelDB <https://github.com/google/leveldb>

### Dependencies

- cmake
- DMDK - https://github.com/pmem/pmdk
- ndctl - https://github.com/pmem/ndctl

### Installation
```
mkdir build
cd build
cmake ..
make -j
```

### db_bench
There are additional options for db_bench. SLM-DB has no recovery functionality implemented yet, thus `db_bench` need re-run fill database for every benchmark
```
 --nvm_dir=<dir>          # path to PMEM directory
 --nvm_size=<size>        # NVM pool size
 --merge_ratio=<ratio>    # live/total key ratio in file to GC
 --range_size=<size>      # key count for range query benchmark (from i to i + range_size)
 --trace_dir=<dir>        # path to YCSB benchmark trace
```
