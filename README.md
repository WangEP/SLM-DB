# PMIndexDB

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
make -j 4
```

### db_bench
There are additional options for db_bench
```
 --nvm_dir=/dir         # path to PMEM directory
 --nvm_size=size        # NVM pool size
 --merge_ratio=ratio    # live/total key ratio in file to GC
 ```