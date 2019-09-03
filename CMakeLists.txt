cmake_minimum_required(VERSION 3.0)

project(SLM-DB C CXX)
set(CMAKE_DEBUG_POSTFIX "d")

set(SNAPPY_LIBRARY "")

string(REGEX MATCH "clang" CLANG ${CMAKE_CXX_COMPILER})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_library(Pthread_LIBRARY pthread)
find_library(Realtime_LIBRARY rt)
find_package(PMDK REQUIRED)
set(Stdcpp_LIBRARY stdc++)

include_directories(
        ${VMEM_INCLUDE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_compile_options(
        -Wno-sign-compare
        -std=c++17
        -O2
        -DNDEBUG
)

add_definitions(-DLEVELDB_ATOMIC_PRESENT)
add_definitions(-DOS_LINUX)
add_definitions(-DLEVELDB_PLATFORM_POSIX)
add_definitions(-fno-builtin-memcmp)
add_definitions(-Wint-to-pointer-cast)
add_definitions(-DPERF_LOG=1)

set(LEVEL_DB_FILES
        include/leveldb/cache.h
        include/leveldb/comparator.h
        include/leveldb/db.h
        include/leveldb/dumpfile.h
        include/leveldb/env.h
        include/leveldb/iterator.h
        include/leveldb/filter_policy.h
        include/leveldb/iterator.h
        include/leveldb/options.h
        include/leveldb/slice.h
        include/leveldb/status.h
        include/leveldb/table.h
        include/leveldb/table_builder.h
        include/leveldb/write_batch.h
        include/leveldb/export.h
        include/leveldb/index.h
        include/leveldb/persistant_pool.h
        include/leveldb/string.h
        db/builder.cc
        db/builder.h
        db/db_impl.cc
        db/db_impl.h
        db/db_iter.cc
        db/db_iter.h
        db/dbformat.cc
        db/dbformat.h
        db/dumpfile.cc
        db/filename.cc
        db/filename.h
        db/log_format.h
        db/log_reader.cc
        db/log_reader.h
        db/log_writer.cc
        db/log_writer.h
        db/mock_log.h
        db/skiplist.h
        db/snapshot.h
        db/memtable.cc
        db/memtable.h
        db/table_cache.cc
        db/table_cache.h
        db/write_batch.cc
        db/write_batch_internal.h
        db/version.cc
        db/version.h
        db/version_control.cc
        db/version_control.h
        db/version_edit.cc
        db/version_edit.h
        db/logger.cc
        db/logger.h
        table/block.cc
        table/block.h
        table/block_builder.cc
        table/block_builder.h
        table/filter_block.cc
        table/filter_block.h
        table/format.cc
        table/format.h
        table/iterator.cc
        table/iterator_wrapper.h
        table/merger.cc
        table/merger.h
        table/table.cc
        table/table_builder.cc
        table/two_level_iterator.cc
        table/two_level_iterator.h
        util/string.cc
        util/arena.cc
        util/arena.h
        util/bloom.cc
        util/cache.cc
        util/coding.cc
        util/coding.h
        util/comparator.cc
        util/crc32c.cc
        util/crc32c.h
        util/env.cc
        util/filter_policy.cc
        util/hash.cc
        util/hash.h
        util/histogram.cc
        util/histogram.h
        util/logging.cc
        util/logging.h
        util/mutexlock.h
        util/options.cc
        util/random.h
        util/status.cc
        util/persist.h
        util/testharness.h
        util/testharness.cc
        util/thread_pool.h
        util/int_based_comparator.h
        util/testutil.h
        util/testutil.cc
        util/perf_log.h
        util/perf_log.cc
        util/persistant_pool.cc
        port/port.h
        port/atomic_pointer.h
        index/btree_index.cc
        index/btree_index.h
        index/index_iterator.cc
        index/index_iterator.h
        index/ff_btree.cc
        index/ff_btree.h
        index/ff_btree_iterator.cc
        index/ff_btree_iterator.h
        index/index.cc)

if(WIN32)
    list(APPEND LEVEL_DB_FILES
            port/port_win.h
            port/port_win.cc
            util/win_logger.h
            util/win_logger.cc
            util/env_boost.cc)
else()
    list(APPEND LEVEL_DB_FILES
            port/port_posix.h
            port/port_posix.cc
            util/posix_logger.h
            util/env_posix.cc)
endif()

add_library(leveldb ${LEVEL_DB_FILES})

target_include_directories(leveldb
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        )

set_target_properties(leveldb PROPERTIES PUBLIC_HEADER
        "include/leveldb/c.h;
        include/leveldb/cache.h;
        include/leveldb/comparator.h;
        include/leveldb/db.h;
        include/leveldb/dumpfile.h;
        include/leveldb/env.h;
        include/leveldb/iterator.h;
        include/leveldb/filter_policy.h;
        include/leveldb/iterator.h;
        include/leveldb/options.h;
        include/leveldb/slice.h;
        include/leveldb/status.h;
        include/leveldb/table.h;
        include/leveldb/table_builder.h;
        include/leveldb/write_batch.h;
        include/leveldb/export.h;
        include/leveldb/index.h
        include/leveldb/persistant_pool.h")

target_link_libraries(leveldb
        PRIVATE
        ${Pthread_LIBRARY}
        ${PMEM_LIBRARY}
        ${PMEMCTO_LIBRARY}
        ${PMEMOBJ_LIBRARY}
        ${PMEMLOG_LIBRARY}
        ${VMEM_LIBRARY}
        )

INSTALL(TARGETS leveldb ARCHIVE DESTINATION /usr/local/lib PUBLIC_HEADER DESTINATION /usr/local/include/leveldb)

add_executable(sanity_check db/sanity_check.cc)
target_link_libraries(sanity_check PUBLIC leveldb)

add_executable(db_bench db/db_bench.cc)
target_link_libraries(db_bench PUBLIC leveldb)

add_executable(ff_btree_test index/ff_btree_test.cc)
target_link_libraries(ff_btree_test PUBLIC leveldb)

add_executable(memtable_bench bench/memtable_bench.cc)
target_link_libraries(memtable_bench PUBLIC leveldb)

add_executable(btree_bench bench/btree_bench.cc)
target_link_libraries(btree_bench PUBLIC leveldb)

add_executable(file_bench bench/file_bench.cc)
target_link_libraries(file_bench PUBLIC leveldb)

add_executable(pm_stream util/persist_stream.cc)
target_link_libraries(pm_stream ${VMEM_LIBRARY})
