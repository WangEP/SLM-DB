cmake_minimum_required(VERSION 3.0)

project(leveldb C CXX)
set(CMAKE_DEBUG_POSTFIX "d")

set(SNAPPY_LIBRARY "")

string(REGEX MATCH "clang" CLANG ${CMAKE_CXX_COMPILER})

if(CMAKE_COMPILER_IS_GNUCXX OR CLANG)
    find_library(Pthread_LIBRARY pthread)
    find_library(Realtime_LIBRARY rt)
    # find library can be problematic with stdc++ which is why we hardwire the link
    set(Stdcpp_LIBRARY stdc++)
else(CMAKE_COMPILER_IS_GNUCXX OR CLANG)
    set(Pthread_LIBRARY "")
    set(Realtime_LIBRARY "")
    set(Stdcpp_LIBRARY "")
endif(CMAKE_COMPILER_IS_GNUCXX OR CLANG)

include_directories(${Boost_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}
        include)

if(MSVC)
    add_compile_options(
            /D_CRT_SECURE_NO_WARNINGS
            /wd4389 # signed/unsigned mismatch
            /wd4800 # constructor never returns, potential memory leak because of a singleton pattern
            /wd4722 # unreachable code because of singleton pattern
            /wd4702 # bool cast performance warning
    )
else()
    add_compile_options(
            -Wno-sign-compare
            -std=c++17
    )
endif()

add_definitions(-DLEVELDB_ATOMIC_PRESENT)
add_definitions(-DOS_LINUX)
add_definitions(-DLEVELDB_PLATFORM_POSIX)
add_definitions(-fno-builtin-memcmp)
add_definitions(-Wint-to-pointer-cast)

set(LEVEL_DB_FILES
        include/leveldb/c.h
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
        db/c.cc
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
        db/repair.cc
        db/table_cache.cc
        db/table_cache.h
        db/version_edit.cc
        db/version_edit.h
        db/version_set.cc
        db/version_set.h
        db/write_batch.cc
        db/write_batch_internal.h
        db/index.cc
        db/nvm_btree.cc
        db/nvm_btree.h
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
        table/raw_block_builder.cc
        table/raw_block_builder.h
        table/raw_table_builder.cc
        table/raw_table_builder.h
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
        port/port.h
        port/atomic_pointer.h
        table/raw_block_iter.cc
        table/raw_block_iter.h
        util/streamer.h
        util/thread_pool.h
        db/persistent_skiplist.h)

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
        "include/leveldb/c.h;include/leveldb/cache.h;include/leveldb/comparator.h;include/leveldb/db.h;include/leveldb/dumpfile.h;include/leveldb/env.h;include/leveldb/iterator.h;include/leveldb/filter_policy.h;include/leveldb/iterator.h;include/leveldb/options.h;include/leveldb/slice.h;include/leveldb/status.h;include/leveldb/table.h;include/leveldb/table_builder.h;include/leveldb/write_batch.h;include/leveldb/export.h; include/leveldb/global_index.h")

target_link_libraries(leveldb
        PRIVATE
        ${Boost_LIBRARIES}
        ${Pthread_LIBRARY}
        )

INSTALL(TARGETS leveldb ARCHIVE DESTINATION /usr/local/lib PUBLIC_HEADER DESTINATION /usr/local/include/leveldb)

add_executable(insert_bench bench/insert_bench.cc)
target_link_libraries(insert_bench PUBLIC leveldb)

add_executable(read_bench bench/read_bench.cc)
target_link_libraries(read_bench PUBLIC leveldb)

add_executable(simple_bench bench/simple_bench.cc)
target_link_libraries(simple_bench PUBLIC leveldb)

add_executable(sanity_check bench/sanity_check.cc)
target_link_libraries(sanity_check PUBLIC leveldb)

add_executable(file_io_test util/file_io_test.cc)
target_link_libraries(file_io_test PUBLIC leveldb)

add_executable(raw_table_test table/raw_table_test.cc)
target_link_libraries(raw_table_test PUBLIC leveldb)

add_executable(persistent_skiplist_test db/persistent_skiplist_test.cc)
target_link_libraries(persistent_skiplist_test PUBLIC leveldb)