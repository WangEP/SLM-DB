project(DB)
cmake_minimum_required(VERSION 3.7)
find_package(Threads REQUIRED)
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
set(SRC_FILES
        db/builder.cc
        db/db_impl.cc
        db/db_iter.cc
        db/dbformat.cc
        db/dumpfile.cc
        db/filename.cc
        db/leveldbutil.cc
        db/log_reader.cc
        db/log_writer.cc
        db/memtable.cc
        db/repair.cc
        db/table_cache.cc
        db/version_edit.cc
        db/version_set.cc
        db/write_batch.cc
        db/mock_log.h
        table/block.cc
        table/block_builder.cc
        table/filter_block.cc
        table/format.cc
        table/iterator.cc
        table/merger.cc
        table/table.cc
        table/table_builder.cc
        table/two_level_iterator.cc
        util/arena.cc
        util/bloom.cc
        util/cache.cc
        util/coding.cc
        util/comparator.cc
        util/env.cc
        util/hash.cc
        util/histogram.cc
        util/logging.cc
        util/options.cc
        util/filter_policy.cc
        util/status.cc
        util/pm_arena.cc)

add_library(leveldb STATIC ${SRC_FILES})
add_subdirectory(quartz)