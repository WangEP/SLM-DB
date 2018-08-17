#include <iostream>
#include <chrono>
#include <thread>
#include <util/perf_log.h>
#include "leveldb/db.h"
#include "index/btree_index.h"
#include "util/testharness.h"


class SanityCheck {};

leveldb::DB* db;
int seq_inserts = 10000000;
int rand_inserts = 3000000;
int rand_reads = 300000;

TEST(SanityCheck, Create) {
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  const char *c = "/tmp/testdb";
  std::string dbpath(c);
  leveldb::DestroyDB(dbpath, leveldb::Options());
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  ASSERT_OK(status);
}

TEST(SanityCheck, SequentialWrite) {
  leveldb::Status status;
  for (auto i = 0; i < seq_inserts; i++) {
    int k = i;
    char key[100];
    snprintf(key, sizeof(key), "%016d", k);
    std::string value = std::string("valuevalue").append(key);
    status = db->Put(leveldb::WriteOptions(), key, value);
    ASSERT_OK(status);
  }
}

TEST(SanityCheck, RandomWrite) {
  leveldb::Status status;
  leveldb::Random rand(time(0));
  for (auto i = 0; i < rand_inserts; i++) {
    int k = rand.Next() % seq_inserts;
    char key[100];
    snprintf(key, sizeof(key), "%016d", k);
    std::string value = std::string("valuevalue").append(key);
    status = db->Put(leveldb::WriteOptions(), key, value);
    ASSERT_OK(status);
  }
}

TEST(SanityCheck, SequentialRead) {
  auto iter = db->NewIterator(leveldb::ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string v = "valuevalue" + iter->key().ToString();
    std::string value = iter->value().ToString();
    ASSERT_OK(iter->status());
    ASSERT_EQ(v, value);
  }
}

TEST(SanityCheck, RandomRead) {
  leveldb::Status status;
  leveldb::Random rand(time(0));
  for (auto i = 0; i < rand_reads; i++) {
    int k = rand.Next() % seq_inserts;
    char key[100];
    snprintf(key, sizeof(key), "%016d", k);
    std::string v = std::string("valuevalue").append(key);
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key, &value);
    ASSERT_OK(status);
    ASSERT_EQ(v, value);
  }
}


int main(int argc, char** argv) {
#ifdef PERF_LOG
  leveldb::benchmark::CreatePerfLog();
#endif
  leveldb::test::RunAllTests();
#ifdef PERF_LOG
  leveldb::benchmark::ClosePerfLog();
#endif

}