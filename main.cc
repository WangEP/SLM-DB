#include <iostream>
#include "leveldb/db.h"
#include "leveldb/table_builder.h"

uint64_t clflush_cnt = 0;

void standard_db_test() {
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  std::string dbpath = "/tmp/testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  std::string key = "test";
  std::string val = "test_value";
  status = db->Put(leveldb::WriteOptions(), key, val);
  if (status.ok())
    std::cout << "success put\n";
  std::string outval;
  status = db->Get(leveldb::ReadOptions(), key, &outval);
  if (status.ok())
    std::cout << "success get " << outval << "\n";
  std::cout << key << " : " << outval << std::endl;
}

void sst_db_test() {
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  std::string dbpath = "/tmp/testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  status = db->Put(leveldb::WriteOptions(), "a1", "value1");
  status = db->Put(leveldb::WriteOptions(), "b2", "value2");
  status = db->Put(leveldb::WriteOptions(), "c3", "value3");
  status = db->Put(leveldb::WriteOptions(), "d4", "value4");
  status = db->Put(leveldb::WriteOptions(), "e5", "value5");
  db->CompactRange(NULL, NULL);
  std::string val;
  status = db->Get(leveldb::ReadOptions(), "d4", &val);
  std::cout << val << "\n";
}

int main(int argc, char** argv) {
  //standard_db_test();
  sst_db_test();
}