#include "iostream"
#include "leveldb/db.h"
#include "leveldb/status.h"

int main(int argc, char** argv) {
  std::cout << "Hello, hac" << std::endl;
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