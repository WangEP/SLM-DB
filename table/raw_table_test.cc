#include "leveldb/env.h"
#include "util/testharness.h"
#include "raw_block_builder.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;

namespace leveldb {

class RAW_TABLE {
};

TEST(RAW_TABLE, Blocks) {
  Options options;
  RawBlockBuilder builder(&options);
  for (int i = 0; i < 20; i++) {
    std::string key = "key";
    key.append(std::to_string(i));
    std::string value = "value";
    value.append(std::to_string(i));
    builder.Add(Slice(key), Slice(value));
  }
  Slice result = builder.Finish();
  char size[32];
  memcpy(size, result.data(), 32);
  ASSERT_EQ(std::stoi(size), result.size()-32);
  //printf("%s", result.data());

};

}

int main() {
  leveldb::test::RunAllTests();
}
