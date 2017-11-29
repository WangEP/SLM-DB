#include <future>
#include "leveldb/env.h"
#include "util/testharness.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;

namespace leveldb {

class FILE_IO {};

TEST(FILE_IO, Test1) {
  Env *env = leveldb::Env::Default();
  ReadAppendFile *file;
  uint64_t file_size = 2 << 20; // 2Mb - default size
  Status s = env->NewReadAppendFile("/tmp/test1.ldb", file_size, &file);
  ASSERT_OK(s);
  std::vector<uint64_t> offsets;
  auto writer = std::async(std::launch::async, [&offsets](leveldb::ReadAppendFile *f) {
    for (auto i = 0; i < 300; i++) {
      offsets.push_back(f->Size());
      std::string s = "valuevalue";
      s.append(std::to_string(i));
      if (f->IsWritable(s.size()))
        f->Append(leveldb::Slice(s));
    }
  }, file);

  writer.wait_for(std::chrono::milliseconds(10));
  for (auto i = 0; i < 300; i++) {
    std::string s = "valuevalue";
    char *scratch = new char[10];
    Slice result;
    file->Read(offsets[i], 10, &result, scratch);
    ASSERT_EQ(s, result.ToString());
  }
  writer.wait();
  delete file;
};

TEST(FILE_IO, Test2) {
  Env *env = leveldb::Env::Default();
  ReadAppendFile *file;
  uint64_t file_size = 2 << 20; // 2Mb - default size
  Status s = env->NewReadAppendFile("/tmp/test1.ldb", file_size, &file);
  ASSERT_OK(s);
  std::vector<uint64_t> offsets;
  auto writer = std::async(std::launch::async, [&offsets](leveldb::ReadAppendFile *f) {
    for (auto i = 0; i < 300; i++) {
      offsets.push_back(f->Size());
      std::string s = "valuevalue";
      s.append(std::to_string(i));
      if (f->IsWritable(s.size()))
        f->Append(leveldb::Slice(s));
    }
  }, file);
  writer.wait();
  s = file->Finish();
  ASSERT_OK(s);
  uint64_t size = file->Size()-32;
  Slice result;
  char* scratch = new char[32];
  file->Read(0, 32, &result, scratch);
  uint64_t written_size = std::stoul(result.ToString());
  ASSERT_EQ(size, written_size);
};

}

int main() {
  leveldb::test::RunAllTests();
  return 0;
}
