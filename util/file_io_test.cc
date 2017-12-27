#include <future>
#include <port/port.h>
#include <map>
#include "leveldb/env.h"
#include "util/testharness.h"
#include "streamer.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;

namespace leveldb {

class FILE_IO {};

TEST(FILE_IO, Test1) {
  Env *env = leveldb::Env::Default();
  MemoryIOFile *file;
  uint64_t file_size = 1 << 20; // 2Mb - default size
  Status s = env->NewMemoryIOFile("/tmp/test1.ldb", file_size, &file);
  ASSERT_OK(s);
  std::vector<uint64_t> offsets;
  auto writer = std::async(std::launch::async, [&offsets](leveldb::MemoryIOFile *f) {
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
  MemoryIOFile *file;
  uint64_t file_size = 1 << 20; // 2Mb - default size
  Status s = env->NewMemoryIOFile("/tmp/test1.ldb", file_size, &file);
  ASSERT_OK(s);
  std::vector<uint64_t> offsets;
  auto writer = std::async(std::launch::async, [&offsets](leveldb::MemoryIOFile *f) {
    for (auto i = 0; i < 300; i++) {
      offsets.push_back(f->Size());
      std::string s = "valuevalue";
      s.append(std::to_string(i));
      if (f->IsWritable(s.size()))
        f->Append(leveldb::Slice(s));
    }
  }, file);
  writer.wait();
  uint64_t size = file->Size()-32;
  s = file->Finish();
  delete file;
  ASSERT_OK(s);
  RandomAccessFile *rfile;
  s = env->NewRandomAccessFile("/tmp/test1.ldb", &rfile);
  Slice result;
  char* scratch = new char[32];
  rfile->Read(0, 32, &result, scratch);
  uint64_t written_size = std::stoul(result.ToString());
  ASSERT_EQ(size, written_size);
};

}

int main() {
  leveldb::test::RunAllTests();
  return 0;
}
