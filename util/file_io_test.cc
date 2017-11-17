#include <future>
#include "leveldb/env.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;

int main() {
  leveldb::Env* env = leveldb::Env::Default();
  leveldb::ReadAppendFile* file;
  uint64_t file_size = 2 << 20; // 2Mb
  leveldb::Status s = env->NewReadAppendFile("/home/olzhas/Projects/test.ldb", file_size, &file);
  auto writer = std::async(std::launch::async, [](leveldb::ReadAppendFile* f){
    for (auto i = 0; i < 20; i++) {
      std::string s = "valuevalue";
      s.append(std::to_string(i));
      if (f->IsWritable(s.size()))
        f->Append(leveldb::Slice(s));
    }
  }, file);

  writer.wait_for(std::chrono::milliseconds(10));
  for (auto i = 0; i < 3; i++) {
    char *scratch = new char[10];
    leveldb::Slice result;
    file->Read(i*11, 11, &result, scratch);
    printf("%s \n", result.data());
  }
  writer.wait();
  delete file;
  return 0;
}