#include <future>
#include "leveldb/env.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;

int main() {
  leveldb::Env* env = leveldb::Env::Default();
  leveldb::ReadAppendFile* file;
  uint64_t file_size = 2 << 20; // 2Mb
  leveldb::Status s = env->NewReadAppendFile("/home/olzhas/Projects/test.ldb", file_size, &file);
  std::vector<uint64_t> offsets;
  auto writer = std::async(std::launch::async, [&offsets](leveldb::ReadAppendFile* f){
    for (auto i = 0; i < 300; i++) {
      std::string s = "valuevalue";
      offsets.push_back(f->Size());
      s.append(std::to_string(i));
      if (f->IsWritable(s.size()))
        f->Append(leveldb::Slice(s));
    }
  }, file);

  writer.wait_for(std::chrono::milliseconds(10));
  for (auto i = 0; i < 300; i++){
    char *scratch = new char[10];
    leveldb::Slice result;
    file->Read(offsets[i], 10, &result, scratch);
    printf("%d %s \n", i, result.data());
  }
  writer.wait();
  delete file;
  return 0;
}