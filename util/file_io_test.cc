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


template <typename Map>
void ParseFile(Map* map_, uint64_t buffer_size, SequentialFile* file, port::Mutex* mutex) {
  Streamer* stream_ = new Streamer(file);
  while (!stream_->eof()) {
    Slice key;
    stream_->Get(&key);
    Slice value;
    stream_->Get(&value);
    mutex->Lock();
    map_->insert({key,value});
    mutex->Unlock();
  }
}


TEST(FILE_IO, Iteration) {
  Status s;
  Env* env = leveldb::Env::Default();
  std::string pathname = "/tmp/testio/test";
  std::vector<SequentialFile*> files;
  uint64_t max_fsize = 0;
  for (int i = 0; i < 4; i++){
    uint64_t fsize = 0;
    ReadAppendFile* file;
    std::string filename = pathname+std::to_string(i);
    s = env->NewReadAppendFile(filename, 2<<19, &file);
    for (int j = 0; j < 1000; j++) {
      std::string key = "key"+std::to_string(j)+std::to_string(i);
      std::string value = "value"+std::to_string(j)+std::to_string(i);
      std::string data = key + "\t" + value + "\t";
      file->Append(data);
      fsize += data.size();
    }
    max_fsize = std::max(max_fsize, fsize);
    file->Finish();
    delete file;
    SequentialFile* read_file;
    s = env->NewSequentialFile(filename, &read_file);
    files.push_back(read_file);
  }
  auto m = std::map<Slice, Slice, std::function<bool(const Slice&, const Slice&)>> {
      [this] (const Slice& a, const Slice& b) {
        return 0 > a.compare(b);
      }
  };
  auto* mem_mutex = new port::Mutex();
  for (auto file: files) {
    //ParseFile(&m, max_fsize, file, mem_mutex);
    Streamer* stream_ = new Streamer(file);
    while (!stream_->eof()) {
      Slice key;
      stream_->Get(&key);
      Slice value;
      stream_->Get(&value);
      mem_mutex->Lock();
      m.insert({key,value});
      mem_mutex->Unlock();
    }
  }
  std::string out_fname = pathname+std::to_string(5);
  WritableFile* output;
  s = env->NewWritableFile(out_fname, &output);
  for (auto iter = m.begin(); iter != m.end(); iter++) {
    Slice key = iter->first;
    Slice value = iter->second;
    std::string data = key.ToString() + "\t" + value.ToString() + "\t";
    output->Append(Slice(data));
  }
  output->Sync();
  output->Close();
  delete output;
};

}

int main() {
  leveldb::test::RunAllTests();
  return 0;
}
