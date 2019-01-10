#include <cstdlib>
#include <string>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "db/version_control.h"
#include "util/perf_log.h"
#include "util/random.h"
#include "leveldb/table_builder.h"

#define N 64000
#define VAL_SIZE 1024

constexpr std::string ssd_dir = "/mnt/ssd";

Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return Slice(*dst);
}

using namespace leveldb;

int main() {
  Status s;
  Options options;
  options.index = CreateBtreeIndex();
  Env* env = Env::Default();
  Random rand(10);
  VersionEdit* edit = nullptr;

  std::string fname = ssd_dir + "/tempfile";
  WritableFile* file;
  s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return 1;
  }
  TableBuilder* builder = new TableBuilder(options, file, 1);
  std::string s;
  for (uint64_t i = 0; i < N; i++) {
    char k[100];
    snprintf(k, sizeof(k), config::key_format, i);
    Slice key = k;
    Slice value = RandomString(&rand, VAL_SIZE, &s);
    if (prev_key.empty() || options.comparator->Compare(ExtractUserKey(prev_key), ExtractUserKey(key)) != 0) {
      builder->Add(key, value);
      prev_key = key;
    }
  }
  // Finish and check for builder errors
  s = builder->Finish(edit);
  delete builder;
  delete file;
}
