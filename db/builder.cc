// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <table/raw_table_builder.h>
#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/index.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    RawTableBuilder* builder = new RawTableBuilder(options, file, meta->number);
    meta->smallest.DecodeFrom(ExtractUserKey(iter->key()));
    // wait if previous compaction indexing not finished
    Index* index = options.index;
    while (!index->Acceptable()) {  }
    index->CompactionStarted();
    for (; iter->Valid(); iter->Next()) {
      Slice key = ExtractUserKey(iter->key());
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
      // prep
      KeyAndMeta key_and_meta;
      key_and_meta.key = stoi(key.data());
      key_and_meta.meta = new IndexMeta(builder->FileSize() - iter->value().size() - 1,
                                iter->value().size(), meta->number);
      index->AsyncInsert(key_and_meta);
    }

    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    s = builder->Finish();
    delete builder;
    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    index->CompactionFinished();
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
