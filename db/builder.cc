// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "version.h"
#include "version_edit.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta,
                  VersionEdit* edit) {
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
    TableBuilder* builder = new TableBuilder(options, file, meta->number);
    meta->smallest.DecodeFrom(iter->key());
    Slice prev_key;
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      Slice value = iter->value();
      if (prev_key.empty() || options.comparator->Compare(ExtractUserKey(prev_key), ExtractUserKey(key)) != 0) {
        builder->Add(key, value);
        prev_key = key;
      }
    }
    meta->largest.DecodeFrom(prev_key);
    meta->total = builder->NumEntries();
    meta->alive = builder->NumEntries();

    // Finish and check for builder errors
    s = builder->Finish(edit);
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
