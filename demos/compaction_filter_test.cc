/*
 * @Author: yangzaorang 
 * @Date: 2021-04-21 17:15:31 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-04-21 17:27:16
 */


// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <iostream>

class MyFilter : public rocksdb::CompactionFilter {
 public:
  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override {
    fprintf(stderr, "Filter(%s, %s)\n", key.ToString().c_str(), existing_value.ToString().c_str());
    ++count_;
    assert(*value_changed == false);
    return false;
  }

  const char* Name() const override { return "MyFilter"; }

  mutable int count_ = 0;
};

int main() {
  rocksdb::DB* raw_db;
  rocksdb::Status status;

  MyFilter filter;

  system("rm ./compactionfiltertest");
  rocksdb::Options options;
  options.create_if_missing = true;
  options.compaction_filter = &filter;
  status = rocksdb::DB::Open(options, "./compactionfiltertest", &raw_db);
  assert(status.ok());
  std::unique_ptr<rocksdb::DB> db(raw_db);

  rocksdb::WriteOptions wopts;
  db->Put(wopts, "0", "01");  // This is filtered out
  db->Put(wopts, "1", "a");
  db->Put(wopts, "1", "b");
  db->Put(wopts, "1", "c");
  db->Put(wopts, "1", "d");
  db->Put(wopts, "3", "31");
  db->Put(wopts, "0", "02");
  db->Put(wopts, "3", "32");
  db->Put(wopts, "4", "41");
  db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  fprintf(stderr, "filter.count_ = %d\n", filter.count_);
}
