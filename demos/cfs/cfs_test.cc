#include <cstdio>
#include <string>
#include <vector>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/checkpoint.h"

using namespace rocksdb;

std::string kDBPath = "./db";

int main() {
  Options options;
  options.create_if_missing = true;
  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  ColumnFamilyHandle* cf;
  s = db->CreateColumnFamily(ColumnFamilyOptions(), "new_cf", &cf);
  assert(s.ok());

  delete cf;
  delete db;

  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions()));
  column_families.push_back(ColumnFamilyDescriptor(
      "new_cf", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;
  DBOptions db_options;
  db_options.wal_dir = kDBPath + "_wal";
  s = DB::Open(db_options, kDBPath, column_families, &handles, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), handles[1], Slice("key"), Slice("value"));
  assert(s.ok());
  std::string value;
  s = db->Get(ReadOptions(), handles[1], Slice("key"), &value);
  assert(s.ok());

  // atomic write
  WriteBatch batch;
  batch.Put(handles[0], Slice("key2"), Slice("value2"));
  batch.Put(handles[1], Slice("key3"), Slice("value3"));
  batch.Delete(handles[0], Slice("key"));
  s = db->Write(WriteOptions(), &batch);
  assert(s.ok());

  Checkpoint* checkpoint = nullptr;
  s = Checkpoint::Create(db, &checkpoint);
  assert(s.ok());

  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);
  std::string snapshot_name = kDBPath + "_ck";

  s = checkpoint->CreateCheckpoint(snapshot_name, std::numeric_limits<uint64_t>::max());
  assert(s.ok());

  for (auto handle : handles) {
    delete handle;
  }
  delete db;

  return 0;
}