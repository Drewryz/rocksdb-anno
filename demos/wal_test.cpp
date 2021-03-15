/*
 * @Author: yangzaorang 
 * @Date: 2021-03-15 11:43:09 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-03-15 11:47:32
 */

#include <cstdio>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

std::string kDBPath = "./wal_test";

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

  // open DB with two column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new one, too
  column_families.push_back(ColumnFamilyDescriptor(
      "new_cf", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;
  s = DB::Open(DBOptions(), kDBPath, column_families, &handles, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), handles[0], Slice("key1"), Slice("value1"));
  assert(s.ok());
  s = db->Put(WriteOptions(), handles[0], Slice("key2"), Slice("value2"));
  assert(s.ok());
  s = db->Put(WriteOptions(), handles[1], Slice("key3"), Slice("value3"));
  assert(s.ok());
  s = db->Put(WriteOptions(), handles[1], Slice("key4"), Slice("value4"));
  assert(s.ok());

  db->Flush(rocksdb::FlushOptions(), handles[1]);

  // close db
  for (auto handle : handles) {
    delete handle;
  }
  delete db;

  return 0;
}
