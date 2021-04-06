/*
 * @Author: yangzaorang 
 * @Date: 2021-04-03 21:04:29 
 * @Last Modified by:   yangzaorang 
 * @Last Modified time: 2021-04-03 21:04:29 
 */
#include "rocksdb/db.h"
#include "utilities/blob_db/blob_db.h"
#include <iostream>
#include <cassert>

int main() {
    rocksdb::blob_db::BlobDB *db;
    rocksdb::Options options;
    rocksdb::blob_db::BlobDBOptions bdb_options;
    options.create_if_missing = true;
    bdb_options.min_blob_size = 0;
    rocksdb::Status status = rocksdb::blob_db::BlobDB::Open(options, bdb_options, "./blobdb_test4", &db);
    assert(status.ok());
    std::string key = "key1";
    std::string value = "value1";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << key << ": " << value << std::endl;
    return 0;
}