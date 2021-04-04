/*
 * @Author: yangzaorang 
 * @Date: 2021-04-03 21:04:29 
 * @Last Modified by:   yangzaorang 
 * @Last Modified time: 2021-04-03 21:04:29 
 */
#include "rocksdb/db.h"
#include <iostream>
#include <cassert>

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    rocksdb::Status status = rocksdb::DB::Open(options, "./blobdb_test2", &db);
    assert(status.ok());
    std::string key = "sfasdf";
    std::string value;
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << key << ": " << value << std::endl;
    return 0;
}