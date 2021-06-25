/*
 * @Author: yangzaorang 
 * @Date: 2021-06-24 16:29:53 
 * @Last Modified by:   yangzaorang 
 * @Last Modified time: 2021-06-24 16:29:53 
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
    rocksdb::Status status = rocksdb::DB::Open(options, "./blobdb_test", &db);
    assert(status.ok());
    std::string key = "key1";
    std::string value = "value1";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    rocksdb::FlushOptions flush_opts;
    status = db->Flush(flush_opts);
    assert(status.ok());
    key = "key2";
    value = "value2";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    status = db->Flush(flush_opts);
    assert(status.ok());
    key = "key3";
    value = "value3";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    status = db->Flush(flush_opts);
    assert(status.ok());
    
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << key << ": " << value << std::endl;
    return 0;
}