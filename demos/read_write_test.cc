/*
 * @Author: yangzaorang 
 * @Date: 2021-03-04 10:48:03 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-03-06 17:57:38
 */

#include <iostream>
#include <cassert>
#include "rocksdb/db.h"

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "./testdb2", &db);
    assert(status.ok());
    std::string key = "key1";
    std::string value = "value1";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());

    rocksdb::FlushOptions flush_opts;
    status = db->Flush(flush_opts);
    assert(status.ok());

    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << key << ": " << value << std::endl;
    return 0;
}