/*
 * @Author: yangzaorang 
 * @Date: 2021-03-23 10:25:43 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-03-23 10:28:44
 */

#include <iostream>
#include <cassert>
#include "rocksdb/db.h"

/*
 * inplace_update_support配置项与allow_concurrent_memtable_write配置项冲突
 * SkipList类型的memtable支持inplace_update
 */
int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.allow_concurrent_memtable_write = false;
    std::cout << options.memtable_factory->Name() << std::endl;
    rocksdb::Status status = rocksdb::DB::Open(options, "./memtable_conf_test", &db);
    std::cout << status.ToString() << std::endl;
    assert(status.ok());
    std::string key = "key1";
    std::string value = "value1";
    rocksdb::WriteOptions writeOptions;
    writeOptions.sync = true;
    status = db->Put(writeOptions, key, value);
    key = "key1";
    value = "value2";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << key << ": " << value << std::endl;
    return 0;
}