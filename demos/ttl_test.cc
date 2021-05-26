/*
 * @Author: yangzaorang 
 * @Date: 2021-05-26 17:22:21 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-05-26 17:46:14
 */

#include <iostream>
#include <cassert>
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include <chrono>
#include <unistd.h>

int main() {
    rocksdb::DBWithTTL* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    int32_t ttl = 2;
    rocksdb::DBWithTTL::Open(options, "./ttl_test", &db, ttl);
    std::string key = "key1";
    std::string value = "value1";
    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
    usleep(1000000*5);
    /*
     * rocksdb的TTL特性是通过compaction实现的，如果没有触发compaction，
     * 那么即使数据过期了，仍旧可以查到过期的数据
     */
    db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
    std::string value2;
    status = db->Get(rocksdb::ReadOptions(), key, &value2);
    std::cout << status.ToString() << std::endl;
    std::cout << key << ": " << value2 << std::endl;
}