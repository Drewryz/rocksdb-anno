/*
 * @Author: yangzaorang 
 * @Date: 2021-05-06 16:05:37 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-05-06 16:16:06
 */

#include <cassert>
#include <stdio.h>
#include <thread>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include <iostream>

using namespace rocksdb;

int main() {
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = 1;
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(0, 0, false);
    table_options.block_cache = cache;

    rocksdb::Options options;
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, "./block_cache_test", &db);
    assert(status.ok());
    status = db->Put(rocksdb::WriteOptions(), "12345", "678910");
    assert(status.ok());

    rocksdb::ReadOptions read_options;
    rocksdb::Iterator* iter = db->NewIterator(read_options);
    iter->Seek("12345");
    assert(status.ok());
}