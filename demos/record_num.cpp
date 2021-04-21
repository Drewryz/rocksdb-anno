/*
 * @Author: yangzaorang 
 * @Date: 2021-04-14 11:43:28 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-04-14 11:48:26
 */

#include "rocksdb/db.h"
#include <iostream>
#include <cassert>

int main(int argc, char* argv[]) {
    std::string db_path;
    if (argc > 1) {
		db_path = std::string(argv[1]);
	} else {
        std::cout << "please specify db path" << std::endl;
        return 0;
    }
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    std::string num;
    db->GetProperty("rocksdb.estimate-num-keys", &num);
    std::cout << num << std::endl;
    return 0;
}