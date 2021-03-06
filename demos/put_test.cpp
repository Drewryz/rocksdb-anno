/*
 * @Author: yangzaorang 
 * @Date: 2021-03-06 17:19:04 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-03-06 17:31:43
 */


#include <cassert>
#include "rocksdb/db.h"

const int INSERT_NUM = 1000*10000;

char* randomstr()
{
	static char buf[1024];
	int len = rand() % 768 + 255;
	for (int i = 0; i < len; ++i) {
		buf[i] = 'A' + rand() % 26;
	}
	buf[len] = '\0';
	return buf;
}

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());

    for (int i=0; i<INSERT_NUM; i++) {
        db->Put(rocksdb::WriteOptions(), randomstr(), randomstr());
    }
    delete db;
    return 0;
}