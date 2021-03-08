/*
 * @Author: yangzaorang 
 * @Date: 2021-03-06 17:19:04 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-03-06 17:31:43
 */

#include <cassert>
#include <stdio.h>
#include <thread>
#include "rocksdb/db.h"

const int INSERT_NUM = 100*10000;
const int THREAD_NUM = 20;

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

void put_func(rocksdb::DB* db, int insert_num, int id) {
    printf("%d start\n", id);
    for (int i=0; i<insert_num; i++) {
        if ( i % 1000 == 0) {
            printf("[%d] insert num: %d\n", id, i);
        }
        db->Put(rocksdb::WriteOptions(), randomstr(), randomstr());
    }
    printf("%d end\n", id);
}

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "./testdb", &db);
    assert(status.ok());
    int insert_num_per_thread = INSERT_NUM / THREAD_NUM;
    std::thread** thread_array = new std::thread*[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; ++i) {
        thread_array[i] = new std::thread(put_func, db, insert_num_per_thread, i);
    }
    for (int i = 0; i <THREAD_NUM; ++i) {
        thread_array[i]->join();
    }
    delete db;
    delete thread_array;
    return 0;
}