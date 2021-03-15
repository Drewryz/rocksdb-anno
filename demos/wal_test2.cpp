/*
 * @Author: yangzaorang 
 * @Date: 2021-03-15 11:43:09 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-03-15 11:47:32
 */

#include <cstdio>
#include <string>
#include <vector>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

std::string kDBPath = "./wal_test";

int main() {
  Options options;
  options.create_if_missing = true;
  DB* db;
  /*
   * 非只读模式下，如果DB有多个列族，必须将其全部打开，不然会报错，eg:
   * You have to open all column families. Column families not opened
   */
  Status s = DB::Open(options, kDBPath, &db);
  std::cout << s.ToString() << std::endl;
  assert(s.ok());

  delete db;

  return 0;
}
