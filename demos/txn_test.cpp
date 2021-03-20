#include <cstdio>
#include <string>
#include <vector>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

using namespace rocksdb;

std::string kDBPath = "./txn_test";


int main() {
    // 基本配置,事务相关操作需要TransactionDB句柄
    Options options;
    options.create_if_missing = true;
    TransactionDBOptions txn_db_options;
    TransactionDB* txn_db;

    // 用支持事务的方式opendb
    Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
    assert(s.ok());

    WriteOptions write_options;
    // 创建一个事务上下文, 类似MySQL的start transaction
    Transaction* txn = txn_db->BeginTransaction(write_options);
    // 直接写入新数据
    s = txn->Put("abc", "def");
    assert(s.ok());
    ReadOptions read_options;
    std::string value;
    // ForUpdate写，类似MySQL的select ... for update
    s = txn->GetForUpdate(read_options, "abc", &value);
    std::cout << value << std::endl;
    assert(s.ok());

    s = txn->Commit();      // or txn->Rollback();
    assert(s.ok());

    return 0;
}




