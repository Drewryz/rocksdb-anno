/*
 * @Author: yangzaorang 
 * @Date: 2021-04-21 15:17:58 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-04-21 16:20:50
 */

#include <iostream>
#include <cassert>
#include "rocksdb/merge_operator.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"

/*
 * rocksdb的Merge接口，仅仅只是将key和value写到了wal/memtable/sst中
 * 
 * merge这个动作真正触发是在读这个key的时候
 * 
 * 现在我们有三个值,key, old_value, new_value, 那么我们真正merge的时候应该怎么处理这三个值呢? 
 * 需要提供一个函数，用来规定merge这个动作。
 * 
 * 通过实现AssociativeMergeOperator接口或者MergeOperator接口，可以规定merge动作
 */
class MyMerge : public rocksdb::AssociativeMergeOperator {
 public:
  /*
   * key: 入参，对哪个key做merge
   * existing_value: 入参，执行到当前merge操作时，key对应的value
   * value: 入参，此次merge操作要merge到existing_value的value
   * new_value: 出参，存储这次merge操作的结果
   * 
   * 返回值为true，表示此次merge操作成功。
   * 返回fasle，merge失败，整个Get的调用链都会返回error，最终导致Get失败。
   */
  virtual bool Merge(const rocksdb::Slice& key,
                     const rocksdb::Slice* existing_value,
                     const rocksdb::Slice& value,
                     std::string* new_value,
                     rocksdb::Logger* logger) const override {
    std::cout << "merge start" << std::endl;
    std::cout << "key: " << key.ToString() << std::endl;
    std::cout << "existing_value: " << existing_value->ToString() << std::endl;
    std::cout << "value: " << value.ToString() << std::endl;
    std::cout << "*new_value: " << *new_value << std::endl;
    std::cout << "(*new_value).size: " << (*new_value).size() << std::endl;
    new_value->append("uio");
    std::cout << "merge end" << std::endl;
    return false
    ;
  }
  const char* Name() const override { return "MyMerge"; }
};

class MyMerge2 : public rocksdb::MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    std::cout << "79000" << std::endl;
    for (const rocksdb::Slice& m : merge_in.operand_list) {
        std::cout << m.ToString() << std::endl;
    }
    std::cout << "79001" << std::endl;
    return true;
  }

  const char* Name() const override { return "MyMerge2"; }
};

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new MyMerge());
    // options.merge_operator.reset(new MyMerge2());
    rocksdb::Status status = rocksdb::DB::Open(options, "./merge_test", &db);
    assert(status.ok());
    std::string key = "key1";
    std::string value = "value1";
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << key << ": " << value << std::endl;
    
    status = db->Merge(rocksdb::WriteOptions(), key, "abc");
    assert(status.ok());
    status = db->Merge(rocksdb::WriteOptions(), key, "def");
    assert(status.ok());
    std::cout << "+++++++++++++++++++++++++++++" << std::endl;
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout << "-----------------------------" << std::endl;
    std::cout << key << ": " << value << std::endl;

    return 0;
}