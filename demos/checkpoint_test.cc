#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include <iostream>

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    std::string dbname = "main";
    options.wal_dir = dbname + "_wal";    
    options.db_paths.emplace_back(dbname + "_db",
                                std::numeric_limits<uint64_t>::max());
    rocksdb::Status status = rocksdb::DB::Open(options, dbname, &db);
    std::cout << status.ToString() << std::endl;
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
    
    std::string checkpoint_dir = "checkpont_dir";
    rocksdb::Checkpoint* checkpoint;
    std::cout << "Start Checkpoint" << std::endl;
    rocksdb::Checkpoint::Create(db, &checkpoint);
    status = checkpoint->CreateCheckpoint(checkpoint_dir);
    std::cout << status.ToString() << std::endl;

    std::cout << "!!!!!" << std::endl;
}