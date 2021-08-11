/*
 * @Author: yangzaorang 
 * @Date: 2021-07-06 16:40:39 
 * @Last Modified by: yangzaorang
 * @Last Modified time: 2021-07-06 18:10:10
 */

#include "rocksdb/db.h"
#include <iostream>
#include <cassert>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/utilities/backupable_db.h"
// #include "util/file_checksum_helper.h"
// #include "rocksdb/file_checksum.h"

using namespace rocksdb;

int main() {
    Options options;                                                                                  
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;   
    // FileChecksumGenCrc32cFactory* file_checksum_gen_factory = new FileChecksumGenCrc32cFactory();
    // options.file_checksum_gen_factory.reset(file_checksum_gen_factory); 
    options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
                                                      
    DB* db;
    Status s = DB::Open(options, "db", &db);
    assert(s.ok());
    std::string key = "key1";
    std::string value = "value1";
    s = db->Put(rocksdb::WriteOptions(), key, value);
    assert(s.ok());
    rocksdb::FlushOptions flush_opts;
    s = db->Flush(flush_opts);
    assert(s.ok());

    BackupEngine* backup_engine;
    s = BackupEngine::Open(Env::Default(), BackupableDBOptions("db_backup"), &backup_engine);
    assert(s.ok());
    s = backup_engine->CreateNewBackup(db);
    assert(s.ok());
    s = db->Put(rocksdb::WriteOptions(), "key2", "value2"); // make some more changes
    assert(s.ok());
    s = db->Flush(flush_opts);
    assert(s.ok());
    s = backup_engine->CreateNewBackup(db);
    assert(s.ok());

    std::vector<BackupInfo> backup_info;
    backup_engine->GetBackupInfo(&backup_info);

    // you can get IDs from backup_info if there are more than two
    s = backup_engine->VerifyBackup(1 /* ID */);
    assert(s.ok());
    s = backup_engine->VerifyBackup(2 /* ID */);
    assert(s.ok());
    delete db;
    delete backup_engine;

    // Restore
    BackupEngineReadOnly* backup_engine_ro;
    s = BackupEngineReadOnly::Open(Env::Default(), BackupableDBOptions("db_backup"), &backup_engine_ro);
    assert(s.ok());
    s = backup_engine_ro->RestoreDBFromBackup(1, "db_backup_restore_1", "db_backup_restore_1");
    assert(s.ok());
    s = backup_engine_ro->RestoreDBFromBackup(2, "db_backup_restore_2", "db_backup_restore_2");
    assert(s.ok());
    delete backup_engine_ro;

}