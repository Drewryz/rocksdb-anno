//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "utilities/checkpoint/checkpoint_impl.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <tuple>
#include <vector>
#include <iostream>

#include "db/wal_manager.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/utilities/checkpoint.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/file_checksum_helper.h"

namespace ROCKSDB_NAMESPACE {

Status Checkpoint::Create(DB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new CheckpointImpl(db);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(const std::string& /*checkpoint_dir*/,
                                    uint64_t /*log_size_for_flush*/,
                                    uint64_t* /*sequence_number_ptr*/) {
  return Status::NotSupported("");
}

void CheckpointImpl::CleanStagingDirectory(
    const std::string& full_private_path, Logger* info_log) {
    std::vector<std::string> subchildren;
  Status s = db_->GetEnv()->FileExists(full_private_path);
  if (s.IsNotFound()) {
    return;
  }
  ROCKS_LOG_INFO(info_log, "File exists %s -- %s",
                 full_private_path.c_str(), s.ToString().c_str());
  s = db_->GetEnv()->GetChildren(full_private_path, &subchildren);
  if (s.ok()) {
    for (auto& subchild : subchildren) {
      std::string subchild_path = full_private_path + "/" + subchild;
      s = db_->GetEnv()->DeleteFile(subchild_path);
      ROCKS_LOG_INFO(info_log, "Delete file %s -- %s", subchild_path.c_str(),
                     s.ToString().c_str());
    }
  }
  // finally delete the private dir
  s = db_->GetEnv()->DeleteDir(full_private_path);
  ROCKS_LOG_INFO(info_log, "Delete dir %s -- %s",
                 full_private_path.c_str(), s.ToString().c_str());
}

Status Checkpoint::ExportColumnFamily(
    ColumnFamilyHandle* /*handle*/, const std::string& /*export_dir*/,
    ExportImportFilesMetaData** /*metadata*/) {
  return Status::NotSupported("");
}

// Builds an openable snapshot of RocksDB
Status CheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir,
                                        uint64_t log_size_for_flush,
                                        uint64_t* sequence_number_ptr) {
  DBOptions db_options = db_->GetDBOptions();

  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  ROCKS_LOG_INFO(
      db_options.info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());
  std::cout << "mark1: " << checkpoint_dir << std::endl;
  size_t final_nonslash_idx = checkpoint_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    // npos means it's only slashes or empty. Non-empty means it's the root
    // directory, but it shouldn't be because we verified above the directory
    // doesn't exist.
    assert(checkpoint_dir.empty());
    return Status::InvalidArgument("invalid checkpoint directory name");
  }

  std::string full_private_path =
      checkpoint_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  ROCKS_LOG_INFO(
      db_options.info_log,
      "Snapshot process -- using temporary directory %s",
      full_private_path.c_str());
  CleanStagingDirectory(full_private_path, db_options.info_log.get());
  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);
  uint64_t sequence_number = 0;
  if (s.ok()) {
    // enable file deletions
    s = db_->DisableFileDeletions();
    const bool disabled_file_deletions = s.ok();

    if (s.ok() || s.IsNotSupported()) {
      s = CreateCustomCheckpoint(
          db_options,
          [&](const std::string& src_dirname, const std::string& fname,
              FileType) {
            ROCKS_LOG_INFO(db_options.info_log, "Hard Linking %s",
                           fname.c_str());
            return db_->GetFileSystem()->LinkFile(src_dirname + fname,
                                                  full_private_path + fname,
                                                  IOOptions(), nullptr);
          } /* link_file_cb */,
          [&](const std::string& src_dirname, const std::string& fname,
              uint64_t size_limit_bytes, FileType,
              const std::string& /* checksum_func_name */,
              const std::string& /* checksum_val */) {
            ROCKS_LOG_INFO(db_options.info_log, "Copying %s", fname.c_str());
            return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                            full_private_path + fname, size_limit_bytes,
                            db_options.use_fsync);
          } /* copy_file_cb */,
          [&](const std::string& fname, const std::string& contents, FileType) {
            ROCKS_LOG_INFO(db_options.info_log, "Creating %s", fname.c_str());
            return CreateFile(db_->GetFileSystem(), full_private_path + fname,
                              contents, db_options.use_fsync);
          } /* create_file_cb */,
          &sequence_number, log_size_for_flush);

      // we copied all the files, enable file deletions
      if (disabled_file_deletions) {
        Status ss = db_->EnableFileDeletions(false);
        assert(ss.ok());
        ss.PermitUncheckedError();
      }
    }
  }

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    std::unique_ptr<Directory> checkpoint_directory;
    s = db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (s.ok() && checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (s.ok()) {
    if (sequence_number_ptr != nullptr) {
      *sequence_number_ptr = sequence_number;
    }
    // here we know that we succeeded and installed the new snapshot
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot DONE. All is good");
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot sequence number: %" PRIu64,
                   sequence_number);
  } else {
    // clean all the files we might have created
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot failed -- %s",
                   s.ToString().c_str());
    CleanStagingDirectory(full_private_path, db_options.info_log.get());
  }
  return s;
}

/*
 * 该函数用于执行checkpoint逻辑，步骤如下：
 * 1. 获取当前数据库系统的文件列表: SST+blob+min_log_num+rocksdb自身元数据文件
 * 2. 遍历获取的文件列表(不包括WAL文件, WAL文件最后处理)：
 *    a. 如果是CURRENT文件，将其名字记录下来，不作任何操作。
 *       因为CURRENT的内容随时可能发生变化，我们需要解析到MANIFEST文件的名字以后，将其写到CURRENT文件
 *    b. 如果是MANIFEST(kDescriptorFile), 将其名字记录下来，并拷贝到dest目录
 *    c. 如果是Options文件，将其拷贝到dest目录
 *    d. 如果是数据文件(SST/blob), 将其加入到一个列表，根据是否可以硬连接做复制或者链接
 * 3. 拷贝/链接数据文件
 * 4. 解析MANIFEST文件的名字，将其写到CURRENT文件
 * 5. 复制WAL文件
 * 
 * TODO:
 * 1. 获取文件列表的时候一定有blob，所以这块要适配，那么确定边界
 */
Status CheckpointImpl::CreateCustomCheckpoint(
    const DBOptions& db_options,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname, FileType type)>
        link_file_cb,
    std::function<Status(
        const std::string& src_dirname, const std::string& src_fname,
        uint64_t size_limit_bytes, FileType type,
        const std::string& checksum_func_name, const std::string& checksum_val)>
        copy_file_cb,
    std::function<Status(const std::string& fname, const std::string& contents,
                         FileType type)>
        create_file_cb,
    uint64_t* sequence_number, uint64_t log_size_for_flush,
    bool get_live_table_checksum) {
  Status s;
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  uint64_t min_log_num = port::kMaxUint64;
  *sequence_number = db_->GetLatestSequenceNumber();
  bool same_fs = true;
  VectorLogPtr live_wal_files;

  /*
   * 如果开启了2pc，那么一定要刷memtable，原因未知
   * 如果没有开启2pc，那么是否要刷memtable，要视情况而定
   */
  bool flush_memtable = true;
  if (!db_options.allow_2pc) {
    /*
     * 以下，刷不刷memtable主要决定了checkpoint的数据的新旧程度 
     */
    if (log_size_for_flush == port::kMaxUint64) {
      flush_memtable = false;
    } else if (log_size_for_flush > 0) {
      // If out standing log files are small, we skip the flush.
      s = db_->GetSortedWalFiles(live_wal_files);

      if (!s.ok()) {
        return s;
      }

      // Don't flush column families if total log size is smaller than
      // log_size_for_flush. We copy the log files instead.
      // We may be able to cover 2PC case too.
      uint64_t total_wal_size = 0;
      for (auto& wal : live_wal_files) {
        total_wal_size += wal->SizeFileBytes();
      }
      if (total_wal_size < log_size_for_flush) {
        flush_memtable = false;
      }
      live_wal_files.clear();
    }
  }

  /* 获取到的文件，不止包括数据文件，还包括rocksdb其他文件 */
  // this will return live_files prefixed with "/"
  s = db_->GetLiveFiles(live_files, &manifest_file_size, flush_memtable);

  if (!db_->GetIntProperty(DB::Properties::kMinLogNumberToKeep, &min_log_num)) {
    return Status::InvalidArgument("cannot get the min log number to keep.");
  }
  // Between GetLiveFiles and getting min_log_num, flush might happen
  // concurrently, so new WAL deletions might be tracked in MANIFEST. If we do
  // not get the new MANIFEST size, the deleted WALs might not be reflected in
  // the checkpoint's MANIFEST.
  //
  // If we get min_log_num before the above GetLiveFiles, then there might
  // be too many unnecessary WALs to be included in the checkpoint.
  //
  // Ideally, min_log_num should be got together with manifest_file_size in
  // GetLiveFiles atomically. But that needs changes to GetLiveFiles' signature
  // which is a public API.
  /*
   * 这一步得到的live_files含有的SST并不能保证数据库处于一致状态，
   * 比如一个flush操作需要刷写多个cf，如果GetLiveFile的时候只刷
   * 写了一个cf，那么这时候数据的状态没有处于一致。所以，还需要拿到
   * WAL日志进行恢复。
   * 
   * 我们整体的目标是WAL的范围一定要与SST数据的范围有重合。所以先GetLiveFiles,
   * 再去拿min_log_num，此时WAL与SST之间会有脱节，这样不能保证正确性。
   * 
   * 先拿min_log_num，再去GetLiveFiles，这样可以保证正确性，但是这种方式不够好，
   * 因为如果GetLiveFiles的时候进行了flush操作，那么先获取的min_log_num和当前
   * 的min_log_num就差距太大了，这会导致复制很多不需要的WAL文件，并且造成recover
   * 的时间过长。
   */
  s = db_->GetLiveFiles(live_files, &manifest_file_size, flush_memtable);
  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:FlushDone");

  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles1");
  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles2");

  if (s.ok()) {
    s = db_->FlushWAL(false /* sync */);
  }

  TEST_SYNC_POINT("CheckpointImpl::CreateCustomCheckpoint:AfterGetLive1");
  TEST_SYNC_POINT("CheckpointImpl::CreateCustomCheckpoint:AfterGetLive2");

  /* 果然和我想的一样，如果没有多个cf，rocksdb的checkpoint根本不需要wal的参与 */
  // if we have more than one column family, we need to also get WAL files
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }
  if (!s.ok()) {
    return s;
  }

  size_t wal_size = live_wal_files.size();

  /*
   * 遍历获取的文件列表(不包括WAL文件, WAL文件最后处理)：
   * 1. 如果是CURRENT文件，将其名字记录下来，不作任何操作。
   *    因为CURRENT的内容随时可能发生变化，我们需要解析到MANIFEST文件的名字以后，将其写到CURRENT文件
   * 2. 如果是MANIFEST(kDescriptorFile), 将其名字记录下来，并拷贝到dest目录
   * 3. 如果是Options文件，将其拷贝到dest目录
   * 4. 如果是数据文件(SST/blob), 将其加入到一个列表，根据是否可以硬连接做复制或者链接
   */
  // process live files, non-table, non-blob files first
  std::string manifest_fname, current_fname;
  // record table and blob files for processing next
  std::vector<std::tuple<std::string, uint64_t, FileType>>
      live_table_and_blob_files;
  for (auto& live_file : live_files) {
    if (!s.ok()) {
      break;
    }
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_file, &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, blob, options, manifest and current files here
    assert(type == kTableFile || type == kBlobFile || type == kDescriptorFile ||
           type == kCurrentFile || type == kOptionsFile);
    assert(live_file.size() > 0 && live_file[0] == '/');
    if (type == kCurrentFile) {
      // We will craft the current file manually to ensure it's consistent with
      // the manifest number. This is necessary because current's file contents
      // can change during checkpoint creation.
      current_fname = live_file;
      continue;
    } else if (type == kDescriptorFile) {
      manifest_fname = live_file;
    }

    if (type != kTableFile && type != kBlobFile) {
      // copy non-table, non-blob files here
      // * if it's kDescriptorFile, limit the size to manifest_file_size
      s = copy_file_cb(db_->GetName(), live_file,
                       (type == kDescriptorFile) ? manifest_file_size : 0, type,
                       kUnknownFileChecksumFuncName, kUnknownFileChecksum);
    } else {
      // process table and blob files below
      live_table_and_blob_files.emplace_back(live_file, number, type);
    }
  }

  // get checksum info for table and blob files.
  // get table and blob file checksums if get_live_table_checksum is true
  std::unique_ptr<FileChecksumList> checksum_list;

  if (s.ok() && get_live_table_checksum) {
    checksum_list.reset(NewFileChecksumList());
    // should succeed even without checksum info present, else manifest
    // is corrupt
    s = GetFileChecksumsFromManifest(db_->GetEnv(),
                                     db_->GetName() + manifest_fname,
                                     manifest_file_size, checksum_list.get());
  }

  /*
   * 拷贝/链接数据文件 
   */
  // copy/hard link live table and blob files
  for (const auto& file : live_table_and_blob_files) {
    if (!s.ok()) {
      break;
    }

    const std::string& src_fname = std::get<0>(file);
    const uint64_t number = std::get<1>(file);
    const FileType type = std::get<2>(file);

    // rules:
    // * for kTableFile/kBlobFile, attempt hard link instead of copy.
    // * but can't hard link across filesystems.
    if (same_fs) {
      s = link_file_cb(db_->GetName(), src_fname, type);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if (!same_fs) {
      std::string checksum_name = kUnknownFileChecksumFuncName;
      std::string checksum_value = kUnknownFileChecksum;

      // we ignore the checksums either they are not required or we failed to
      // obtain the checksum list for old table files that have no file
      // checksums
      if (get_live_table_checksum) {
        // find checksum info for table files
        Status search = checksum_list->SearchOneFileChecksum(
            number, &checksum_value, &checksum_name);

        // could be a legacy file lacking checksum info. overall OK if
        // not found
        if (!search.ok()) {
          assert(checksum_name == kUnknownFileChecksumFuncName);
          assert(checksum_value == kUnknownFileChecksum);
        }
      }
      s = copy_file_cb(db_->GetName(), src_fname, 0, type, checksum_name,
                       checksum_value);
    }
  }

  /*
   * 解析MANIFEST文件的名字，将其写到CURRENT文件
   */
  if (s.ok() && !current_fname.empty() && !manifest_fname.empty()) {
    s = create_file_cb(current_fname, manifest_fname.substr(1) + "\n",
                       kCurrentFile);
  }
  ROCKS_LOG_INFO(db_options.info_log, "Number of log files %" ROCKSDB_PRIszt,
                 live_wal_files.size());


  /*
   * 复制WAL文件 
   */
  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (!flush_memtable ||
         live_wal_files[i]->LogNumber() >= min_log_num)) {
      if (i + 1 == wal_size) {
        s = copy_file_cb(db_options.wal_dir, live_wal_files[i]->PathName(),
                         live_wal_files[i]->SizeFileBytes(), kWalFile,
                         kUnknownFileChecksumFuncName, kUnknownFileChecksum);
        break;
      }
      if (same_fs) {
        // we only care about live log files
        s = link_file_cb(db_options.wal_dir, live_wal_files[i]->PathName(),
                         kWalFile);
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
      }
      if (!same_fs) {
        s = copy_file_cb(db_options.wal_dir, live_wal_files[i]->PathName(), 0,
                         kWalFile, kUnknownFileChecksumFuncName,
                         kUnknownFileChecksum);
      }
    }
  }

  return s;
}

// Exports all live SST files of a specified Column Family onto export_dir,
// returning SST files information in metadata.
Status CheckpointImpl::ExportColumnFamily(
    ColumnFamilyHandle* handle, const std::string& export_dir,
    ExportImportFilesMetaData** metadata) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handle);
  const auto cf_name = cfh->GetName();
  const auto db_options = db_->GetDBOptions();

  assert(metadata != nullptr);
  assert(*metadata == nullptr);
  auto s = db_->GetEnv()->FileExists(export_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Specified export_dir exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  const auto final_nonslash_idx = export_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    return Status::InvalidArgument("Specified export_dir invalid");
  }
  ROCKS_LOG_INFO(db_options.info_log,
                 "[%s] export column family onto export directory %s",
                 cf_name.c_str(), export_dir.c_str());

  // Create a temporary export directory.
  const auto tmp_export_dir =
      export_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  s = db_->GetEnv()->CreateDir(tmp_export_dir);

  if (s.ok()) {
    s = db_->Flush(ROCKSDB_NAMESPACE::FlushOptions(), handle);
  }

  ColumnFamilyMetaData db_metadata;
  if (s.ok()) {
    // Export live sst files with file deletions disabled.
    s = db_->DisableFileDeletions();
    if (s.ok()) {
      db_->GetColumnFamilyMetaData(handle, &db_metadata);

      s = ExportFilesInMetaData(
          db_options, db_metadata,
          [&](const std::string& src_dirname, const std::string& fname) {
            ROCKS_LOG_INFO(db_options.info_log, "[%s] HardLinking %s",
                           cf_name.c_str(), fname.c_str());
            return db_->GetEnv()->LinkFile(src_dirname + fname,
                                           tmp_export_dir + fname);
          } /*link_file_cb*/,
          [&](const std::string& src_dirname, const std::string& fname) {
            ROCKS_LOG_INFO(db_options.info_log, "[%s] Copying %s",
                           cf_name.c_str(), fname.c_str());
            return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                            tmp_export_dir + fname, 0, db_options.use_fsync);
          } /*copy_file_cb*/);

      const auto enable_status = db_->EnableFileDeletions(false /*force*/);
      if (s.ok()) {
        s = enable_status;
      }
    }
  }

  auto moved_to_user_specified_dir = false;
  if (s.ok()) {
    // Move temporary export directory to the actual export directory.
    s = db_->GetEnv()->RenameFile(tmp_export_dir, export_dir);
  }

  if (s.ok()) {
    // Fsync export directory.
    moved_to_user_specified_dir = true;
    std::unique_ptr<Directory> dir_ptr;
    s = db_->GetEnv()->NewDirectory(export_dir, &dir_ptr);
    if (s.ok()) {
      assert(dir_ptr != nullptr);
      s = dir_ptr->Fsync();
    }
  }

  if (s.ok()) {
    // Export of files succeeded. Fill in the metadata information.
    auto result_metadata = new ExportImportFilesMetaData();
    result_metadata->db_comparator_name = handle->GetComparator()->Name();
    for (const auto& level_metadata : db_metadata.levels) {
      for (const auto& file_metadata : level_metadata.files) {
        LiveFileMetaData live_file_metadata;
        live_file_metadata.size = file_metadata.size;
        live_file_metadata.name = std::move(file_metadata.name);
        live_file_metadata.file_number = file_metadata.file_number;
        live_file_metadata.db_path = export_dir;
        live_file_metadata.smallest_seqno = file_metadata.smallest_seqno;
        live_file_metadata.largest_seqno = file_metadata.largest_seqno;
        live_file_metadata.smallestkey = std::move(file_metadata.smallestkey);
        live_file_metadata.largestkey = std::move(file_metadata.largestkey);
        live_file_metadata.oldest_blob_file_number =
            file_metadata.oldest_blob_file_number;
        live_file_metadata.level = level_metadata.level;
        result_metadata->files.push_back(live_file_metadata);
      }
      *metadata = result_metadata;
    }
    ROCKS_LOG_INFO(db_options.info_log, "[%s] Export succeeded.",
                   cf_name.c_str());
  } else {
    // Failure: Clean up all the files/directories created.
    ROCKS_LOG_INFO(db_options.info_log, "[%s] Export failed. %s",
                   cf_name.c_str(), s.ToString().c_str());
    std::vector<std::string> subchildren;
    const auto cleanup_dir =
        moved_to_user_specified_dir ? export_dir : tmp_export_dir;
    db_->GetEnv()->GetChildren(cleanup_dir, &subchildren);
    for (const auto& subchild : subchildren) {
      const auto subchild_path = cleanup_dir + "/" + subchild;
      const auto status = db_->GetEnv()->DeleteFile(subchild_path);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options.info_log, "Failed to cleanup file %s: %s",
                       subchild_path.c_str(), status.ToString().c_str());
      }
    }
    const auto status = db_->GetEnv()->DeleteDir(cleanup_dir);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options.info_log, "Failed to cleanup dir %s: %s",
                     cleanup_dir.c_str(), status.ToString().c_str());
    }
  }
  return s;
}

Status CheckpointImpl::ExportFilesInMetaData(
    const DBOptions& db_options, const ColumnFamilyMetaData& metadata,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname)>
        link_file_cb,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname)>
        copy_file_cb) {
  Status s;
  auto hardlink_file = true;

  // Copy/hard link files in metadata.
  size_t num_files = 0;
  for (const auto& level_metadata : metadata.levels) {
    for (const auto& file_metadata : level_metadata.files) {
      uint64_t number;
      FileType type;
      const auto ok = ParseFileName(file_metadata.name, &number, &type);
      if (!ok) {
        s = Status::Corruption("Could not parse file name");
        break;
      }

      // We should only get sst files here.
      assert(type == kTableFile);
      assert(file_metadata.size > 0 && file_metadata.name[0] == '/');
      const auto src_fname = file_metadata.name;
      ++num_files;

      if (hardlink_file) {
        s = link_file_cb(db_->GetName(), src_fname);
        if (num_files == 1 && s.IsNotSupported()) {
          // Fallback to copy if link failed due to cross-device directories.
          hardlink_file = false;
          s = Status::OK();
        }
      }
      if (!hardlink_file) {
        s = copy_file_cb(db_->GetName(), src_fname);
      }
      if (!s.ok()) {
        break;
      }
    }
  }
  ROCKS_LOG_INFO(db_options.info_log, "Number of table files %" ROCKSDB_PRIszt,
                 num_files);

  return s;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
