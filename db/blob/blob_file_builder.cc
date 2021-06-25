//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cassert>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_completion_callback.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

BlobFileBuilder::BlobFileBuilder(
    VersionSet* versions, FileSystem* fs,
    const ImmutableCFOptions* immutable_cf_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::IOPriority io_priority,
    Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCompletionCallback* blob_callback,
    std::vector<std::string>* blob_file_paths,
    std::vector<BlobFileAddition>* blob_file_additions)
    : BlobFileBuilder([versions]() { return versions->NewFileNumber(); }, fs,
                      immutable_cf_options, mutable_cf_options, file_options,
                      job_id, column_family_id, column_family_name, io_priority,
                      write_hint, io_tracer, blob_callback, blob_file_paths,
                      blob_file_additions) {}

BlobFileBuilder::BlobFileBuilder(
    std::function<uint64_t()> file_number_generator, FileSystem* fs,
    const ImmutableCFOptions* immutable_cf_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    int job_id, uint32_t column_family_id,
    const std::string& column_family_name, Env::IOPriority io_priority,
    Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCompletionCallback* blob_callback,
    std::vector<std::string>* blob_file_paths,
    std::vector<BlobFileAddition>* blob_file_additions)
    : file_number_generator_(std::move(file_number_generator)),
      fs_(fs),
      immutable_cf_options_(immutable_cf_options),
      min_blob_size_(mutable_cf_options->min_blob_size),
      blob_file_size_(mutable_cf_options->blob_file_size),
      blob_compression_type_(mutable_cf_options->blob_compression_type),
      file_options_(file_options),
      job_id_(job_id),
      column_family_id_(column_family_id),
      column_family_name_(column_family_name),
      io_priority_(io_priority),
      write_hint_(write_hint),
      io_tracer_(io_tracer),
      blob_callback_(blob_callback),
      blob_file_paths_(blob_file_paths),
      blob_file_additions_(blob_file_additions),
      blob_count_(0),
      blob_bytes_(0) {
  assert(file_number_generator_);
  assert(fs_);
  assert(immutable_cf_options_);
  assert(file_options_);
  assert(blob_file_paths_);
  assert(blob_file_paths_->empty());
  assert(blob_file_additions_);
  assert(blob_file_additions_->empty());
}

BlobFileBuilder::~BlobFileBuilder() = default;

/* 
 * flush table调用Add函数的调用栈
#0  rocksdb::BlobFileBuilder::Add (this=0x7fffe8013040, key=..., value=..., blob_index=0x7ffff49c11f8) at /root/code/rocksdb/db/blob/blob_file_builder.cc:93
#1  0x00007ffff6ed1132 in rocksdb::CompactionIterator::ExtractLargeValueIfNeededImpl (this=0x7ffff49c0fb0) at /root/code/rocksdb/db/compaction/compaction_iterator.cc:822
#2  0x00007ffff6ed1242 in rocksdb::CompactionIterator::ExtractLargeValueIfNeeded (this=0x7ffff49c0fb0) at /root/code/rocksdb/db/compaction/compaction_iterator.cc:843
#3  0x00007ffff6ed196f in rocksdb::CompactionIterator::PrepareOutput (this=0x7ffff49c0fb0) at /root/code/rocksdb/db/compaction/compaction_iterator.cc:949
#4  0x00007ffff6ece3fe in rocksdb::CompactionIterator::SeekToFirst (this=0x7ffff49c0fb0) at /root/code/rocksdb/db/compaction/compaction_iterator.cc:153
#5  0x00007ffff6e73737 in rocksdb::BuildTable (dbname=..., versions=0x65fea0, db_options=..., ioptions=..., mutable_cf_options=..., file_options=..., table_cache=0x66b410,
    iter=0x7ffff49c1c50, range_del_iters=..., meta=0x7ffff49c3260, blob_file_additions=0x7ffff49c2660, internal_comparator=..., int_tbl_prop_collector_factories=0x664d48,
    column_family_id=0, column_family_name=..., snapshots=..., earliest_write_conflict_snapshot=72057594037927935, snapshot_checker=0x0, compression=rocksdb::kNoCompression,
    compression_opts=..., paranoid_file_checks=false, internal_stats=0x665470, reason=rocksdb::kFlush, io_status=0x7ffff49c24d0, io_tracer=..., event_logger=0x656cc8,
    job_id=3, io_priority=rocksdb::Env::IO_HIGH, table_properties=0x7ffff49c30b0, level=0, creation_time=1624591002, oldest_key_time=1624591002,
    write_hint=rocksdb::Env::WLTH_MEDIUM, file_creation_time=1624591002, db_id=..., db_session_id=..., full_history_ts_low=0x0, blob_callback=0x656ed8)
    at /root/code/rocksdb/db/builder.cc:207
#6  0x00007ffff70080d6 in rocksdb::FlushJob::WriteLevel0Table (this=0x7ffff49c2fb0) at /root/code/rocksdb/db/flush_job.cc:450
#7  0x00007ffff7006a7e in rocksdb::FlushJob::Run (this=0x7ffff49c2fb0, prep_tracker=0x656ce8, file_meta=0x7ffff49c2f10) at /root/code/rocksdb/db/flush_job.cc:233
#8  0x00007ffff6f7709b in rocksdb::DBImpl::FlushMemTableToOutputFile (this=0x655910, cfd=0x664cf0, mutable_cf_options=..., made_progress=0x7ffff49c477f,
    job_context=0x7ffff49c4580, superversion_context=0x7fffe8000c40, snapshot_seqs=..., earliest_write_conflict_snapshot=72057594037927935, snapshot_checker=0x0,
    log_buffer=0x7ffff49c3c80, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:210
#9  0x00007ffff6f77af9 in rocksdb::DBImpl::FlushMemTablesToOutputFiles (this=0x655910, bg_flush_args=..., made_progress=0x7ffff49c477f, job_context=0x7ffff49c4580,
    log_buffer=0x7ffff49c3c80, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:337
#10 0x00007ffff6f84cc4 in rocksdb::DBImpl::BackgroundFlush (this=0x655910, made_progress=0x7ffff49c477f, job_context=0x7ffff49c4580, log_buffer=0x7ffff49c3c80,
    reason=0x7ffff49c3c6c, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:2558
#11 0x00007ffff6f852d6 in rocksdb::DBImpl::BackgroundCallFlush (this=0x655910, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:2598
#12 0x00007ffff6f84013 in rocksdb::DBImpl::BGWorkFlush (arg=0x678690) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:2428
#13 0x00007ffff73749e1 in std::__invoke_impl<void, void (*&)(void*), void*&> (__f=@0x678d90: 0x7ffff6f83f5c <rocksdb::DBImpl::BGWorkFlush(void*)>,
    __args#0=@0x678d98: 0x678690) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:60
#14 0x00007ffff73745e3 in std::__invoke<void (*&)(void*), void*&> (__fn=@0x678d90: 0x7ffff6f83f5c <rocksdb::DBImpl::BGWorkFlush(void*)>, __args#0=@0x678d98: 0x678690)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:95
#15 0x00007ffff7373dae in std::_Bind<void (*(void*))(void*)>::__call<void, , 0ul>(std::tuple<>&&, std::_Index_tuple<0ul>) (this=0x678d90,
    __args=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1719, DIE 0x4113215>) at /opt/rh/devtoolset-8/root/usr/include/c++/8/functional:400
#16 0x00007ffff737311c in std::_Bind<void (*(void*))(void*)>::operator()<, void>() (this=0x678d90) at /opt/rh/devtoolset-8/root/usr/include/c++/8/functional:484
#17 0x00007ffff73720ea in std::_Function_handler<void (), std::_Bind<void (*(void*))(void*)> >::_M_invoke(std::_Any_data const&) (__functor=...)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/std_function.h:297
#18 0x00007ffff7051f18 in std::function<void ()>::operator()() const (this=0x7ffff49c49a0) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/std_function.h:687
#19 0x00007ffff736eab7 in rocksdb::ThreadPoolImpl::Impl::BGThread (this=0x653110, thread_id=0) at /root/code/rocksdb/util/threadpool_imp.cc:266
#20 0x00007ffff736ec90 in rocksdb::ThreadPoolImpl::Impl::BGThreadWrapper (arg=0x657540) at /root/code/rocksdb/util/threadpool_imp.cc:307
#21 0x00007ffff73718d3 in std::__invoke_impl<void, void (*)(void*), rocksdb::BGThreadMetadata*>(std::__invoke_other, void (*&&)(void*), rocksdb::BGThreadMetadata*&&) (
    __f=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1719, DIE 0x4115e7e>,
    __args#0=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1719, DIE 0x4115e9b>) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:60
#22 0x00007ffff7370d2a in std::__invoke<void (*)(void*), rocksdb::BGThreadMetadata*>(void (*&&)(void*), rocksdb::BGThreadMetadata*&&) (
---Type <return> to continue, or q <return> to quit---
    __fn=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1719, DIE 0x41173c2>,
    __args#0=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1719, DIE 0x41173df>) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:95
#23 0x00007ffff737506f in std::thread::_Invoker<std::tuple<void (*)(void*), rocksdb::BGThreadMetadata*> >::_M_invoke<0ul, 1ul> (this=0x657568)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/thread:244
#24 0x00007ffff737502a in std::thread::_Invoker<std::tuple<void (*)(void*), rocksdb::BGThreadMetadata*> >::operator() (this=0x657568)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/thread:253
#25 0x00007ffff737500e in std::thread::_State_impl<std::thread::_Invoker<std::tuple<void (*)(void*), rocksdb::BGThreadMetadata*> > >::_M_run (this=0x657560)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/thread:196
#26 0x00007ffff5d78630 in execute_native_thread_routine () at ../../../../../libstdc++-v3/src/c++11/thread.cc:80
#27 0x00007ffff6090dd5 in start_thread () from /lib64/libpthread.so.0
#28 0x00007ffff54c902d in clone () from /lib64/libc.so.6
 */
Status BlobFileBuilder::Add(const Slice& key, const Slice& value,
                            std::string* blob_index) {
  assert(blob_index);
  assert(blob_index->empty());

  if (value.size() < min_blob_size_) {
    return Status::OK();
  }

  {
    const Status s = OpenBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  Slice blob = value;
  std::string compressed_blob;

  {
    const Status s = CompressBlobIfNeeded(&blob, &compressed_blob);
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t blob_file_number = 0;
  uint64_t blob_offset = 0;

  {
    const Status s =
        WriteBlobToFile(key, blob, &blob_file_number, &blob_offset);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = CloseBlobFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  BlobIndex::EncodeBlob(blob_index, blob_file_number, blob_offset, blob.size(),
                        blob_compression_type_);

  return Status::OK();
}

Status BlobFileBuilder::Finish() {
  if (!IsBlobFileOpen()) {
    return Status::OK();
  }

  return CloseBlobFile();
}

bool BlobFileBuilder::IsBlobFileOpen() const { return !!writer_; }

Status BlobFileBuilder::OpenBlobFileIfNeeded() {
  if (IsBlobFileOpen()) {
    return Status::OK();
  }

  assert(!blob_count_);
  assert(!blob_bytes_);

  assert(file_number_generator_);
  const uint64_t blob_file_number = file_number_generator_();

  assert(immutable_cf_options_);
  assert(!immutable_cf_options_->cf_paths.empty());
  std::string blob_file_path = BlobFileName(
      immutable_cf_options_->cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;

  {
    assert(file_options_);
    Status s = NewWritableFile(fs_, blob_file_path, &file, *file_options_);

    TEST_SYNC_POINT_CALLBACK(
        "BlobFileBuilder::OpenBlobFileIfNeeded:NewWritableFile", &s);

    if (!s.ok()) {
      return s;
    }
  }

  // Note: files get added to blob_file_paths_ right after the open, so they
  // can be cleaned up upon failure. Contrast this with blob_file_additions_,
  // which only contains successfully written files.
  assert(blob_file_paths_);
  blob_file_paths_->emplace_back(std::move(blob_file_path));

  assert(file);
  file->SetIOPriority(io_priority_);
  file->SetWriteLifeTimeHint(write_hint_);
  FileTypeSet tmp_set = immutable_cf_options_->checksum_handoff_file_types;
  Statistics* const statistics = immutable_cf_options_->statistics;
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_paths_->back(), *file_options_,
      immutable_cf_options_->clock, io_tracer_, statistics,
      immutable_cf_options_->listeners,
      immutable_cf_options_->file_checksum_gen_factory,
      tmp_set.Contains(FileType::kBlobFile)));

  constexpr bool do_flush = false;

  std::unique_ptr<BlobLogWriter> blob_log_writer(new BlobLogWriter(
      std::move(file_writer), immutable_cf_options_->clock, statistics,
      blob_file_number, immutable_cf_options_->use_fsync, do_flush));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id_, blob_compression_type_, has_ttl,
                       expiration_range);

  {
    Status s = blob_log_writer->WriteHeader(header);

    TEST_SYNC_POINT_CALLBACK(
        "BlobFileBuilder::OpenBlobFileIfNeeded:WriteHeader", &s);

    if (!s.ok()) {
      return s;
    }
  }

  writer_ = std::move(blob_log_writer);

  assert(IsBlobFileOpen());

  return Status::OK();
}

Status BlobFileBuilder::CompressBlobIfNeeded(
    Slice* blob, std::string* compressed_blob) const {
  assert(blob);
  assert(compressed_blob);
  assert(compressed_blob->empty());

  if (blob_compression_type_ == kNoCompression) {
    return Status::OK();
  }

  CompressionOptions opts;
  CompressionContext context(blob_compression_type_);
  constexpr uint64_t sample_for_compression = 0;

  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                       blob_compression_type_, sample_for_compression);

  constexpr uint32_t compression_format_version = 2;

  if (!CompressData(*blob, info, compression_format_version, compressed_blob)) {
    return Status::Corruption("Error compressing blob");
  }

  *blob = Slice(*compressed_blob);

  return Status::OK();
}

Status BlobFileBuilder::WriteBlobToFile(const Slice& key, const Slice& blob,
                                        uint64_t* blob_file_number,
                                        uint64_t* blob_offset) {
  assert(IsBlobFileOpen());
  assert(blob_file_number);
  assert(blob_offset);

  uint64_t key_offset = 0;

  Status s = writer_->AddRecord(key, blob, &key_offset, blob_offset);

  TEST_SYNC_POINT_CALLBACK("BlobFileBuilder::WriteBlobToFile:AddRecord", &s);

  if (!s.ok()) {
    return s;
  }

  *blob_file_number = writer_->get_log_number();

  ++blob_count_;
  blob_bytes_ += BlobLogRecord::kHeaderSize + key.size() + blob.size();

  return Status::OK();
}

Status BlobFileBuilder::CloseBlobFile() {
  assert(IsBlobFileOpen());

  BlobLogFooter footer;
  footer.blob_count = blob_count_;

  std::string checksum_method;
  std::string checksum_value;

  Status s = writer_->AppendFooter(footer, &checksum_method, &checksum_value);

  TEST_SYNC_POINT_CALLBACK("BlobFileBuilder::WriteBlobToFile:AppendFooter", &s);

  if (!s.ok()) {
    return s;
  }

  const uint64_t blob_file_number = writer_->get_log_number();

  assert(blob_file_additions_);
  blob_file_additions_->emplace_back(blob_file_number, blob_count_, blob_bytes_,
                                     std::move(checksum_method),
                                     std::move(checksum_value));

  assert(immutable_cf_options_);
  ROCKS_LOG_INFO(immutable_cf_options_->info_log,
                 "[%s] [JOB %d] Generated blob file #%" PRIu64 ": %" PRIu64
                 " total blobs, %" PRIu64 " total bytes",
                 column_family_name_.c_str(), job_id_, blob_file_number,
                 blob_count_, blob_bytes_);
  if (blob_callback_) {
    s = blob_callback_->OnBlobFileCompleted(blob_file_paths_->back());
  }

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;

  return s;
}

Status BlobFileBuilder::CloseBlobFileIfNeeded() {
  assert(IsBlobFileOpen());

  const WritableFileWriter* const file_writer = writer_->file();
  assert(file_writer);

  if (file_writer->GetFileSize() < blob_file_size_) {
    return Status::OK();
  }

  return CloseBlobFile();
}

void BlobFileBuilder::Abandon() {
  if (!IsBlobFileOpen()) {
    return;
  }

  if (blob_callback_) {
    // BlobFileBuilder::Abandon() is called because of error while writing to
    // Blob files. So we can ignore the below error.
    blob_callback_->OnBlobFileCompleted(blob_file_paths_->back())
        .PermitUncheckedError();
  }

  writer_.reset();
  blob_count_ = 0;
  blob_bytes_ = 0;
}
}  // namespace ROCKSDB_NAMESPACE
