//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#include <cinttypes>

#include <algorithm>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/event_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

const char* GetFlushReasonString (FlushReason flush_reason) {
  switch (flush_reason) {
    case FlushReason::kOthers:
      return "Other Reasons";
    case FlushReason::kGetLiveFiles:
      return "Get Live Files";
    case FlushReason::kShutDown:
      return "Shut down";
    case FlushReason::kExternalFileIngestion:
      return "External File Ingestion";
    case FlushReason::kManualCompaction:
      return "Manual Compaction";
    case FlushReason::kWriteBufferManager:
      return "Write Buffer Manager";
    case FlushReason::kWriteBufferFull:
      return "Write Buffer Full";
    case FlushReason::kTest:
      return "Test";
    case FlushReason::kDeleteFiles:
      return "Delete Files";
    case FlushReason::kAutoCompaction:
      return "Auto Compaction";
    case FlushReason::kManualFlush:
      return "Manual Flush";
    case FlushReason::kErrorRecovery:
      return "Error Recovery";
    case FlushReason::kWalFull:
      return "WAL Full";
    default:
      return "Invalid";
  }
}

FlushJob::FlushJob(
    const std::string& dbname, ColumnFamilyData* cfd,
    const ImmutableDBOptions& db_options,
    const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
    const FileOptions& file_options, VersionSet* versions,
    InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, JobContext* job_context,
    LogBuffer* log_buffer, FSDirectory* db_directory,
    FSDirectory* output_file_directory, CompressionType output_compression,
    Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
    const bool sync_output_directory, const bool write_manifest,
    Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
    const std::string& db_id, const std::string& db_session_id,
    std::string full_history_ts_low, BlobFileCompletionCallback* blob_callback)
    : dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      max_memtable_id_(max_memtable_id),
      file_options_(file_options),
      versions_(versions),
      db_mutex_(db_mutex),
      shutting_down_(shutting_down),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      job_context_(job_context),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_file_directory_(output_file_directory),
      output_compression_(output_compression),
      stats_(stats),
      event_logger_(event_logger),
      measure_io_stats_(measure_io_stats),
      sync_output_directory_(sync_output_directory),
      write_manifest_(write_manifest),
      edit_(nullptr),
      base_(nullptr),
      pick_memtable_called(false),
      thread_pri_(thread_pri),
      io_tracer_(io_tracer),
      clock_(db_options_.clock),
      full_history_ts_low_(std::move(full_history_ts_low)),
      blob_callback_(blob_callback) {
  // Update the thread status to indicate flush.
  ReportStartedFlush();
  TEST_SYNC_POINT("FlushJob::FlushJob()");
}

FlushJob::~FlushJob() {
  io_status_.PermitUncheckedError();
  ThreadStatusUtil::ResetThreadStatus();
}

void FlushJob::ReportStartedFlush() {
  ThreadStatusUtil::SetColumnFamily(cfd_, cfd_->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_JOB_ID,
      job_context_->job_id);
  IOSTATS_RESET(bytes_written);
}

void FlushJob::ReportFlushInputSize(const autovector<MemTable*>& mems) {
  uint64_t input_size = 0;
  for (auto* mem : mems) {
    input_size += mem->ApproximateMemoryUsage();
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_MEMTABLES,
      input_size);
}

void FlushJob::RecordFlushIOStats() {
  RecordTick(stats_, FLUSH_WRITE_BYTES, IOSTATS(bytes_written));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

/*
 * 获取需要做flush的memtable
 * 1. 通过PickMemtablesToFlush，pick出当前cfd所有需要做flush的memtable，获取的逻辑很简单，
 *    cfd immutable list中未开始做flush的memtable都会被pick。
 * 2. 为本次flush记录一些元数据。比如记录将要做flush的这批memtable对应的wal日志编号，当故障恢复
 *    时就可以跳过这些wal日志文件了。此时还记录一个SST文件的编号，这次flush生成的SST文件就会写到
 *    这个文件中。
 */
void FlushJob::PickMemTable() {
  db_mutex_->AssertHeld();
  assert(!pick_memtable_called);
  pick_memtable_called = true;
  // Save the contents of the earliest memtable as a new Table
  cfd_->imm()->PickMemtablesToFlush(max_memtable_id_, &mems_);
  if (mems_.empty()) {
    return;
  }

  ReportFlushInputSize(mems_);

  // entries mems are (implicitly) sorted in ascending order by their created
  // time. We will use the first memtable's `edit` to keep the meta info for
  // this flush.
  MemTable* m = mems_[0];
  edit_ = m->GetEdits();
  edit_->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  edit_->SetLogNumber(mems_.back()->GetNextLogNumber());
  edit_->SetColumnFamily(cfd_->GetID());
  /*
   * reading here. 2021-6-29-14:51 
   */
  // path 0 for level 0 file.
  meta_.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);

  base_ = cfd_->current();
  base_->Ref();  // it is likely that we do not need this reference
}

/*
 * 1. WriteLevel0Table
 * 2. TryInstallMemtableFlushResults
 */
Status FlushJob::Run(LogsWithPrepTracker* prep_tracker,
                     FileMetaData* file_meta) {
  TEST_SYNC_POINT("FlushJob::Start");
  db_mutex_->AssertHeld();
  assert(pick_memtable_called);
  AutoThreadOperationStageUpdater stage_run(
      ThreadStatus::STAGE_FLUSH_RUN);
  if (mems_.empty()) {
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Nothing in memtable to flush",
                     cfd_->GetName().c_str());
    return Status::OK();
  }

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }

  // This will release and re-acquire the mutex.
  Status s = WriteLevel0Table();

  if (s.ok() && cfd_->IsDropped()) {
    s = Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((s.ok() || s.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_acquire)) {
    s = Status::ShutdownInProgress("Database shutdown");
  }

  if (!s.ok()) {
    cfd_->imm()->RollbackMemtableFlush(mems_, meta_.fd.GetNumber());
  } else if (write_manifest_) { /* 通常来说write_manifest_为true */
    TEST_SYNC_POINT("FlushJob::InstallResults");
    // Replace immutable memtable with the generated Table
    IOStatus tmp_io_s;
    s = cfd_->imm()->TryInstallMemtableFlushResults(
        cfd_, mutable_cf_options_, mems_, prep_tracker, versions_, db_mutex_,
        meta_.fd.GetNumber(), &job_context_->memtables_to_free, db_directory_,
        log_buffer_, &committed_flush_jobs_info_, &tmp_io_s);
    if (!tmp_io_s.ok()) {
      io_status_ = tmp_io_s;
    }
  }

  if (s.ok() && file_meta != nullptr) {
    *file_meta = meta_;
  }
  RecordFlushIOStats();

  // When measure_io_stats_ is true, the default 512 bytes is not enough.
  auto stream = event_logger_->LogToBuffer(log_buffer_, 1024);
  stream << "job" << job_context_->job_id << "event"
         << "flush_finished";
  stream << "output_compression"
         << CompressionTypeToString(output_compression_);
  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    stream << "blob_file_head" << blob_files.begin()->first;
    stream << "blob_file_tail" << blob_files.rbegin()->first;
  }

  stream << "immutable_memtables" << cfd_->imm()->NumNotFlushed();

  if (measure_io_stats_) {
    if (prev_perf_level != PerfLevel::kEnableTime) {
      SetPerfLevel(prev_perf_level);
    }
    stream << "file_write_nanos" << (IOSTATS(write_nanos) - prev_write_nanos);
    stream << "file_range_sync_nanos"
           << (IOSTATS(range_sync_nanos) - prev_range_sync_nanos);
    stream << "file_fsync_nanos" << (IOSTATS(fsync_nanos) - prev_fsync_nanos);
    stream << "file_prepare_write_nanos"
           << (IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos);
    stream << "file_cpu_write_nanos"
           << (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos);
    stream << "file_cpu_read_nanos"
           << (IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos);
  }

  return s;
}

void FlushJob::Cancel() {
  db_mutex_->AssertHeld();
  assert(base_ != nullptr);
  base_->Unref();
}

/*
 * 调用栈：
#0  rocksdb::FlushJob::WriteLevel0Table (this=0x7ffff49c2fb0) at /root/code/rocksdb/db/flush_job.cc:314
#1  0x00007ffff7006a7e in rocksdb::FlushJob::Run (this=0x7ffff49c2fb0, prep_tracker=0x656ce8, file_meta=0x7ffff49c2f10) at /root/code/rocksdb/db/flush_job.cc:233
#2  0x00007ffff6f7709b in rocksdb::DBImpl::FlushMemTableToOutputFile (this=0x655910, cfd=0x66cfa0, mutable_cf_options=..., made_progress=0x7ffff49c477f,
    job_context=0x7ffff49c4580, superversion_context=0x7fffe8000c40, snapshot_seqs=..., earliest_write_conflict_snapshot=72057594037927935, snapshot_checker=0x0,
    log_buffer=0x7ffff49c3c80, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:210
#3  0x00007ffff6f77af9 in rocksdb::DBImpl::FlushMemTablesToOutputFiles (this=0x655910, bg_flush_args=..., made_progress=0x7ffff49c477f, job_context=0x7ffff49c4580,
    log_buffer=0x7ffff49c3c80, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:337
#4  0x00007ffff6f84cc4 in rocksdb::DBImpl::BackgroundFlush (this=0x655910, made_progress=0x7ffff49c477f, job_context=0x7ffff49c4580, log_buffer=0x7ffff49c3c80,
    reason=0x7ffff49c3c6c, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:2558
#5  0x00007ffff6f852d6 in rocksdb::DBImpl::BackgroundCallFlush (this=0x655910, thread_pri=rocksdb::Env::HIGH) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:2598
#6  0x00007ffff6f84013 in rocksdb::DBImpl::BGWorkFlush (arg=0x677d20) at /root/code/rocksdb/db/db_impl/db_impl_compaction_flush.cc:2428
#7  0x00007ffff73749e1 in std::__invoke_impl<void, void (*&)(void*), void*&> (__f=@0x6785d0: 0x7ffff6f83f5c <rocksdb::DBImpl::BGWorkFlush(void*)>,
    __args#0=@0x6785d8: 0x677d20) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:60
#8  0x00007ffff73745e3 in std::__invoke<void (*&)(void*), void*&> (__fn=@0x6785d0: 0x7ffff6f83f5c <rocksdb::DBImpl::BGWorkFlush(void*)>, __args#0=@0x6785d8: 0x677d20)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:95
#9  0x00007ffff7373dae in std::_Bind<void (*(void*))(void*)>::__call<void, , 0ul>(std::tuple<>&&, std::_Index_tuple<0ul>) (this=0x6785d0,
    __args=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1718, DIE 0x4113214>) at /opt/rh/devtoolset-8/root/usr/include/c++/8/functional:400
#10 0x00007ffff737311c in std::_Bind<void (*(void*))(void*)>::operator()<, void>() (this=0x6785d0) at /opt/rh/devtoolset-8/root/usr/include/c++/8/functional:484
#11 0x00007ffff73720ea in std::_Function_handler<void (), std::_Bind<void (*(void*))(void*)> >::_M_invoke(std::_Any_data const&) (__functor=...)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/std_function.h:297
#12 0x00007ffff7051f18 in std::function<void ()>::operator()() const (this=0x7ffff49c49a0) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/std_function.h:687
#13 0x00007ffff736eab7 in rocksdb::ThreadPoolImpl::Impl::BGThread (this=0x653110, thread_id=0) at /root/code/rocksdb/util/threadpool_imp.cc:266
#14 0x00007ffff736ec90 in rocksdb::ThreadPoolImpl::Impl::BGThreadWrapper (arg=0x657540) at /root/code/rocksdb/util/threadpool_imp.cc:307
#15 0x00007ffff73718d3 in std::__invoke_impl<void, void (*)(void*), rocksdb::BGThreadMetadata*>(std::__invoke_other, void (*&&)(void*), rocksdb::BGThreadMetadata*&&) (
    __f=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1718, DIE 0x4115e7d>,
    __args#0=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1718, DIE 0x4115e9a>) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:60
#16 0x00007ffff7370d2a in std::__invoke<void (*)(void*), rocksdb::BGThreadMetadata*>(void (*&&)(void*), rocksdb::BGThreadMetadata*&&) (
    __fn=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1718, DIE 0x41173c1>,
    __args#0=<unknown type in /root/code/rocksdb/build/librocksdb.so.6, CU 0x40e1718, DIE 0x41173de>) at /opt/rh/devtoolset-8/root/usr/include/c++/8/bits/invoke.h:95
#17 0x00007ffff737506f in std::thread::_Invoker<std::tuple<void (*)(void*), rocksdb::BGThreadMetadata*> >::_M_invoke<0ul, 1ul> (this=0x657568)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/thread:244
#18 0x00007ffff737502a in std::thread::_Invoker<std::tuple<void (*)(void*), rocksdb::BGThreadMetadata*> >::operator() (this=0x657568)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/thread:253
#19 0x00007ffff737500e in std::thread::_State_impl<std::thread::_Invoker<std::tuple<void (*)(void*), rocksdb::BGThreadMetadata*> > >::_M_run (this=0x657560)
    at /opt/rh/devtoolset-8/root/usr/include/c++/8/thread:196
#20 0x00007ffff5d78630 in execute_native_thread_routine () at ../../../../../libstdc++-v3/src/c++11/thread.cc:80
#21 0x00007ffff6090dd5 in start_thread () from /lib64/libpthread.so.0
#22 0x00007ffff54c902d in clone () from /lib64/libc.so.6 
 */
Status FlushJob::WriteLevel0Table() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_FLUSH_WRITE_L0);
  db_mutex_->AssertHeld();
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUNanos() / 1000;
  Status s;

  std::vector<BlobFileAddition> blob_file_additions;

  {
    auto write_hint = cfd_->CalculateSSTWriteHint(0);
    db_mutex_->Unlock();
    if (log_buffer_) {
      log_buffer_->FlushBufferToLog();
    }
    // memtables and range_del_iters store internal iterators over each data
    // memtable and its associated range deletion memtable, respectively, at
    // corresponding indexes.
    std::vector<InternalIterator*> memtables;
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters;
    ReadOptions ro;
    ro.total_order_seek = true;
    Arena arena;
    uint64_t total_num_entries = 0, total_num_deletes = 0;
    uint64_t total_data_size = 0;
    size_t total_memory_usage = 0;
    for (MemTable* m : mems_) {
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Flushing memtable with next log file: %" PRIu64 "\n",
          cfd_->GetName().c_str(), job_context_->job_id, m->GetNextLogNumber());
      memtables.push_back(m->NewIterator(ro, &arena));
      auto* range_del_iter =
          m->NewRangeTombstoneIterator(ro, kMaxSequenceNumber);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }
      total_num_entries += m->num_entries();
      total_num_deletes += m->num_deletes();
      total_data_size += m->get_data_size();
      total_memory_usage += m->ApproximateMemoryUsage();
    }

    event_logger_->Log() << "job" << job_context_->job_id << "event"
                         << "flush_started"
                         << "num_memtables" << mems_.size() << "num_entries"
                         << total_num_entries << "num_deletes"
                         << total_num_deletes << "total_data_size"
                         << total_data_size << "memory_usage"
                         << total_memory_usage << "flush_reason"
                         << GetFlushReasonString(cfd_->GetFlushReason());

    {
      ScopedArenaIterator iter(
          NewMergingIterator(&cfd_->internal_comparator(), &memtables[0],
                             static_cast<int>(memtables.size()), &arena));
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": started",
                     cfd_->GetName().c_str(), job_context_->job_id,
                     meta_.fd.GetNumber());

      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:output_compression",
                               &output_compression_);
      int64_t _current_time = 0;
      auto status = clock_->GetCurrentTime(&_current_time);
      // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
      if (!status.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Failed to get current time to populate creation_time property. "
            "Status: %s",
            status.ToString().c_str());
      }
      const uint64_t current_time = static_cast<uint64_t>(_current_time);

      uint64_t oldest_key_time =
          mems_.front()->ApproximateOldestKeyTime();

      // It's not clear whether oldest_key_time is always available. In case
      // it is not available, use current_time.
      uint64_t oldest_ancester_time = std::min(current_time, oldest_key_time);

      TEST_SYNC_POINT_CALLBACK(
          "FlushJob::WriteLevel0Table:oldest_ancester_time",
          &oldest_ancester_time);
      meta_.oldest_ancester_time = oldest_ancester_time;

      meta_.file_creation_time = current_time;

      uint64_t creation_time = (cfd_->ioptions()->compaction_style ==
                                CompactionStyle::kCompactionStyleFIFO)
                                   ? current_time
                                   : meta_.oldest_ancester_time;

      IOStatus io_s;
      const std::string* const full_history_ts_low =
          (full_history_ts_low_.empty()) ? nullptr : &full_history_ts_low_;
      s = BuildTable(
          dbname_, versions_, db_options_, *cfd_->ioptions(),
          mutable_cf_options_, file_options_, cfd_->table_cache(), iter.get(),
          std::move(range_del_iters), &meta_, &blob_file_additions,
          cfd_->internal_comparator(), cfd_->int_tbl_prop_collector_factories(),
          cfd_->GetID(), cfd_->GetName(), existing_snapshots_,
          earliest_write_conflict_snapshot_, snapshot_checker_,
          output_compression_, mutable_cf_options_.compression_opts,
          mutable_cf_options_.paranoid_file_checks, cfd_->internal_stats(),
          TableFileCreationReason::kFlush, &io_s, io_tracer_, event_logger_,
          job_context_->job_id, Env::IO_HIGH, &table_properties_, 0 /* level */,
          creation_time, oldest_key_time, write_hint, current_time, db_id_,
          db_session_id_, full_history_ts_low, blob_callback_);
      if (!io_s.ok()) {
        io_status_ = io_s;
      }
      LogFlush(db_options_.info_log);
    }
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": %" PRIu64
                   " bytes %s"
                   "%s",
                   cfd_->GetName().c_str(), job_context_->job_id,
                   meta_.fd.GetNumber(), meta_.fd.GetFileSize(),
                   s.ToString().c_str(),
                   meta_.marked_for_compaction ? " (needs compaction)" : "");

    if (s.ok() && output_file_directory_ != nullptr && sync_output_directory_) {
      s = output_file_directory_->Fsync(IOOptions(), nullptr);
    }
    TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table", &mems_);
    db_mutex_->Lock();
  }
  base_->Unref();

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  const bool has_output = meta_.fd.GetFileSize() > 0;

  if (s.ok() && has_output) {
    // if we have more than 1 background thread, then we cannot
    // insert files directly into higher levels because some other
    // threads could be concurrently producing compacted files for
    // that key range.
    // Add file to L0
    edit_->AddFile(0 /* level */, meta_.fd.GetNumber(), meta_.fd.GetPathId(),
                   meta_.fd.GetFileSize(), meta_.smallest, meta_.largest,
                   meta_.fd.smallest_seqno, meta_.fd.largest_seqno,
                   meta_.marked_for_compaction, meta_.oldest_blob_file_number,
                   meta_.oldest_ancester_time, meta_.file_creation_time,
                   meta_.file_checksum, meta_.file_checksum_func_name);

    edit_->SetBlobFileAdditions(std::move(blob_file_additions));
  }
#ifndef ROCKSDB_LITE
  // Piggyback FlushJobInfo on the first first flushed memtable.
  mems_[0]->SetFlushJobInfo(GetFlushJobInfo());
#endif  // !ROCKSDB_LITE

  // Note that here we treat flush as level 0 compaction in internal stats
  InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
  stats.micros = clock_->NowMicros() - start_micros;
  stats.cpu_micros = clock_->CPUNanos() / 1000 - start_cpu_micros;

  if (has_output) {
    stats.bytes_written = meta_.fd.GetFileSize();
    stats.num_output_files = 1;
  }

  const auto& blobs = edit_->GetBlobFileAdditions();
  for (const auto& blob : blobs) {
    stats.bytes_written_blob += blob.GetTotalBlobBytes();
  }

  stats.num_output_files_blob = static_cast<int>(blobs.size());

  RecordTimeToHistogram(stats_, FLUSH_TIME, stats.micros);
  cfd_->internal_stats()->AddCompactionStats(0 /* level */, thread_pri_, stats);
  cfd_->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED,
      stats.bytes_written + stats.bytes_written_blob);
  RecordFlushIOStats();
  /*
   * 这里返回的状态信息，是buildtable返回的
   * 所以如果buildtable出错，那么也会返回错误 
   */
  return s;
}

#ifndef ROCKSDB_LITE
std::unique_ptr<FlushJobInfo> FlushJob::GetFlushJobInfo() const {
  db_mutex_->AssertHeld();
  std::unique_ptr<FlushJobInfo> info(new FlushJobInfo{});
  info->cf_id = cfd_->GetID();
  info->cf_name = cfd_->GetName();

  const uint64_t file_number = meta_.fd.GetNumber();
  info->file_path =
      MakeTableFileName(cfd_->ioptions()->cf_paths[0].path, file_number);
  info->file_number = file_number;
  info->oldest_blob_file_number = meta_.oldest_blob_file_number;
  info->thread_id = db_options_.env->GetThreadID();
  info->job_id = job_context_->job_id;
  info->smallest_seqno = meta_.fd.smallest_seqno;
  info->largest_seqno = meta_.fd.largest_seqno;
  info->table_properties = table_properties_;
  info->flush_reason = cfd_->GetFlushReason();
  return info;
}
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
