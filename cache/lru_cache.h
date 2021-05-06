//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <string>

#include "cache/sharded_cache.h"

#include "port/port.h"
#include "util/autovector.h"

namespace rocksdb {

// LRU cache implementation

/*
 * LRU cache中存储的元素的内存都是在堆上的
 * LRU cache的的元素会被cache引用、被外部引用
 * LRU cache会将所有的元素放在hash-table中, 一些元素同时会被存放在LRU list中
 */
// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created LRUHandles are in state 1. If you call
// LRUCacheShard::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call LRUCacheShard::Erase or LRUCacheShard::Insert with the
// same key.
// To move from state 2 to state 1, use LRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCacheShard::Lookup/LRUCacheShard::Insert have a
// matching
// RUCache::Release (to move into state 2) or LRUCacheShard::Erase (for state 3)

/*
 * cache存储的最基本的元素 
 */
struct LRUHandle {
  /* 实际的value */
  void* value;
  /* 析构函数 */
  void (*deleter)(const Slice&, void* value);
  /* 用于hashtable，拉链法的下一个元素 */
  LRUHandle* next_hash;
  /* 用于LRU链表 */
  LRUHandle* next;
  LRUHandle* prev;
  /* TODO: 占用的空间 */
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  /* 引用计数 */
  uint32_t refs;     // a number of refs to this entry
                     // cache itself is counted as 1

  /*
   * 记录了该Handle是否在cache中。
   * 该Handle是否为高优先级Handle，由调用者插数据时指定。
   * 是否位于高优先级队列中，如果该handle为高优先级Handle，则会将其插入LRU链表。
   */
  /* TOOD: 优先级需要留意下 */
  // Include the following flags:
  //   in_cache:    whether this entry is referenced by the hash table.
  //   is_high_pri: whether this entry is high priority entry.
  //   in_high_pro_pool: whether this entry is in high-pri pool.
  char flags; /* 记录了该Handle是否在cache中，该Handle是否为高优先级Handle,由调用者插数据时指定。以及是否位于高优先级队列中 */

  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons

  /* 真正的key数据： key_data[0] -- key_data(key_length) */
  char key_data[1];  // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) { /* TODO: ??? */
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }

  bool InCache() { return flags & 1; }
  bool IsHighPri() { return flags & 2; }
  bool InHighPriPool() { return flags & 4; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= 1;
    } else {
      flags &= ~1;
    }
  }

  void SetPriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      flags |= 2;
    } else {
      flags &= ~2;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      flags |= 4;
    } else {
      flags &= ~4;
    }
  }

  void Free() {
    assert((refs == 1 && InCache()) || (refs == 0 && !InCache()));
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] reinterpret_cast<char*>(this);
  }
};

/* rocksdb自己实现的hashtable，采用拉链法处理hash碰撞 */
// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {
 public:
  LRUHandleTable();
  ~LRUHandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToAllCacheEntries(T func) {
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_; /* bucket长度 */
  uint32_t elems_; /* hashtable真正有多少元素 */
  LRUHandle** list_; /* bucket */
};

/* 单个LRU cache实例 */
// A single shard of sharded cache.
class LRUCacheShard : public CacheShard {
 public:
  LRUCacheShard();
  virtual ~LRUCacheShard();

  /* TODO: reading here */
  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Cache::Handle** handle,
                        Cache::Priority priority) override;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;
  virtual bool Ref(Cache::Handle* handle) override;
  virtual bool Release(Cache::Handle* handle,
                       bool force_erase = false) override;
  virtual void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

  void TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Insert(LRUHandle* e);

  // 当高优先级链表引用的数据超过一个阈值时，将高优先级链表引用的数据，调整到低优先级链表上
  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, autovector<LRUHandle*>* deleted);

  /*
   * LRUCahche管理的内存上限。
   * 以下几个关于LRUCache的内存相关的数据指标，都仅仅只包括caller传入的charge，
   * 不包括LRUCache自身数据结构占用的内存
   */
  // Initialized before use.
  size_t capacity_; /* 总容量，但是没有计算LRU list的容量。TODO: 不知道这么设计的原因 */

  // Memory size for entries residing in the cache
  size_t usage_; /* 所有驻留在cache中的元素所占的内存大小 */

  // Memory size for entries residing only in the LRU list
  size_t lru_usage_; /* LRUList管理的内存大小 */

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable port::Mutex mutex_;

  /*
   * 整体来看，所谓的lru队列并不占用内存空间，它只是通过指针将hashtable的元素穿起来
   * 位于LRU链表中的Handle，引用即使一定为1
   */
  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  LRUHandle* lru_low_pri_;

  LRUHandleTable table_;
};

class LRUCache : public ShardedCache {
 public:
  LRUCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
           double high_pri_pool_ratio);
  virtual ~LRUCache();
  virtual const char* Name() const override { return "LRUCache"; }
  virtual CacheShard* GetShard(int shard) override;
  virtual const CacheShard* GetShard(int shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual void DisownData() override;

 private:
  LRUCacheShard* shards_;
};

}  // namespace rocksdb
