/**
 * concurrent_hash_map.h
 *
 * This implementation borrows from folly::ConcurrentHashMap the idea of sharding key space
 * into different segments. However, each segment here is only coarsely guarded with a read-write
 * latch as opposed to a more granular approach in the highly-optimized folly::ConcurrentHashMap.
 *
 * This map should be used in conjuction with shared_ptr because its destructor is not
 * thread-safe. With shared_ptr, the last thread that releases the pointer will be the only
 * one accessing the map at destruction time.
 */
#pragma once

#include <atomic>
#include <memory>

#include "data_structure/rwlatch.h"

namespace slog {

namespace concurrent_hash_map {

template <typename KeyType, typename ValueType>
struct NodeT {
  NodeT() = default;

  NodeT(const NodeT& other) {
    next = other.next;
    key = other.key;
    value = other.value;
  }

  NodeT* next{nullptr};
  KeyType key;
  ValueType value;
};

template <typename KeyType, typename ValueType, typename HashFn = std::hash<KeyType>, uint8_t ShardBits = 8>
class SegmentT {
  using Node = NodeT<KeyType, ValueType>;

  static constexpr float kLoadFactor = 1.05;

 public:
  /**
   * initial_bucket_count must be a power of 2
   */
  SegmentT(size_t initial_bucket_count = 8)
      : load_factor_max_size_(static_cast<size_t>(kLoadFactor * initial_bucket_count)), size_(0) {
    buckets_.reset(Buckets::CreateBuckets(initial_bucket_count));
  }

  bool Get(ValueType& res, const KeyType& key) const {
    auto h = HashFn{}(key);
    bool found = false;

    rw_latch_.RLock();

    auto idx = GetIndex(buckets_->count, h);
    auto node = buckets_->bucket_roots[idx];
    while (node) {
      if (key == node->key) {
        break;
      }
      node = node->next;
    }
    if (node) {
      res = node->value;
      found = true;
    }

    rw_latch_.RUnlock();

    return found;
  }

  ValueType* GetUnsafe(const KeyType& key) {
    auto h = HashFn{}(key);
    auto idx = GetIndex(buckets_->count, h);
    auto node = buckets_->bucket_roots[idx];
    while (node) {
      if (key == node->key) {
        return &node->value;
      }
      node = node->next;
    }
    return nullptr;
  }

  bool InsertOrUpdate(const KeyType& key, const ValueType& value) {
    auto h = HashFn{}(key);

    rw_latch_.WLock();

    auto idx = GetIndex(buckets_->count, h);
    Node* prev = nullptr;
    bool key_exists = false;
    auto node = buckets_->bucket_roots[idx];
    while (node) {
      if (key == node->key) {
        key_exists = true;
        // If key already exists, replace the corresponding
        // node with a new node containing the new value
        auto new_node = new Node();
        new_node->key = key;
        new_node->value = value;
        new_node->next = node->next;
        if (prev) {
          prev->next = new_node;
        } else {
          buckets_->bucket_roots[idx] = new_node;
        }
        delete node;

        break;
      }
      prev = node;
      node = node->next;
    }

    if (!key_exists) {
      // If key does not exist, at new node to the bucket
      auto new_node = new Node();
      new_node->key = key;
      new_node->value = value;
      new_node->next = buckets_->bucket_roots[idx];
      buckets_->bucket_roots[idx] = new_node;
      size_++;
    }

    if (size_ >= load_factor_max_size_) {
      Rehash();
    }

    rw_latch_.WUnlock();

    return key_exists;
  }

  bool Erase(const KeyType& key) {
    auto h = HashFn{}(key);

    rw_latch_.WLock();

    bool key_exists = false;
    auto idx = GetIndex(buckets_->count, h);
    Node* prev = nullptr;
    auto node = buckets_->bucket_roots[idx];
    while (node) {
      if (key == node->key) {
        key_exists = true;
        if (prev) {
          prev->next = node->next;
        } else {
          buckets_->bucket_roots[idx] = node->next;
        }
        delete node;
        break;
      }
      prev = node;
      node = node->next;
    }

    rw_latch_.WUnlock();

    return key_exists;
  }

 private:
  // Must hold lock
  static uint64_t GetIndex(size_t nbuckets, size_t hash) { return (hash >> ShardBits) & (nbuckets - 1); }

  // Must hold lock
  void Rehash() {
    auto new_bucket_count = buckets_->count << 1;
    auto new_buckets = Buckets::CreateBuckets(new_bucket_count);

    for (size_t idx = 0; idx < buckets_->count; idx++) {
      auto node = buckets_->bucket_roots[idx];
      if (node == nullptr) {
        continue;
      }

      while (node) {
        auto next_node = node->next;

        auto idx = GetIndex(new_bucket_count, HashFn{}(node->key));
        node->next = new_buckets->bucket_roots[idx];
        new_buckets->bucket_roots[idx] = node;

        node = next_node;
      }
      buckets_->bucket_roots[idx] = nullptr;
    }
    buckets_.reset(new_buckets);
    load_factor_max_size_ = static_cast<size_t>(kLoadFactor * buckets_->count);
  }

  struct Buckets {
    static Buckets* CreateBuckets(size_t num_buckets) {
      auto buckets = new Buckets();
      buckets->count = num_buckets;
      buckets->bucket_roots = std::make_unique<Node*[]>(num_buckets);
      return buckets;
    }

    ~Buckets() {
      for (size_t i = 0; i < count; i++) {
        auto node = bucket_roots[i];
        while (node) {
          auto next = node->next;
          delete node;
          node = next;
        }
      }
    }

    size_t count;
    std::unique_ptr<Node*[]> bucket_roots;
  };

  mutable bustub::ReaderWriterLatch rw_latch_;
  std::unique_ptr<Buckets> buckets_;
  size_t load_factor_max_size_;
  size_t size_;
};

}  // namespace concurrent_hash_map

template <typename KeyType, typename ValueType, typename HashFn = std::hash<KeyType>, uint8_t ShardBits = 8>
class ConcurrentHashMap {
  using Segment = concurrent_hash_map::SegmentT<KeyType, ValueType, HashFn, ShardBits>;

 public:
  ConcurrentHashMap() {
    for (uint64_t i = 0; i < NumShards; i++) {
      segments_[i].store(nullptr);
    }
  }

  ~ConcurrentHashMap() {
    for (uint64_t i = 0; i < NumShards; i++) {
      auto segment = segments_[i].load();
      if (segment) {
        delete segment;
      }
    }
  }

  ValueType* GetUnsafe(const KeyType& key) {
    auto idx = PickSegment(key);
    return EnsureSegment(idx)->GetUnsafe(key);
  }

  bool Get(ValueType& res, const KeyType& key) const {
    auto idx = PickSegment(key);
    return EnsureSegment(idx)->Get(res, key);
  }

  bool InsertOrUpdate(const KeyType& key, const ValueType& value) {
    auto idx = PickSegment(key);
    return EnsureSegment(idx)->InsertOrUpdate(key, value);
  }

  bool Erase(const KeyType& key) {
    auto idx = PickSegment(key);
    return EnsureSegment(idx)->Erase(key);
  }

 private:
  uint64_t PickSegment(const KeyType& key) const {
    auto h = HashFn{}(key);
    return h & (NumShards - 1);
  }

  Segment* EnsureSegment(uint64_t idx) const {
    auto segment = segments_[idx].load();
    if (segment == nullptr) {
      auto new_segment = new Segment();
      if (!segments_[idx].compare_exchange_strong(segment, new_segment)) {
        delete new_segment;
      } else {
        segment = new_segment;
      }
    }
    return segment;
  }

  static constexpr uint64_t NumShards = (1LL << ShardBits);

  mutable std::atomic<Segment*> segments_[NumShards];
};

}  // namespace slog