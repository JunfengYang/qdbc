#include "hash/extendible_hash.h"

#include "page/page.h"
#include <list>
#include <map>
#include <mutex>

using std::hash;
using std::map;

namespace cmudb {

template <typename K, typename V>
bool ExtendibleHash<K, V>::Bucket::IsFull() const {
    return entries_.size() >= extendible_hash_.bucket_size_;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Bucket::AddEntry(K key, V val) {
    entries_[key] = val;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Bucket::DeleteEntry(K key) {
    entries_.erase(key);
}

template <typename K, typename V>
bool ExtendibleHash<K, V>::Bucket::IsExists(K key) {
    return entries_.find(key) != entries_.end();
}

template <typename K, typename V>
const V& ExtendibleHash<K, V>::Bucket::GetValue(K key) const {
    return entries_.at(key);
}

template <typename K, typename V>
const std::map<K, V>& ExtendibleHash<K, V>::Bucket::GetEntries() const {
    return entries_;
}

template <typename K, typename V>
uint32_t ExtendibleHash<K, V>::Bucket::GetDepth() const {
    return local_depth_;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Bucket::SetDepth(uint32_t new_depth) {
    local_depth_ = new_depth;
}

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :
    gloable_depth_(0), bucket_size_(size), bucket_number(0) {
    auto bucket = std::make_shared<typename ExtendibleHash<K, V>::Bucket>(*this);
    buckets_.push_back(bucket);
    bucket_number++;
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    size_t hash_value = hash<K>{}(key);
    size_t bucket_id = hash_value & ((1 << gloable_depth_) -1);
    return bucket_id;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return gloable_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    auto bucket_ptr = buckets_[bucket_id];
    return bucket_ptr->GetDepth();
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    return bucket_number;
}

template <typename K, typename V>
std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> ExtendibleHash<K, V>::
LockOperateBucket(std::unique_lock<std::mutex>* bucket_latch, const K &key) {
    auto bucket_ptr = buckets_[HashKey(key)];
    while (true) {
        *bucket_latch = std::unique_lock<std::mutex> (bucket_ptr->latch, std::defer_lock);
        bucket_latch->lock();
        auto new_bucket_ptr = buckets_[HashKey(key)];
        if (bucket_ptr == new_bucket_ptr) {
            break;
        } else {
            bucket_latch->unlock();
            bucket_ptr = new_bucket_ptr;
        }
    }
    return bucket_ptr;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::unique_lock<std::mutex> find_latch;
    auto bucket_ptr = LockOperateBucket(&find_latch, key);
    if (bucket_ptr->IsExists(key)) {
        value = bucket_ptr->GetValue(key);
        return true;
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    std::unique_lock<std::mutex> remove_latch;
    auto bucket_ptr = LockOperateBucket(&remove_latch, key);
    if (bucket_ptr->IsExists(key)) {
        bucket_ptr->DeleteEntry(key);
        return true;
    }
    return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    std::unique_lock<std::mutex> insert_latch;
    auto bucket_ptr = LockOperateBucket(&insert_latch, key);
    if (bucket_ptr->IsFull()) {
        bucket_ptr = SplitBucket(bucket_ptr, key);
    }
    bucket_ptr->AddEntry(key, value);
}

template <typename K, typename V>
std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> ExtendibleHash<K, V>::
SplitBucket(
    std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> bucket_ptr, K key) {
    size_t current_depth = bucket_ptr->GetDepth();
    if (gloable_depth_ == current_depth) {
        std::lock_guard<std::mutex> split_lock(latch_);
        if (gloable_depth_ == current_depth) {
            size_t current_size = buckets_.size();
            buckets_.resize(current_size * 2);
            std::copy(buckets_.begin(), buckets_.begin() + current_size,
                      buckets_.begin() + current_size);
            gloable_depth_++;
        }
    }

    if (gloable_depth_ > bucket_ptr->GetDepth()) {
        std::lock_guard<std::mutex> split_lock(latch_);
        if (gloable_depth_ > bucket_ptr->GetDepth()) {
            auto bucket1 = std::make_shared<typename ExtendibleHash<K, V>::Bucket>(*this);
            auto bucket2 = std::make_shared<typename ExtendibleHash<K, V>::Bucket>(*this);
            std::map<K, V> entries = bucket_ptr->GetEntries();
            for (auto it = entries.begin(); it != entries.end(); it++) {
                size_t current_bucket_id = HashKey(it->first);
                if (((current_bucket_id >> bucket_ptr->GetDepth()) & 1) == 0) {
                    bucket1->AddEntry(it->first, it->second);
                } else {
                    bucket2->AddEntry(it->first, it->second);
                }
            }
            for (size_t i = 0; i < buckets_.size(); i++) {
                if (buckets_[i] == bucket_ptr) {
                    if (((i >> bucket_ptr->GetDepth()) & 1) == 0) {
                        buckets_[i] = bucket1;
                    } else {
                        buckets_[i] = bucket2;
                    }
                }
            }
            bucket1->SetDepth(bucket_ptr->GetDepth() + 1);
            bucket2->SetDepth(bucket_ptr->GetDepth() + 1);
            bucket_number++;
        }
    }
    size_t bucket_id = HashKey(key);
    auto bucket_to_add = buckets_[bucket_id];
    if (bucket_to_add->IsFull()) {
        return SplitBucket(bucket_to_add, key);
    }
    return bucket_to_add;
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
