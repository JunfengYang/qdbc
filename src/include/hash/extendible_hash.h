/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  // add your own member variables here
    class Bucket {
    public:
        Bucket(ExtendibleHash& extendible_hash) : local_depth_(0), extendible_hash_(extendible_hash) { }
        bool IsFull() const;
        void AddEntry(K key, V val);
        const V& GetValue(K key) const;

    private:
        uint32_t local_depth_;
        std::map<K, V> entries_;
        ExtendibleHash& extendible_hash_;
    };

    typedef std::shared_ptr<Bucket> BucketPtr;
    uint32_t gloable_depth_;
    std::vector<BucketPtr> buckets_;
    size_t bucket_size_;
};
} // namespace cmudb
