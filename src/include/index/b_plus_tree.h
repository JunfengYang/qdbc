/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <queue>
#include <vector>
#include <mutex>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

enum class BTreeOper { SEARCH = 0, INSERTION, REMOVE };

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                           BufferPoolManager *buffer_pool_manager,
                           const KeyComparator &comparator,
                           page_id_t root_page_id = INVALID_PAGE_ID);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // Print this B+ tree to stdout using a simple command-line
  std::string ToString(bool verbose = false);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // expose for test purpose
  B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key,
                                           Transaction *transaction,
                                           BTreeOper bTreeOper,
                                           bool leftMost = false);

private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N> N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(
      N *&neighbor_node, N *&node,
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
      int index, Transaction *transaction = nullptr);

  template <typename N> void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = false);

  void LockPageForOperation(Page *page, BTreeOper bTreeOper) {
    switch (bTreeOper) {
      case BTreeOper::SEARCH:
        page->RLatch();
        break;
      default:
        page->WLatch();
        break;
    }
  }

  void UnLockPageForOperation(Page *page, BTreeOper bTreeOper) {
    switch (bTreeOper) {
      case BTreeOper::SEARCH:
        page->RUnlatch();
            break;
      default:
        page->WUnlatch();
            break;
    }
  }

  void ReleaseTransactionLocks(Transaction *transaction, BTreeOper bTreeOper,
                               bool isDirty = false) {
    assert(transaction);
    while (!transaction->GetPageSet()->empty()) {
      Page *toUnlock = transaction->GetPageSet()->front();
      UnLockPageForOperation(toUnlock, bTreeOper);
      transaction->GetPageSet()->pop_front();
      buffer_pool_manager_->UnpinPage(toUnlock->GetPageId(), isDirty);
    }
    if (bTreeOper == BTreeOper::REMOVE) {
      std::unordered_set<page_id_t> & ref = *transaction->GetDeletedPageSet();
      for(auto iter = ref.begin(); iter !=ref.end(); iter++){
        Page* page = buffer_pool_manager_->FetchPage(*iter);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(*iter, false);//unpin page
        buffer_pool_manager_->DeletePage(*iter);//do delete
      }
      ref.clear();
    }
  }

  void ReleaseSafeAncestorsLocks(Transaction *transaction, BTreeOper bTreeOper, Page *page) {
    auto *node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(page->GetData());
    switch (bTreeOper) {
      case BTreeOper::SEARCH:
        // Release ancestors locks.
        break;
      case BTreeOper::INSERTION:
        if (node->GetSize() >= node->GetMaxSize() - 1) {
          return;
        }
        // Safe to release ancestors locks.
        break;
      case BTreeOper::REMOVE:
        if (node->GetSize() <= node->GetMinSize()) {
          return;
        }
        // Safe to release ancestors locks.
        break;
    }
    ReleaseTransactionLocks(transaction, bTreeOper);
  }

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  mutable std::mutex btree_latch;
};

} // namespace cmudb
