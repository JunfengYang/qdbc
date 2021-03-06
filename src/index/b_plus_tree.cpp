/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    bool find = false;
    auto *leaf_node = FindLeafPage(key);
    ValueType value;
    if (leaf_node->Lookup(key, value, comparator_)) {
        result.push_back(value);
        find =  true;
    }
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return find;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    if (IsEmpty()) {
        StartNewTree(key, value);
        return true;
    } else {
        return InsertIntoLeaf(key, value, transaction);
    }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    page_id_t  page_id;
    auto *page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr)
        throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
    root_page_id_ = page_id;
    UpdateRootPageId(true);
    auto *node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(
        page->GetData());
    node->Init(page_id);
    node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    // Find leaf node and insert new key value into it.
    auto *leaf_node = FindLeafPage(key);
    ValueType old_value;
    if (leaf_node->Lookup(key, old_value, comparator_)) {
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
        return false;
    }
    leaf_node->Insert(key, value, comparator_);
    // If leaf node exceed max_size, split it.
    if (leaf_node->GetSize() >= leaf_node->GetMaxSize()) {
        auto* new_node = Split(leaf_node);
        InsertIntoParent(leaf_node, new_node->KeyAt(0), new_node);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t split_page_id;
    auto split_page = buffer_pool_manager_->NewPage(
        split_page_id);
    if (split_page == nullptr)
        throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
    auto *new_node = reinterpret_cast<N *>(split_page->GetData());
    new_node->Init(split_page_id, node->GetParentPageId());
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    // If current node is root, generate new root node.
    if (old_node->IsRootPage()) {
        page_id_t new_root_id;
        auto new_root_page = buffer_pool_manager_->NewPage(
            new_root_id);
        if (new_root_page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
        auto *new_root_node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(
            new_root_page->GetData());
        new_root_node->Init(new_root_id);
        old_node->SetParentPageId(new_root_id);
        new_node->SetParentPageId(new_root_id);
        root_page_id_ = new_root_id;
        new_root_node->PopulateNewRoot(
            old_node->GetPageId(), key,
            new_node->GetPageId());
        UpdateRootPageId();
        buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    } else {
        auto page_id = old_node->GetParentPageId();
        auto parent_page = buffer_pool_manager_->FetchPage(page_id);
        auto* parent_node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(
            parent_page->GetData());
        parent_node->InsertNodeAfter(
            old_node->GetPageId(), key, new_node->GetPageId());
        if(parent_node->GetSize() >= parent_node->GetMaxSize()) {
            auto* split_node = Split(parent_node);
            InsertIntoParent(parent_node, split_node->KeyAt(0),
                             split_node, transaction);
            buffer_pool_manager_->UnpinPage(split_node->GetPageId(), true);
        }
        buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    if (IsEmpty()) {
        return;
    }
    auto* leaf_node = FindLeafPage(key);
    if (leaf_node->RemoveAndDeleteRecord(key, comparator_) < leaf_node->GetMinSize()) {
        if (CoalesceOrRedistribute(leaf_node, transaction)) {
            buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
            if (!buffer_pool_manager_->DeletePage(leaf_node->GetPageId())) {
                throw Exception(EXCEPTION_TYPE_INDEX, "Page still in use.");
            }
            return;
        }
    }
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribu te. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    if (node->IsRootPage()) {
        return AdjustRoot(node);
    }
    bool node_delete = false;
    auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto* parent_node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(
        parent_page->GetData());
    int index = parent_node->ValueIndex(node->GetPageId());
    Page *sibling_page;
    N *sibling_node;
    bool is_left_sibling = false;
    if (0 == index) {
        sibling_page = buffer_pool_manager_->FetchPage(
            parent_node->ValueAt(index + 1));
    } else {
        is_left_sibling = true;
        sibling_page = buffer_pool_manager_->FetchPage(
            parent_node->ValueAt(index - 1));
    }
    sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    if (sibling_node->GetSize() + node->GetSize() >= node->GetMaxSize()) {
        Redistribute(sibling_node, node, index);
        buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
    } else {
        if (is_left_sibling) {
            if (Coalesce(sibling_node, node, parent_node, index, transaction)) {
                buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
                buffer_pool_manager_->DeletePage(parent_node->GetPageId());
            }
            node_delete = true;
            buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
        } else {
            if (Coalesce(node, sibling_node, parent_node, index + 1, transaction)) {
                buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
                buffer_pool_manager_->DeletePage(parent_node->GetPageId());
            }
            buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), false);
            buffer_pool_manager_->DeletePage(sibling_node->GetPageId());
        }
    }
    return node_delete;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    if (parent->GetSize() < parent->GetMinSize()) {
        if (CoalesceOrRedistribute(parent, transaction)) {
            return true;
        }
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    if (0 == index) {
        neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    } else {
        neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->IsRootPage()) {
        if (1 == old_root_node->GetSize() && !old_root_node->IsLeafPage()) {
            auto* parent_node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(
                old_root_node);
            root_page_id_ = parent_node->RemoveAndReturnOnlyChild();
            auto* new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
            auto* new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
            new_root_node->SetParentPageId(INVALID_PAGE_ID);
            buffer_pool_manager_->UnpinPage(root_page_id_, true);
            UpdateRootPageId();
            return true;
        }
        if (old_root_node->IsLeafPage() && old_root_node->GetSize() < 1) {
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId();
            return true;
        }
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
    KeyType tmp_key;
    auto *leaf_node = FindLeafPage(tmp_key, true);
    return INDEXITERATOR_TYPE(leaf_node, buffer_pool_manager_, comparator_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
    auto *leaf_node = FindLeafPage(key);
  return INDEXITERATOR_TYPE(leaf_node, key, buffer_pool_manager_, comparator_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while printing");
    }
    auto *node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(page->GetData());
    page_id_t value;
    while (!node->IsLeafPage()) {
        if (leftMost) {
            value = node->ValueAt(0);
        } else {
            value = node->Lookup(key, comparator_);
        }
        if (value == INVALID_PAGE_ID) {
            return nullptr;
        }
        page = buffer_pool_manager_->FetchPage(value);
        if (page == nullptr)
        {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        }
        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
        node = reinterpret_cast<BPLUSTREE_INTERNAL_NODE_TYPE *>(
            page->GetData());
    }
    auto *leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
    return leaf_node;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
    return "Empty tree";
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
