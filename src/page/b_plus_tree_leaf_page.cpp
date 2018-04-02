/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);
    SetMaxSize((PAGE_SIZE -  24) / sizeof(MappingType));
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
    for(int i = 0; i < GetSize(); i++) {
        if (0 <= comparator(key, array[i].first)) {
            return i;
        }
    }
    return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
    KeyType key = array[index].first;
    return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
    return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    int insert_pos = 0;
    bool find = false;
    for(; insert_pos < GetSize() - 1; insert_pos++) {
        if (comparator(key, array[insert_pos].first) < 0) {
            find = true;
            break;
        }
    }
    if (find) {
        memmove(array + insert_pos + 1, array + insert_pos,
                (GetSize() - insert_pos)*sizeof(MappingType));
        array[insert_pos].first = key;
        array[insert_pos].second = value;
    } else {
        array[GetSize()] = std::make_pair(key, value);
    }
    SetSize(GetSize() + 1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    auto current_size = GetSize();
    auto start_index = current_size / 2;
    recipient->CopyHalfFrom(
        &array[start_index], current_size - start_index);
    SetSize(start_index);
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    auto start_index = GetSize() - 1;
    for(int i = 0; i < size; i++) {
        array[start_index + i] = std::move(*items);
        items++;
    }
    SetSize(GetSize() + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    for(int i = 0; i < GetSize(); i++) {
        if (0 == comparator(key, array[i].first)) {
            value = array[i].second;
            return true;
        }
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
    for(int i = 0; i < GetSize(); i++) {
        if (0 == comparator(key, array[i].first)) {
            if (i != (GetSize() - 1)) {
                memmove(array + i, array + i + 1,
                        (GetSize() - i - 1) * sizeof(MappingType));
            }
            SetSize(GetSize() - 1);
            break;
        }
    }
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(
    BPlusTreeLeafPage *recipient, int index_in_parent,
    BufferPoolManager * buffer_pool_manager) {
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while printing");
    }
    auto *parrent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    if (KeyAt(0) < recipient->KeyAt(0)) {
        parrent_node->SetKeyAt(index_in_parent + 1, KeyAt(0));
    }
    parrent_node->Remove(index_in_parent);
    recipient->CopyAllFrom(array, GetSize());
    recipient->SetNextPageId(GetNextPageId());
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    auto start_index = GetSize();
    if ((*items).first < KeyAt(0)) {
        memmove(array + size + 1, array, GetSize() * sizeof(MappingType));
        start_index = 0;
    }
    for(int i = 0; i < size; i++) {
        array[start_index + i] = std::move(*items);
        items++;
    }
    SetSize(GetSize() + size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(array[0]);
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while printing");
    }
    auto *parrent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    auto parrent_index = parrent_node->ValueIndex(array[0].second);
    if (parrent_index < 0) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "old value not exists");
    }
    parrent_node->SetKeyAt(parrent_index, array[1].first);
    for(int i = 0; i < GetSize() - 1; i++) {
        array[i] = std::move(array[i + 1]);
    }
    SetSize(GetSize() - 1);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array[GetSize()] = std::move(item);
    SetSize(GetSize() + 1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyFirstFrom(
        array[GetSize() - 1], parent_index, buffer_pool_manager);
    SetSize(GetSize() - 1);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    memmove(array + 1, array, GetSize() * sizeof(MappingType));
    array[0] = std::move(item);
    SetSize(GetSize() + 1);
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while printing");
    }
    auto *parrent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    parrent_node->SetKeyAt(parent_index, array[0].first);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
