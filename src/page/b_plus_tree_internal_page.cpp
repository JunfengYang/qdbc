/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(1);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize((PAGE_SIZE -  24) / sizeof(MappingType));
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); i++) {
        ValueType current_value = array[i].second;
        if (current_value == value) {
            return i;
        }
    }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    for (int i = 1; i < GetSize(); i++) {
        if (comparator(key, array[i].first) < 0) {
            return array[i-1].second;
        }
    }
  return array[GetSize()-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    array[0] = std::make_pair<KeyType, ValueType>(nullptr, old_value);
    array[1] = std::make_pair<KeyType, ValueType>(new_key, new_value);
    SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    auto index = ValueIndex(old_value);
    if (index < 0) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "old value not exists");
    }
    memmove(array + index + 2, array + index + 1, sizeof(MappingType) * (GetSize() - index - 1));
    array[index + 1].first = new_key;
    array[index + 1].second = new_value;
    auto new_size = GetSize() + 1;
    SetSize(new_size);
  return new_size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    auto current_size = GetSize();
    auto start_index = current_size / 2;
    recipient->CopyHalfFrom(
        &array[start_index], current_size - start_index, buffer_pool_manager);
    SetSize(start_index);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    auto start_index = GetSize() - 1;
    for(int i = 0; i < size; i++) {
        array[start_index + i] = std::move(*items);
        items++;
    }
    SetSize(GetSize() + size);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    if (index > GetSize() - 1) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "out of index");
    }
    for(int i = index; i < GetSize() - 1; i++) {
        array[i] = std::move(array[i + 1]);
    }
    SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    SetSize(GetSize() - 1);
    return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
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
    recipient->CopyAllFrom(&array[0], GetSize(), buffer_pool_manager);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
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
    buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(array[0], buffer_pool_manager);
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
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    array[GetSize()] = std::move(pair);
    SetSize(GetSize() + 1);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyFirstFrom(
            array[GetSize() - 1], parent_index, buffer_pool_manager);
    SetSize(GetSize() - 1);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    memmove(array + 1, array, GetSize() * sizeof(MappingType));
    array[0] = std::move(pair);
    SetSize(GetSize() + 1);
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while printing");
    }
    auto *parrent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    parrent_node->SetKeyAt(parent_index, array[0].first);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
