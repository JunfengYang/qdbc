/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(
    B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node,
    BufferPoolManager* buffer_pool_manager,
    KeyComparator comparator) : current_position_(0), comparator_(comparator) {
    current_node_ = leaf_node;
    buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(
    B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_node,
    KeyType key,
    BufferPoolManager* buffer_pool_manager,
    KeyComparator comparator) : comparator_(comparator) {
    current_node_ = leaf_node;
    buffer_pool_manager_ = buffer_pool_manager;
    current_position_ = current_node_->KeyIndex(key, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    if (current_node_ == nullptr) {
        return true;
    }
    if (current_position_ >= current_node_->GetSize()) {
        if (current_node_->GetNextPageId() == INVALID_PAGE_ID) {
            return true;
        }
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*() {
    if (current_node_ == nullptr) {
        throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE,
                        "Null pointer error.");
    }
    return current_node_->GetItem(current_position_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++() {
    if (current_node_ != nullptr) {
        if (current_position_ >= current_node_->GetSize() - 1 ) {
            if (current_node_->GetNextPageId() != INVALID_PAGE_ID) {
                auto* page = buffer_pool_manager_->FetchPage(
                    current_node_->GetNextPageId());
                if (current_node_->GetPageId() == 14) {
                    std::cout << 14 << std::endl;
                }
                buffer_pool_manager_->UnpinPage(
                    current_node_->GetPageId(), false);
                current_node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(
                    page->GetData());
                current_position_ = 0;
            } else {
                buffer_pool_manager_->UnpinPage(
                    current_node_->GetPageId(), false);
                current_node_ = nullptr;
            }
        } else {
            current_position_++;
        }
    }
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
