/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

#include <list>
#include <mutex>


namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() :
        hash_table(HASH_TABLE_BUCKET_SIZE) {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    typename  std::list<T>::iterator iter;
    std::lock_guard<std::mutex> guard(latch);
    if (hash_table.Find(value, iter)) {
        queue.splice(queue.begin(), queue, iter);
    } else {
        queue.push_front(value);
        hash_table.Insert(value, queue.begin());
    }
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    std::lock_guard<std::mutex> guard(latch);
    if (queue.empty()) {
        return false;
    }
    value = queue.back();
    hash_table.Remove(value);
    queue.pop_back();
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    typename std::list<T>::iterator erase_it;
    std::lock_guard<std::mutex> guard(latch);
    if (hash_table.Find(value, erase_it)) {
        hash_table.Remove(value);
        queue.erase(erase_it);
        return true;
    }
    return false;
}

template <typename T> size_t LRUReplacer<T>::Size() {
    std::lock_guard<std::mutex> guard(latch);
    return queue.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
