/**
 * lock_manager.cpp
 */

#include <assert.h>
#include <future>
#include "concurrency/lock_manager.h"

namespace cmudb {

bool LockManager::isValidToAcquireLock(Transaction *txn) {
  if (txn->GetState() == TransactionState::ABORTED) { return false; }
  if (txn->GetState() == TransactionState::COMMITTED) { return false; }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return true;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!isValidToAcquireLock(txn)) {
    return false;
  }
  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> grand(record_map_latch);
  if (record_lock_table.find(rid) == record_lock_table.end()) {
    // No lock found for the record.
    //create a waitlist and add into record_lock_table
    auto wait_list_sptr = std::make_shared<WaitList>(txn, WaitState::SHARED);
    assert(wait_list_sptr);
    record_lock_table[rid] = wait_list_sptr;
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }

  auto wait_list_sptr = record_lock_table[rid];
  assert(wait_list_sptr);
  assert(wait_list_sptr->state == WaitState::SHARED
         || wait_list_sptr->state == WaitState::EXCLUSIVE);
  if (wait_list_sptr->state == WaitState::EXCLUSIVE) {
    assert(wait_list_sptr->granted.size() == 1);
    if (txn->GetTransactionId() > wait_list_sptr->granted.front()->GetTransactionId()) {
      // Wait-Die policy
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else if (txn->GetTransactionId() == wait_list_sptr->granted.front()->GetTransactionId()) {
      return true;
    }
    wait_list_sptr->wait_list.emplace_back(txn, WaitState::SHARED);
    auto promise = wait_list_sptr->wait_list.back().promise;
    grand.unlock();
    std::future<bool> future = promise->get_future();
    future.wait();
    grand.lock();
    if (!future.get()) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    txn->GetSharedLockSet()->insert(rid);
    wait_list_sptr->granted.push_back(txn);
    return true;
  }
  assert(wait_list_sptr->state == WaitState::SHARED);
  if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
    return true;
  }
  wait_list_sptr->granted.push_back(txn);
  txn->GetSharedLockSet()->insert(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!isValidToAcquireLock(txn)) {
    return false;
  }
  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> grand(record_map_latch);
  if (record_lock_table.find(rid) == record_lock_table.end()) {
    // No lock found for the record.
    //create a waitlist and add into record_lock_table
    auto wait_list_sptr = std::make_shared<WaitList>(txn, WaitState::EXCLUSIVE);
    assert(wait_list_sptr);
    record_lock_table[rid] = wait_list_sptr;
    txn->GetExclusiveLockSet()->insert(rid);
    return true;
  }
  auto wait_list_sptr = record_lock_table[rid];
  assert(wait_list_sptr);
  assert(wait_list_sptr->state == WaitState::SHARED
         || wait_list_sptr->state == WaitState::EXCLUSIVE);
  for (auto grant_txn_iter = wait_list_sptr->granted.cbegin();
       grant_txn_iter != wait_list_sptr->granted.cend(); grant_txn_iter++) {
    if (txn->GetTransactionId() > (*grant_txn_iter)->GetTransactionId()) {
      // Wait-Die policy
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else if (wait_list_sptr->state == WaitState::EXCLUSIVE
               && txn->GetTransactionId() == (*grant_txn_iter)->GetTransactionId()) {
      assert(wait_list_sptr->granted.size() == 1);
      return true;
    }
  }
  wait_list_sptr->wait_list.emplace_back(txn, WaitState::EXCLUSIVE);
  auto promise = wait_list_sptr->wait_list.back().promise;
  grand.unlock();
  std::future<bool> future = promise->get_future();
  future.wait();
  grand.lock();
  if (!future.get()) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  wait_list_sptr->granted.push_back(txn);
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!isValidToAcquireLock(txn)) {
    return false;
  }
  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> grand(record_map_latch);
  if (record_lock_table.find(rid) == record_lock_table.end()) {
    return false;
  }
  auto wait_list_sptr = record_lock_table[rid];
  assert(wait_list_sptr);
  auto granted_txn = std::find(wait_list_sptr->granted.cbegin(),
                               wait_list_sptr->granted.cend(), txn);
  if (granted_txn == wait_list_sptr->granted.cend()) {
    return false;
  }
  if (wait_list_sptr->state == WaitState::EXCLUSIVE) {
    assert(wait_list_sptr->granted.size() == 1);
    return true;
  }
  grand.unlock();
  return Unlock(txn, rid) && LockExclusive(txn, rid);
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (strict_2PL_) {
    if (!(txn->GetState() == TransactionState::COMMITTED
          || txn->GetState() == TransactionState::ABORTED)) {
      return false;
    }
  }
  std::unique_lock<std::mutex> grand(record_map_latch);
  if (record_lock_table.find(rid) == record_lock_table.end()) {
    assert(false);
    return false;
  }
  auto wait_list_sptr = record_lock_table[rid];
  assert(wait_list_sptr);
  auto granted_txn_citer = std::find(wait_list_sptr->granted.cbegin(),
                                     wait_list_sptr->granted.cend(), txn);
  if (granted_txn_citer == wait_list_sptr->granted.cend()) {
    assert(false);
    return false;
  }
  wait_list_sptr->granted.erase(granted_txn_citer);
  if (wait_list_sptr->state == WaitState::EXCLUSIVE) {
    assert(txn->GetExclusiveLockSet()->erase(rid) == 1);
  } else {
    assert(txn->GetSharedLockSet()->erase(rid) == 1);
  }
  if (!strict_2PL_ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (!wait_list_sptr->granted.empty()) {
    assert(wait_list_sptr->state == WaitState::SHARED);
    return true;
  }
  if (wait_list_sptr->wait_list.empty()) {
    record_lock_table.erase(rid);
    return true;
  }
  auto promise = wait_list_sptr->wait_list.back().promise;
  auto wake_txn_id = wait_list_sptr->wait_list.back().transaction->GetTransactionId();
  promise->set_value(true);
  wait_list_sptr->state = wait_list_sptr->wait_list.back().target_state;
  wait_list_sptr->wait_list.pop_back();
  for (auto wait_txn_iter = wait_list_sptr->wait_list.cbegin();
       wait_txn_iter != wait_list_sptr->wait_list.cend(); wait_txn_iter++) {
    if ((*wait_txn_iter).transaction->GetTransactionId() > wake_txn_id) {
      // Wait-Die policy
      (*wait_txn_iter).promise->set_value(false);
    }
  }
  return true;
}

} // namespace cmudb
