/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <future>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {
enum class WaitState { INIT, SHARED, EXCLUSIVE };

class WaitList {
  public:
    WaitList(const WaitState &) = delete;
    WaitList &operator=(const WaitList &) = delete;
    WaitList(Transaction * txn, WaitState target) : state(target) {
      granted.insert(txn);
    }

    class WaitItem {
      public:
        WaitItem(const WaitItem &) = delete;
        WaitItem &operator=(const WaitItem &) = delete;
        WaitItem(Transaction * txn, WaitState ws) : transaction(txn), target_state(ws) {}

        Transaction * transaction;
        WaitState target_state;
        std::shared_ptr<std::promise<bool> > promise =
            std::make_shared<std::promise<bool>>();
    };

    std::list<Transaction *> granted;
    WaitState state;
    std::list<WaitItem> wait_list;
};

class LockManager {

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  bool strict_2PL_;
  std::mutex record_map_latch;
  std::unordered_map<RID, std::shared_ptr<WaitList>>record_lock_table;
  bool isValidToAcquireLock(Transaction *txn);
};

} // namespace cmudb
