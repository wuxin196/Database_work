#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING)
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);

    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    LockRequest req(txn->get_transaction_id(), LockMode::SHARED);

    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[lock_id];

    // 检查冲突
    if (is_conflict(req.lock_mode_, queue.group_lock_mode_)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 成功加锁
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING)
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);

    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    LockRequest req(txn->get_transaction_id(), LockMode::EXLUCSIVE);

    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[lock_id];

    if (is_conflict(req.lock_mode_, queue.group_lock_mode_)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    req.granted_ = true;
    queue.request_queue_.push_back(req);
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING)
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    LockRequest req(txn->get_transaction_id(), LockMode::SHARED);

    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[lock_id];

    if (is_conflict(req.lock_mode_, queue.group_lock_mode_))
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);

    req.granted_ = true;
    queue.request_queue_.push_back(req);
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);
    txn->get_lock_set()->insert(lock_id);
    return true;
}


/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    LockRequest req(txn->get_transaction_id(), LockMode::EXLUCSIVE);

    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[lock_id];

    if (is_conflict(req.lock_mode_, queue.group_lock_mode_))
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);

    req.granted_ = true;
    queue.request_queue_.push_back(req);
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    LockRequest req(txn->get_transaction_id(), LockMode::INTENTION_SHARED);

    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[lock_id];

    if (is_conflict(req.lock_mode_, queue.group_lock_mode_))
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);

    req.granted_ = true;
    queue.request_queue_.push_back(req);
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);
    txn->get_lock_set()->insert(lock_id);
    return true;
}
/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    LockRequest req(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);

    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[lock_id];

    if (is_conflict(req.lock_mode_, queue.group_lock_mode_))
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);

    req.granted_ = true;
    queue.request_queue_.push_back(req);
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = lock_table_.find(lock_data_id);
    if (it == lock_table_.end()) return false;

    auto& queue = it->second;
    for (auto iter = queue.request_queue_.begin(); iter != queue.request_queue_.end(); ++iter) {
        if (iter->txn_id_ == txn->get_transaction_id()) {
            queue.request_queue_.erase(iter);
            break;
        }
    }

    // 更新队列状态
    queue.group_lock_mode_ = compute_group_mode(queue.request_queue_);

    // 从事务 lock_set 移除
    txn->get_lock_set()->erase(lock_data_id);
    return true;
}
