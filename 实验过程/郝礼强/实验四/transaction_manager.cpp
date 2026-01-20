#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (txn == nullptr) {
        // 分配事务 ID
        txn_id_t txn_id = next_txn_id_.fetch_add(1);
        txn = new Transaction(txn_id);

        // 设置事务模式：显式事务
        txn->set_txn_mode(true);
    }
    timestamp_t start_ts = next_timestamp_.fetch_add(1);
    txn->set_start_ts(start_ts);
    txn->set_state(TransactionState::GROWING);
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn_map[txn->get_transaction_id()] = txn;
    }
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    
    if (log_manager != nullptr) {
       auto *log_record = new CommitLogRecord(txn->get_transaction_id());
        log_record->prev_lsn_ = txn->get_prev_lsn();

        lsn_t lsn = log_manager->add_log_to_buffer(log_record);
        txn->set_prev_lsn(lsn);

        log_manager->flush_log_to_disk();
    }

    // 2. 进入 SHRINKING 阶段
    txn->set_state(TransactionState::SHRINKING);

    // 3. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (const auto& lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 4. 更新事务状态
    txn->set_state(TransactionState::COMMITTED);

    // 5. 从事务表移除
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn_map.erase(txn->get_transaction_id());
    }

    // 6. 释放事务对象
    delete txn;
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    assert(txn != nullptr);

    // 1. 回滚所有写操作（逆序 undo）
    auto write_set = txn->get_write_set();
    for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
        WriteRecord* wr = *it;
        const std::string& tab_name = wr->GetTableName();

        auto fh = sm_manager_->fhs_.at(tab_name).get();

        if (wr->GetWriteType() == WType::INSERT_TUPLE) {
            fh->delete_record(wr->GetRid(), nullptr);
        } else if (wr->GetWriteType() == WType::DELETE_TUPLE) {
            fh->insert_record(wr->GetRecord().data, nullptr);
        } else if (wr->GetWriteType() == WType::UPDATE_TUPLE) {
            fh->update_record(wr->GetRid(), wr->GetRecord().data, nullptr);
        }
    }

    // 2. 写 ABORT 日志并刷盘（如果开启日志）
    if (log_manager != nullptr) {
        auto *log_record = new AbortLogRecord(txn->get_transaction_id());
        log_record->prev_lsn_ = txn->get_prev_lsn();

        lsn_t lsn = log_manager->add_log_to_buffer(log_record);
        txn->set_prev_lsn(lsn);

        log_manager->flush_log_to_disk();
    }

    // 3. 进入 SHRINKING 阶段
    txn->set_state(TransactionState::SHRINKING);

    // 4. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (const auto& lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);

    // 6. 从事务表移除
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn_map.erase(txn->get_transaction_id());
    }

    // 7. 释放事务对象
    delete txn;
}