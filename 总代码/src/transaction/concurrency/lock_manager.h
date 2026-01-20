#pragma once

#include <mutex>
#include <condition_variable>
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX};

    /* 事务的加锁申请 */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode)
            : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;   // 申请加锁的事务ID
        LockMode lock_mode_;    // 事务申请加锁的类型
        bool granted_;          // 该事务是否已经被赋予锁
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        std::list<LockRequest> request_queue_;  // 加锁队列
        std::condition_variable cv_;            // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;   // 加锁队列的锁模式
    };

public:
    // 判断请求锁mode是否与队列当前状态冲突
    bool is_conflict(LockManager::LockMode req_mode, LockManager::GroupLockMode group_mode) {
        switch (req_mode) {
            case LockManager::LockMode::SHARED:
                return group_mode == GroupLockMode::X || group_mode == GroupLockMode::SIX;
            case LockManager::LockMode::EXLUCSIVE:
                return group_mode != GroupLockMode::NON_LOCK;
            case LockManager::LockMode::INTENTION_SHARED:
                return group_mode == GroupLockMode::X || group_mode == GroupLockMode::SIX;
            case LockManager::LockMode::INTENTION_EXCLUSIVE:
                return group_mode == GroupLockMode::X || group_mode == GroupLockMode::S || group_mode == GroupLockMode::SIX;
            case LockManager::LockMode::S_IX:
                return group_mode != GroupLockMode::NON_LOCK;
            default:
                return true;
        }
    }

    // 根据队列状态更新 group_lock_mode_
    LockManager::GroupLockMode compute_group_mode(const std::list<LockManager::LockRequest>& queue) {
        bool has_s = false, has_x = false, has_is = false, has_ix = false, has_six = false;
        for (auto &req : queue) {
            switch (req.lock_mode_) {
                case LockManager::LockMode::SHARED: has_s = true; break;
                case LockManager::LockMode::EXLUCSIVE: has_x = true; break;
                case LockManager::LockMode::INTENTION_SHARED: has_is = true; break;
                case LockManager::LockMode::INTENTION_EXCLUSIVE: has_ix = true; break;
                case LockManager::LockMode::S_IX: has_six = true; break;
            }
        }
        if (has_x) return GroupLockMode::X;
        if (has_six) return GroupLockMode::SIX;
        if (has_s) return GroupLockMode::S;
        if (has_ix) return GroupLockMode::IX;
        if (has_is) return GroupLockMode::IS;
        return GroupLockMode::NON_LOCK;
    }

    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

private:
    std::mutex latch_;      // 用于锁表的并发
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;   // 全局锁表
};
