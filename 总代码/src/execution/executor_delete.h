#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord>Next() override {
        auto &indexes = tab_.indexes;
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            for (auto &index : indexes) {
                auto ih = sm_manager_->ihs_.at(index.tab_name).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (auto &col : index.cols) {
                    memcpy(key + offset,
                        rec->data + col.offset,
                        col.len);
                    offset += col.len;
                }
                ih->delete_entry(key, context_->txn_);
                delete[] key;
            }
            fh_->delete_record(rid, context_);
        }

        return nullptr;
    }


    Rid &rid() override { return _abstract_rid; }
};