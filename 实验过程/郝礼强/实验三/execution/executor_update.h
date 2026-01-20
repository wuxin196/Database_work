#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        std::vector<std::unique_ptr<IxIndexHandle>> ihs;
        ihs.reserve(tab_.cols.size());

        for (size_t i = 0; i < tab_.cols.size(); ++i) {
            if (tab_.cols[i].index) {
                ihs.push_back(
                    sm_manager_->get_ix_manager()
                        ->open_index(tab_.name, tab_.cols)
                );
            } else {
                ihs.push_back(nullptr);
            }
        }
        for (auto &rid : rids_) {
            auto record = fh_->get_record(rid, context_);
            return std::make_unique<RmRecord>(*record);
        }
        return nullptr;
    }


    Rid &rid() override { return _abstract_rid; }
};