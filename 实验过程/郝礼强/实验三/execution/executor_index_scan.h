#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        auto ix_hdl = sm_manager_->ihs_.at(index_meta_.tab_name).get();
        std::vector<char> key(index_meta_.col_tot_len);
        size_t offset = 0;
        bool can_use_index = false;
        for (auto &col_name : index_col_names_) {
            bool found = false;
            for (auto &cond : fed_conds_) {
                if (cond.lhs_col.col_name == col_name &&
                    cond.op == OP_EQ &&
                    cond.is_rhs_val) {
                    memcpy(key.data() + offset,
                        cond.rhs_val.raw->data,
                        cond.rhs_val.raw->size);

                    offset += cond.rhs_val.raw->size;
                    found = true;
                    can_use_index = true;
                    break;
                }
            }
            if (!found) break;
        }
        scan_->next();
    }
    void nextTuple() override {
        if (scan_ != nullptr) {
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
         while (scan_ != nullptr && !scan_->is_end()) {
            rid_ = scan_->rid();
            auto record = fh_->get_record(rid_, context_);
            scan_->next();
            bool ok = true;
            for (auto &cond : fed_conds_) {
                const ColMeta *lhs_meta = nullptr;
                for (auto &c : cols_) {
                    if (c.name == cond.lhs_col.col_name) {
                        lhs_meta = &c;
                        break;
                    }
                }
                if (!lhs_meta) {
                    ok = false;
                    break;
                }
                const char *lhs_ptr = record->data + lhs_meta->offset;
                const char *rhs_ptr = nullptr;
                ColMeta rhs_meta;
                if (cond.is_rhs_val) {
                    rhs_ptr = cond.rhs_val.raw->data;
                } else {
                    for (auto &c : cols_) {
                        if (c.name == cond.rhs_col.col_name) {
                            rhs_meta = c;
                            rhs_ptr = record->data + rhs_meta.offset;
                            break;
                        }
                    }
                }
                int cmp = 0;
                if (lhs_meta->type == TYPE_INT) {
                    int l = *reinterpret_cast<const int *>(lhs_ptr);
                    int r = cond.is_rhs_val
                                ? *reinterpret_cast<const int *>(rhs_ptr)
                                : *reinterpret_cast<const int *>(rhs_ptr);
                    cmp = (l < r) ? -1 : (l > r);
                } else if (lhs_meta->type == TYPE_FLOAT) {
                    float l = *reinterpret_cast<const float *>(lhs_ptr);
                    float r = cond.is_rhs_val
                                ? *reinterpret_cast<const float *>(rhs_ptr)
                                : *reinterpret_cast<const float *>(rhs_ptr);
                    cmp = (l < r) ? -1 : (l > r);
                } else if (lhs_meta->type == TYPE_STRING) {
                    cmp = memcmp(lhs_ptr, rhs_ptr, lhs_meta->len);
                }
                bool cond_ok = false;
                switch (cond.op) {
                    case OP_EQ: cond_ok = (cmp == 0); break;
                    case OP_NE: cond_ok = (cmp != 0); break;
                    case OP_LT: cond_ok = (cmp < 0); break;
                    case OP_GT: cond_ok = (cmp > 0); break;
                    case OP_LE: cond_ok = (cmp <= 0); break;
                    case OP_GE: cond_ok = (cmp >= 0); break;
                    default: cond_ok = false;
                }

                if (!cond_ok) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return record;
            }
        }

        return nullptr;
    }

    Rid &rid() override { return rid_; }
};