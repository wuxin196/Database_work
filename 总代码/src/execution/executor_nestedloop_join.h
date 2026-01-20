#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        isend=false;
        left_->beginTuple();
        right_->beginTuple();
    }

    void nextTuple() override {
        if(isend)return;
    }

    std::unique_ptr<RmRecord> Next() override {
        static std::unique_ptr<RmRecord> left_rec = nullptr;
        static std::unique_ptr<RmRecord> right_rec = nullptr;
        if (isend) {
            return nullptr;
        }
        while (true) {
            if (!left_rec) {
                left_rec = left_->Next();
                if (!left_rec) {
                    isend = true;
                    return nullptr;
                }
                right_->beginTuple();
                right_rec = nullptr;
            }
            if (!right_rec) {
                right_rec = right_->Next();
            }
            while (right_rec) {
                bool ok = true;
                for (auto &cond : fed_conds_) {
                    const char *lhs_ptr = nullptr;
                    const char *rhs_ptr = nullptr;
                    for (auto &c : cols_) {
                        if (c.name == cond.lhs_col.col_name) {
                            lhs_ptr = (c.offset < left_->tupleLen())
                                        ? left_rec->data + c.offset
                                        : right_rec->data + (c.offset - left_->tupleLen());
                        }
                        if (!cond.is_rhs_val && c.name == cond.rhs_col.col_name) {
                            rhs_ptr = (c.offset < left_->tupleLen())
                                        ? left_rec->data + c.offset
                                        : right_rec->data + (c.offset - left_->tupleLen());
                        }
                    }

                    if (cond.is_rhs_val) {
                        rhs_ptr = cond.rhs_val.raw->data;
                    }

                    int cmp = 0;
                    if (cond.rhs_val.type == TYPE_INT) {
                        int l = *reinterpret_cast<const int *>(lhs_ptr);
                        int r = *reinterpret_cast<const int *>(rhs_ptr);
                        cmp = (l < r) ? -1 : (l > r);
                    } else if (cond.rhs_val.type == TYPE_FLOAT) {
                        float l = *reinterpret_cast<const float *>(lhs_ptr);
                        float r = *reinterpret_cast<const float *>(rhs_ptr);
                        cmp = (l < r) ? -1 : (l > r);
                    } else if (cond.rhs_val.type == TYPE_STRING) {
                        cmp = memcmp(lhs_ptr, rhs_ptr,
                                    cond.rhs_val.raw->size);
                    }
                    bool cond_ok = false;
                    switch (cond.op) {
                        case OP_EQ: cond_ok = (cmp == 0); break;
                        case OP_NE: cond_ok = (cmp != 0); break;
                        case OP_LT: cond_ok = (cmp < 0); break;
                        case OP_GT: cond_ok = (cmp > 0); break;
                        case OP_LE: cond_ok = (cmp <= 0); break;
                        case OP_GE: cond_ok = (cmp >= 0); break;
                    }
                    if (!cond_ok) {
                        ok = false;
                        break;
                    }
                }
                auto cur_right = std::move(right_rec);
                right_rec = right_->Next();
                if (ok) {
                    auto res = std::make_unique<RmRecord>(len_);
                    memcpy(res->data, left_rec->data, left_->tupleLen());
                    memcpy(res->data + left_->tupleLen(),
                        cur_right->data,
                        right_->tupleLen());
                    return res;
                }
            }
            left_rec = nullptr;
        }
    }
    Rid &rid() override { return _abstract_rid; }
};