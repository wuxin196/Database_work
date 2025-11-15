#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;  

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::scoped_lock lock{latch_};  //  如果编译报错可以替换成其他lock

    // Todo:
    //  利用lru_replacer中的LRUlist_,LRUhash_实现LRU策略
    //  选择合适的frame指定为淘汰页面,赋值给*frame_id
    // 如果没有可淘汰的帧，返回 false
    if (LRUlist_.empty()) {
        return false;
    }
    // LRU 策略把最久未被访问/取消固定的帧放在 list 的头部（front）
    // 取出头部作为 victim（注意先复制一个值出来，避免后续对容器的修改影响）
    frame_id_t victim_frame = LRUlist_.front();
    // 从 list 中移除头部元素（这样 list 的迭代器和大小都会被正确更新）
    LRUlist_.pop_front();
    // 从哈希表中删除对应条目，保持两者一致
    // 注意：因为我们在同一把锁下操作，erase 不会和其他线程的读/写冲突
    LRUhash_.erase(victim_frame);
    // 把淘汰的 frame id 写回调用者提供的指针
    *frame_id = victim_frame;
    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    // Todo:
    // 固定指定id的frame
    // 在数据结构中移除该frame
    auto it = LRUhash_.find(frame_id);
    if (it != LRUhash_.end()) {
        LRUlist_.erase(it->second);
        LRUhash_.erase(it);
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    // Todo:
    //  支持并发锁
    //  选择一个frame取消固定
    std::scoped_lock lock{latch_};
    // 如果已经在 LRUhash_ 中，说明该 frame 已经可淘汰，直接返回（避免重复插入）
    if (LRUhash_.count(frame_id)) {
        return;
    }
    // 如果 LRU 列表已达到最大容量，则不再插入。
    // 这一步是根据实现策略选择的行为：缓冲池大小固定，replacer 不应超过 max_size_
    if (LRUlist_.size() >= max_size_) {
        return;
    }
    // 将 frame 插入到队尾，表示“最近刚被 unpin”（队尾 = 最近使用/最近可淘汰）
    LRUlist_.push_back(frame_id);
    // 立刻拿到刚插入元素的迭代器并写入哈希表
    // 这样保证了哈希表中保存的迭代器是有效的、并且始终和 list 保持一致
    auto iter = std::prev(LRUlist_.end());
    LRUhash_[frame_id] = iter;
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
