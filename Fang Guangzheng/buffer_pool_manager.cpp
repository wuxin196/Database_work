#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面

    // 1) 优先使用 free_list 中的空闲帧（O(1)）
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }

    // 2) free_list 空，则使用替换器选择 victim（LRU）
    // replacer_->victim 会在没有可 victim 时返回 false
    if (replacer_->victim(frame_id)) {
        return true;
    }

    // 3) 没有可用帧
    return false;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id

    PageId old_id = page->get_page_id();

    // 1. 如果旧页面是脏页，需要写回磁盘
    if (old_id.page_no != INVALID_PAGE_ID && page->is_dirty_) {
        disk_manager_->write_page(old_id.fd, old_id.page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 2. 移除旧页表映射（如果有效）
    if (old_id.page_no != INVALID_PAGE_ID) {
        page_table_.erase(old_id);
    }

    // 3. 重置 page 内容，并更新元信息
    page->reset_memory();
    page->id_ = new_page_id;
    page->pin_count_ = 0;
    page->is_dirty_ = false;

    // 4. 添加新映射
    page_table_[new_page_id] = new_frame_id;
}

Page* BufferPoolManager::fetch_page(PageId page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // 1) 查页表（命中）
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page *page = &pages_[frame_id];
        page->pin_count_++;          // 增加引用
        replacer_->pin(frame_id);    // 固定该 frame，不可被替换
        return page;
    }

    // 2) 未命中：找一个可替换的 frame
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        // 没有可替换 frame（所有页都被 pinned），无法加载
        return nullptr;
    }

    // 找到 victim frame
    Page *victim = &pages_[frame_id];
    PageId old_id = victim->get_page_id();

    // 3) 若 victim 是有效页并且为脏页，则写回磁盘
    if (old_id.page_no != INVALID_PAGE_ID && victim->is_dirty_) {
        disk_manager_->write_page(old_id.fd, old_id.page_no, victim->get_data(), PAGE_SIZE);
        victim->is_dirty_ = false;
    }

    // 4) 更新页表：移除旧 page 的映射（如果存在）
    if (old_id.page_no != INVALID_PAGE_ID) {
        page_table_.erase(old_id);
    }

    // 5) 从磁盘读取目标页到该 frame 中
    // 注意：page_id 包含 fd 与 page_no（调用方应传入正确的 PageId）
    disk_manager_->read_page(page_id.fd, page_id.page_no, victim->get_data(), PAGE_SIZE);

    // 6) 更新 victim 的元数据并固定它（pin_count = 1）
    victim->id_ = page_id;
    victim->pin_count_ = 1;
    victim->is_dirty_ = false;

    // 7) 更新页表并在 replacer 中固定该 frame
    page_table_[page_id] = frame_id;
    replacer_->pin(frame_id);

    return victim;
}
/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_

    std::lock_guard<std::mutex> lock(latch_);

    // 1) 在页表里查找 page
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false; // page 不在缓冲池
    }

    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    // 2) 检查 pin_count
    if (page->pin_count_ <= 0) {
        return false; // 已经为 0 或非法
    }

    // 3) 减少引用计数
    page->pin_count_--;

    // 4) 若 pin_count 变为 0，则通知 replacer 可替换
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }

    // 5) 根据参数设置脏标志
    if (is_dirty) {
        page->is_dirty_ = true;
    }

    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    std::lock_guard<std::mutex> lock(latch_);

    std::cerr << "[DEBUG] flush_page: fd=" << page_id.fd
              << " page_no=" << page_id.page_no << std::endl;

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false; // page 不在缓冲池
    }

    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    // 调用 DiskManager 写回（写整个 PAGE_SIZE）
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->get_data(), PAGE_SIZE);
    page->is_dirty_ = false;

    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page
    std::lock_guard<std::mutex> lock(latch_);

    // 1) 找 victim frame
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }

    Page *page = &pages_[frame_id];
    PageId old_id = page->get_page_id();

    // 2) 若 victim 为脏页，写回磁盘
    if (old_id.page_no != INVALID_PAGE_ID && page->is_dirty_) {
        disk_manager_->write_page(old_id.fd, old_id.page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 3) 分配新的 page_id（这里使用 DiskManager::get_file_fd(LOG_FILE_NAME) 获取 fd）
    int fd = page_id->fd;
    page_id_t new_page_no = disk_manager_->allocate_page(fd);
    PageId new_id;
    new_id.fd = fd;
    new_id.page_no = new_page_no;
    *page_id = new_id; // 返回给调用者

    // 4) 更新页表：移除旧映射（若存在），加入新映射
    if (old_id.page_no != INVALID_PAGE_ID) {
        page_table_.erase(old_id);
    }
    page_table_[new_id] = frame_id;

    // 5) 重置 page 内容并更新元信息
    page->reset_memory();
    page->id_ = new_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;

    // 6) 固定该 frame（不可被替换）
    replacer_->pin(frame_id);

    return page;
    return nullptr;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // page 不在缓冲池，认为删除成功（或文件级别删除另行处理）
        return true;
    }

    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    // 如果正在被引用（pin_count > 0），不能删除
    if (page->pin_count_ > 0) {
        return false;
    }

    // 如果为脏页，先写回磁盘
    if (page->is_dirty_) {
        disk_manager_->write_page(page_id.fd, page_id.page_no, page->get_data(), PAGE_SIZE);
    }

    // 从页表中移除
    page_table_.erase(page_id);

    // 重置 page 元数据并把 frame 回收到 free_list_
    page->reset_memory();
    page->id_.page_no = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;

    // 回收 frame 到 free_list_
    free_list_.push_back(frame_id);

    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::lock_guard<std::mutex> lock(latch_);

    // 遍历 page_table_，把属于 fd 的 page 写回
    for (auto &entry : page_table_) {
        const PageId &pid = entry.first;
        if (pid.fd != fd) continue;

        frame_id_t frame_id = entry.second;
        Page *page = &pages_[frame_id];

        disk_manager_->write_page(pid.fd, pid.page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }
}