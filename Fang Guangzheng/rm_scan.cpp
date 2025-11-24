#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    
    // 初始化rid为第一个页面第一个slot
    rid_.page_no = 0;
    rid_.slot_no = -1;  // 设置为-1，这样第一次调用next()会从0开始
    
    // 找到第一个有记录的slot
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    
    if (is_end()) {
        return;  // 如果已经到达末尾，直接返回
    }
    
    int current_page_no = rid_.page_no;
    int current_slot_no = rid_.slot_no;
    
    // 从当前slot的下一个slot开始查找
    current_slot_no++;
    
    while (current_page_no < file_handle_->file_hdr_.num_pages) {
        // 获取当前页面
        RmPageHandle page_handle = file_handle_->fetch_page_handle(current_page_no);
        
        // 在当前页面中查找下一个有记录的slot
        while (current_slot_no < file_handle_->file_hdr_.num_records_per_page) {
            if (Bitmap::is_set(page_handle.bitmap, current_slot_no)) {
                // 找到有记录的slot，更新rid并返回
                rid_.page_no = current_page_no;
                rid_.slot_no = current_slot_no;
                return;
            }
            current_slot_no++;
        }
        
        // 当前页面没有更多记录，转到下一个页面
        current_page_no++;
        current_slot_no = 0;  // 从新页面的第一个slot开始
    }
    
    // 没有找到更多记录，设置rid为结束状态
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = -1;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    
    // 当page_no为RM_NO_PAGE时表示到达文件末尾
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}