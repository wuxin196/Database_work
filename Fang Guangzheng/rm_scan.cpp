#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    
    // 获取文件头信息
    RmFileHdr hdr = const_cast<RmFileHandle*>(file_handle_)->get_file_hdr();
    
    // 初始化rid为第一个数据页面（页面号从1开始），slot_no设为-1
    rid_.page_no = 1;
    rid_.slot_no = -1;
    
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
    
    // 获取文件头信息
    RmFileHdr hdr = const_cast<RmFileHandle*>(file_handle_)->get_file_hdr();
    
    int current_page_no = rid_.page_no;
    int current_slot_no = rid_.slot_no + 1;  // 从下一个slot开始查找
    
    while (current_page_no < hdr.num_pages) {
        // 获取当前页面
        RmPageHandle page_handle = file_handle_->fetch_page_handle(current_page_no);
        
        // 在当前页面中查找下一个有记录的slot
        while (current_slot_no < hdr.num_records_per_page) {
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
    rid_.page_no = hdr.num_pages;
    rid_.slot_no = 0;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    RmFileHdr hdr = const_cast<RmFileHandle*>(file_handle_)->get_file_hdr();
    
    // 当page_no大于等于总页数时表示到达文件末尾
    return rid_.page_no >= hdr.num_pages;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}