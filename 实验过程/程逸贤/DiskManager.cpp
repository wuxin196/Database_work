#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek
#include <fcntl.h>     // for open

#include "defs.h"

DiskManager::DiskManager() { 
    // 替换memset避免警告
    for (int i = 0; i < MAX_FD; i++) {
        fd2pageno_[i] = 0;
    }
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    off_t file_offset = page_no * num_bytes;
    off_t seek_result = lseek(fd, file_offset, SEEK_SET);
    if(seek_result == -1){
        throw UnixError();
    }
    ssize_t bytes_written = write(fd, offset, num_bytes);
    if(bytes_written != num_bytes){
        throw InternalError("DiskManager::write_page Error");
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    off_t file_offset = page_no * num_bytes;
    off_t seek_result = lseek(fd, file_offset, SEEK_SET);
    if(seek_result == -1){
        throw UnixError();
    }
    ssize_t bytes_read = read(fd, offset, num_bytes);
    if(bytes_read != num_bytes){
        throw InternalError("DiskManager::read_page Error");
    }
}

/**
 * @description: 分配一个新的页号
 */
page_id_t DiskManager::allocate_page(int fd) {
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 */
bool DiskManager::is_file(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 */
void DiskManager::create_file(const std::string &path) {
    if(is_file(path)){
        throw FileExistsError(path);
    }
    int fd = open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if(fd == -1){
        throw UnixError();
    }
    close(fd);
}

/**
 * @description: 删除指定路径的文件
 */
void DiskManager::destroy_file(const std::string &path) {
    if(path2fd_.count(path)){
        throw FileNotClosedError(path);
    }
    
    // 添加文件存在性检查
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    
    if(unlink(path.c_str()) == -1){
        throw UnixError();
    }
}

/**
 * @description: 打开指定路径文件 
 */
int DiskManager::open_file(const std::string &path) {
    if(path2fd_.count(path)){
        throw FileExistsError(path);
    }
    
    // 添加文件存在性检查
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    
    int fd = open(path.c_str(), O_RDWR);
    if(fd == -1){
        throw UnixError();
    }
    path2fd_[path] = fd;
    fd2path_[fd] = path;
    return fd;
}

/**
 * @description:用于关闭指定路径文件 
 */
void DiskManager::close_file(int fd) {
    if(fd2path_.count(fd) == 0){
        throw FileNotOpenError(fd);
    }
    std::string path = fd2path_[fd];
    if(close(fd) == -1){
        throw UnixError();
    }
    path2fd_.erase(path);
    fd2path_.erase(fd);
}

/**
 * @description: 获得文件的大小
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

/**
 * @description:  读取日志文件内容
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

/**
 * @description: 写日志内容
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}