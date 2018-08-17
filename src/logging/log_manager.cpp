/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
    std::lock_guard<std::mutex> grand(latch_);
    if (!flush_thread_on_) {
        ENABLE_LOGGING = true;
        flush_thread_on_ = true;
        flush_thread_ = new std::thread(&LogManager::BackgroundFsync, this);
    }
}

void LogManager::BackgroundFsync() {
    while(flush_thread_on_) {
        std::unique_lock<std::mutex> grand(latch_);
        while (log_buffer_size_ < 1) {
            auto ret = cv_.wait_for(grand, LOG_TIMEOUT);
            if (ret == std::cv_status::no_timeout || !flush_thread_on_) {
                //required for force flushing
                break;
            }
        }
        std::swap(flush_buffer_, log_buffer_);
        flush_buffer_size_ = log_buffer_size_;
        log_buffer_size_ = 0;
        lsn_t current_lsn = next_lsn_ - 1;
        grand.unlock();
        disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
        grand.lock();
        assert(lastLsn(flush_buffer_, flush_buffer_size_) == current_lsn);
        flush_buffer_size_ = 0;
        persistent_lsn_ = current_lsn;
        flushed_cv_.notify_all();
    }
}

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
    std::unique_lock<std::mutex> grand(latch_);
    if (flush_thread_on_ == true) {
        flush_thread_on_ = false;
        ENABLE_LOGGING = false;
        grand.unlock();
        //wake up working thread, or it may take a long time waiting before it's been joined
        TriggerFlush();
        assert(flush_thread_->joinable());
        flush_thread_->join();
        grand.lock();
        delete flush_thread_;
    }
}

void LogManager::WaitUntilBgTaskFinish() {
    std::unique_lock<std::mutex> grand(latch_);
    while (flush_buffer_size_ != 0) {
        flushed_cv_.wait(grand);
    }
}

void LogManager::TriggerFlush() {
    cv_.notify_one();
}

//it's also applicable that using two variables to tract last lsn in log buffer and flush buffer
//this is method below is more helpful during debugging
lsn_t LogManager::lastLsn(char *buff, int size) {
    lsn_t cur = INVALID_LSN;
    char *ptr = buff;
    while (ptr < buff + size) {
        auto rec = reinterpret_cast<LogRecord *>(ptr);
        cur = rec->GetLSN();
        auto len = rec->GetSize();
        ptr = ptr + len;
    }
    return cur;
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
    std::unique_lock<std::mutex> append_grand(append_latch_);
    std::unique_lock<std::mutex> grand(latch_);
    //check whether the log_buffer can hold current record
    if (log_buffer_size_ + log_record.size_ > LOG_BUFFER_SIZE) {
        // need trigger flush cause no room left in log_buffer.
        grand.unlock();
        TriggerFlush();
        WaitUntilBgTaskFinish();
        grand.lock();
        assert(log_buffer_size_ == 0);
    }
    log_record.lsn_ = next_lsn_++;
    int pos = log_buffer_size_;
    memcpy(log_buffer_ + pos, &log_record, LogRecord::HEADER_SIZE);
    pos += LogRecord::HEADER_SIZE;
    if (log_record.log_record_type_ == LogRecordType::INSERT) {
        memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
        pos += sizeof(RID);
        // we have provided serialize function for tuple class
        log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE
               || log_record.log_record_type_ == LogRecordType::MARKDELETE
               || log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
        memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
        memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
        pos += log_record.old_tuple_.GetLength() + sizeof(int32_t);
        log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
        memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(log_record.prev_page_id_));
    } else {
        assert(false);
    }
    log_buffer_size_ += log_record.GetSize();
    return log_record.lsn_;
}

} // namespace cmudb
