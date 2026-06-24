//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//         Michal Simon
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/*----------------------------------------------------------------------------*/
#include "Journal.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucCRC32C.hh"
#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__
#include <sys/uio.h>
#define pread64 pread
#define pwrite64 pwrite
#endif

namespace {

ssize_t write_full(int fd, const void *buf, size_t count, off_t offset) {
  const char *cursor = static_cast<const char *>(buf);
  size_t remaining = count;
  off_t off = offset;

  while (remaining > 0) {
    ssize_t written = ::pwrite(fd, cursor, remaining, off);
    if (written <= 0) {
      return written;
    }
    cursor += written;
    off += written;
    remaining -= written;
  }

  return static_cast<ssize_t>(count);
}

uint64_t abs_u64_diff(uint64_t a, uint64_t b) {
  return a > b ? a - b : b - a;
}

} // namespace

bool Journal::sDefaultEnableCrc = false;
Journal::LogCallback Journal::sLogCallback = nullptr;
void *Journal::sLogContext = nullptr;

void Journal::SetLogCallback(LogCallback cb, void *ctx) {
  sLogCallback = cb;
  sLogContext = ctx;
}

void Journal::log(int level, const char *fmt, ...) const {
  char buffer[512];
  va_list ap;
  va_start(ap, fmt);
  std::vsnprintf(buffer, sizeof(buffer), fmt, ap);
  va_end(ap);

  if (sLogCallback) {
    sLogCallback(sLogContext, level, buffer);
    return;
  }

  if (level <= 1) {
    std::cerr << "error: " << buffer << std::endl;
  } else if (level == 2) {
    std::cerr << "warning: " << buffer << std::endl;
  } else {
    std::cerr << buffer << std::endl;
  }
}

//------------------------------------------------------------------------------
//! Journal Constructor
//------------------------------------------------------------------------------
Journal::Journal() : cachesize(0), max_offset(0), fd(-1) {
  std::lock_guard<std::mutex> guard(mtx);
  jheader.magic = JOURNAL_MAGIC;
  jheader.mtime = 0;
  jheader.mtime_nsec = 0;
  jheader.filesize = 0;
  jheader.placeholder1 = 0;
  jheader.placeholder2 = 0;
  jheader.placeholder3 = 0;
  jheader.placeholder4 = 0;
  journal_version = JOURNAL_VERSION_LEGACY;
}

void Journal::close_fd() {
  if (fd == -1) {
    return;
  }

  sync();

  struct flock lock;
  std::memset(&lock, 0, sizeof(lock));
  lock.l_type = F_UNLCK;
  if (fcntl(fd, F_SETLK, &lock) == -1) {
    log(1, "failed to unlock journal %s: %s", path.c_str(),
        std::strerror(errno));
  }

  if (close(fd) != 0) {
    log(1, "failed to close journal %s: %s", path.c_str(),
        std::strerror(errno));
  }

  fd = -1;
}

//------------------------------------------------------------------------------
//! Journal Destructor
//------------------------------------------------------------------------------
Journal::~Journal() {
  std::lock_guard<std::mutex> guard(mtx);
  close_fd();
}

//------------------------------------------------------------------------------
//! Routine to read a journal header
//------------------------------------------------------------------------------
bool Journal::read_jheader() {
  jheader_t fheader;
  bool exists = false;
  auto hr = ::pread64(fd, &fheader, sizeof(jheader), 0);
  if ((hr > 0) &&
      ((hr != sizeof(jheader)) || (fheader.magic != JOURNAL_MAGIC))) {
    log(2, "inconsistent journal header found (I) - purging path: %s",
        path.c_str());
    reset();
    return true;
  }

  exists = (hr == sizeof(jheader));

  if (exists) {
    jheader.placeholder1 = fheader.placeholder1;
  }

  if (jheader.mtime) {
    if (exists) {
      if ((abs_u64_diff(fheader.mtime, jheader.mtime) > MTIME_TOLERANCE_SEC) ||
          (fheader.mtime_nsec != jheader.mtime_nsec) ||
          (jheader.filesize && (fheader.filesize != jheader.filesize))) {
        log(2, "remote file change detected - purging path: %s", path.c_str());
        reset();
        return true;
      }
    }
  } else {
    jheader.mtime = fheader.mtime;
    jheader.mtime_nsec = fheader.mtime_nsec;
    jheader.filesize = fheader.filesize;
  }

  return false;
}

//------------------------------------------------------------------------------
//! Routine to write a journal header
//------------------------------------------------------------------------------
int Journal::write_jheader() {
  auto hw = ::pwrite64(fd, &jheader, sizeof(jheader), 0);
  if ((hw != sizeof(jheader))) {
    log(2, "failed to write journal header - purging path: %s", path.c_str());
    return -errno;
  }
  return 0;
}

void Journal::sync_journal_version() {
  journal_version = (jheader.placeholder1 >= JOURNAL_VERSION_CRC32C)
                        ? JOURNAL_VERSION_CRC32C
                        : JOURNAL_VERSION_LEGACY;
}

bool Journal::uses_crc() const {
  return journal_version >= JOURNAL_VERSION_CRC32C;
}

size_t Journal::entry_trailer_size() const {
  return uses_crc() ? sizeof(uint32_t) : 0;
}

bool Journal::verify_entry_crc(uint64_t header_offset, uint64_t data_size,
                               const void *data) const {
  if (!uses_crc()) {
    return true;
  }

  uint32_t stored = 0;
  off_t crc_off = header_offset + sizeof(header_t) + data_size;
  if (::pread(fd, &stored, sizeof(stored), crc_off) != (ssize_t)sizeof(stored)) {
    return false;
  }

  return stored == crc32c(0, data, data_size);
}

bool Journal::append_entry_crc(uint64_t header_offset, uint64_t data_size,
                               const void *data) {
  if (!uses_crc()) {
    return true;
  }

  uint32_t checksum = crc32c(0, data, data_size);
  off_t crc_off = header_offset + sizeof(header_t) + data_size;
  return write_full(fd, &checksum, sizeof(checksum), crc_off) ==
         (ssize_t)sizeof(checksum);
}

bool Journal::update_entry_crc(uint64_t header_offset, uint64_t data_size) {
  if (!uses_crc()) {
    return true;
  }

  std::vector<char> buffer(data_size);
  off_t data_off = header_offset + sizeof(header_t);
  if (::pread(fd, buffer.data(), data_size, data_off) != (ssize_t)data_size) {
    return false;
  }

  return append_entry_crc(header_offset, data_size, buffer.data());
}

//------------------------------------------------------------------------------
//! Routine to read a journal
//------------------------------------------------------------------------------
int Journal::read_journal() {
  journal.clear();
  max_offset = 0;
  const size_t bufsize = sizeof(header_t);
  char buffer[bufsize];
  ssize_t bytesRead = 0;
  ssize_t totalBytesRead = sizeof(jheader_t);

  do {
    bytesRead = ::pread(fd, buffer, bufsize, totalBytesRead);
    if (bytesRead < (ssize_t)bufsize) {
      if (bytesRead) {
        log(2, "inconsistent journal found - purging path: %s", path.c_str());
        reset();
        return cachesize;
      }
    } else {
      header_t *header = reinterpret_cast<header_t *>(buffer);
      const uint64_t header_off = totalBytesRead;
      const uint64_t data_size = header->size;
      const off_t data_off = header_off + sizeof(header_t);

      if (uses_crc()) {
        std::vector<char> data(data_size);
        if (::pread(fd, data.data(), data_size, data_off) !=
            (ssize_t)data_size) {
          log(2, "failed to read journal entry at %s offset %llu - skipping",
              path.c_str(), static_cast<unsigned long long>(header_off));
          totalBytesRead += sizeof(header_t) + data_size + sizeof(uint32_t);
          continue;
        }
        if (!verify_entry_crc(header_off, data_size, data.data())) {
          log(2, "journal crc mismatch at %s offset %llu - skipping entry",
              path.c_str(), static_cast<unsigned long long>(header_off));
          totalBytesRead += sizeof(header_t) + data_size + sizeof(uint32_t);
          continue;
        }
      }

      journal.insert(header->offset, header->offset + header->size, header_off);
      if (header->offset + header->size > (uint64_t)max_offset) {
        max_offset = header->offset + header->size;
      }
      totalBytesRead += header->size;
      totalBytesRead += bytesRead;
      if (uses_crc()) {
        totalBytesRead += sizeof(uint32_t);
      }
    }
  } while (bytesRead);

  return totalBytesRead;
}

//------------------------------------------------------------------------------
//! Journal attach
//------------------------------------------------------------------------------
int Journal::attach(const std::string &lpath, uint64_t mtime,
                    uint64_t mtime_nsec, uint64_t size, bool ifexists) {
  std::lock_guard<std::mutex> guard(mtx);

  if (fd != -1 && !path.empty() && path != lpath) {
    close_fd();
  }

  path = lpath;

  if (!ifexists) {
    jheader.mtime = mtime;
    jheader.mtime_nsec = mtime_nsec;
    jheader.filesize = size;
    if (sDefaultEnableCrc) {
      jheader.placeholder1 = JOURNAL_VERSION_CRC32C;
    }
  }

  if (ifexists) {
    struct stat buf;
    if (::stat(path.c_str(), &buf)) {
      return -ENOENT;
    }
    if ((size_t)buf.st_size < sizeof(jheader_t)) {
      return -EINVAL;
    }
  }

  if (fd == -1) {
    fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRWXU);
    if (fd < 0) {
      return -errno;
    }

    struct flock lock;
    std::memset(&lock, 0, sizeof(lock));
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;

    if (fcntl(fd, F_SETLK, &lock) == -1) {
      if (errno == EACCES || errno == EAGAIN) {
        log(1, "journal file is already locked by another process: %s",
            path.c_str());
      } else {
        log(1, "failed to lock journal file %s: %s", path.c_str(),
            std::strerror(errno));
      }
      close(fd);
      fd = -1;
      return -errno;
    }

    const uint64_t attach_mtime = jheader.mtime;
    const uint64_t attach_mtime_nsec = jheader.mtime_nsec;
    const uint64_t attach_filesize = jheader.filesize;

    const bool purged = read_jheader();
    sync_journal_version();
    if (purged && !ifexists) {
      jheader.mtime = attach_mtime;
      jheader.mtime_nsec = attach_mtime_nsec;
      jheader.filesize = attach_filesize;
    }
    cachesize = read_journal();
    if (write_jheader()) {
      return -errno;
    }
  } else {
    const uint64_t attach_mtime = jheader.mtime;
    const uint64_t attach_mtime_nsec = jheader.mtime_nsec;
    const uint64_t attach_filesize = jheader.filesize;

    const bool purged = read_jheader();
    sync_journal_version();
    if (purged) {
      if (!ifexists) {
        jheader.mtime = attach_mtime;
        jheader.mtime_nsec = attach_mtime_nsec;
        jheader.filesize = attach_filesize;
      }
      cachesize = read_journal();
    }
    if (!ifexists && write_jheader()) {
      return -errno;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Journal detach
//------------------------------------------------------------------------------
int Journal::detach() {
  std::lock_guard<std::mutex> guard(mtx);
  close_fd();
  return 0;
}

//------------------------------------------------------------------------------
//! Journal unlink
//------------------------------------------------------------------------------
int Journal::unlink() {
  std::lock_guard<std::mutex> guard(mtx);
  struct stat buf;
  int rc = stat(path.c_str(), &buf);
  if (!rc) {
    rc = ::unlink(path.c_str());
  }

  return rc;
}

//------------------------------------------------------------------------------
//! Journal pread
//------------------------------------------------------------------------------
ssize_t Journal::pread(void *buf, size_t count, off_t offset, bool &eof) {
  if (fd < 0) {
    return 0;
  }

  std::lock_guard<std::mutex> guard(mtx);

  if ((off_t)(offset + count) > (off_t)jheader.filesize) {
    if ((off_t)jheader.filesize > offset) {
      count = (off_t)jheader.filesize - offset;
    } else {
      count = 0;
    }
    eof = true;
  }

  auto result = journal.query(offset, offset + count);

  if (result.empty()) {
    return 0;
  }

  char *buffer = reinterpret_cast<char *>(buf);
  uint64_t off = offset;
  uint64_t bytesRead = 0;

  for (auto &itr : result) {
    if (itr->low <= off && off < itr->high) {
      uint64_t frag_size = itr->high - itr->low;
      uint64_t header_off = itr->value;
      uint64_t skip = off - itr->low;
      int64_t intervalsize = itr->high - off;
      int64_t bytesLeft = count - bytesRead;
      int64_t bufsize = intervalsize < bytesLeft ? intervalsize : bytesLeft;

      if (uses_crc()) {
        std::vector<char> fragment(frag_size);
        off_t data_off = header_off + sizeof(header_t);
        if (::pread(fd, fragment.data(), frag_size, data_off) !=
            (ssize_t)frag_size) {
          return 0;
        }
        if (!verify_entry_crc(header_off, frag_size, fragment.data())) {
          log(2, "journal crc mismatch at path: %s", path.c_str());
          return 0;
        }
        std::memcpy(buffer, fragment.data() + skip, bufsize);
      } else {
        uint64_t cacheoff = header_off + sizeof(header_t) + skip;
        ssize_t ret = ::pread(fd, buffer, bufsize, cacheoff);
        if (ret < 0) {
          return -1;
        }
        if (ret != bufsize) {
          return 0;
        }
      }

      bytesRead += bufsize;
      off += bufsize;
      buffer += bufsize;

      if (bytesRead >= count) {
        break;
      }
    }
  }

  if (bytesRead != count) {
    return 0;
  }

  return bytesRead;
}

//------------------------------------------------------------------------------
//! Journal process intersection
//------------------------------------------------------------------------------
void Journal::process_intersection(
    interval_tree<uint64_t, const void *> &to_write,
    interval_tree<uint64_t, uint64_t>::iterator itr,
    std::vector<chunk_t> &updates) {
  auto result = to_write.query(itr->low, itr->high);

  if (result.empty()) {
    return;
  }

  if (result.size() > 1) {
    throw std::logic_error("Journal: overlapping journal entries");
  }

  const interval_tree<uint64_t, const void *>::iterator to_wrt =
      *result.begin();
  uint64_t low = std::max(to_wrt->low, itr->low);
  uint64_t high = std::min(to_wrt->high, itr->high);
  chunk_t update;
  update.offset = offset_for_update(itr->value, low - itr->low);
  update.size = high - low;
  update.buff = static_cast<const char *>(to_wrt->value) + (low - to_wrt->low);
  updates.push_back(std::move(update));
  uint64_t wrtlow = to_wrt->low;
  uint64_t wrthigh = to_wrt->high;
  const void *wrtbuff = to_wrt->value;
  to_write.erase(wrtlow, wrthigh);

  if (low == wrtlow && high == wrthigh) {
    return;
  }

  if (high < wrthigh) {
    const char *buff = static_cast<const char *>(wrtbuff) + (high - wrtlow);
    to_write.insert(high, wrthigh, buff);
  }

  if (low > wrtlow) {
    to_write.insert(wrtlow, low, wrtbuff);
  }
}

//------------------------------------------------------------------------------
//! Journal update
//------------------------------------------------------------------------------
int Journal::update_cache(std::vector<chunk_t> &updates) {
  std::sort(updates.begin(), updates.end());

  for (auto &u : updates) {
    if (write_full(fd, u.buff, u.size, u.offset) != (ssize_t)u.size) {
      return -errno;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Journal pwrite
//------------------------------------------------------------------------------
ssize_t Journal::pwrite(const void *buf, size_t count, off_t offset) {
  if (fd < 0) {
    return 0;
  }

  std::lock_guard<std::mutex> guard(mtx);
  if (count <= 0) {
    return 0;
  }

  interval_tree<uint64_t, const void *> to_write;
  std::vector<chunk_t> updates;
  to_write.insert(offset, offset + count, buf);
  auto res = journal.query(offset, offset + count);

  for (auto itr : res) {
    process_intersection(to_write, itr, updates);
  }

  int rc = update_cache(updates);

  if (rc) {
    return -1;
  }

  for (auto itr : res) {
    if (!update_entry_crc(itr->value, itr->high - itr->low)) {
      return -1;
    }
  }

  interval_tree<uint64_t, const void *>::iterator itr;

  for (itr = to_write.begin(); itr != to_write.end(); ++itr) {
    uint64_t size = itr->high - itr->low;
    uint64_t header_off = cachesize;
    header_t header;
    header.offset = itr->low;
    header.size = size;

    if (write_full(fd, &header, sizeof(header), header_off) !=
        (ssize_t)sizeof(header)) {
      return -1;
    }
    if (write_full(fd, itr->value, size, header_off + sizeof(header)) !=
        (ssize_t)size) {
      return -1;
    }
    if (!append_entry_crc(header_off, size, itr->value)) {
      return -1;
    }

    journal.insert(itr->low, itr->high, header_off);
    cachesize = header_off + sizeof(header_t) + size + entry_trailer_size();
  }

  if ((ssize_t)(offset + count) > max_offset) {
    max_offset = offset + count;
  }

  return count;
}

//------------------------------------------------------------------------------
//! Journal data sync
//------------------------------------------------------------------------------
int Journal::sync() {
  if (fd < 0) {
    return -1;
  }
#ifdef __APPLE__
  return ::fsync(fd);
#else
  return ::fdatasync(fd);
#endif
}

//------------------------------------------------------------------------------
//! Journal get size
//------------------------------------------------------------------------------
size_t Journal::size() {
  std::lock_guard<std::mutex> guard(mtx);
  return cachesize;
}

//------------------------------------------------------------------------------
//! Journal get max offset in the journal
//------------------------------------------------------------------------------
off_t Journal::get_max_offset() {
  std::lock_guard<std::mutex> guard(mtx);
  return max_offset;
}

//------------------------------------------------------------------------------
//! Journal reset
//------------------------------------------------------------------------------
int Journal::reset() {
  journal.clear();
  int retc = 0;
  max_offset = 0;
  jheader.magic = JOURNAL_MAGIC;
  jheader.mtime = 0;
  jheader.mtime_nsec = 0;
  jheader.filesize = 0;
  jheader.placeholder1 =
      sDefaultEnableCrc ? JOURNAL_VERSION_CRC32C : JOURNAL_VERSION_LEGACY;
  jheader.placeholder2 = 0;
  jheader.placeholder3 = 0;
  jheader.placeholder4 = 0;
  sync_journal_version();
  if (fd >= 0) {
    retc = ftruncate(fd, 0);
    retc |= write_jheader();
    cachesize = sizeof(jheader_t);
  }
  return retc;
}

std::string Journal::dump() {
  std::lock_guard<std::mutex> guard(mtx);
  std::string out;
  out += "fd=";
  out += std::to_string(fd);
  out += " cachesize=";
  out += std::to_string(cachesize);
  out += " maxoffset=";
  out += std::to_string(max_offset);
  return out;
}

//------------------------------------------------------------------------------
//! Journal get chunks
//------------------------------------------------------------------------------
std::vector<Journal::chunk_t> Journal::get_chunks(off_t offset, size_t size) {
  std::lock_guard<std::mutex> guard(mtx);
  auto result = journal.query(offset, offset + size);
  std::vector<chunk_t> ret;

  if (fd < 0) {
    return ret;
  }

  for (auto &itr : result) {
    uint64_t off = (off_t)itr->low < (off_t)offset ? offset : itr->low;
    uint64_t count =
        itr->high < offset + size ? itr->high - off : offset + size - off;
    uint64_t header_off = itr->value;
    uint64_t frag_size = itr->high - itr->low;
    uint64_t skip = off - itr->low;
    std::unique_ptr<char[]> buffer(new char[count]);

    if (uses_crc()) {
      std::vector<char> fragment(frag_size);
      off_t data_off = header_off + sizeof(header_t);
      if (::pread(fd, fragment.data(), frag_size, data_off) !=
          (ssize_t)frag_size) {
        continue;
      }
      if (!verify_entry_crc(header_off, frag_size, fragment.data())) {
        continue;
      }
      std::memcpy(buffer.get(), fragment.data() + skip, count);
    } else {
      uint64_t cacheoff = header_off + sizeof(header_t) + skip;
      ssize_t rc = ::pread(fd, buffer.get(), count, cacheoff);
      if (rc < 0 || (size_t)rc != count) {
        continue;
      }
    }

    ret.push_back(chunk_t(off, count, std::move(buffer)));
  }

  return ret;
}
