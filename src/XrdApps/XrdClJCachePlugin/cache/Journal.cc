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
#include <algorithm>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <cstring>
#include <cerrno>
/*----------------------------------------------------------------------------*/

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
}
//------------------------------------------------------------------------------
//! Journal Destructor
//------------------------------------------------------------------------------
Journal::~Journal() {
  std::lock_guard<std::mutex> guard(mtx);
  if (fd > 0) {
    struct flock lock;
    std::memset(&lock, 0, sizeof(lock));

    lock.l_type = F_UNLCK; // Unlock the file
    if (fcntl(fd, F_SETLK, &lock) == -1) {
        std::cerr << "error: failed to unlock journal: " << std::strerror(errno) << std::endl;
    }

    int rc = close(fd);

    if (rc) {
      std::abort();
    }

    fd = -1;
  }
}

//------------------------------------------------------------------------------
//! Routine to read a journal header
//------------------------------------------------------------------------------
void Journal::read_jheader() {
  jheader_t fheader;
  auto hr = ::pread64(fd, &fheader, sizeof(jheader), 0);
  if ((hr > 0) &&
      ((hr != sizeof(jheader)) || (fheader.magic != JOURNAL_MAGIC))) {
    std::cerr
        << "warning: inconsistent journal header found (I) - purging path:"
        << path << std::endl;
    reset();
    return;
  }
  // TODO: understand why the mtime is +-1s
  if (jheader.mtime) {
    if ((abs(fheader.mtime - jheader.mtime) > 1) ||
        (fheader.mtime_nsec != jheader.mtime_nsec) ||
        (jheader.filesize && (fheader.filesize != jheader.filesize))) {
      std::cerr << "warning: remote file change detected - purging path:"
                << path << std::endl;
      std::cerr << fheader.mtime << ":" << jheader.mtime << " "
                << fheader.mtime_nsec << ":" << jheader.mtime_nsec << " "
                << fheader.filesize << ":" << jheader.filesize << std::endl;
      reset();
      return;
    }
  } else {
    // we assume the contents referenced in the header is ok to allow disconnected ops
    jheader.mtime = fheader.mtime;
    jheader.filesize = fheader.filesize;
  }
}

//------------------------------------------------------------------------------
//! Routine to write a journal header
//------------------------------------------------------------------------------
int Journal::write_jheader() {
  auto hw = ::pwrite64(fd, &jheader, sizeof(jheader), 0);
  if ((hw != sizeof(jheader))) {
    std::cerr << "warning: failed to write journal header - purging path:"
              << path << std::endl;
    return -errno;
  }
  return 0;
}

//------------------------------------------------------------------------------
//! Routine to read a journal
//------------------------------------------------------------------------------
int Journal::read_journal() {
  journal.clear();
  const size_t bufsize = sizeof(header_t);
  char buffer[bufsize];
  ssize_t bytesRead = 0, totalBytesRead = sizeof(jheader_t);
  int64_t pos = 0;

  ssize_t journalsize = lseek(fd, 0, SEEK_END);

  do {
    bytesRead = ::pread(fd, buffer, bufsize, totalBytesRead);
    if (bytesRead < (ssize_t)bufsize) {
      if (bytesRead == 0 && (totalBytesRead == journalsize)) {
        break;
      } else {
        std::cerr << "warning: inconsistent journal found - purging path:"
                  << path << std::endl;
        reset();
        return 0;
      }
    }
    header_t *header = reinterpret_cast<header_t *>(buffer);
    journal.insert(header->offset, header->offset + header->size,
                   totalBytesRead);
    totalBytesRead += header->size; // size of the fragment
    totalBytesRead += bytesRead;    // size of the header
  } while (pos < bytesRead);

  return totalBytesRead;
}

//------------------------------------------------------------------------------
//! Journal attach
//------------------------------------------------------------------------------
int Journal::attach(const std::string &lpath, uint64_t mtime,
                    uint64_t mtime_nsec, uint64_t size, bool ifexists) {
  std::lock_guard<std::mutex> guard(mtx);
  path = lpath;

  if (!ifexists) {
    jheader.mtime = mtime;
    jheader.mtime_nsec = mtime_nsec;
    jheader.filesize = size;
  }

  if (ifexists) {
    struct stat buf;
    // check if there is already a journal for this file known
    if (::stat(path.c_str(), &buf)) {
      return -ENOENT;
    } else {
      if ((size_t)buf.st_size < sizeof(jheader_t)) {
        return -EINVAL;
      }
    }
  }
  if ((fd == -1)) {
    // need to open the file
    size_t tries = 0;

    do {
      fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRWXU);

      if (fd < 0) {
        if (errno == ENOENT) {
          tries++;
          if (tries < 10) {
            continue;
          } else {
            return -errno;
          }
        }

        return -errno;
      }

      // get a POSIX lock on the file
      struct flock lock;
      std::memset(&lock, 0, sizeof(lock));
      lock.l_type = F_WRLCK;    // Request a write lock
      lock.l_whence = SEEK_SET; // Lock from the beginning of the file
      lock.l_start = 0;         // Starting offset for lock
      lock.l_len = 0;           // 0 means to lock the entire file

      if (fcntl(fd, F_SETLK, &lock) == -1) {
        if (errno == EACCES || errno == EAGAIN) {
          std::cerr << "error: journal file is already locked by another process."
                    << std::endl;
          close(fd);
          fd = -1;
          return -errno;
        } else {
          std::cerr << "error: failed to lock journal file: " << std::strerror(errno)
                    << std::endl;
          close(fd);
          fd = -1;
          return -errno;
        }
      }

      break;
    } while (1);

    read_jheader(); // this might fail, no problem
    cachesize = read_journal();
    if (write_jheader()) {
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

  // rewrite reads to not go over EOF!
  if ((off_t)(offset + count) > (off_t)jheader.filesize) {
    if ((off_t)jheader.filesize > offset) {
      count = (off_t)jheader.filesize - offset;
    } else {
      count = 0;
    }
    eof = true;
  }

  std::lock_guard<std::mutex> guard(mtx);
  auto result = journal.query(offset, offset + count);

  // there is not a single interval that overlaps
  if (result.empty()) {
    return 0;
  }

  char *buffer = reinterpret_cast<char *>(buf);
  uint64_t off = offset;
  uint64_t bytesRead = 0;

  for (auto &itr : result) {
    if (itr->low <= off && off < itr->high) {
      // read from cache
      uint64_t cacheoff = itr->value + sizeof(header_t) + (off - itr->low);
      int64_t intervalsize = itr->high - off;
      int64_t bytesLeft = count - bytesRead;
      int64_t bufsize = intervalsize < bytesLeft ? intervalsize : bytesLeft;
      ssize_t ret = ::pread(fd, buffer, bufsize, cacheoff);
      if (ret < 0) {
        return -1;
      }

      bytesRead += ret;
      off += ret;
      buffer += ret;

      if (bytesRead >= count) {
        break;
      }
    }
  }

  if (eof && (bytesRead != count)) {
    // if we have EOF and missing part, we return 0
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
  // the intersection
  uint64_t low = std::max(to_wrt->low, itr->low);
  uint64_t high = std::min(to_wrt->high, itr->high);
  // update
  chunk_t update;
  update.offset = offset_for_update(itr->value, low - itr->low);
  update.size = high - low;
  update.buff = static_cast<const char *>(to_wrt->value) + (low - to_wrt->low);
  updates.push_back(std::move(update));
  // update the 'to write' intervals
  uint64_t wrtlow = to_wrt->low;
  uint64_t wrthigh = to_wrt->high;
  const void *wrtbuff = to_wrt->value;
  to_write.erase(wrtlow, wrthigh);

  // the intersection overlaps with the given
  // interval so there is nothing more to do
  if (low == wrtlow && high == wrthigh) {
    return;
  }

  if (high < wrthigh) {
    // the remaining right-hand-side interval
    const char *buff = static_cast<const char *>(wrtbuff) + (high - wrtlow);
    to_write.insert(high, wrthigh, buff);
  }

  if (low > wrtlow) {
    // the remaining left-hand-side interval
    to_write.insert(wrtlow, low, wrtbuff);
  }
}

//------------------------------------------------------------------------------
//! Journal update
//------------------------------------------------------------------------------
int Journal::update_cache(std::vector<chunk_t> &updates) {
  // make sure we are updating the cache in ascending order
  std::sort(updates.begin(), updates.end());
  int rc = 0;

  for (auto &u : updates) {
    rc = ::pwrite(fd, u.buff, u.size,
                  u.offset); // TODO is it safe to assume it will write it all

    if (rc <= 0) {
      return errno;
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

  interval_tree<uint64_t, const void *>::iterator itr;

  for (itr = to_write.begin(); itr != to_write.end(); ++itr) {
    uint64_t size = itr->high - itr->low;
    header_t header;
    header.offset = itr->low;
    header.size = size;
    iovec iov[2];
    iov[0].iov_base = &header;
    iov[0].iov_len = sizeof(header_t);
    iov[1].iov_base = const_cast<void *>(itr->value);
    iov[1].iov_len = size;

    rc = ::pwrite(fd, iov[0].iov_base, iov[0].iov_len, cachesize);
    rc += ::pwrite(fd, iov[1].iov_base, iov[1].iov_len,
                   cachesize + iov[0].iov_len);

    if (rc <= 0) {
      return -1;
    }

    journal.insert(itr->low, itr->high, cachesize);
    cachesize += sizeof(header_t) + size;
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
  return ::fdatasync(fd);
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
  if (fd >= 0) {
    retc = ftruncate(fd, 0);
    retc |= write_jheader();
  }
  cachesize = 0;
  max_offset = 0;
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
  auto result = journal.query(offset, offset + size);
  std::vector<chunk_t> ret;

  for (auto &itr : result) {
    uint64_t off = (off_t)itr->low < (off_t)offset ? offset : itr->low;
    uint64_t count =
        itr->high < offset + size ? itr->high - off : offset + size - off;
    uint64_t cacheoff = itr->value + sizeof(header_t) + (off - itr->low);
    std::unique_ptr<char[]> buffer(new char[count]);
    ssize_t rc = ::pread(fd, buffer.get(), count, cacheoff);

    if (rc < 0) {
      return ret;
    }

    ret.push_back(chunk_t(off, count, std::move(buffer)));
  }

  return ret;
}
