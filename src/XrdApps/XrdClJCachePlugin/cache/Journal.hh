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

#pragma once
/*----------------------------------------------------------------------------*/
#include "IntervalTree.hh"
/*----------------------------------------------------------------------------*/
#include <stdint.h>
#include <string>
#include <mutex>
#include <map>
/*----------------------------------------------------------------------------*/
class Journal
{
  static constexpr uint64_t JOURNAL_MAGIC = 0xcafecafecafecafe;

  struct jheader_t {
    jheader_t() {
      mtime = mtime_nsec = filesize = placeholder1 = placeholder2 = placeholder3 = placeholder4 = magic = 0;
    }
    uint64_t magic;
    uint64_t mtime;
    uint64_t mtime_nsec; // XRootD does not support this though
    uint64_t filesize;
    uint64_t placeholder1;
    uint64_t placeholder2;
    uint64_t placeholder3;
    uint64_t placeholder4;
  };
  
  struct header_t {
    uint64_t offset;
    uint64_t size;
  };

public:

  struct chunk_t {

    chunk_t() : offset(0), size(0), buff(0) { }

    /* constructor - no ownership of underlying buffer */
    chunk_t(off_t offset, size_t size, const void* buff) : offset(offset),
      size(size), buff(buff) { }

    /* constructor - with ownership of underlying buffer */
    chunk_t(off_t offset, size_t size, std::unique_ptr<char[]> buff) :
      offset(offset), size(size), buffOwnership(std::move(buff)),
      buff((const void*) buffOwnership.get()) {}

    off_t offset;
    size_t size;
    std::unique_ptr<char[]> buffOwnership;
    const void* buff;

    bool operator<(const chunk_t& u) const
    {
      return offset < u.offset;
    }
  };

  Journal();
  virtual ~Journal();

  // base class interface
  int attach(const std::string& path, uint64_t mtime, uint64_t mtime_nsec, uint64_t size);
  int detach();
  int unlink();

  ssize_t pread(void* buf, size_t count, off_t offset, bool& eof);
  ssize_t pwrite(const void* buf, size_t count, off_t offset);

  int sync();

  size_t size();

  off_t get_max_offset();

  int reset();

  std::vector<chunk_t> get_chunks(off_t offset, size_t size);

  std::string dump();
private:

  void process_intersection(interval_tree<uint64_t, const void*>& write,
                            interval_tree<uint64_t, uint64_t>::iterator acr, std::vector<chunk_t>& updates);

  static uint64_t offset_for_update(uint64_t offset, uint64_t shift)
  {
    return offset + sizeof(header_t) + shift;
  }

  int update_cache(std::vector<chunk_t>& updates);
  int read_journal();
 
  jheader_t jheader;
  int write_jheader();
  void read_jheader();
  
  std::string path;
  size_t cachesize;
  off_t max_offset;
  int fd;

  // the value is the offset in the cache file
  interval_tree<uint64_t, uint64_t> journal;
  std::mutex mtx;
  int flags;

};

class JournalManager {
private:
    std::map<std::string, std::shared_ptr<Journal>> journals;
    std::mutex jMutex;

public:
  JournalManager() {}
  virtual ~JournalManager() {}
  
    // Attach method: creates or retrieves a Journal object by key
    std::shared_ptr<Journal> attach(const std::string &key) {
        std::lock_guard<std::mutex> guard(jMutex);
        auto it = journals.find(key);
        if (it == journals.end()) {
            // Create a new Journal object if it doesn't exist
            auto journal = std::make_shared<Journal>();
            journals[key] = journal;
            return journal;
        } else {
            // Return the existing Journal object
            return it->second;
        }
    }

    // Detach method: checks reference count and removes Journal object if necessary
    void detach(const std::string &key) {
        std::lock_guard<std::mutex> guard(jMutex);
        auto it = journals.find(key);
        if (it != journals.end()) {
            if (it->second.use_count() == 1) {
                // Only one reference exists, so erase the entry from the map
                journals.erase(it);
            }
            // If more than one reference exists, do nothing
        }
    }
};
