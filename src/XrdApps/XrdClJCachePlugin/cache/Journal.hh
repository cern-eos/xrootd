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
/*----------------------------------------------------------------------------*/

class Journal
{
  static constexpr uint64_t JOURNAL_MAGIC = 0xcafecafecafecafe;

  struct jheader_t {
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

  ssize_t pread(void* buf, size_t count, off_t offset);
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

