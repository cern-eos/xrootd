//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
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
#include "XrdCl/XrdClPlugInInterface.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
/*----------------------------------------------------------------------------*/
#include "../file/XrdClJCacheFile.hh"
/*----------------------------------------------------------------------------*/
#include <memory>
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

namespace XrdCl
{
//------------------------------------------------------------------------------
//! XrdCl JCache plug-in factory
//------------------------------------------------------------------------------
class JCacheFactory : public PlugInFactory
{
  public:
    //----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param config map containing configuration parameters
    //----------------------------------------------------------------------------
    JCacheFactory( const std::map<std::string, std::string>* config )
    {
      if( config )
      {
	auto itc = config->find( "cache" );
	JCacheFile::SetCache( itc != config->end() ? itc->second : "" );
        auto itv = config->find( "vector" );
        JCacheFile::SetVector( itv != config->end() ? itv->second == "true": false );
        auto itj = config->find( "journal" );
        JCacheFile::SetJournal( itj != config->end() ? itj->second == "true": false );
	auto itjson = config->find( "json" );
	JCacheFile::SetJsonPath( itjson != config->end() ? itjson->second : "./" );
	auto its = config->find( "summary" );
	JCacheFile::SetSummary( its != config->end() ? its->second != "false": true );

	if (const char *v = getenv("XRD_JCACHE_CACHE")) {
	  JCacheFile::SetCache( (std::string(v).length()) ? std::string(v) : "");
	}

	if (const char *v = getenv("XRD_JCACHE_SUMMARY")) {
	  JCacheFile::SetSummary( (std::string(v) == "true") ? true : false);
	}

	if (const char *v = getenv("XRD_JCACHE_JOURNAL")) {
	  JCacheFile::SetJournal( (std::string(v) == "true") ? true : false);
	}

	if (const char *v = getenv("XRD_JCACHE_VECTOR")) {
	  JCacheFile::SetVector( (std::string(v) == "true") ? true : false);
	}

	if (const char *v = getenv("XRD_JCACHE_JSON")) {
	  JCacheFile::SetJsonPath( (std::string(v).length()) ? std::string(v) : "");
	}

        Log* log = DefaultEnv::GetLog();
        log->Info(1, "JCache : cache directory: %s", JCacheFile::sCachePath.c_str());
        log->Info(1, "JCache : caching readv in vector cache : %s", JCacheFile::sEnableVectorCache ? "true" : "false");
        log->Info(1, "JCache : caching reads in journal cache: %s", JCacheFile::sEnableJournalCache ? "true" : "false");
	log->Info(1, "JCache : summary output is: %s", JCacheFile::sEnableSummary ? "true" : "false");
	if (JCacheFile::sJsonPath.length()) {
	  log->Info(1, "JCache : json output to prefix: %s", JCacheFile::sJsonPath.c_str());
	} else {
	  log->Info(1, "JCache : json output is disabled", JCacheFile::sJsonPath.c_str());
	}
      }
    }

    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual ~JCacheFactory()
    {
    }

    //----------------------------------------------------------------------------
    //! Create a file plug-in for the given URL
    //----------------------------------------------------------------------------
    virtual FilePlugIn* CreateFile(const std::string& url)
    {
      std::unique_ptr<JCacheFile> ptr( new JCacheFile() );
      if( !ptr->IsValid() )
        return nullptr;
      return static_cast<FilePlugIn*>( ptr.release() );
    }

    //----------------------------------------------------------------------------
    //! Create a file system plug-in for the given URL
    //----------------------------------------------------------------------------
    virtual FileSystemPlugIn* CreateFileSystem(const std::string& url)
    {
      Log* log = DefaultEnv::GetLog();
      log->Error(1, "FileSystem plugin implementation not supported");
      return static_cast<FileSystemPlugIn*>(0);
    }
};

} // namespace XrdCl
