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
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClPlugInInterface.hh"
/*----------------------------------------------------------------------------*/
#include "../file/XrdClJournalCacheFile.hh"
#include "../file/PolicyConfig.hh"
#include "../file/PolicyRuntime.hh"
#include "../system/XrdClJournalCacheSystem.hh"
/*----------------------------------------------------------------------------*/
#include <map>
#include <memory>
#include <string>
/*----------------------------------------------------------------------------*/

namespace XrdCl {
//------------------------------------------------------------------------------
//! XrdCl JournalCache plug-in factory
//------------------------------------------------------------------------------
class JournalCacheFactory : public PlugInFactory {
public:
  static bool sEnableFileSystem;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param config map containing configuration parameters
  //----------------------------------------------------------------------------
  JournalCacheFactory(const std::map<std::string, std::string> *config) {
    if (config) {
      auto itc = config->find("cache");
      JournalCacheFile::SetCache(itc != config->end() ? itc->second : "");

      auto itsz = config->find("size");
      JournalCacheFile::SetSize(itsz != config->end()
                              ? std::stoll(std::string(itsz->second), 0, 10)
                              : 0);

      auto itj = config->find("journal");
      JournalCacheFile::SetJournal(itj != config->end() ? (itj->second == "true") ||
                                                        (itj->second == "1")
                                                  : true);
      auto itcrc = config->find("crc");
      JournalCacheFile::SetJournalCrc(itcrc != config->end()
                                          ? (itcrc->second == "true") ||
                                                (itcrc->second == "1")
                                          : false);
      auto ita = config->find("async");
      JournalCacheFile::SetAsync(ita != config->end()
                               ? (ita->second == "true") || (ita->second == "1")
                               : false);

      auto itb = config->find("bypass");
      JournalCacheFile::SetBypass(itb != config->end() ? (itb->second == "true") ||
                                                       (itb->second == "1")
                                                 : false);
      auto itjson = config->find("json");
      JournalCacheFile::SetJsonPath(itjson != config->end() ? itjson->second : "");

      auto its = config->find("summary");
      JournalCacheFile::SetSummary(its != config->end() ? (its->second == "true") ||
                                                        (its->second == "1")
                                                  : true);
      auto itsi = config->find("stats");
      JournalCacheFile::SetStatsInterval(
          itsi != config->end() ? std::stoll(std::string(itsi->second), 0, 10)
                                : 0);

      auto itf = config->find("flat");
      JournalCacheFile::SetFlatHierarchy(
          itf != config->end() ? (itf->second == "true") || (itf->second == "1")
                               : false);

      auto itbp = config->find("basepath");
      if (itbp != config->end() && !itbp->second.empty()) {
	JournalCacheFile::SetBasePath(itbp->second);
      }

      auto itmux = config->find("demux");
      JournalCacheFile::SetThreadConnectionDemultiplexing(
          itmux != config->end()
              ? (itmux->second == "true") || (itmux->second == "1")
              : false);

      auto itmo = config->find("multi_origin");
      JournalCacheFile::SetMultiOriginUnwrap(
          itmo != config->end()
              ? (itmo->second == "true") || (itmo->second == "1")
              : false);

      auto itao = config->find("allow_origin");
      if (itao != config->end() && !itao->second.empty()) {
        JournalCacheFile::SetAllowedOrigins(itao->second);
      }

      auto iterd = config->find("external_redirect");
      if (iterd != config->end() && !iterd->second.empty()) {
        JournalCacheFile::SetExternalRedirects(iterd->second);
      }

      // if we are supposed to load also the System plug-in
      auto itss = config->find("system");
      sEnableFileSystem = (itss != config->end() ? (itss->second == "true") ||
                                                       (itss->second == "1")
                                                 : true);

      auto itlt = config->find("listttl");
      JournalCacheSystem::sListTtl =
          itlt != config->end() ? std::stoull(std::string(itlt->second), 0, 10) : 0;

      auto itls = config->find("liststat");
      JournalCacheSystem::sEnableListStat =
          itls != config->end() ? (itls->second == "true") || (itls->second == "1")
                                : true;

      if (const char *v = getenv("XRD_JOURNALCACHE_CACHE")) {
        JournalCacheFile::SetCache((std::string(v).length()) ? std::string(v) : "");
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_SIZE")) {
        JournalCacheFile::SetSize(
            (std::string(v).length()) ? std::stoll(std::string(v), 0, 10) : 0);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_SUMMARY")) {
        JournalCacheFile::SetSummary(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_JOURNAL")) {
        JournalCacheFile::SetJournal(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_CRC")) {
        JournalCacheFile::SetJournalCrc(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_ASYNC")) {
        JournalCacheFile::SetAsync(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_BYPASS")) {
        JournalCacheFile::SetBypass(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_JSON")) {
        JournalCacheFile::SetJsonPath((std::string(v).length()) ? std::string(v)
                                                          : "");
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_STATS")) {
        JournalCacheFile::SetStatsInterval(
            (std::string(v).length()) ? std::stoll(std::string(v), 0, 10) : 0);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_FLAT")) {
        JournalCacheFile::SetFlatHierarchy(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_BASEPATH")) {
	JournalCacheFile::SetBasePath(v);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_DEMUX")) {
        JournalCacheFile::SetThreadConnectionDemultiplexing(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_MULTI_ORIGIN")) {
        JournalCacheFile::SetMultiOriginUnwrap(
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_ALLOW_ORIGIN")) {
        JournalCacheFile::SetAllowedOrigins(v);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_EXTERNAL_REDIRECT")) {
        JournalCacheFile::SetExternalRedirects(v);
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_SYSTEM")) {
        sEnableFileSystem =
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false;
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_LISTTTL")) {
        JournalCacheSystem::sListTtl =
            (std::string(v).length()) ? std::stoull(std::string(v), 0, 10) : 0;
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_LISTSTAT")) {
        JournalCacheSystem::sEnableListStat =
            ((std::string(v) == "true") || (std::string(v) == "1")) ? true
                                                                    : false;
      }

      Env *env = DefaultEnv::GetEnv();
      std::string appName;
      env->GetString("AppName", appName);
      std::string noApps;
      auto itapp = config->find("noapp");
      if (itapp != config->end()) {
        noApps = itapp->second;
      }

      if (const char *v = getenv("XRD_JOURNALCACHE_NOAPP")) {
        noApps = v;
      }

      if (noApps.length() && (noApps.find(appName) != std::string::npos)) {
        // switch the cache on bypass if we are in the 'noapp' entry list
        JournalCacheFile::SetBypass(true);
      }

      unsigned policyPoll = 2;
      auto itpp = config->find("policy_poll");
      if (itpp != config->end() && !itpp->second.empty()) {
        policyPoll = static_cast<unsigned>(std::stoul(itpp->second));
      }

      std::string policyPath;
      auto itpol = config->find("policy");
      if (itpol != config->end()) {
        policyPath = itpol->second;
      }
      if (const char *v = getenv("XRD_JOURNALCACHE_POLICY")) {
        policyPath = v;
      }
      if (policyPath.empty()) {
        policyPath =
            JournalCache::defaultPolicyPath(JournalCacheFile::sCachePath);
      }
      if (const char *v = getenv("XRD_JOURNALCACHE_POLICY_POLL")) {
        policyPoll = static_cast<unsigned>(std::stoul(v));
      }

      JournalCache::PolicySettings bootstrap;
      bootstrap.bypass = JournalCacheFile::sEnableBypass;
      bootstrap.multiOriginUnwrap = JournalCacheFile::sMultiOriginUnwrap;
      bootstrap.originAllowlist = JournalCacheFile::sOriginAllowlist;
      bootstrap.externalRedirect = JournalCacheFile::sExternalRedirect;
      JournalCache::PolicyRuntime::instance().configure(policyPath, bootstrap);
      JournalCache::PolicyRuntime::instance().startWatcher(policyPoll);

      Journal::sDefaultEnableCrc = JournalCacheFile::sEnableJournalCrc;

      Log *log = DefaultEnv::GetLog();
      log->Info(1, "JournalCache : cache directory: %s",
                JournalCacheFile::sCachePath.c_str());
      log->Info(1, "JournalCache : caching reads in journal cache: %s",
                JournalCacheFile::sEnableJournalCache ? "true" : "false");
      log->Info(1, "JournalCache : journal crc32c checksums: %s",
                JournalCacheFile::sEnableJournalCrc ? "true" : "false");
      log->Info(1, "JournalCache : summary output is: %s",
                JournalCacheFile::sEnableSummary ? "true" : "false");
      log->Info(1, "JournalCache : asynchrous/disconnected operation: %s",
                JournalCacheFile::sOpenAsync ? "true" : "false");
      log->Info(1, "JournalCache : bypass operation: %s",
                JournalCacheFile::sEnableBypass ? "true" : "false");
      log->Info(1, "JournalCache : connection demultiplexing: %s",
                JournalCacheFile::sThreadConnectionDemultiplexing ? "true" : "false");
      log->Info(1, "JournalCache : multi-origin unwrap: %s",
                JournalCacheFile::sMultiOriginUnwrap ? "true" : "false");
      log->Info(1, "JournalCache : allowed origin regex: %s",
                JournalCacheFile::sOriginAllowlist.empty() ? "none"
                                                           : "configured");
      log->Info(1, "JournalCache : external redirect rules: %zu",
                JournalCacheFile::sExternalRedirect.rules().size());
      if (!policyPath.empty()) {
        log->Info(1, "JournalCache : runtime policy file: %s (poll=%us)",
                  policyPath.c_str(), policyPoll);
      }
      log->Info(1, "JournalCache : running app: %s", appName.c_str());

      log->Info(1, "JournalCache : basepath: '%s'", JournalCacheFile::sBasePath.c_str());
      log->Info(1, "JournalCache : filesystem plug-in : %s",
                sEnableFileSystem ? "true" : "false");
      log->Info(1, "JournalCache : listing cache ttl (s) : %llu",
                static_cast<unsigned long long>(JournalCacheSystem::sListTtl));
      log->Info(1, "JournalCache : stat cache : %s",
                JournalCacheSystem::sEnableListStat ? "true" : "false");

      if (noApps.length()) {
        log->Info(1, "JournalCache : filtered apps: %s", noApps.c_str());
      }
      if (JournalCacheFile::sJsonPath.length()) {
        log->Info(1, "JournalCache : json output to prefix: %s",
                  JournalCacheFile::sJsonPath.c_str());
      } else {
        log->Info(1, "JournalCache : json output is disabled");
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~JournalCacheFactory() {}

  //----------------------------------------------------------------------------
  //! Create a file plug-in for the given URL
  //----------------------------------------------------------------------------
  virtual FilePlugIn *CreateFile(const std::string &url) {
    std::unique_ptr<JournalCacheFile> ptr(new JournalCacheFile());
    if (!ptr->IsValid())
      return nullptr;
    return static_cast<FilePlugIn *>(ptr.release());
  }

  //----------------------------------------------------------------------------
  //! Create a file system plug-in for the given URL
  //----------------------------------------------------------------------------
  virtual FileSystemPlugIn *CreateFileSystem(const std::string &url) {
    if (JournalCacheFactory::sEnableFileSystem) {
      std::unique_ptr<JournalCacheSystem> ptr(new JournalCacheSystem(url));
      return static_cast<FileSystemPlugIn *>(ptr.release());
    } else {
      return static_cast<FileSystemPlugIn *>(0);
    }
  }
};

} // namespace XrdCl
