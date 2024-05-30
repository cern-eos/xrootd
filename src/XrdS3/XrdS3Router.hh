//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
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
//------------------------------------------------------------------------------

#pragma once

//------------------------------------------------------------------------------
#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>
//------------------------------------------------------------------------------
#include "XrdS3Action.hh"
#include "XrdS3Req.hh"
#include "XrdS3Log.hh"
#include "XrdSys/XrdSysError.hh"
//------------------------------------------------------------------------------

namespace S3 {

enum class PathMatch { MatchObject, MatchBucket, MatchNoBucket };

//------------------------------------------------------------------------------
//! S3Route is a class that represents a route in the S3 server.
//! It contains a handler function and a list of matchers.
//! The matchers are used to match the request to the route.
//------------------------------------------------------------------------------
class S3Route {
 public:
  explicit S3Route(HandlerFunc fn) : handler(std::move(fn)){};
  S3Route(std::string name, HandlerFunc fn)
      : handler(std::move(fn)), name(std::move(name)){};

  ~S3Route() = default;

  S3Route &Method(const HttpMethod &m);

  S3Route &Path(const PathMatch &p);

  S3Route &Queries(const std::vector<std::pair<std::string, std::string>> &q);

  S3Route &Headers(const std::vector<std::pair<std::string, std::string>> &h);

  bool Match(const XrdS3Req &req) const;

  const HandlerFunc &Handler() const;

  const std::string &GetName() const;

 private:
  using Matcher = std::function<bool(const XrdS3Req &)>;

  static bool MatchMethod(const HttpMethod &method, const XrdS3Req &req);

  static bool MatchPath(const PathMatch &path, const XrdS3Req &req);

  static bool MatchMap(
      const std::vector<std::pair<std::string, std::string>> &required,
      const std::map<std::string, std::string> &map);

  std::vector<Matcher> matchers;
  const HandlerFunc handler;
  const std::string name;
};

//------------------------------------------------------------------------------
//! S3Router is a class that represents a router in the S3 server.
//! It contains a list of routes and a not found handler.
//! The router is used to match the request to the route.
//------------------------------------------------------------------------------
class S3Router {
 public:
  explicit S3Router(S3Log &log, HandlerFunc fn)
    : mLog(&log), not_found_handler(std::move(fn)){};

  ~S3Router() = default;

  void AddRoute(S3Route &route);

  int ProcessReq(XrdS3Req &req);

 private:
  S3Log* mLog;
  std::vector<S3Route> routes;

  HandlerFunc not_found_handler;
};

}  // namespace S3


