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

//------------------------------------------------------------------------------
#include "XrdS3Router.hh"
//------------------------------------------------------------------------------

namespace S3 {

//------------------------------------------------------------------------------
//! \brief match the method and the request
//! \param m the method
//! \param req the request
//! \return s3route
//------------------------------------------------------------------------------
S3Route &S3Route::Method(const HttpMethod &m) {
  matchers.emplace_back(
      [m](const XrdS3Req &req) { return MatchMethod(m, req); });
  return *this;
}

//------------------------------------------------------------------------------
//! \brief match the path
//! \param p the path
//! \return s3route
//------------------------------------------------------------------------------
S3Route &S3Route::Path(const PathMatch &p) {
  matchers.emplace_back([p](const XrdS3Req &req) { return MatchPath(p, req); });
  return *this;
}

//------------------------------------------------------------------------------
//! \brief match the bucket
//! \param b the bucket
//! \return s3route
//------------------------------------------------------------------------------
S3Route &S3Route::Queries(
    const std::vector<std::pair<std::string, std::string>> &q) {
  matchers.emplace_back(
      [q](const XrdS3Req &req) { return MatchMap(q, req.query); });
  return *this;
}

//------------------------------------------------------------------------------
//! \brief match headers
//! \param h the headers
//! \return s3route
//------------------------------------------------------------------------------
S3Route &S3Route::Headers(
    const std::vector<std::pair<std::string, std::string>> &h) {
  matchers.emplace_back(
      [h](const XrdS3Req &req) { return MatchMap(h, req.lowercase_headers); });
  return *this;
}

//------------------------------------------------------------------------------
//! \brief match request
//! \param req the request
//! \return true if the request matches the route
//------------------------------------------------------------------------------
bool S3Route::Match(const XrdS3Req &req) const {
  return std::all_of(matchers.begin(), matchers.end(),
                     [req](const auto &matcher) { return matcher(req); });
}

//------------------------------------------------------------------------------
//! \brief return handler
//! \return handler
//------------------------------------------------------------------------------
const HandlerFunc &S3Route::Handler() const { return handler; };

//------------------------------------------------------------------------------
//! \brief return name
//! \return name
//------------------------------------------------------------------------------
const std::string &S3Route::GetName() const { return name; };

//------------------------------------------------------------------------------
//! \brief match the method
//! \param method the method
//! \param req the request
//! \return true if the method matches the request
//------------------------------------------------------------------------------
bool S3Route::MatchMethod(const HttpMethod &method, const XrdS3Req &req) {
  return method == req.method;
}

//------------------------------------------------------------------------------
//! \brief match the path
//! \param path the path
//! \param req the request
//! \return true if the path matches the request
//------------------------------------------------------------------------------
bool S3Route::MatchPath(const PathMatch &path, const XrdS3Req &req) {
  // Path will always start with '/'
  switch (path) {
    case PathMatch::MatchObject:
      return !req.object.empty();
    case PathMatch::MatchBucket:
      return !req.bucket.empty() && req.object.empty();
    case PathMatch::MatchNoBucket:
      return req.bucket.empty() && req.object.empty();
  }
  return false;
}

//------------------------------------------------------------------------------
//! \brief match requirements against map
//! \param required the requirements
//! \param map the map
//! \return true if the requirements match the map
//------------------------------------------------------------------------------
bool S3Route::MatchMap(
    const std::vector<std::pair<std::string, std::string>> &required,
    const std::map<std::string, std::string> &map) {
  return std::all_of(
      required.begin(), required.end(), [map](const auto &param) {
        auto it = map.find(param.first);

        return (it != map.end() &&
                (param.second == "*" ||
                 (param.second == "+" ? !it->second.empty()
                                      : it->second == param.second)));
      });
}

//------------------------------------------------------------------------------
//! \brief add route to router
//! \param route the route
//------------------------------------------------------------------------------
void S3Router::AddRoute(S3Route &route) {
  mLog.Say("Registered route:", route.GetName().c_str());
  routes.push_back(route);
}

//------------------------------------------------------------------------------
//! \brief process request
//! \param req the request
//! \return the status code
//------------------------------------------------------------------------------
int S3Router::ProcessReq(XrdS3Req &req) {
  for (const auto &route : routes) {
    if (route.Match(req)) {
      mLog.Say("Found matching route for req:", route.GetName().c_str());
      return route.Handler()(req);
    }
  }
  mLog.Say("Unable to find matching route for request...");
  return not_found_handler(req);
}

}  // namespace S3
