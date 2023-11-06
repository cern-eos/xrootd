//
// Created by segransm on 11/9/23.
//

#include "XrdS3Router.hh"

namespace S3 {

S3Route &S3Route::Method(const HttpMethod &m) {
  matchers.emplace_back(
      [m](const XrdS3Req &req) { return MatchMethod(m, req); });
  return *this;
}

S3Route &S3Route::Path(const PathMatch &p) {
  matchers.emplace_back([p](const XrdS3Req &req) { return MatchPath(p, req); });
  return *this;
}

S3Route &S3Route::Queries(
    const std::vector<std::pair<std::string, std::string>> &q) {
  matchers.emplace_back(
      [q](const XrdS3Req &req) { return MatchMap(q, req.query); });
  return *this;
}

S3Route &S3Route::Headers(
    const std::vector<std::pair<std::string, std::string>> &h) {
  matchers.emplace_back(
      [h](const XrdS3Req &req) { return MatchMap(h, req.lowercase_headers); });
  return *this;
}

bool S3Route::Match(const XrdS3Req &req) const {
  return std::all_of(matchers.begin(), matchers.end(),
                     [req](const auto &matcher) { return matcher(req); });
}

const HandlerFunc &S3Route::Handler() const { return handler; };

const std::string &S3Route::GetName() const { return name; };

bool S3Route::MatchMethod(const HttpMethod &method, const XrdS3Req &req) {
  return method == req.method;
}

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

void S3Router::AddRoute(S3Route &route) {
  mLog.Say("Registered route:", route.GetName().c_str());
  routes.push_back(route);
}

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
