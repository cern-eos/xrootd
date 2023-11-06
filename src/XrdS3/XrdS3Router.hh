//
// Created by segransm on 11/9/23.
//

#ifndef XROOTD_XRDS3ROUTER_HH
#define XROOTD_XRDS3ROUTER_HH

#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "XrdS3Action.hh"
#include "XrdS3Req.hh"
#include "XrdSys/XrdSysError.hh"

namespace S3 {

enum class PathMatch { MatchObject, MatchBucket, MatchNoBucket };

class S3Route {
 public:
  explicit S3Route(HandlerFunc fn) : handler(std::move(fn)){};
  S3Route(Action _, std::string name, HandlerFunc fn)
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

class S3Router {
 public:
  explicit S3Router(XrdSysError *log, HandlerFunc fn)
      : mLog(log->logger(), "S3Router_"), not_found_handler(std::move(fn)){};

  ~S3Router() = default;

  void AddRoute(S3Route &route);

  int ProcessReq(XrdS3Req &req);

 private:
  XrdSysError mLog;
  std::vector<S3Route> routes;

  HandlerFunc not_found_handler;
};

}  // namespace S3

#endif  // XROOTD_XRDS3ROUTER_HH
