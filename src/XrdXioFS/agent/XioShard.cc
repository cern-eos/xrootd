//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioShard.hh"

#ifdef HAVE_XRDCL
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#endif

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sstream>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Node {
  std::string host;
  int port{0};
  bool live{true};
  Clock::time_point down_until{};
};

std::mutex gMu;
XioFS::Url gUrl;
std::vector<XioFS::Hop> gHops;
std::vector<Node> gNodes;
bool gInited{false};
bool gHard{false};

bool sameNode(const Node &a, const std::string &host, int port)
{
  return a.port == port && a.host == host;
}

int findNode(const std::string &host, int port)
{
  for (size_t i = 0; i < gNodes.size(); ++i) {
    if (sameNode(gNodes[i], host, port))
      return static_cast<int>(i);
  }
  return -1;
}

void addUnique(std::vector<Node> &dst, const std::string &host, int port)
{
  for (const auto &n : dst) {
    if (sameNode(n, host, port))
      return;
  }
  if (host.empty() || port <= 0 || port > 65535)
    return;
  Node n;
  n.host = host;
  n.port = port;
  n.live = true;
  dst.push_back(n);
}

void parseAddr(const std::string &addr, std::string &host, int &port)
{
  host.clear();
  port = 0;
  if (addr.empty())
    return;
  if (addr.front() == '[') {
    auto rb = addr.find(']');
    if (rb == std::string::npos)
      return;
    host = addr.substr(1, rb - 1);
    if (rb + 1 < addr.size() && addr[rb + 1] == ':')
      port = std::atoi(addr.c_str() + rb + 2);
    return;
  }
  auto c = addr.rfind(':');
  if (c == std::string::npos || addr.find(':') != c) {
    host = addr;
    return;
  }
  host = addr.substr(0, c);
  port = std::atoi(addr.c_str() + c + 1);
}

void locateAlias(const XioFS::Hop &hop, const std::string &path,
                 std::vector<Node> &out)
{
#ifdef HAVE_XRDCL
  std::string root = std::string("root://") + hop.host + ":" +
                     std::to_string(hop.port);
  XrdCl::URL url(root);
  if (!url.IsValid())
    return;
  XrdCl::FileSystem fs(url);
  XrdCl::LocationInfo *info = nullptr;
  std::string locpath = path.empty() ? "/" : path;
  XrdCl::XRootDStatus st =
      fs.DeepLocate(locpath, XrdCl::OpenFlags::PrefName, info, 10);
  if (!st.IsOK() || !info) {
    delete info;
    info = nullptr;
    st = fs.Locate(locpath, XrdCl::OpenFlags::PrefName, info, 10);
  }
  if (st.IsOK() && info) {
    for (auto it = info->Begin(); it != info->End(); ++it) {
      if (!it->IsServer() && info->GetSize() > 1)
        continue;
      std::string host;
      int port = hop.port;
      parseAddr(it->GetAddress(), host, port);
      if (port <= 0)
        port = hop.port;
      addUnique(out, host, port);
    }
  }
  delete info;
#else
  (void)hop;
  (void)path;
  (void)out;
#endif
}

void dnsAlias(const XioFS::Hop &hop, std::vector<Node> &out)
{
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;
  addrinfo *res = nullptr;
  if (getaddrinfo(hop.host.c_str(), std::to_string(hop.port).c_str(), &hints,
                  &res) != 0)
    return;
  for (addrinfo *ai = res; ai; ai = ai->ai_next) {
    char buf[INET6_ADDRSTRLEN] = {};
    const void *src = nullptr;
    if (ai->ai_family == AF_INET)
      src = &reinterpret_cast<sockaddr_in *>(ai->ai_addr)->sin_addr;
    else if (ai->ai_family == AF_INET6)
      src = &reinterpret_cast<sockaddr_in6 *>(ai->ai_addr)->sin6_addr;
    if (!src || !inet_ntop(ai->ai_family, src, buf, sizeof(buf)))
      continue;
    addUnique(out, buf, hop.port);
  }
  freeaddrinfo(res);
}

bool tcpProbe(const std::string &host, int port)
{
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;
  addrinfo *res = nullptr;
  if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0)
    return false;
  bool ok = false;
  for (addrinfo *ai = res; ai && !ok; ai = ai->ai_next) {
    int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (s < 0)
      continue;
    timeval tv{};
    tv.tv_sec = 5;
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (::connect(s, ai->ai_addr, ai->ai_addrlen) == 0)
      ok = true;
    ::close(s);
  }
  freeaddrinfo(res);
  return ok;
}

std::vector<Node> expandNow()
{
  std::vector<Node> next;
  for (const auto &h : gHops) {
    if (!h.alias) {
      addUnique(next, h.host, h.port);
      continue;
    }
    std::vector<Node> got;
    locateAlias(h, gUrl.path, got);
    if (got.size() < 2)
      dnsAlias(h, got);
    if (got.empty())
      addUnique(got, h.host, h.port);
    for (const auto &n : got)
      addUnique(next, n.host, n.port);
  }
  if (next.empty() && !gHops.empty())
    addUnique(next, gHops[0].host, gHops[0].port);
  return next;
}

void mergeMembership(const std::vector<Node> &next)
{
  std::vector<Node> kept;
  for (const auto &n : next) {
    int i = findNode(n.host, n.port);
    if (i >= 0)
      kept.push_back(gNodes[static_cast<size_t>(i)]);
    else
      kept.push_back(n);
  }
  gNodes = kept;
}

void probeDown()
{
  auto now = Clock::now();
  for (auto &n : gNodes) {
    if (n.live)
      continue;
    if (!gHard && now < n.down_until)
      continue;
    if (tcpProbe(n.host, n.port)) {
      n.live = true;
      n.down_until = Clock::time_point{};
    } else {
      n.down_until = now + std::chrono::seconds(kShardCooldownSec);
    }
  }
}

void fillIoctl(xiofs_targets &tg, const XioFS::Url &id)
{
  memset(&tg, 0, sizeof(tg));
  tg.port = static_cast<__u16>(id.port);
  std::snprintf(tg.host, sizeof(tg.host), "%s", id.host.c_str());
  std::snprintf(tg.export_path, sizeof(tg.export_path), "%s",
                id.path.empty() ? "/" : id.path.c_str());
  for (const auto &n : gNodes) {
    if (!n.live && !gHard)
      continue;
    if (tg.n >= XIOFS_MAX_TARGETS)
      break;
    std::snprintf(tg.ent[tg.n].host, sizeof(tg.ent[tg.n].host), "%s",
                  n.host.c_str());
    tg.ent[tg.n].port = static_cast<__u16>(n.port);
    tg.n++;
  }
  if (!tg.n && !gNodes.empty()) {
    std::snprintf(tg.ent[0].host, sizeof(tg.ent[0].host), "%s",
                  gNodes[0].host.c_str());
    tg.ent[0].port = static_cast<__u16>(gNodes[0].port);
    tg.n = 1;
  }
}

} // namespace

bool shardInit(const XioFS::Url &url, std::string &err)
{
  std::lock_guard<std::mutex> g(gMu);
  gUrl = url;
  gHops = url.hops;
  if (gHops.empty()) {
    XioFS::Hop h;
    h.host = url.host;
    h.port = url.port;
    gHops.push_back(h);
  }
  gNodes = expandNow();
  gInited = true;
  if (gNodes.empty()) {
    err = "no HTTP targets after alias expansion";
    return false;
  }
  return true;
}

bool shardActive()
{
  std::lock_guard<std::mutex> g(gMu);
  if (!gInited)
    return false;
  if (gHops.size() > 1)
    return true;
  for (const auto &h : gHops) {
    if (h.alias)
      return true;
  }
  return false;
}

void shardSetHard(bool hard)
{
  std::lock_guard<std::mutex> g(gMu);
  gHard = hard;
}

void shardFail(const std::string &host, int port)
{
  std::lock_guard<std::mutex> g(gMu);
  int i = findNode(host, port);
  if (i < 0)
    return;
  gNodes[static_cast<size_t>(i)].live = false;
  if (gHard)
    gNodes[static_cast<size_t>(i)].down_until = Clock::now();
  else
    gNodes[static_cast<size_t>(i)].down_until =
        Clock::now() + std::chrono::seconds(kShardCooldownSec);
}

bool shardPick(__u32 uid, std::string &host, int &port, std::string &err)
{
  std::lock_guard<std::mutex> g(gMu);
  std::vector<size_t> live;
  for (size_t i = 0; i < gNodes.size(); ++i) {
    if (gNodes[i].live)
      live.push_back(i);
  }
  if (live.empty()) {
    if (!gHard || gNodes.empty()) {
      err = "no live HTTP targets";
      return false;
    }
    size_t i = uid % gNodes.size();
    host = gNodes[i].host;
    port = gNodes[i].port;
    return true;
  }
  size_t i = live[uid % live.size()];
  host = gNodes[i].host;
  port = gNodes[i].port;
  return true;
}

int shardPush(int ctl, const XioFS::Url &id, std::string &err)
{
  xiofs_targets tg{};
  {
    std::lock_guard<std::mutex> g(gMu);
    fillIoctl(tg, id);
  }
  if (!tg.n) {
    err = "no live HTTP targets to push";
    return -1;
  }
  if (ioctl(ctl, XIOFS_IOC_SET_TARGETS, &tg) < 0) {
    err = std::string("XIOFS_IOC_SET_TARGETS: ") + strerror(errno);
    return -1;
  }
  return 0;
}

void shardRefresh(const XioFS::Url &url, int ctl)
{
  std::string err;
  {
    std::lock_guard<std::mutex> g(gMu);
    gUrl = url;
    auto next = expandNow();
    mergeMembership(next);
    probeDown();
  }
  if (ctl >= 0)
    shardPush(ctl, url, err);
}
