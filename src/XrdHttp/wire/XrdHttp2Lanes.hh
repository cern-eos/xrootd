//------------------------------------------------------------------------------
// Per-file work lanes for one HTTP/2 connection.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP2LANES_HH
#define XRDHTTP2LANES_HH

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Jobs with the same key run one at a time, in submit order. Jobs with
// different keys run together, up to the worker count.
class XrdHttp2Lanes
{
public:
  using Job = std::function<void(int worker)>;

  explicit XrdHttp2Lanes(int workers = 8);
  ~XrdHttp2Lanes();

  XrdHttp2Lanes(const XrdHttp2Lanes &) = delete;
  XrdHttp2Lanes &operator=(const XrdHttp2Lanes &) = delete;

  // False when the pool is stopped.
  bool submit(const std::string &key, Job job);

  void shutdown();

  int workerCount() const { return workers_; }

private:
  struct Lane
  {
    std::deque<Job> q;
    bool running{false};
  };

  void ensureStarted();
  void loop(int index);

  int workers_;
  bool stop_{false};
  bool started_{false};
  std::mutex mu_;
  std::condition_variable cv_;
  std::map<std::string, std::shared_ptr<Lane>> lanes_;
  std::deque<std::shared_ptr<Lane>> ready_;
  std::vector<std::thread> threads_;
};

#endif
