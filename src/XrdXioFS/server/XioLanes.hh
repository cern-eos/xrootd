//------------------------------------------------------------------------------
// Per-key work lanes for one Xio server.
//
// Same scheduling as XrdHttp2Lanes: one key runs one job at a time, in submit
// order, and different keys run together. This copy lives with the Xio server
// so that server does not link XrdHttpUtils (and through it the XRootD bridge).
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_LANES_HH
#define XIOFS_LANES_HH

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

namespace XioFS {

class Lanes
{
public:
  using Job = std::function<void(int worker)>;

  explicit Lanes(int workers = 8);
  ~Lanes();

  Lanes(const Lanes &) = delete;
  Lanes &operator=(const Lanes &) = delete;

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

} // namespace XioFS

#endif
