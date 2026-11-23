//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioLanes.hh"

namespace XioFS {

Lanes::Lanes(int workers)
  : workers_(workers < 1 ? 1 : workers)
{}

Lanes::~Lanes()
{
  shutdown();
}

void Lanes::ensureStarted()
{
  if (started_)
    return;
  started_ = true;
  stop_ = false;
  threads_.reserve(static_cast<size_t>(workers_));
  for (int i = 0; i < workers_; i++)
    threads_.emplace_back([this, i] { loop(i); });
}

bool Lanes::submit(const std::string &key, Job job)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (stop_)
    return false;
  ensureStarted();

  std::shared_ptr<Lane> &lane = lanes_[key];
  if (!lane)
    lane = std::make_shared<Lane>();
  const bool start = !lane->running && lane->q.empty();
  lane->q.push_back(std::move(job));
  if (start)
    ready_.push_back(lane);
  cv_.notify_one();
  return true;
}

void Lanes::shutdown()
{
  std::vector<std::thread> threads;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!started_)
      return;
    stop_ = true;
    ready_.clear();
    lanes_.clear();
    threads.swap(threads_);
    started_ = false;
  }
  cv_.notify_all();
  for (std::thread &t : threads)
    if (t.joinable())
      t.join();
  std::lock_guard<std::mutex> lock(mu_);
  stop_ = false;
}

void Lanes::loop(int index)
{
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    cv_.wait(lock, [&] { return stop_ || !ready_.empty(); });
    if (stop_)
      break;
    std::shared_ptr<Lane> lane = ready_.front();
    ready_.pop_front();
    if (!lane || lane->q.empty())
      continue;
    lane->running = true;
    Job job = std::move(lane->q.front());
    lane->q.pop_front();
    lock.unlock();
    job(index);
    lock.lock();
    if (lane) {
      lane->running = false;
      if (!stop_ && !lane->q.empty())
        ready_.push_back(lane);
    }
  }
}

} // namespace XioFS
