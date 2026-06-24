#pragma once

#include "file/PolicyConfig.hh"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace JournalCache {

//! Thread-safe runtime policy with optional mtime polling reload.
class PolicyRuntime {
public:
  static PolicyRuntime &instance();

  void configure(const std::string &policyPath,
                   const PolicySettings &bootstrap);
  void startWatcher(unsigned pollSeconds = 2);
  void stopWatcher();
  void reloadIfChanged();

  PolicySettings snapshot() const;
  std::string policyPath() const;

private:
  PolicyRuntime() = default;
  ~PolicyRuntime();
  PolicyRuntime(const PolicyRuntime &) = delete;
  PolicyRuntime &operator=(const PolicyRuntime &) = delete;

  void watcherLoop(unsigned pollSeconds);

  mutable std::mutex mMutex;
  PolicySettings mSettings;
  std::string mPolicyPath;
  std::chrono::system_clock::time_point mLastWriteTime{};
  bool mHasWriteTime = false;

  std::thread mWatcher;
  std::atomic<bool> mStopWatcher{false};
  std::atomic<bool> mWatcherRunning{false};
};

} // namespace JournalCache
