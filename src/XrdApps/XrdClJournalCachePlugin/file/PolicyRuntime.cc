#include "file/PolicyRuntime.hh"

#include <filesystem>

namespace JournalCache {
namespace {

std::chrono::system_clock::time_point fileWriteTime(const std::string &path) {
  std::error_code ec;
  const auto mtime = std::filesystem::last_write_time(path, ec);
  if (ec) {
    return {};
  }
#if defined(__cpp_lib_chrono) && __cpp_lib_chrono >= 201907L
  return std::chrono::clock_cast<std::chrono::system_clock>(mtime);
#else
  using namespace std::chrono;
  return system_clock::time_point(
      duration_cast<system_clock::duration>(mtime.time_since_epoch()));
#endif
}

} // namespace

PolicyRuntime &PolicyRuntime::instance() {
  static PolicyRuntime runtime;
  return runtime;
}

PolicyRuntime::~PolicyRuntime() { stopWatcher(); }

void PolicyRuntime::configure(const std::string &policyPath,
                              const PolicySettings &bootstrap) {
  std::lock_guard lock(mMutex);
  mPolicyPath = policyPath;
  mSettings = bootstrap;

  if (mPolicyPath.empty()) {
    mHasWriteTime = false;
    return;
  }

  PolicySettings loaded;
  if (loadPolicyFile(mPolicyPath, loaded)) {
    mSettings = loaded;
    mLastWriteTime = fileWriteTime(mPolicyPath);
    mHasWriteTime = mLastWriteTime != std::chrono::system_clock::time_point{};
  } else {
    savePolicyFile(mPolicyPath, mSettings);
    mLastWriteTime = fileWriteTime(mPolicyPath);
    mHasWriteTime = mLastWriteTime != std::chrono::system_clock::time_point{};
  }
}

void PolicyRuntime::startWatcher(unsigned pollSeconds) {
  if (pollSeconds == 0) {
    return;
  }
  if (mWatcherRunning.exchange(true)) {
    return;
  }
  mStopWatcher = false;
  mWatcher = std::thread([this, pollSeconds]() { watcherLoop(pollSeconds); });
}

void PolicyRuntime::stopWatcher() {
  if (!mWatcherRunning.load()) {
    return;
  }
  mStopWatcher = true;
  if (mWatcher.joinable()) {
    mWatcher.join();
  }
  mWatcherRunning = false;
}

void PolicyRuntime::reloadIfChanged() {
  std::string path;
  std::chrono::system_clock::time_point lastWrite;
  bool hasWrite = false;
  {
    std::lock_guard lock(mMutex);
    path = mPolicyPath;
    lastWrite = mLastWriteTime;
    hasWrite = mHasWriteTime;
  }
  if (path.empty()) {
    return;
  }

  const auto writeTime = fileWriteTime(path);
  if (writeTime == std::chrono::system_clock::time_point{}) {
    return;
  }
  if (hasWrite && writeTime == lastWrite) {
    return;
  }

  PolicySettings loaded;
  if (!loadPolicyFile(path, loaded)) {
    return;
  }

  std::lock_guard lock(mMutex);
  if (mHasWriteTime && writeTime == mLastWriteTime) {
    return;
  }
  mSettings = loaded;
  mLastWriteTime = writeTime;
  mHasWriteTime = true;
}

PolicySettings PolicyRuntime::snapshot() const {
  std::lock_guard lock(mMutex);
  return mSettings;
}

std::string PolicyRuntime::policyPath() const {
  std::lock_guard lock(mMutex);
  return mPolicyPath;
}

void PolicyRuntime::watcherLoop(unsigned pollSeconds) {
  while (!mStopWatcher.load()) {
    reloadIfChanged();
    for (unsigned i = 0; i < pollSeconds * 10 && !mStopWatcher.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

} // namespace JournalCache
