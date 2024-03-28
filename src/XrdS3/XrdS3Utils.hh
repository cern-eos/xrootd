//
// Created by segransm on 11/3/23.
//

#ifndef XROOTD_XRDS3UTILS_HH
#define XROOTD_XRDS3UTILS_HH

#include <dirent.h>

#include <bitset>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <map>
#include <numeric>
#include <sstream>
#include <string>

namespace S3 {

class S3Utils {
 public:
  S3Utils();

  ~S3Utils() = default;

  std::string UriEncode(const std::string &str);
  std::string ObjectUriEncode(const std::string &str);

  std::string UriDecode(const std::string &str);

  template <typename T>
  static std::string HexEncode(const T &s) {
    std::stringstream ss;

    for (const auto &c : s) {
      ss << std::hex << std::setw(2) << std::setfill('0') << (unsigned int)c;
    }

    return ss.str();
  }

  static void TrimAll(std::string &str);

  template <typename... T>
  static std::string stringJoin(const char delim, const T &...args) {
    std::string res;

    size_t size = 0;
    for (const auto &x : {args...}) {
      size += x.size() + 1;
    }

    res.reserve(size);

    for (const auto &x : {args...}) {
      res += x;
      res += delim;
    }
    res.pop_back();

    return res;
  }

  template <typename T>
  static std::string stringJoin(const char delim, const T &src) {
    std::string res;

    size_t size = 0;
    for (const auto &x : src) {
      size += x.size() + 1;
    }

    res.reserve(size);

    for (const auto &x : src) {
      res += x;
      res += delim;
    }
    res.pop_back();

    return res;
  }

  static std::string UriEncode(const std::bitset<256> &encoder,
                               const std::string &str);
  static bool MapHasKey(const std::map<std::string, std::string> &map,
                        const std::string &key);
  static bool MapHasEntry(const std::map<std::string, std::string> &map,
                          const std::string &key, const std::string &val);
  static bool MapEntryStartsWith(const std::map<std::string, std::string> &map,
                                 const std::string &key,
                                 const std::string &val);

  template <typename T, typename U>
  static U MapGetValue(const std::map<T, U> &map, const T &key) {
    auto it = map.find(key);

    if (it == map.end()) {
      return {};
    }

    return it->second;
  }

  static std::string timestampToIso8016(const std::string &t);
  static std::string timestampToIso8016(const time_t &t);
  static std::string timestampToIso8016(const tm *t);

  static int makePath(char *path, mode_t mode);

  static void RmPath(std::filesystem::path path,
                     const std::filesystem::path &stop);
  static std::string GetXattr(const std::filesystem::path &path,
                              const std::string &key);

  static int SetXattr(const std::filesystem::path &path, const std::string &key,
                      const std::string &value, int flags);
  static bool IsDirEmpty(const std::filesystem::path &path);

  static int DirIterator(const std::filesystem::path &path,
                         const std::function<void(dirent *)> &f);

 private:
  std::bitset<256> mEncoder;
  std::bitset<256> mObjectEncoder;
  unsigned char mDecoder[256];
};

}  // namespace S3

#endif  // XROOTD_XRDS3UTILS_HH
