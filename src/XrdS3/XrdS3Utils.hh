//
// Created by segransm on 11/3/23.
//

#ifndef XROOTD_XRDS3UTILS_HH
#define XROOTD_XRDS3UTILS_HH

#include <bitset>
#include <iomanip>
#include <map>
#include <numeric>
#include <sstream>
#include <string>
#include <filesystem>

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
  static std::string stringJoin(char delim, const T &...args) {
    std::string res;

    size_t size = 0;
    for (const auto &x : {args...}) {
      size += x.size();
    }

    res.reserve(size + sizeof...(args));

    for (const auto &x : {args...}) {
      res += x;
      res += delim;
    }
    res.pop_back();

    return res;
  }

  static std::string UriEncode(const std::bitset<256> &encoder,
                               const std::string &str);
  static bool HasHeader(const std::map<std::string, std::string> &header,
                        const std::string &key);
  static bool HeaderEq(const std::map<std::string, std::string> &header,
                       const std::string &key, const std::string &val);
  static bool HeaderStartsWith(const std::map<std::string, std::string> &header,
                               const std::string &key, const std::string &val);

  static std::string timestampToIso8016(const std::string &t);
  static std::string timestampToIso8016(const time_t &t);
  static std::string timestampToIso8016(const tm *t);
  static std::string timestampToIso8016(const std::filesystem::file_time_type &t);
 private:
  std::bitset<256> mEncoder;
  std::bitset<256> mObjectEncoder;
  unsigned char mDecoder[256]{};
};

}  // namespace S3

#endif  // XROOTD_XRDS3UTILS_HH
