//
// Created by segransm on 11/3/23.
//

#include "XrdS3Utils.hh"

#include <format>
#include <iomanip>
#include <sstream>

namespace S3 {

std::string S3Utils::UriEncode(const std::bitset<256> &encoder,
                               const std::string &str) {
  std::ostringstream res;

  for (const auto &c : str) {
    if (encoder[(unsigned char)c]) {
      res << c;
    } else {
      res << "%" << std::uppercase << std::hex << std::setw(2)
          << std::setfill('0') << (int)c;
    }
  }

  return res.str();
}

std::string S3Utils::UriDecode(const std::string &str) {
  size_t i = 0;
  std::string res;
  res.reserve(str.size());
  unsigned char v1, v2;

  while (i < str.size()) {
    if (str[i] == '%' && i + 2 < str.size()) {
      v1 = mDecoder[(unsigned char)str[i + 1]];
      v2 = mDecoder[(unsigned char)str[i + 2]];
      if ((v1 | v2) == 0xFF) {
        res += str[i++];
      } else {
        res += (char)((v1 << 4) | v2);
        i += 3;
      }
    } else
      res += str[i++];
  }

  return res;
}

S3Utils::S3Utils() {
  for (int i = 0; i < 256; ++i) {
    mEncoder[i] = isalnum(i) || i == '-' || i == '.' || i == '_' || i == '~';
    mObjectEncoder[i] = mEncoder[i];

    if (isxdigit(i)) {
      mDecoder[i] = (i < 'A') ? (i - '0') : ((i & ~0x20) - 'A' + 10);
    } else {
      mDecoder[i] = 0xFF;
    }
  }
  mObjectEncoder['/'] = true;
}

std::string S3Utils::UriEncode(const std::string &str) {
  return UriEncode(mEncoder, str);
}

std::string S3Utils::ObjectUriEncode(const std::string &str) {
  return UriEncode(mObjectEncoder, str);
}

void S3Utils::TrimAll(std::string &str) {
  // Trim leading non-letters
  size_t n = 0;
  while (n < str.size() && isspace(str[n])) {
    n++;
  }
  str.erase(0, n);

  if (str.empty()) {
    return;
  }

  // Trim trailing non-letters
  n = str.size() - 1;
  while (n > 0 && isspace(str[n])) {
    n--;
  }
  str.erase(n + 1);

  // Squash sequential spaces
  n = 0;
  while (n < str.size()) {
    size_t x = 0;
    while (n + x < str.size() && isspace(str[n + x])) {
      x++;
    }
    if (x > 1) {
      str.erase(n, x - 1);
    }
    n++;
  }
}

bool S3Utils::HasHeader(const std::map<std::string, std::string> &header,
                        const std::string &key) {
  return (header.find(key) != header.end());
}

bool S3Utils::HeaderEq(const std::map<std::string, std::string> &header,
                       const std::string &key, const std::string &val) {
  auto it = header.find(key);

  return (it != header.end() && it->second == val);
}

bool S3Utils::HeaderStartsWith(const std::map<std::string, std::string> &header,
                               const std::string &key, const std::string &val) {
  auto it = header.find(key);

  return (it != header.end() && it->second.substr(0, val.length()) == val);
}

std::string S3Utils::timestampToIso8016(const std::string &t) {
  try {
    return timestampToIso8016(std::stol(t));
  } catch (std::exception &e) {
    return "";
  }
}

std::string S3Utils::timestampToIso8016(
    const std::filesystem::file_time_type &t) {
  fprintf(stderr, "time since epoch: %ld\n", std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count());
  return timestampToIso8016(std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch())
          .count());
}
std::string S3Utils::timestampToIso8016(const time_t &t) {
  struct tm *date = gmtime(&t);
  return timestampToIso8016(date);
}

std::string S3Utils::timestampToIso8016(const struct tm *t) {
  char date_iso8601[17]{};
  if (!t || strftime(date_iso8601, 17, "%Y%m%dT%H%M%SZ", t) != 16) {
    return "";
  }
  return date_iso8601;
}

}  // namespace S3
