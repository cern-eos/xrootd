//------------------------------------------------------------------------------
// This file is part of XrdHTTP: A pragmatic implementation of the
// HTTP/WebDAV protocol for the Xrootd framework
//
// Copyright (c) 2025 by European Organization for Nuclear Research (CERN)
// Author: Cedric Caffy <ccaffy@cern.ch>
// File Date: Jun 2025
//------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------
#include "XrdHttpHeaderUtils.hh"
#include "XrdOuc/XrdOucTUtils.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdHttpUtils.hh"

#include <vector>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <string_view>


void XrdHttpHeaderUtils::parseReprDigest(const std::string &value, std::map<std::string, std::string> &output) {
  // Expected format per entry: <cksumType>=:<digestValue>:
  std::vector<std::string> digestNameValuePairs;
  XrdOucTUtils::splitString(digestNameValuePairs, value, ",");

  for (const auto &digestNameValue : digestNameValuePairs) {
    std::string_view digestNameValueSV {digestNameValue};
    auto equalPos = digestNameValueSV.find('=');
    if (equalPos == std::string_view::npos || equalPos >= digestNameValueSV.size() - 1)
      continue;

    std::string_view cksumTypeSV = digestNameValueSV.substr(0, equalPos);
    XrdOucUtils::trim(cksumTypeSV);
    if (cksumTypeSV.empty())
      continue;

    std::string_view cksumValueInSV = digestNameValueSV.substr(equalPos + 1);
    size_t beginCksumPos = cksumValueInSV.find(':');
    size_t endCksumPos = cksumValueInSV.rfind(':');

    // Check that the string starts with ':' and contains two distinct colons
    if (beginCksumPos == 0 && endCksumPos > beginCksumPos + 1 && endCksumPos < cksumValueInSV.size()) {
      std::string_view cksumValue = cksumValueInSV.substr(beginCksumPos + 1, endCksumPos - beginCksumPos - 1);
      XrdOucUtils::trim(cksumValue);
      if (!cksumValue.empty()) {
        //What we get as checksum value is a base64-encoded hexadecimal bytes
        //Let's decode that.
        std::string chksumDecoded;
        base64DecodeHex(std::string(cksumValue), chksumDecoded);
        std::string cksumTypeLC {cksumTypeSV};
        std::transform(cksumTypeLC.begin(), cksumTypeLC.end(), cksumTypeLC.begin(), ::tolower);
        output[cksumTypeLC] = chksumDecoded;
      }
    }
    // Malformed entries are silently ignored
  }
}

void XrdHttpHeaderUtils::parseWantReprDigest(const std::string & value, std::map<std::string, uint8_t> &output) {
  size_t pos = 0;
  std::string_view value_sv {value};
  while(pos <= value_sv.size()) {
    // find comma
    size_t comma = value.find(',',pos);
    // extract item, no comma means the item is the full string
    std::string_view item = (comma == std::string_view::npos) ? value_sv.substr(pos) : value_sv.substr(pos, comma - pos);
    // move current cursor to 'comma + 1' or after the string end
    pos = (comma == std::string_view::npos) ? value.size() + 1 : comma + 1;
    // trim the item
    XrdOucUtils::trim(item);
    if(item.empty()) continue;

    size_t eq = item.find('=');
    // If no '=' sign, we discard this entry as it is malformed
    if(eq == std::string_view::npos) continue;
    // We found the equal sign on the item
    std::string_view digestName {item.substr(0, eq)};
    XrdOucUtils::trim(digestName);
    std::string_view preference {item.substr(eq+1)};
    XrdOucUtils::trim(preference);

    std::string key_lower {digestName};
    std::transform(key_lower.begin(),key_lower.end(),key_lower.begin(),::tolower);

    try {
      uint8_t preference_us = XrdOucUtils::touint8_t(preference);
      // Max allowed value for Repr-Digest is 10
      preference_us = std::min(preference_us,(uint8_t)10);
      output[key_lower] = preference_us;
    } catch (...) {
      // discard invalid values
    }
  }
}

ssize_t XrdHttpHeaderUtils::parseContentLength(const std::string & value) {
  std::string_view sv{value};
  // parseLine forwards the trailing CRLF (and any preceding spaces/tabs).
  while (!sv.empty() && (sv.back() == '\r' || sv.back() == '\n' ||
                         sv.back() == ' '  || sv.back() == '\t')) {
    sv.remove_suffix(1);
  }
  if (sv.empty()) {
    return -1;
  }
  // RFC 9112 §6.2: 1*DIGIT only — no sign, no embedded whitespace.
  for (char c : sv) {
    if (c < '0' || c > '9') {
      return -2;
    }
  }
  std::string nullTerm{sv};
  errno = 0;
  long long parsed = std::strtoll(nullTerm.c_str(), nullptr, 10);
  if (errno == ERANGE ||
      parsed > static_cast<long long>(std::numeric_limits<ssize_t>::max())) {
    return -3;
  }
  return static_cast<ssize_t>(parsed);
}

int XrdHttpHeaderUtils::parseTransferEncoding(const std::string & value) {
  std::vector<std::string> tokens;
  XrdOucTUtils::splitString(tokens, value, ",");

  bool chunked_seen = false;
  bool last_was_chunked = false;
  bool any_token = false;

  for (auto & tok : tokens) {
    std::string_view sv{tok};
    XrdOucUtils::trim(sv);
    if (sv.empty()) continue;

    // RFC 9112 §6.1: transfer-coding names are case-insensitive. Whole-token
    // match (not strstr) so "chunkedX" or "x-chunked" no longer falsely hit.
    std::string tlow{sv};
    std::transform(tlow.begin(), tlow.end(), tlow.begin(), ::tolower);
    any_token = true;
    if (tlow == "chunked") {
      chunked_seen = true;
      last_was_chunked = true;
    } else {
      last_was_chunked = false;
    }
  }

  if (!any_token)        return -1;
  if (!chunked_seen)     return -2;
  if (!last_was_chunked) return -3;
  return 0;
}

int XrdHttpHeaderUtils::parseContentRangeWrite(const std::string & value,
                                               long long & first,
                                               long long & last,
                                               long long & complete) {
  first = last = complete = -1;
  std::string_view sv{value};
  while (!sv.empty() && (sv.back() == '\r' || sv.back() == '\n' ||
                         sv.back() == ' '  || sv.back() == '\t')) {
    sv.remove_suffix(1);
  }
  while (!sv.empty() && (sv.front() == ' ' || sv.front() == '\t')) {
    sv.remove_prefix(1);
  }
  if (sv.size() < 6) {
    return -1;
  }
  const char *unit = "bytes";
  for (int i = 0; i < 5; ++i) {
    if (std::tolower(static_cast<unsigned char>(sv[i])) != unit[i]) {
      return -1;
    }
  }
  sv.remove_prefix(5);
  if (sv.empty() || (sv.front() != ' ' && sv.front() != '\t')) {
    return -1;
  }
  while (!sv.empty() && (sv.front() == ' ' || sv.front() == '\t')) {
    sv.remove_prefix(1);
  }

  const auto parse_nn = [](std::string_view s, long long &out) -> int {
    if (s.empty()) {
      return -1;
    }
    for (char c : s) {
      if (c < '0' || c > '9') {
        return -1;
      }
    }
    std::string nullTerm{s};
    errno = 0;
    long long parsed = std::strtoll(nullTerm.c_str(), nullptr, 10);
    if (errno == ERANGE || parsed < 0) {
      return -1;
    }
    out = parsed;
    return 0;
  };

  const size_t dash = sv.find('-');
  const size_t slash = sv.find('/');
  if (dash == std::string_view::npos || slash == std::string_view::npos ||
      slash < dash + 1) {
    return -1;
  }
  if (parse_nn(sv.substr(0, dash), first) < 0) {
    return -1;
  }
  if (parse_nn(sv.substr(dash + 1, slash - dash - 1), last) < 0) {
    return -1;
  }
  std::string_view cl = sv.substr(slash + 1);
  if (cl == "*") {
    complete = -1;
  } else if (parse_nn(cl, complete) < 0) {
    return -1;
  }
  if (first < 0 || last < first) {
    return -2;
  }
  if (complete >= 0 && last >= complete) {
    return -3;
  }
  return 0;
}

int XrdHttpHeaderUtils::parseIfMatch(const std::string & value,
                                     std::vector<std::string> & tags,
                                     bool & star)
{
  tags.clear();
  star = false;
  std::string_view sv{value};
  while (!sv.empty() && (sv.back() == '\r' || sv.back() == '\n' ||
                         sv.back() == ' ' || sv.back() == '\t'))
    sv.remove_suffix(1);
  while (!sv.empty() && (sv.front() == ' ' || sv.front() == '\t'))
    sv.remove_prefix(1);
  if (sv.empty())
    return -1;
  if (sv == "*") {
    star = true;
    return 0;
  }

  size_t start = 0;
  while (start <= sv.size()) {
    size_t comma = sv.find(',', start);
    std::string_view tok = (comma == std::string_view::npos)
                               ? sv.substr(start)
                               : sv.substr(start, comma - start);
    while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t'))
      tok.remove_prefix(1);
    while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t'))
      tok.remove_suffix(1);
    if (!tok.empty()) {
      if (tok == "*") {
        star = true;
      } else {
        if (tok.size() >= 2 && (tok[0] == 'W' || tok[0] == 'w') && tok[1] == '/') {
          tok.remove_prefix(2);
          while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t'))
            tok.remove_prefix(1);
        }
        if (tok.size() >= 2 && tok.front() == '"' && tok.back() == '"')
          tok = tok.substr(1, tok.size() - 2);
        else if (!tok.empty() && tok.front() == '"')
          return -1;
        tags.emplace_back(tok);
      }
    }
    if (comma == std::string_view::npos)
      break;
    start = comma + 1;
  }
  if (!star && tags.empty())
    return -1;
  return 0;
}
