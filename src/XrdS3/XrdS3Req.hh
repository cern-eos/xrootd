//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
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
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#pragma once

//------------------------------------------------------------------------------
#include <functional>
//------------------------------------------------------------------------------
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3ErrorResponse.hh"
#include "XrdS3Utils.hh"
//------------------------------------------------------------------------------

namespace S3 {

using Headers = std::map<std::string, std::string>;

enum HttpMethod {
  Get,
  Head,
  Post,
  Put,
  Patch,
  Delete,
  Connect,
  Options,
  Trace
};

const std::map<HttpMethod, std::string> HttpMethodMap{
    {Get, "GET"},         {Head, "HEAD"},       {Post, "POST"},
    {Put, "PUT"},         {Patch, "PATCH"},     {Delete, "DELETE"},
    {Connect, "CONNECT"}, {Options, "OPTIONS"}, {Trace, "TRACE"},
};

struct Context {
  S3Utils utils;
  XrdSysError *log;
};

class XrdS3Req : protected XrdHttpExtReq {
 public:
  XrdS3Req(Context *ctx, XrdHttpExtReq &req);

  ~XrdS3Req() = default;

  [[nodiscard]] bool isValid() const { return valid; }

  int ParseReq();

  bool valid;
  std::string bucket;
  std::string object;
  Context *ctx;

  using Headers = std::map<std::string, std::string>;

  HttpMethod method;
  std::string uri_path;
  struct tm date {};
  std::string id;
  std::vector<unsigned char> md5;

  std::map<std::string, std::string> query;
  std::map<std::string, std::string> lowercase_headers;
  bool ParseDateHeader(const Headers &headers);
  bool ValidateAuth();
  static S3Error ValidatePath(const std::string &path);
  bool ParseMd5Header();
  bool ParseContentLengthHeader();

  int Ok();
  int S3Response(int code);
  int S3Response(int code, const std::map<std::string, std::string> &headers,
                 const std::string &body);
  int S3Response(int code, const std::map<std::string, std::string> &headers,
                 const char *body, long long size);

  int S3ErrorResponse(S3Error err);
  int S3ErrorResponse(S3Error err, const std::string &ressource,
                      const std::string &request_id, bool chunked);

  int ReadBody(int blen, char **data, bool wait);

  int StartChunkedOk();
  int StartChunkedResp(int code, const Headers &headers);
  using XrdHttpExtReq::BuffgetLine;
  using XrdHttpExtReq::ChunkResp;
  using XrdHttpExtReq::GetSecEntity;

 private:
  bool has_read{};
};

using HandlerFunc = std::function<int(XrdS3Req &)>;

}  // namespace S3

