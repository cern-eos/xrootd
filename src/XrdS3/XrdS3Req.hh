//
// Created by segransm on 11/9/23.
//

#ifndef XROOTD_XRDS3REQ_HH
#define XROOTD_XRDS3REQ_HH

#include <functional>

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3ErrorResponse.hh"
#include "XrdS3Utils.hh"

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

  bool isValid() const { return valid; }

  void ParseReq();

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
 private:
  bool has_read{};
};

using HandlerFunc = std::function<int(XrdS3Req &)>;

}  // namespace S3

#endif  // XROOTD_XRDS3REQ_HH
