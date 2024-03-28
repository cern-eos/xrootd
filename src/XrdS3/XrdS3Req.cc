//
// Created by segransm on 11/9/23.
//

#include "XrdS3Req.hh"

#include "XrdOfs/XrdOfs.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTUtils.hh"
#include "XrdS3.hh"
#include "XrdS3Auth.hh"
#include "XrdS3Xml.hh"

namespace S3 {

XrdS3Req::XrdS3Req(Context *ctx, XrdHttpExtReq &req)
    : XrdHttpExtReq(req), ctx(ctx) {
  valid = false;
  if (!ParseReq()) {
    return;
  }

  if (!ValidateAuth()) {
    return;
  }

  auto error = ValidatePath(object);
  if (error != S3Error::None) {
    S3ErrorResponse(error, "", "", false);
    return;
  }

  valid = true;
};

int XrdS3Req::ParseReq() {
  // Convert headers to lowercase
  for (const auto &hd : headers) {
    XrdOucString key(hd.first.c_str());
    key.lower(0, 0);
    lowercase_headers.insert({key.c_str(), hd.second});
  }

  std::string uri = headers["xrd-http-fullresource"];

  size_t pos = uri.find('?');
  if (pos == std::string::npos) {
    // XrdReq already decoded the uri
    uri_path = uri;
  } else {
    uri_path = uri.substr(0, pos);
    std::vector<std::string> tokens;
    XrdOucTUtils::splitString(tokens, uri.substr(pos + 1), "&");

    for (const auto &param : tokens) {
      pos = param.find('=');
      if (pos == std::string::npos) {
        query.insert({ctx->utils.UriDecode(param), ""});
      } else {
        query.insert({ctx->utils.UriDecode(param.substr(0, pos)),
                      ctx->utils.UriDecode(param.substr(pos + 1))});
      }
    }
  }

  // TODO: support virtual hosted request
  bucket = uri_path.substr(1);
  pos = bucket.find('/');
  if (pos == std::string::npos) {
    object = "";
  } else {
    object = bucket.substr(pos + 1);
    bucket.erase(pos);
  }

  if (verb == "GET") {
    method = Get;
  } else if (verb == "HEAD") {
    method = Head;
  } else if (verb == "POST") {
    method = Post;
  } else if (verb == "PUT") {
    method = Put;
  } else if (verb == "PATCH") {
    method = Patch;
  } else if (verb == "DELETE") {
    method = Delete;
  } else if (verb == "CONNECT") {
    method = Connect;
  } else if (verb == "OPTIONS") {
    method = Options;
  } else if (verb == "TRACE") {
    method = Trace;
  } else {
    return 0;
  }
  return 1;
}

bool XrdS3Req::ValidateAuth() {
  switch (S3Auth::GetRequestAuthType(*this)) {
    case AuthType::Signed:
    case AuthType::StreamingSigned:
    case AuthType::StreamingSignedTrailer: {
      // TODO: Make sure date is not too far away from current date
      if (!ParseDateHeader(lowercase_headers)) {
        S3ErrorResponse(S3Error::AccessDenied);
        return false;
      }
      if (!ParseMd5Header()) {
        S3ErrorResponse(S3Error::InvalidDigest);
        return false;
      }
      if (!ParseContentLengthHeader()) {
        S3ErrorResponse(S3Error::InvalidRequest);
        return false;
      }
      return true;
    }
    case AuthType::Unknown:
      S3ErrorResponse(S3Error::AccessDenied);
      return false;
    default:
      S3ErrorResponse(S3Error::AccessDenied);
      return false;
  }
}

bool XrdS3Req::ParseDateHeader(const Headers &headers) {
  auto it = headers.find("x-amz-date");
  if (it != headers.end()) {
    auto ret = strptime(it->second.c_str(), "%Y%m%dT%H%M%SZ", &date);
    if (ret != nullptr && *ret == '\0') {
      return true;
    }
  }

  it = headers.find("date");
  if (it == headers.end()) {
    return false;
  }
  // Try ISO8601
  auto ret = strptime(it->second.c_str(), "%Y%m%dT%H%M%SZ", &date);
  if (ret != nullptr && *ret == '\0') {
    return true;
  }
  // Try http date format
  ret = strptime(it->second.c_str(), "%a, %d %b %Y %H:%M:%S GMT", &date);
  if (ret != nullptr && *ret == '\0') {
    return true;
  }

  return false;
}

bool XrdS3Req::ParseMd5Header() {
  auto it = lowercase_headers.find("content-md5");
  if (it == lowercase_headers.end()) {
    return true;
  }
  if (it->second.empty()) {
    return false;
  }
  md5 = S3Crypt::Base64::decode(it->second);

  if (md5.size() != 16) {
    return false;
  }
  return true;
}

bool XrdS3Req::ParseContentLengthHeader() {
  auto it = lowercase_headers.find("content-length");
  if (it == lowercase_headers.end()) {
    return true;
  }
  if (it->second.empty()) {
    return false;
  }

  try {
    int i = std::stoi(it->second);
    if (i < 0) {
      return false;
    }
  } catch (std::exception &e) {
    return false;
  }

  return true;
}

S3Error XrdS3Req::ValidatePath(const std::string &path) {
  std::vector<std::string> components;

  if (path.empty()) {
    return S3Error::None;
  }

  if (path[path.size() - 1] == '/') {
    return S3Error::InvalidObjectName;
  }

  XrdOucTUtils::splitString(components, path, "/");
  if (!std::all_of(components.begin(), components.end(), [](const auto &c) {
        return (!c.empty() && c != "." && c != "..");
      })) {
    return S3Error::InvalidObjectName;
  }

  // Key names containing only whitespaces get skipped when parsing xml with
  // tinyxml2
  auto name = components.end() - 1;
  if (std::all_of(name->begin(), name->end(),
                  [](const auto &c) { return isspace(c); })) {
    return S3Error::InvalidObjectName;
  }

  return S3Error::None;
}

int XrdS3Req::S3ErrorResponse(S3Error err) {
  return S3ErrorResponse(err, "", "", false);
}

int XrdS3Req::S3ErrorResponse(S3Error err, const string &ressource,
                              const string &request_id, bool chunked) {
  S3Xml printer;
  S3ErrorCode error_code;

  auto e = S3ErrorMap.find(err);
  if (e == S3ErrorMap.end()) {
    // Error is not in S3ErrorMap, return an internal server error instead.
    error_code = {
        .code = "InternalError",
        .description = "Internal server error",
        .httpCode = 500,
    };
  } else {
    error_code = e->second;
  }

  printer.OpenElement("Error");
  printer.AddElement("Code", error_code.code);
  printer.AddElement("Message", error_code.description);
  printer.AddElement("Ressource", ressource);
  printer.AddElement("RequestId", request_id);
  printer.CloseElement();

  if (chunked) {
    return ChunkResp(printer.CStr(), printer.CStrSize() - 1);
  } else {
    return SendSimpleResp(error_code.httpCode, nullptr, nullptr, printer.CStr(),
                          printer.CStrSize() - 1);
  }
}

std::string MergeHeaders(const std::map<std::string, std::string> &headers) {
  std::stringstream ss;

  for (const auto &hd : headers) {
    ss << hd.first << ": " << hd.second << "\r\n";
  }
  std::string res = ss.str();

  return res.substr(0, res.empty() ? 0 : res.length() - 2);
}

int XrdS3Req::S3Response(int code, const map<std::string, std::string> &headers,
                         const string &body) {
  std::string headers_str = MergeHeaders(headers);

  return SendSimpleResp(code, nullptr, headers_str.c_str(), body.c_str(),
                        body.size());
}

int XrdS3Req::S3Response(int code, const map<std::string, std::string> &headers,
                         const char *body, long long size) {
  std::string headers_str = MergeHeaders(headers);

  return SendSimpleResp(code, nullptr, headers_str.c_str(), body, size);
}

int XrdS3Req::Ok() { return S3Response(200); }

int XrdS3Req::StartChunkedOk() {
  return XrdHttpExtReq::StartChunkedResp(200, nullptr, nullptr);
}

int XrdS3Req::StartChunkedResp(int code, const Headers &headers) {
  return XrdHttpExtReq::StartChunkedResp(200, nullptr,
                                         MergeHeaders(headers).c_str());
}

int XrdS3Req::S3Response(int code) {
  return SendSimpleResp(code, nullptr, nullptr, nullptr, 0);
}

// Send a 100-continue response if the client expects it on the first read.
int XrdS3Req::ReadBody(int blen, char **data, bool wait) {
  if (!has_read) {
    has_read = true;
    if (S3Utils::MapHasEntry(lowercase_headers, "expect", "100-continue")) {
      S3Response(100);
    }
  }

  return XrdHttpExtReq::BuffgetData(blen, data, wait);
}

}  // namespace S3
