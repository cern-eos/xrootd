//------------------------------------------------------------------------------
// Pull third-party copy over one libparsocket HTTP/2 session.
//
// The handler claims COPY only when Source is spt:// or parsock://, or the
// request carries X-Xrd-Transport: parsock. A normal https COPY stays with
// XrdHttpTPC. Bytes are written at an absolute offset, so a dropped session
// retries the remainder and a later COPY resumes from the local file size
// when the .xrdtpc marker still matches the source.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------

#include "XrdHttpParsockTpc.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdHttp/XrdHttpReq.hh"
#include "XrdHttp/XrdHttpUtils.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdVersion.hh"

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <ctime>
#include <memory>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include <strings.h>
#include <sys/stat.h>

XrdVERSIONINFO(XrdHttpGetExtHandler, PsTpc);

namespace {

const char kPlainHdr[] =
    "Content-Type: text/plain\r\nContent-Encoding: identity";
const char kMarkerSuffix[] = ".xrdtpc";
const int kIdleMs = 120000;
const int kAttempts = 4;
const mode_t kCreateMode = static_cast<mode_t>(0644 | SFS_O_MKPTH);

int nextMonId()
{
  static std::atomic<int> id{1};
  return id.fetch_add(1);
}

std::string oneLine(std::string s)
{
  for (char &c : s) {
    if (c == '\n' || c == '\r')
      c = ' ';
  }
  return s;
}

bool parseI64(const std::string &s, int64_t &out)
{
  const std::string t = XrdPsTpc::trimCopy(s);
  if (t.empty())
    return false;
  char *end = nullptr;
  errno = 0;
  long long v = std::strtoll(t.c_str(), &end, 10);
  if (end == t.c_str() || errno || (end && *end) || v < 0)
    return false;
  out = static_cast<int64_t>(v);
  return true;
}

bool sameHost(const std::string &a, const std::string &b)
{
  return XrdPsTpc::asciiLower(a) == XrdPsTpc::asciiLower(b);
}

std::string describePeer(const XioFS::Url &u)
{
  std::string host = u.host;
  if (host.find(':') != std::string::npos)
    host = "[" + host + "]";
  return std::string(u.parsock ? "parsock:" : "tcp:") + host + ":" +
         std::to_string(u.port);
}

std::string perfMarker(int64_t bytes, const std::string &remote)
{
  std::ostringstream ss;
  ss << "Perf Marker\n"
     << "Timestamp: " << time(nullptr) << "\n"
     << "Stripe Index: 0\n"
     << "Stripe Bytes Transferred: " << bytes << "\n"
     << "Total Stripe Count: 1\n";
  if (!remote.empty())
    ss << "RemoteConnections: " << remote << "\n";
  ss << "End\n";
  return ss.str();
}

int replyFail(XrdHttpExtReq &req, bool chunked, int status, const std::string &why)
{
  const std::string body = "failure: " + oneLine(why) + "\n";
  if (!chunked) {
    return req.SendSimpleResp(status, nullptr, kPlainHdr, body.c_str(),
                              static_cast<long long>(body.size()));
  }
  if (req.ChunkResp(body.c_str(), 0))
    return -1;
  return req.ChunkResp(nullptr, 0);
}

std::string fileErr(XrdSfsFile &fh, const char *fallback)
{
  int code = 0;
  const char *msg = fh.error.getErrText(code);
  if (msg && *msg)
    return msg;
  if (code)
    return std::string(fallback) + ": " + strerror(code);
  return fallback;
}

int openWait(XrdSfsFile &fh, const std::string &path, const std::string &opaque,
             int mode, mode_t create_mode, const XrdSecEntity &sec)
{
  for (int attempt = 0; attempt < 8; ++attempt) {
    fh.error.setUCap(fh.error.getUCap() | XrdOucEI::uIPv64);
    const int rc = fh.open(path.c_str(), mode, create_mode, &sec,
                           opaque.empty() ? "" : opaque.c_str());
    if (rc != SFS_STALL && rc != SFS_STARTED)
      return rc;
    int secs = fh.error.getErrInfo();
    if (rc == SFS_STARTED)
      secs = secs / 2 + 5;
    if (secs < 1)
      secs = 1;
    if (secs > 60)
      secs = 60;
    std::this_thread::sleep_for(std::chrono::seconds(secs));
  }
  return SFS_STALL;
}

int writeAll(XrdSfsFile &fh, int64_t off, const char *data, size_t len,
             int64_t &wrote, std::string &err)
{
  wrote = 0;
  while (len > 0) {
    const size_t chunk = std::min(len, static_cast<size_t>(1 << 20));
    const auto n = fh.write(static_cast<XrdSfsFileOffset>(off), data,
                            static_cast<XrdSfsXferSize>(chunk));
    if (n < 0) {
      err = fileErr(fh, "local write failed");
      return -1;
    }
    if (n == 0) {
      err = "local write returned no bytes";
      return -1;
    }
    off += n;
    data += n;
    len -= static_cast<size_t>(n);
    wrote += n;
  }
  return 0;
}

void collectForwardHeaders(const std::map<std::string, std::string> &in,
                           const std::map<std::string, std::string> &digests,
                           bool drop_auth,
                           std::vector<std::pair<std::string, std::string>> &out)
{
  out.clear();
  for (const auto &kv : in) {
    if (kv.first.size() >= 14 &&
        strncasecmp(kv.first.c_str(), "transferheader", 14) == 0) {
      std::string name = kv.first.substr(14);
      if (name.empty())
        continue;
      if (drop_auth && strcasecmp(name.c_str(), "authorization") == 0)
        continue;
      out.emplace_back(std::move(name), kv.second);
    } else if (strncasecmp(kv.first.c_str(), "copy-header", 11) == 0) {
      const auto colon = kv.second.find(':');
      if (colon == std::string::npos)
        continue;
      std::string name = XrdPsTpc::trimCopy(kv.second.substr(0, colon));
      std::string value = XrdPsTpc::trimCopy(kv.second.substr(colon + 1));
      if (name.empty())
        continue;
      if (drop_auth && strcasecmp(name.c_str(), "authorization") == 0)
        continue;
      out.emplace_back(std::move(name), std::move(value));
    }
  }
  if (!digests.empty()) {
    std::string v;
    for (const auto &kv : digests) {
      if (!v.empty())
        v += ',';
      v += kv.first;
      v += "=5";
    }
    out.emplace_back("want-repr-digest", std::move(v));
  }
}

struct HeldFile {
  std::unique_ptr<XrdSfsFile> fh;
  bool live{false};

  void close()
  {
    if (live && fh) {
      fh->close();
      live = false;
    }
  }

  ~HeldFile() { close(); }
};

enum class MarkerRead { Missing, Ok, Error };

MarkerRead readMarker(XrdSfsFileSystem *sfs, char *user, const XrdSecEntity &sec,
                      const std::string &path, const std::string &opaque,
                      std::string &url, std::string &etag, int64_t &size,
                      std::string &err)
{
  std::unique_ptr<XrdSfsFile> fh(sfs->newFile(user, nextMonId()));
  if (!fh) {
    err = "failed to allocate resume marker";
    return MarkerRead::Error;
  }
  const int rc = openWait(*fh, path, opaque, SFS_O_RDONLY, 0644, sec);
  if (rc != SFS_OK) {
    int code = 0;
    fh->error.getErrText(code);
    if (code == ENOENT)
      return MarkerRead::Missing;
    err = fileErr(*fh, "failed to open resume marker");
    return MarkerRead::Error;
  }
  std::string text;
  char buf[4096];
  int64_t off = 0;
  while (text.size() < 65536) {
    const auto n = fh->read(static_cast<XrdSfsFileOffset>(off), buf,
                            static_cast<XrdSfsXferSize>(sizeof(buf)));
    if (n < 0) {
      err = fileErr(*fh, "failed to read resume marker");
      fh->close();
      return MarkerRead::Error;
    }
    if (n == 0)
      break;
    text.append(buf, buf + n);
    off += n;
  }
  fh->close();
  if (text.size() >= 65536) {
    err = "resume marker is too large";
    return MarkerRead::Error;
  }
  if (!XrdPsTpc::parseMarker(text, url, etag, size))
    return MarkerRead::Missing;
  return MarkerRead::Ok;
}

int writeMarker(XrdSfsFileSystem *sfs, char *user, const XrdSecEntity &sec,
                const std::string &path, const std::string &opaque,
                const std::string &url, const std::string &etag, int64_t size,
                std::string &err)
{
  std::unique_ptr<XrdSfsFile> fh(sfs->newFile(user, nextMonId()));
  if (!fh) {
    err = "failed to allocate resume marker";
    return -1;
  }
  const int rc = openWait(*fh, path, opaque,
                          SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY, kCreateMode,
                          sec);
  if (rc != SFS_OK) {
    err = fileErr(*fh, "failed to write resume marker");
    return -1;
  }
  const std::string body = XrdPsTpc::formatMarker(url, etag, size);
  int64_t wrote = 0;
  const int wrc = writeAll(*fh, 0, body.data(), body.size(), wrote, err);
  fh->close();
  if (wrc || wrote != static_cast<int64_t>(body.size())) {
    if (err.empty())
      err = "short write of resume marker";
    return -1;
  }
  return 0;
}

struct Sink {
  XrdSfsFile *fh{nullptr};
  XioFS::HttpResponse *resp{nullptr};
  std::atomic<int64_t> cursor{0};
  std::atomic<bool> stop{false};
  int64_t attempt_base{0};
  int64_t remote_size{0};
  bool fatal{false};
  bool saw_status{false};
  std::string err;

  int take(const uint8_t *data, size_t len)
  {
    if (stop.load() || fatal)
      return -1;
    if (!resp || !fh) {
      err = "transfer writer is not ready";
      fatal = true;
      return -1;
    }
    if (!saw_status) {
      saw_status = true;
      const int st = resp->status;
      if (st >= 500) {
        err = "source returned HTTP " + std::to_string(st);
        return -1;
      }
      if (st <= 0 || st >= 400 ||
          (attempt_base > 0 && st != 206) ||
          (attempt_base == 0 && st != 200 && st != 206)) {
        err = st > 0 ? "source returned HTTP " + std::to_string(st)
                     : "source response missing status";
        fatal = true;
        return -1;
      }
      int64_t start = 0;
      if (st == 206 &&
          XrdPsTpc::contentRangeStart(resp->header("content-range"), start) &&
          start != attempt_base) {
        err = "Content-Range does not match the resume offset";
        fatal = true;
        return -1;
      }
    }
    if (len == 0)
      return 0;
    const int64_t off = cursor.load();
    if (off > remote_size ||
        static_cast<int64_t>(len) > remote_size - off) {
      err = "source sent more bytes than Content-Length";
      fatal = true;
      return -1;
    }
    int64_t wrote = 0;
    if (writeAll(*fh, off, reinterpret_cast<const char *>(data), len, wrote, err)) {
      cursor.store(off + wrote);
      fatal = true;
      return -1;
    }
    cursor.store(off + wrote);
    return 0;
  }
};

class Handler : public XrdHttpExtHandler {
public:
  Handler(XrdSysError *log, XrdSfsFileSystem *sfs, std::string cafile,
          std::string cadir)
      : log_(log), sfs_(sfs), cafile_(std::move(cafile)), cadir_(std::move(cadir))
  {
  }

  bool MatchesPath(const char *, const char *) override { return false; }

  int Init(const char *) override { return sfs_ ? 0 : 1; }

  int ProcessReq(XrdHttpExtReq &req) override;

private:
  void note(const std::string &msg) const
  {
    if (log_)
      log_->Emsg("PsTpc", msg.c_str());
  }

  XrdSysError *log_;
  XrdSfsFileSystem *sfs_;
  std::string cafile_;
  std::string cadir_;
};

int Handler::ProcessReq(XrdHttpExtReq &req)
{
  std::atomic<bool> client_gone{false};
  bool chunked = false;
  auto failTo = [&](int status, const std::string &why) -> int {
    note(why);
    if (client_gone.load())
      return -1;
    return replyFail(req, chunked, status, why);
  };

  if (XrdPsTpc::asciiLower(req.verb) != "copy")
    return failTo(405, "parsock TPC only handles COPY");

  const std::string credential = XrdPsTpc::headerValue(req.headers, "credential");
  if (!credential.empty() && credential != "none")
    return failTo(400, "COPY requested an unsupported credential type");

  const std::string source =
      XrdPsTpc::unwrapHttpUrl(XrdPsTpc::headerValue(req.headers, "source"));
  if (source.empty())
    return failTo(400, "parsock TPC is pull-only and requires Source");

  std::string parse_err;
  XioFS::Url url;
  if (!XioFS::parseUrl(source, url, parse_err))
    return failTo(400, parse_err.empty() ? "invalid Source URL" : parse_err);

  XioFS::Http2Session::Options opt;
  opt.cacert = cafile_;
  opt.cadir = cadir_;
  opt.verify_peer = true;
  opt.timeout_ms = kIdleMs;
  opt.parsock = url.parsock;
  const std::string hint = XrdPsTpc::headerValue(req.headers, "x-xrd-parsock");
  if (!XrdPsTpc::applyTransport(url, opt,
                               XrdPsTpc::transportKind(XrdPsTpc::headerValue(
                                   req.headers, "x-xrd-transport")),
                               hint, parse_err))
    return failTo(400, parse_err);

  const bool overwrite =
      XrdPsTpc::headerValue(req.headers, "overwrite") == "T";
  const std::string authz = [&]() {
    const std::string auth = XrdPsTpc::headerValue(req.headers, "authorization");
    return auth.empty() ? std::string() : "authz=" + encode_str(auth);
  }();

  std::string full = XrdPsTpc::headerValue(req.headers, "xrd-http-fullresource");
  if (full.empty())
    full = req.resource;
  std::string local_path = full;
  std::string local_opaque;
  {
    const auto q = full.find('?');
    if (q == std::string::npos) {
      local_opaque.clear();
    } else {
      local_path = full.substr(0, q);
      local_opaque = full.substr(q + 1);
    }
  }
  const std::string sidecar = local_path + kMarkerSuffix;
  const std::string origin_host = url.host;

  Sink sink;
  bool started = false;
  std::string remote;
  auto last_marker = std::chrono::steady_clock::now();
  opt.on_wait = [&]() {
    if (req.PumpResponse() < 0) {
      client_gone.store(true);
      sink.stop.store(true);
      return;
    }
    if (!started)
      return;
    const auto now = std::chrono::steady_clock::now();
    if (now - last_marker < std::chrono::seconds(5))
      return;
    last_marker = now;
    const std::string body = perfMarker(sink.cursor.load(), remote);
    if (req.ChunkResp(body.c_str(), 0)) {
      client_gone.store(true);
      sink.stop.store(true);
    }
  };

  note("pull " + local_path + " from " + source);

  XioFS::Http2Session sess;
  std::string err;
  if (sess.connect(url, opt, err))
    return failTo(502, err.empty() ? "connect to source failed" : err);

  std::vector<std::pair<std::string, std::string>> head_headers;
  collectForwardHeaders(req.headers, req.mReprDigest, false, head_headers);

  XioFS::HttpRequest head;
  head.method = "HEAD";
  head.path = url.path.empty() ? "/" : url.path;
  head.headers = head_headers;
  XioFS::HttpResponse resp;
  if (sess.request(head, resp, err)) {
    if (client_gone.load())
      return -1;
    return failTo(502, err.empty() ? "HEAD to source failed" : err);
  }
  if (client_gone.load())
    return -1;
  if (resp.status != 200) {
    const int status = (resp.status >= 400 && resp.status <= 599) ? resp.status : 502;
    return failTo(status, "source HEAD returned HTTP " + std::to_string(resp.status));
  }

  int64_t remote_size = 0;
  if (!parseI64(resp.header("content-length"), remote_size))
    return failTo(502, "source HEAD did not return Content-Length");
  std::string etag = resp.header("etag");
  if (etag.empty())
    etag = resp.header("repr-digest");
  sink.remote_size = remote_size;
  remote = describePeer(sess.peer());

  if (!sfs_)
    return failTo(500, "no filesystem for parsock TPC");
  char *user = req.GetSecEntity().name;
  HeldFile data;
  data.fh.reset(sfs_->newFile(user, nextMonId()));
  if (!data.fh)
    return failTo(500, "failed to allocate a local file handle");

  std::string open_opaque = local_opaque;
  if (!open_opaque.empty())
    open_opaque += '&';
  open_opaque += "oss.asize=" + std::to_string(remote_size);
  if (!authz.empty())
    open_opaque += "&" + authz;

  const int open_rc = openWait(*data.fh, local_path, open_opaque,
                               SFS_O_CREAT | SFS_O_WRONLY, kCreateMode,
                               req.GetSecEntity());
  if (open_rc == SFS_REDIRECT) {
    int port = 0;
    const char *host = data.fh->error.getErrText(port);
    if (!host || !*host || port == 0)
      return failTo(500, "redirect without a host");
    std::string scheme = XrdPsTpc::headerValue(req.headers, "xrd-http-prot");
    if (scheme != "http" && scheme != "https")
      scheme = "https";
    std::string redir = XrdPsTpc::headerValue(req.headers, "xrd-http-fullresource");
    if (redir.empty())
      redir = req.resource;
    const std::string rdr_info = host;
    const auto qpos = rdr_info.find('?');
    const std::string rdr_host = rdr_info.substr(0, qpos);
    const std::string rdr_opaque =
        qpos == std::string::npos ? std::string() : rdr_info.substr(qpos + 1);
    std::ostringstream loc;
    loc << "Location: " << scheme << "://" << rdr_host << ":" << port;
    if (redir.empty() || redir.front() != '/')
      loc << '/';
    loc << redir;
    if (!rdr_opaque.empty())
      loc << (redir.find('?') == std::string::npos ? '?' : '&') << rdr_opaque;
    note("redirect " + loc.str());
    if (client_gone.load())
      return -1;
    return req.SendSimpleResp(307, nullptr, loc.str().c_str(), nullptr, 0);
  }
  if (open_rc == SFS_STALL || open_rc == SFS_STARTED)
    return failTo(503, "filesystem stalled too many times");
  if (open_rc != SFS_OK) {
    int code = 0;
    data.fh->error.getErrText(code);
    return failTo(mapErrNoToHttp(code), fileErr(*data.fh, "failed to open local file"));
  }
  data.live = true;

  struct stat st;
  memset(&st, 0, sizeof(st));
  if (data.fh->stat(&st) != SFS_OK)
    return failTo(500, fileErr(*data.fh, "failed to stat local file"));

  XrdPsTpc::ResumeInput resume_in;
  resume_in.overwrite = overwrite;
  resume_in.local_size = static_cast<int64_t>(st.st_size);
  resume_in.remote_etag = etag;
  resume_in.remote_size = remote_size;
  resume_in.source_url = source;
  if (!overwrite) {
    const MarkerRead mr = readMarker(sfs_, user, req.GetSecEntity(), sidecar, authz,
                                     resume_in.marker_url, resume_in.marker_etag,
                                     resume_in.marker_size, err);
    if (mr == MarkerRead::Error)
      return failTo(500, err.empty() ? "failed to read resume marker" : err);
    resume_in.have_marker = mr == MarkerRead::Ok;
  }
  const XrdPsTpc::ResumeDecision decision = XrdPsTpc::decideResume(resume_in);
  if (decision.action == XrdPsTpc::ResumeAction::Truncate) {
    if (data.fh->truncate(0) != SFS_OK)
      return failTo(500, fileErr(*data.fh, "failed to truncate local file"));
  }
  sink.cursor.store(decision.cursor);
  if (writeMarker(sfs_, user, req.GetSecEntity(), sidecar, authz, source, etag,
                  remote_size, err))
    return failTo(500, err.empty() ? "failed to write resume marker" : err);

  if (req.StartChunkedResp(201, nullptr, kPlainHdr))
    return -1;
  chunked = true;
  started = true;
  last_marker = std::chrono::steady_clock::now();
  {
    const std::string body = perfMarker(sink.cursor.load(), remote);
    if (req.ChunkResp(body.c_str(), 0))
      return -1;
  }

  bool transferred = decision.action == XrdPsTpc::ResumeAction::Complete;
  if (!transferred) {
    const bool drop_auth = !sameHost(origin_host, sess.peer().host);
    std::vector<std::pair<std::string, std::string>> get_headers;
    collectForwardHeaders(req.headers, req.mReprDigest, drop_auth, get_headers);
    sink.fh = data.fh.get();

    for (int attempt = 0; attempt < kAttempts; ++attempt) {
      if (client_gone.load() || sink.stop.load())
        return -1;
      if (sink.cursor.load() > remote_size)
        return failTo(500, "local file is larger than the source");
      if (sink.cursor.load() == remote_size) {
        transferred = true;
        break;
      }
      if (attempt > 0 || !sess.connected()) {
        const XioFS::Url hop = sess.peer();
        const XioFS::Http2Session::Options hop_opt = sess.options();
        sess.close();
        if (sess.connect(hop, hop_opt, err)) {
          note(std::string("reconnect failed: ") + err);
          continue;
        }
      }

      sink.attempt_base = sink.cursor.load();
      sink.saw_status = false;
      sink.resp = &resp;
      XioFS::HttpRequest get;
      get.method = "GET";
      get.path = sess.peer().path.empty() ? "/" : sess.peer().path;
      get.headers = get_headers;
      if (sink.attempt_base > 0) {
        get.headers.emplace_back("range",
                                 "bytes=" + std::to_string(sink.attempt_base) + "-");
      }
      get.on_chunk = [&sink](const uint8_t *p, size_t n) { return sink.take(p, n); };

      resp = XioFS::HttpResponse{};
      err.clear();
      const int rc = sess.request(get, resp, err);
      sink.resp = nullptr;
      if (client_gone.load() || sink.stop.load())
        return -1;

      const int64_t wrote = sink.cursor.load() - sink.attempt_base;
      const int64_t expect = remote_size - sink.attempt_base;
      if (!sink.fatal && rc == 0 && (resp.status == 200 || resp.status == 206) &&
          wrote == expect) {
        const std::string got = resp.header("etag");
        if (!etag.empty() && !got.empty() && got != etag)
          return failTo(409, "source ETag changed during the transfer");
        transferred = true;
        break;
      }
      if (sink.fatal || (rc == 0 && resp.status >= 400 && resp.status < 500)) {
        if (sink.err.empty())
          sink.err = err.empty() ? "source rejected the transfer" : err;
        return failTo(502, sink.err);
      }
      if (sink.err.empty())
        sink.err = err.empty() ? "source transfer failed" : err;
      note("retry " + oneLine(sink.err));
      sink.err.clear();
    }
  }

  if (!transferred)
    return failTo(502, err.empty() ? "source transfer failed" : err);

  if (data.fh->sync() != SFS_OK)
    return failTo(500, fileErr(*data.fh, "failed to sync local file"));
  if (data.fh->close() != SFS_OK) {
    data.live = false;
    return failTo(500, "failed to close local file");
  }
  data.live = false;

  XrdOucErrInfo einfo;
  if (sfs_->rem(sidecar.c_str(), einfo, &req.GetSecEntity(),
                authz.empty() ? "" : authz.c_str()) != SFS_OK) {
    const char *msg = einfo.getErrText();
    note(std::string("resume marker left in place: ") + (msg && *msg ? msg : sidecar));
  }

  if (client_gone.load())
    return -1;
  if (req.ChunkResp("success: Created\n", 0))
    return -1;
  note("pull complete " + local_path + " bytes " + std::to_string(remote_size));
  return req.ChunkResp(nullptr, 0);
}

} // namespace

extern "C" {

XrdHttpExtHandler *XrdHttpGetExtHandler(XrdSysError *log, const char *,
                                       const char *, XrdOucEnv *env)
{
  if (!log)
    return nullptr;
  if (!env) {
    log->Emsg("PsTpc", "missing environment");
    return nullptr;
  }
  void *raw = env->GetPtr("XrdSfsFileSystem*");
  if (!raw) {
    log->Emsg("PsTpc", "No filesystem object available");
    return nullptr;
  }
  const char *cafile = env->Get("http.cafile");
  const char *cadir = env->Get("http.cadir");
  return new Handler(log, static_cast<XrdSfsFileSystem *>(raw),
                     cafile ? cafile : "", cadir ? cadir : "");
}

bool XrdHttpExtClaim(XrdHttpExtHandler *, const XrdHttpReq *req)
{
  if (!req)
    return false;
  return XrdPsTpc::claimsParsockCopy(req->requestverb, req->allheaders);
}

} // extern "C"
