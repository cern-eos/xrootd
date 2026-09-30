//------------------------------------------------------------------------------
// xiofscli — XIOFS HTTP/2 client talking to XrdHttp.
//
// Usage:
//   xiofscli [--cacert FILE] [--insecure] [--token TOK] URL COMMAND [args]
//
// Commands: stat | ls | cat | read OFFSET LENGTH | put LOCALFILE
//           | write OFFSET [LOCALFILE] | rm | mkdir | chmod MODE | ln DESTPATH
//           | chown UID GID | symlink TARGET | readlink | utime ATIME MTIME
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioClient.hh"
#include "XioBearer.hh"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/sysmacros.h>
#endif
#include <ctime>
#include <vector>

using XioFS::Client;
using XioFS::Http2Session;

static void usage(const char *argv0)
{
  std::cerr
      << "Usage: " << argv0
      << " [--cacert FILE] [--insecure] [--token TOK] URL COMMAND\n"
      << "\n"
      << "  URL is an https:// (ALPN h2) or http:// (h2c) XrdHttp path.\n"
      << "  Commands:\n"
      << "    stat\n"
      << "    ls\n"
      << "    cat\n"
      << "    read OFFSET LENGTH\n"
      << "    put LOCALFILE\n"
      << "    write OFFSET [LOCALFILE]\n"
      << "    rm\n"
      << "    mkdir\n"
      << "    chmod MODE\n"
      << "    ln DESTPATH\n"
      << "    chown UID GID\n"
      << "    symlink TARGET\n"
      << "    readlink\n"
      << "    utime ATIME MTIME\n"
      << "    mknod MODE [MAJOR MINOR]\n"
      << "    getxattr NAME\n"
      << "    setxattr NAME VALUE\n"
      << "    listxattr\n"
      << "    rmxattr NAME\n"
      << "    lock [SETLK|SETLKW|GETLK] [RDLCK|WRLCK] START LEN\n"
      << "    unlock\n"
      << "    flock [SH|EX|UN]\n";
}

static int fail(const std::string &msg, int rc)
{
  std::cerr << "xiofscli: " << msg << "\n";
  return rc;
}

int main(int argc, char **argv)
{
  Http2Session::Options opt;
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--cacert" && i + 1 < argc)
      opt.cacert = argv[++i];
    else if (a == "--insecure")
      opt.verify_peer = false;
    else if (a == "--token" && i + 1 < argc)
      opt.bearer = argv[++i];
    else if (a == "-h" || a == "--help") {
      usage(argv[0]);
      return 0;
    } else if (a.size() && a[0] == '-')
      return fail("unknown option " + a, 2);
    else
      args.push_back(a);
  }

  if (args.size() < 2) {
    usage(argv[0]);
    return 2;
  }

  if (opt.bearer.empty()) {
    std::string tok, berr;
    bool missing = false;
    if (XioFS::loadBearerToken(geteuid(), tok, berr, &missing))
      opt.bearer = tok;
    else if (!missing)
      return fail(berr, 1);
  }

  const std::string &url = args[0];
  const std::string &cmd = args[1];

  Client c;
  std::string err;
  int rc = c.open(url, opt, err);
  if (rc)
    return fail(err, 1);

  // The URL path is the object; commands operate on that path (rel = "/").
  const std::string rel = "/";

  if (cmd == "stat") {
    XioFS::Attr a;
    rc = c.getattr(rel, a, err);
    if (rc)
      return fail(err, 1);
    char mbuf[16];
    std::snprintf(mbuf, sizeof(mbuf), "%04o",
                  static_cast<unsigned>(a.mode & 07777));
    std::cout << (a.is_dir ? "dir" : (a.is_lnk ? "lnk"
                                               : (a.is_fifo ? "fifo"
                                                            : (a.is_chr ? "chr"
                                                                        : (a.is_blk ? "blk" : "file")))))
              << " size=" << a.size << " mtime=" << a.mtime
              << " atime=" << a.atime << " mode=" << mbuf
              << " uid=" << a.uid << " gid=" << a.gid << " ino=" << a.ino;
    if (!a.etag.empty())
      std::cout << " etag=" << a.etag;
    std::cout << "\n";
    return 0;
  }

  if (cmd == "ls") {
    std::vector<XioFS::DavEntry> ents;
    rc = c.readdir(rel, ents, err);
    if (rc)
      return fail(err, 1);
    for (const auto &e : ents) {
      std::cout << (e.is_dir ? "d " : (e.is_lnk ? "l " : (e.is_fifo ? "p "
                                                                   : (e.is_chr ? "c "
                                                                               : (e.is_blk ? "b " : "f ")))))
                << e.size
                << " " << e.name << "\n";
    }
    return 0;
  }

  if (cmd == "cat") {
    XioFS::Attr a;
    rc = c.getattr(rel, a, err);
    if (rc)
      return fail(err, 1);
    uint64_t off = 0;
    const uint64_t chunk = 1024 * 1024;
    while (off < static_cast<uint64_t>(a.size < 0 ? 0 : a.size) || a.size < 0) {
      std::string buf;
      uint64_t n = chunk;
      if (a.size >= 0 && off + n > static_cast<uint64_t>(a.size))
        n = static_cast<uint64_t>(a.size) - off;
      if (n == 0)
        break;
      rc = c.read(rel, off, n, buf, err);
      if (rc)
        return fail(err, 1);
      if (buf.empty())
        break;
      if (fwrite(buf.data(), 1, buf.size(), stdout) != buf.size())
        return fail("short write to stdout", 1);
      off += buf.size();
      if (a.size < 0)
        break;
    }
    return 0;
  }

  if (cmd == "read") {
    if (args.size() < 4)
      return fail("read OFFSET LENGTH", 2);
    uint64_t off = std::strtoull(args[2].c_str(), nullptr, 10);
    uint64_t len = std::strtoull(args[3].c_str(), nullptr, 10);
    std::string buf;
    rc = c.read(rel, off, len, buf, err);
    if (rc)
      return fail(err, 1);
    if (fwrite(buf.data(), 1, buf.size(), stdout) != buf.size())
      return fail("short write to stdout", 1);
    return 0;
  }

  if (cmd == "put") {
    if (args.size() < 3)
      return fail("put LOCALFILE", 2);
    std::ifstream in(args[2], std::ios::binary);
    if (!in)
      return fail("cannot open " + args[2], 1);
    std::ostringstream ss;
    ss << in.rdbuf();
    rc = c.put(rel, ss.str(), err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "write") {
    if (args.size() < 3)
      return fail("write OFFSET [LOCALFILE]", 2);
    uint64_t off = std::strtoull(args[2].c_str(), nullptr, 10);
    std::string body;
    if (args.size() >= 4) {
      std::ifstream in(args[3], std::ios::binary);
      if (!in)
        return fail("cannot open " + args[3], 1);
      std::ostringstream ss;
      ss << in.rdbuf();
      body = ss.str();
    } else {
      std::ostringstream ss;
      ss << std::cin.rdbuf();
      body = ss.str();
    }
    rc = c.write(rel, off, body, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "rm") {
    rc = c.unlink(rel, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "mkdir") {
    rc = c.mkdir(rel, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "chmod") {
    if (args.size() < 3)
      return fail("chmod MODE", 2);
    mode_t mode = static_cast<mode_t>(std::strtoul(args[2].c_str(), nullptr, 8));
    rc = c.chmod(rel, mode, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "ln") {
    if (args.size() < 3)
      return fail("ln DESTPATH", 2);
    rc = c.link(rel, args[2], err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "chown") {
    if (args.size() < 4)
      return fail("chown UID GID", 2);
    uid_t uid = static_cast<uid_t>(std::strtoul(args[2].c_str(), nullptr, 10));
    gid_t gid = static_cast<gid_t>(std::strtoul(args[3].c_str(), nullptr, 10));
    rc = c.chown(rel, uid, gid, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "symlink") {
    if (args.size() < 3)
      return fail("symlink TARGET", 2);
    rc = c.symlink(rel, args[2], err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "readlink") {
    std::string target;
    rc = c.readlink(rel, target, err);
    if (rc)
      return fail(err, 1);
    std::cout << target << "\n";
    return 0;
  }

  if (cmd == "utime") {
    if (args.size() < 4)
      return fail("utime ATIME MTIME", 2);
    struct timespec tv[2] = {};
    tv[0].tv_sec = static_cast<time_t>(std::strtoll(args[2].c_str(), nullptr, 10));
    tv[1].tv_sec = static_cast<time_t>(std::strtoll(args[3].c_str(), nullptr, 10));
    rc = c.utimens(rel, tv, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "mknod") {
    if (args.size() < 3)
      return fail("mknod MODE [MAJOR MINOR]", 2);
    mode_t mode = static_cast<mode_t>(std::strtoul(args[2].c_str(), nullptr, 8));
    dev_t rdev = 0;
    if (args.size() >= 5)
      rdev = makedev(static_cast<unsigned>(std::strtoul(args[3].c_str(), nullptr, 10)),
                     static_cast<unsigned>(std::strtoul(args[4].c_str(), nullptr, 10)));
    rc = c.mknod(rel, mode, rdev, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "getxattr") {
    if (args.size() < 3)
      return fail("getxattr NAME", 2);
    std::string value;
    rc = c.getxattr(rel, args[2], value, err);
    if (rc)
      return fail(err, 1);
    std::cout << value << "\n";
    return 0;
  }

  if (cmd == "setxattr") {
    if (args.size() < 4)
      return fail("setxattr NAME VALUE", 2);
    rc = c.setxattr(rel, args[2], args[3], err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "listxattr") {
    std::string names;
    rc = c.listxattr(rel, names, err);
    if (rc)
      return fail(err, 1);
    size_t i = 0;
    while (i < names.size()) {
      auto z = names.find('\0', i);
      if (z == std::string::npos)
        z = names.size();
      if (z > i)
        std::cout << names.substr(i, z - i) << "\n";
      i = z + 1;
    }
    return 0;
  }

  if (cmd == "rmxattr") {
    if (args.size() < 3)
      return fail("rmxattr NAME", 2);
    rc = c.removexattr(rel, args[2], err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "lock") {
    std::string lcmd = args.size() > 2 ? args[2] : "SETLK";
    std::string typ = args.size() > 3 ? args[3] : "WRLCK";
    long long start = args.size() > 4 ? std::strtoll(args[4].c_str(), nullptr, 10) : 0;
    long long len = args.size() > 5 ? std::strtoll(args[5].c_str(), nullptr, 10) : 0;
    rc = c.lock(rel, lcmd, typ, "SET", start, len, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "unlock") {
    rc = c.unlock(rel, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  if (cmd == "flock") {
    std::string op = args.size() > 2 ? args[2] : "EX";
    rc = c.flock(rel, op, err);
    if (rc)
      return fail(err, 1);
    return 0;
  }

  return fail("unknown command " + cmd, 2);
}
