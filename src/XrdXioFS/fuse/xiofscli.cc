//------------------------------------------------------------------------------
// xiofscli — XIOFS HTTP/2 client talking to XrdHttp.
//
// Usage:
//   xiofscli [--cacert FILE] [--insecure] [--token TOK] URL COMMAND [args]
//
// Commands: stat | ls | cat | read OFFSET LENGTH | put LOCALFILE
//           | write OFFSET [LOCALFILE] | rm | mkdir | chmod MODE | ln DESTPATH
//           | chown UID GID | symlink TARGET | readlink | utime ATIME MTIME
//           | test [PREFIX]
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioClient.hh"
#include "XioBearer.hh"

#include <cerrno>
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
      << "    flock [SH|EX|UN]\n"
      << "    test [PREFIX]\n"
      << "\n"
      << "  test creates objects under PREFIX (default /xiofscli-test),\n"
      << "  checks each operation, then removes them. PREFIX is relative\n"
      << "  to URL.\n";
}

static std::string childPath(const std::string &dir, const std::string &name)
{
  std::string d = dir.empty() ? "/" : dir;
  while (d.size() > 1 && d.back() == '/')
    d.pop_back();
  if (d.empty() || d == "/")
    return "/" + name;
  return d + "/" + name;
}

static bool dirHas(const std::vector<XioFS::DavEntry> &ents, const std::string &name)
{
  for (const auto &e : ents)
    if (e.name == name)
      return true;
  return false;
}

static int runTest(Client &c, std::string prefix)
{
  if (prefix.empty())
    prefix = "/xiofscli-test";
  if (prefix.front() != '/')
    prefix.insert(prefix.begin(), '/');
  while (prefix.size() > 1 && prefix.back() == '/')
    prefix.pop_back();

  const std::string file = childPath(prefix, "file.bin");
  const std::string renamed = childPath(prefix, "file2.bin");
  const std::string hard = childPath(prefix, "hard.bin");
  const std::string link = childPath(prefix, "link");
  const std::string sub = childPath(prefix, "sub");
  const std::string fifo = childPath(prefix, "fifo");
  const std::string payload = "hello-xiofs\n";
  const std::string patched = "hello-XIOFS\n";
  int failed = 0;

  auto check = [&](const char *step, int rc, const std::string &err) -> bool {
    if (rc) {
      std::cerr << "xiofscli test: FAIL " << step << ": " << err << "\n";
      failed++;
      return false;
    }
    std::cout << "ok " << step << "\n";
    return true;
  };

  const bool exportRoot = (prefix == "/");
  auto cleanup = [&]() {
    std::string e;
    c.unlock(file, e);
    c.unlock(renamed, e);
    c.flock(file, "UN", e);
    c.flock(renamed, "UN", e);
    c.unlink(file, e);
    c.unlink(renamed, e);
    c.unlink(hard, e);
    c.unlink(link, e);
    c.unlink(fifo, e);
    c.unlink(sub, e);
    // "/" is the export itself. Deleting it rmdirs localroot.
    if (!exportRoot)
      c.unlink(prefix, e);
  };

  std::cout << "xiofscli test: prefix " << prefix << "\n";
  cleanup();

  std::string err;
  int rc = c.mkdir(prefix, err);
  if (rc == -EEXIST)
    rc = 0;
  if (!check("mkdir", rc, err)) {
    cleanup();
    return 1;
  }

  if (!check("put", c.put(file, payload, err), err)) {
    cleanup();
    return 1;
  }

  XioFS::Attr a;
  if (!check("stat", c.getattr(file, a, err), err)) {
    cleanup();
    return 1;
  }
  if (!a.is_dir && !a.is_lnk && a.size == static_cast<int64_t>(payload.size()))
    std::cout << "ok stat size\n";
  else {
    std::cerr << "xiofscli test: FAIL stat size: got " << a.size
              << " want " << payload.size() << "\n";
    failed++;
  }

  std::string buf;
  if (check("read", c.read(file, 0, payload.size(), buf, err), err)) {
    if (buf == payload)
      std::cout << "ok read bytes\n";
    else {
      std::cerr << "xiofscli test: FAIL read bytes\n";
      failed++;
    }
  }

  buf.clear();
  if (check("read range", c.read(file, 6, 5, buf, err), err)) {
    if (buf == "xiofs")
      std::cout << "ok read range bytes\n";
    else {
      std::cerr << "xiofscli test: FAIL read range bytes\n";
      failed++;
    }
  }

  if (check("write", c.write(file, 6, "XIOFS", err), err)) {
    buf.clear();
    if (check("read after write", c.read(file, 0, patched.size(), buf, err), err)) {
      if (buf == patched)
        std::cout << "ok write bytes\n";
      else {
        std::cerr << "xiofscli test: FAIL write bytes\n";
        failed++;
      }
    }
  }

  if (check("chmod", c.chmod(file, 0640, err), err)) {
    if (check("stat after chmod", c.getattr(file, a, err), err) &&
        (a.mode & 07777) != 0640) {
      std::cerr << "xiofscli test: FAIL chmod mode: got "
                << std::oct << (a.mode & 07777) << std::dec << "\n";
      failed++;
    } else if ((a.mode & 07777) == 0640) {
      std::cout << "ok chmod mode\n";
    }
  }

  if (check("stat before utime", c.getattr(file, a, err), err)) {
    struct timespec tv[2] = {};
    tv[0].tv_sec = 1700000000;
    tv[1].tv_sec = 1700000001;
    if (check("utime", c.utimens(file, tv, err), err)) {
      XioFS::Attr b;
      if (check("stat after utime", c.getattr(file, b, err), err)) {
        if (b.mtime == tv[1].tv_sec)
          std::cout << "ok utime mtime\n";
        else {
          std::cerr << "xiofscli test: FAIL utime mtime: got " << b.mtime << "\n";
          failed++;
        }
      }
    }
  }

  if (a.uid != static_cast<uid_t>(-1) && a.gid != static_cast<gid_t>(-1)) {
    if (check("chown", c.chown(file, a.uid, a.gid, err), err)) {
      XioFS::Attr b;
      if (check("stat after chown", c.getattr(file, b, err), err)) {
        if (b.uid == a.uid && b.gid == a.gid)
          std::cout << "ok chown ids\n";
        else {
          std::cerr << "xiofscli test: FAIL chown ids\n";
          failed++;
        }
      }
    }
  } else {
    std::cout << "skip chown (server did not return uid/gid)\n";
  }

  if (check("setxattr", c.setxattr(file, "user.xiofs", "testval", err), err)) {
    std::string value;
    if (check("getxattr", c.getxattr(file, "user.xiofs", value, err), err)) {
      if (value == "testval")
        std::cout << "ok getxattr value\n";
      else {
        std::cerr << "xiofscli test: FAIL getxattr value: [" << value << "]\n";
        failed++;
      }
    }
    std::string names;
    if (check("listxattr", c.listxattr(file, names, err), err)) {
      if (names.find("user.xiofs") != std::string::npos)
        std::cout << "ok listxattr\n";
      else {
        std::cerr << "xiofscli test: FAIL listxattr missing user.xiofs\n";
        failed++;
      }
    }
    check("rmxattr", c.removexattr(file, "user.xiofs", err), err);
  }

  check("mkdir sub", c.mkdir(sub, err), err);

  std::vector<XioFS::DavEntry> ents;
  if (check("ls", c.readdir(prefix, ents, err), err)) {
    if (dirHas(ents, "file.bin") && dirHas(ents, "sub"))
      std::cout << "ok ls entries\n";
    else {
      std::cerr << "xiofscli test: FAIL ls entries";
      if (ents.empty())
        std::cerr << " (no names)"
                  << (err.empty() ? "" : (": " + err));
      else {
        std::cerr << ":";
        for (const auto &e : ents)
          std::cerr << " [" << (e.name.empty() ? e.href : e.name) << "]";
      }
      std::cerr << "\n";
      failed++;
    }
  }

  if (check("symlink", c.symlink(link, "file.bin", err), err)) {
    std::string target;
    if (check("readlink", c.readlink(link, target, err), err)) {
      if (target == "file.bin")
        std::cout << "ok readlink target\n";
      else {
        std::cerr << "xiofscli test: FAIL readlink target: " << target << "\n";
        failed++;
      }
    }
  }

  if (check("ln", c.link(file, hard, err), err)) {
    buf.clear();
    if (check("read hardlink", c.read(hard, 0, patched.size(), buf, err), err)) {
      if (buf == patched)
        std::cout << "ok hardlink bytes\n";
      else {
        std::cerr << "xiofscli test: FAIL hardlink bytes\n";
        failed++;
      }
    }
  }

  check("mknod", c.mknod(fifo, static_cast<mode_t>(S_IFIFO | 0644), 0, err), err);

  if (check("lock", c.lock(file, "SETLK", "WRLCK", "SET", 0, 0, err), err))
    check("unlock", c.unlock(file, err), err);
  if (check("flock", c.flock(file, "EX", err), err))
    check("flock unlock", c.flock(file, "UN", err), err);

  if (check("rename", c.rename(file, renamed, err), err)) {
    XioFS::Attr gone;
    int grc = c.getattr(file, gone, err);
    if (grc)
      std::cout << "ok rename removed source\n";
    else {
      std::cerr << "xiofscli test: FAIL rename source still exists\n";
      failed++;
    }
    if (check("stat renamed", c.getattr(renamed, a, err), err)) {
      if (a.size == static_cast<int64_t>(patched.size()))
        std::cout << "ok rename size\n";
      else {
        std::cerr << "xiofscli test: FAIL rename size\n";
        failed++;
      }
    }
  }

  cleanup();
  if (exportRoot) {
    std::cout << "ok cleanup (left /)\n";
  } else {
    XioFS::Attr left;
    rc = c.getattr(prefix, left, err);
    if (rc)
      std::cout << "ok cleanup\n";
    else {
      std::cerr << "xiofscli test: FAIL cleanup: prefix still exists\n";
      failed++;
    }
  }

  if (failed) {
    std::cerr << "xiofscli test: " << failed << " check(s) failed\n";
    return 1;
  }
  std::cout << "xiofscli test: passed\n";
  return 0;
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

  if (cmd == "test") {
    std::string prefix = args.size() > 2 ? args[2] : "/xiofscli-test";
    return runTest(c, prefix);
  }

  return fail("unknown command " + cmd, 2);
}
