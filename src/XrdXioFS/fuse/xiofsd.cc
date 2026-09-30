//------------------------------------------------------------------------------
// xiofsd — FUSE mount of an XrdHttp HTTP/2 export.
//
//   xiofsd [--cacert FILE] [--insecure] [--token TOK] URL MOUNTPOINT [fuse-opts]
//
// Reads are Range GETs. Writes are PATCH with Content-Range; create/truncate
// to empty use PUT. mkdir/unlink/rename map to MKCOL/DELETE/MOVE.
// chmod/chown/utimens are PROPPATCH. symlink is SYMLINK (LINK fallback);
// readlink is READLINK (GET + Xrd-Readlink: 1 fallback).
// mknod of fifo/device is MKNOD (PUT + Xrd-Mknod fallback). xattrs are GET
// Xrd-Xattr / PROPPATCH. POSIX locks are LOCK/UNLOCK.
// Writes send If-Match from the ETag captured at open.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifdef __APPLE__
#ifndef _DARWIN_USE_64_BIT_INODE
#define _DARWIN_USE_64_BIT_INODE 1
#endif
#endif
#define FUSE_USE_VERSION 26

#include "XioClient.hh"

#include <fuse.h>

#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef __linux__
#include <sys/file.h>
#include <sys/sysmacros.h>
#include <sys/xattr.h>
#endif
#include <time.h>
#include <unistd.h>
#include <vector>

namespace {

XioFS::Client g_client;

struct FileState {
  std::string etag;
};

FileState *fileState(struct fuse_file_info *fi)
{
  if (!fi || !fi->fh)
    return nullptr;
  return reinterpret_cast<FileState *>(fi->fh);
}

void fillStat(const XioFS::Attr &a, struct stat *st)
{
  memset(st, 0, sizeof(*st));
  st->st_ino = a.ino ? static_cast<ino_t>(a.ino) : 1;
  st->st_nlink = a.is_dir ? 2 : 1;
  mode_t type = S_IFREG;
  if (a.is_dir)
    type = S_IFDIR;
  else if (a.is_lnk)
    type = S_IFLNK;
  else if (a.is_fifo)
    type = S_IFIFO;
  else if (a.is_chr)
    type = S_IFCHR;
  else if (a.is_blk)
    type = S_IFBLK;
  const mode_t perm = a.mode ? (a.mode & 07777) : (a.is_dir ? 0755 : 0644);
  st->st_mode = type | perm;
  st->st_rdev = a.rdev;
  st->st_size = a.size < 0 ? 0 : a.size;
  st->st_mtime = a.mtime;
  st->st_atime = a.atime ? a.atime : a.mtime;
  st->st_ctime = a.mtime;
  st->st_uid = a.uid != static_cast<uid_t>(-1) ? a.uid : getuid();
  st->st_gid = a.gid != static_cast<gid_t>(-1) ? a.gid : getgid();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + 511) / 512;
}

int posixAccess(const XioFS::Attr &a, int mask)
{
  if (mask == F_OK)
    return 0;
  if (geteuid() == 0)
    return 0;
  const mode_t mode = a.mode ? (a.mode & 07777) : (a.is_dir ? 0755 : 0644);
  int shift = 0;
  if (a.uid != static_cast<uid_t>(-1) && geteuid() == a.uid)
    shift = 6;
  else if (a.gid != static_cast<gid_t>(-1) && getegid() == a.gid)
    shift = 3;
  if ((mask & R_OK) && !((mode >> shift) & 4))
    return -EACCES;
  if ((mask & W_OK) && !((mode >> shift) & 2))
    return -EACCES;
  if ((mask & X_OK) && !((mode >> shift) & 1))
    return -EACCES;
  return 0;
}

int putEmpty(const char *path, const std::string &if_match = {},
             const std::string &if_none_match = {})
{
  std::string err;
  int rc = g_client.put(path, {}, err, if_match, if_none_match);
  if (rc == -ESTALE && !if_none_match.empty())
    return -EEXIST;
  return rc;
}

std::string currentEtag(const char *path)
{
  XioFS::Attr a;
  std::string err;
  if (g_client.getattr(path, a, err))
    return {};
  return a.etag;
}

int xiofs_getattr(const char *path, struct stat *st)
{
  XioFS::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  fillStat(a, st);
  return 0;
}

int xiofs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                off_t, struct fuse_file_info *)
{
  filler(buf, ".", nullptr, 0);
  filler(buf, "..", nullptr, 0);
  std::vector<XioFS::DavEntry> ents;
  std::string err;
  int rc = g_client.readdir(path, ents, err);
  if (rc)
    return rc;
  for (const auto &e : ents) {
    if (e.name.empty() || e.name == "." || e.name == "..")
      continue;
    struct stat st {};
    XioFS::Attr a;
    a.size = e.size;
    a.mtime = e.mtime;
    a.atime = e.atime;
    a.mode = e.mode;
    a.uid = e.uid;
    a.gid = e.gid;
    a.is_dir = e.is_dir;
    a.is_lnk = e.is_lnk;
    a.is_fifo = e.is_fifo;
    a.is_chr = e.is_chr;
    a.is_blk = e.is_blk;
    a.rdev = e.rdev;
    fillStat(a, &st);
    if (filler(buf, e.name.c_str(), &st, 0) != 0)
      break;
  }
  return 0;
}

int xiofs_open(const char *path, struct fuse_file_info *fi)
{
  XioFS::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  if (a.is_dir)
    return -EISDIR;
  if ((fi->flags & O_TRUNC) && (fi->flags & O_ACCMODE) != O_RDONLY) {
    rc = putEmpty(path, a.etag);
    if (rc)
      return rc;
    a.etag = currentEtag(path);
  }
  auto *st = new FileState;
  st->etag = a.etag;
  fi->fh = reinterpret_cast<uint64_t>(st);
  return 0;
}

int xiofs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  std::string none;
  if (fi && (fi->flags & O_EXCL))
    none = "*";
  int rc = putEmpty(path, {}, none);
  if (rc)
    return rc;
  if (mode & 07777) {
    std::string err;
    rc = g_client.chmod(path, mode, err);
    if (rc)
      return rc;
  }
  auto *st = new FileState;
  st->etag = currentEtag(path);
  fi->fh = reinterpret_cast<uint64_t>(st);
  return 0;
}

int xiofs_release(const char *, struct fuse_file_info *fi)
{
  delete fileState(fi);
  if (fi)
    fi->fh = 0;
  return 0;
}

int xiofs_read(const char *path, char *buf, size_t size, off_t offset,
             struct fuse_file_info *)
{
  if (offset < 0)
    return -EINVAL;
  std::string body;
  std::string err;
  int rc = g_client.read(path, static_cast<uint64_t>(offset), size, body, err);
  if (rc)
    return rc;
  if (body.size() > size)
    body.resize(size);
  memcpy(buf, body.data(), body.size());
  return static_cast<int>(body.size());
}

int xiofs_write(const char *path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi)
{
  if (offset < 0)
    return -EINVAL;
  FileState *st = fileState(fi);
  std::string etag = st ? st->etag : currentEtag(path);
  std::string err;
  std::string new_etag;
  int rc = g_client.write(path, static_cast<uint64_t>(offset),
                          std::string(buf, size), err, etag, &new_etag);
  if (rc)
    return rc;
  if (st && !new_etag.empty())
    st->etag = new_etag;
  return static_cast<int>(size);
}

int xiofs_truncate(const char *path, off_t size)
{
  if (size < 0)
    return -EINVAL;
  std::string err;
  const std::string etag = currentEtag(path);
  if (size == 0)
    return g_client.put(path, {}, err, etag);

  XioFS::Attr a;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  const int64_t cur = a.size < 0 ? 0 : a.size;
  if (size == cur)
    return 0;
  if (size < cur) {
    std::string body;
    rc = g_client.read(path, 0, static_cast<uint64_t>(size), body, err);
    if (rc)
      return rc;
    if (body.size() > static_cast<size_t>(size))
      body.resize(static_cast<size_t>(size));
    else if (body.size() < static_cast<size_t>(size))
      body.resize(static_cast<size_t>(size), '\0');
    return g_client.put(path, body, err, a.etag);
  }
  // Extend: one-byte PATCH at the last offset. XRootD grows the file;
  // the gap is a hole (reads as zeros on typical OSS).
  return g_client.write(path, static_cast<uint64_t>(size - 1),
                        std::string(1, '\0'), err, a.etag);
}

int xiofs_mkdir(const char *path, mode_t mode)
{
  std::string err;
  int rc = g_client.mkdir(path, err);
  if (rc)
    return rc;
  if (mode & 07777)
    return g_client.chmod(path, mode, err);
  return 0;
}

int xiofs_unlink(const char *path)
{
  std::string err;
  return g_client.unlink(path, err, currentEtag(path));
}

int xiofs_rmdir(const char *path)
{
  return xiofs_unlink(path);
}

int xiofs_rename(const char *from, const char *to)
{
  std::string err;
  return g_client.rename(from, to, err, currentEtag(from));
}

int xiofs_chmod(const char *path, mode_t mode)
{
  std::string err;
  return g_client.chmod(path, mode, err);
}

int xiofs_chown(const char *path, uid_t uid, gid_t gid)
{
  std::string err;
  return g_client.chown(path, uid, gid, err);
}

int xiofs_link(const char *from, const char *to)
{
  std::string err;
  return g_client.link(from, to, err);
}

int xiofs_symlink(const char *target, const char *linkpath)
{
  std::string err;
  return g_client.symlink(linkpath, target, err);
}

int xiofs_readlink(const char *path, char *buf, size_t size)
{
  if (!buf || size == 0)
    return -EINVAL;
  std::string target;
  std::string err;
  int rc = g_client.readlink(path, target, err);
  if (rc)
    return rc;
  if (target.size() >= size)
    target.resize(size - 1);
  memcpy(buf, target.data(), target.size());
  buf[target.size()] = 0;
  return 0;
}

int xiofs_mknod(const char *path, mode_t mode, dev_t rdev)
{
  if (S_ISREG(mode) || (mode & S_IFMT) == 0) {
    int rc = putEmpty(path);
    if (rc)
      return rc;
    if (mode & 07777) {
      std::string err;
      return g_client.chmod(path, mode, err);
    }
    return 0;
  }
  std::string err;
  return g_client.mknod(path, mode, rdev, err);
}

int xiofs_utimens(const char *path, const struct timespec tv[2])
{
  std::string err;
  return g_client.utimens(path, tv, err);
}

int xiofs_access(const char *path, int mask)
{
  XioFS::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  return posixAccess(a, mask);
}

int xiofs_fsync(const char *, int, struct fuse_file_info *)
{
  return 0;
}

#if defined(__APPLE__)
int xiofs_setxattr(const char *path, const char *name, const char *value,
                   size_t size, int, uint32_t)
#else
int xiofs_setxattr(const char *path, const char *name, const char *value,
                   size_t size, int)
#endif
{
  std::string err;
  return g_client.setxattr(path, name, std::string(value, size), err);
}

#if defined(__APPLE__)
int xiofs_getxattr(const char *path, const char *name, char *buf, size_t size,
                   uint32_t)
#else
int xiofs_getxattr(const char *path, const char *name, char *buf, size_t size)
#endif
{
  std::string value;
  std::string err;
  int rc = g_client.getxattr(path, name, value, err);
  if (rc)
    return rc;
  if (!buf)
    return static_cast<int>(value.size());
  if (size < value.size())
    return -ERANGE;
  memcpy(buf, value.data(), value.size());
  return static_cast<int>(value.size());
}

int xiofs_listxattr(const char *path, char *buf, size_t size)
{
  std::string names;
  std::string err;
  int rc = g_client.listxattr(path, names, err);
  if (rc)
    return rc;
  if (!buf)
    return static_cast<int>(names.size());
  if (size < names.size())
    return -ERANGE;
  memcpy(buf, names.data(), names.size());
  return static_cast<int>(names.size());
}

int xiofs_removexattr(const char *path, const char *name)
{
  std::string err;
  return g_client.removexattr(path, name, err);
}

int xiofs_lock(const char *path, struct fuse_file_info *, int cmd,
               struct flock *fl)
{
  if (!fl)
    return -EINVAL;
  std::string err;
  if (fl->l_type == F_UNLCK || cmd == F_UNLCK)
    return g_client.unlock(path, err);
  const char *lcmd = "SETLK";
  if (cmd == F_SETLKW)
    lcmd = "SETLKW";
  else if (cmd == F_GETLK)
    lcmd = "GETLK";
  const char *typ = "WRLCK";
  if (fl->l_type == F_RDLCK)
    typ = "RDLCK";
  const char *wh = "SET";
  if (fl->l_whence == SEEK_CUR)
    wh = "CUR";
  else if (fl->l_whence == SEEK_END)
    wh = "END";
  return g_client.lock(path, lcmd, typ, wh, static_cast<long long>(fl->l_start),
                       static_cast<long long>(fl->l_len), err);
}

#ifdef __linux__
int xiofs_flock(const char *path, struct fuse_file_info *, int op)
{
  std::string err;
  std::string name = "EX";
  if (op & LOCK_UN)
    name = "UN";
  else if (op & LOCK_SH)
    name = "SH";
  if (op & LOCK_NB)
    name += "NB";
  if (op & LOCK_UN)
    return g_client.unlock(path, err);
  return g_client.flock(path, name, err);
}
#endif

fuse_operations xiofs_ops()
{
  fuse_operations ops{};
  ops.getattr = xiofs_getattr;
  ops.readdir = xiofs_readdir;
  ops.open = xiofs_open;
  ops.create = xiofs_create;
  ops.release = xiofs_release;
  ops.read = xiofs_read;
  ops.write = xiofs_write;
  ops.truncate = xiofs_truncate;
  ops.mkdir = xiofs_mkdir;
  ops.unlink = xiofs_unlink;
  ops.rmdir = xiofs_rmdir;
  ops.rename = xiofs_rename;
  ops.chmod = xiofs_chmod;
  ops.chown = xiofs_chown;
  ops.link = xiofs_link;
  ops.symlink = xiofs_symlink;
  ops.readlink = xiofs_readlink;
  ops.mknod = xiofs_mknod;
  ops.utimens = xiofs_utimens;
  ops.access = xiofs_access;
  ops.fsync = xiofs_fsync;
  ops.setxattr = xiofs_setxattr;
  ops.getxattr = xiofs_getxattr;
  ops.listxattr = xiofs_listxattr;
  ops.removexattr = xiofs_removexattr;
  ops.lock = xiofs_lock;
#ifdef __linux__
  ops.flock = xiofs_flock;
#endif
  return ops;
}

void usage(const char *argv0)
{
  std::cerr << "Usage: " << argv0
            << " [--cacert FILE] [--insecure] [--token TOK] URL MOUNTPOINT "
               "[fuse options]\n";
}

} // namespace

int main(int argc, char **argv)
{
  XioFS::Http2Session::Options opt;
  std::vector<char *> fuse_argv;
  fuse_argv.push_back(argv[0]);
  std::string url;
  std::string mount;

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
    } else if (url.empty() && !a.empty() && a[0] != '-')
      url = a;
    else if (mount.empty() && !a.empty() && a[0] != '-')
      mount = a;
    else
      fuse_argv.push_back(argv[i]);
  }

  if (url.empty() || mount.empty()) {
    usage(argv[0]);
    return 2;
  }

  std::string err;
  int rc = g_client.open(url, opt, err);
  if (rc) {
    std::cerr << "xiofsd: " << err << "\n";
    return 1;
  }

  fuse_argv.push_back(const_cast<char *>(mount.c_str()));
  fuse_argv.push_back(const_cast<char *>("-o"));
  fuse_argv.push_back(const_cast<char *>(
      "fsname=xiofs,subtype=xiofs,auto_cache,big_writes,max_readahead=4194304"));
  fuse_argv.push_back(nullptr);

  auto ops = xiofs_ops();
  return fuse_main(static_cast<int>(fuse_argv.size() - 1), fuse_argv.data(),
                   &ops, nullptr);
}
