//------------------------------------------------------------------------------
// kfsd — FUSE mount of an XrdHttp HTTP/2 export.
//
//   kfsd [--cacert FILE] [--insecure] [--token TOK] URL MOUNTPOINT [fuse-opts]
//
// Reads are Range GETs. Writes are PATCH with Content-Range; create/truncate
// to empty use PUT. mkdir/unlink/rename map to MKCOL/DELETE/MOVE.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifdef __APPLE__
#ifndef _DARWIN_USE_64_BIT_INODE
#define _DARWIN_USE_64_BIT_INODE 1
#endif
#endif
#define FUSE_USE_VERSION 26

#include "KfsClient.hh"

#include <fuse.h>

#include <cerrno>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

Kfs::Client g_client;

void fillStat(const Kfs::Attr &a, struct stat *st)
{
  memset(st, 0, sizeof(*st));
  st->st_ino = a.ino ? static_cast<ino_t>(a.ino) : 1;
  st->st_nlink = a.is_dir ? 2 : 1;
  st->st_mode = a.is_dir ? (S_IFDIR | 0755) : (S_IFREG | 0644);
  st->st_size = a.size < 0 ? 0 : a.size;
  st->st_mtime = a.mtime;
  st->st_atime = a.mtime;
  st->st_ctime = a.mtime;
  st->st_uid = getuid();
  st->st_gid = getgid();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + 511) / 512;
}

int putEmpty(const char *path)
{
  std::string err;
  return g_client.put(path, {}, err);
}

int kfs_getattr(const char *path, struct stat *st)
{
  Kfs::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  fillStat(a, st);
  return 0;
}

int kfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                off_t, struct fuse_file_info *)
{
  filler(buf, ".", nullptr, 0);
  filler(buf, "..", nullptr, 0);
  std::vector<Kfs::DavEntry> ents;
  std::string err;
  int rc = g_client.readdir(path, ents, err);
  if (rc)
    return rc;
  for (const auto &e : ents) {
    if (e.name.empty() || e.name == "." || e.name == "..")
      continue;
    struct stat st {};
    st.st_mode = e.is_dir ? (S_IFDIR | 0755) : (S_IFREG | 0644);
    st.st_size = e.size < 0 ? 0 : e.size;
    st.st_mtime = e.mtime;
    if (filler(buf, e.name.c_str(), &st, 0) != 0)
      break;
  }
  return 0;
}

int kfs_open(const char *path, struct fuse_file_info *fi)
{
  Kfs::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  if (a.is_dir)
    return -EISDIR;
  if ((fi->flags & O_TRUNC) && (fi->flags & O_ACCMODE) != O_RDONLY)
    return putEmpty(path);
  return 0;
}

int kfs_create(const char *path, mode_t, struct fuse_file_info *)
{
  return putEmpty(path);
}

int kfs_read(const char *path, char *buf, size_t size, off_t offset,
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

int kfs_write(const char *path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *)
{
  if (offset < 0)
    return -EINVAL;
  std::string err;
  int rc = g_client.write(path, static_cast<uint64_t>(offset),
                          std::string(buf, size), err);
  if (rc)
    return rc;
  return static_cast<int>(size);
}

int kfs_truncate(const char *path, off_t size)
{
  if (size < 0)
    return -EINVAL;
  std::string err;
  if (size == 0)
    return g_client.put(path, {}, err);

  Kfs::Attr a;
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
    return g_client.put(path, body, err);
  }
  // Extend: one-byte PATCH at the last offset. XRootD grows the file;
  // the gap is a hole (reads as zeros on typical OSS).
  return g_client.write(path, static_cast<uint64_t>(size - 1),
                        std::string(1, '\0'), err);
}

int kfs_mkdir(const char *path, mode_t)
{
  std::string err;
  return g_client.mkdir(path, err);
}

int kfs_unlink(const char *path)
{
  std::string err;
  return g_client.unlink(path, err);
}

int kfs_rmdir(const char *path)
{
  return kfs_unlink(path);
}

int kfs_rename(const char *from, const char *to)
{
  std::string err;
  return g_client.rename(from, to, err);
}

int kfs_chmod(const char *, mode_t)
{
  return 0;
}

int kfs_chown(const char *, uid_t, gid_t)
{
  return 0;
}

int kfs_utimens(const char *, const struct timespec[2])
{
  return 0;
}

int kfs_fsync(const char *, int, struct fuse_file_info *)
{
  return 0;
}

fuse_operations kfs_ops()
{
  fuse_operations ops{};
  ops.getattr = kfs_getattr;
  ops.readdir = kfs_readdir;
  ops.open = kfs_open;
  ops.create = kfs_create;
  ops.read = kfs_read;
  ops.write = kfs_write;
  ops.truncate = kfs_truncate;
  ops.mkdir = kfs_mkdir;
  ops.unlink = kfs_unlink;
  ops.rmdir = kfs_rmdir;
  ops.rename = kfs_rename;
  ops.chmod = kfs_chmod;
  ops.chown = kfs_chown;
  ops.utimens = kfs_utimens;
  ops.fsync = kfs_fsync;
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
  Kfs::Http2Session::Options opt;
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
    std::cerr << "kfsd: " << err << "\n";
    return 1;
  }

  fuse_argv.push_back(const_cast<char *>(mount.c_str()));
  fuse_argv.push_back(const_cast<char *>("-o"));
  fuse_argv.push_back(const_cast<char *>(
      "auto_cache,big_writes,max_readahead=4194304"));
  fuse_argv.push_back(nullptr);

  auto ops = kfs_ops();
  return fuse_main(static_cast<int>(fuse_argv.size() - 1), fuse_argv.data(),
                   &ops, nullptr);
}
