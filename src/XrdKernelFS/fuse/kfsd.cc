//------------------------------------------------------------------------------
// kfsd — read-only FUSE mount of an XrdHttp HTTP/2 export.
//
//   kfsd [--cacert FILE] [--insecure] [--token TOK] URL MOUNTPOINT [fuse-opts]
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
#include <unistd.h>
#include <vector>

namespace {

Kfs::Client g_client;

int kfs_getattr(const char *path, struct stat *st)
{
  memset(st, 0, sizeof(*st));
  Kfs::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  st->st_ino = a.ino ? static_cast<ino_t>(a.ino) : 1;
  st->st_nlink = a.is_dir ? 2 : 1;
  st->st_mode = a.is_dir ? (S_IFDIR | 0555) : (S_IFREG | 0444);
  st->st_size = a.size < 0 ? 0 : a.size;
  st->st_mtime = a.mtime;
  st->st_atime = a.mtime;
  st->st_ctime = a.mtime;
  st->st_uid = getuid();
  st->st_gid = getgid();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + 511) / 512;
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
    st.st_mode = e.is_dir ? (S_IFDIR | 0555) : (S_IFREG | 0444);
    st.st_size = e.size < 0 ? 0 : e.size;
    st.st_mtime = e.mtime;
    if (filler(buf, e.name.c_str(), &st, 0) != 0)
      break;
  }
  return 0;
}

int kfs_open(const char *path, struct fuse_file_info *fi)
{
  if ((fi->flags & O_ACCMODE) != O_RDONLY)
    return -EROFS;
  Kfs::Attr a;
  std::string err;
  int rc = g_client.getattr(path, a, err);
  if (rc)
    return rc;
  if (a.is_dir)
    return -EISDIR;
  return 0;
}

int kfs_read(const char *path, char *buf, size_t size, off_t offset,
             struct fuse_file_info *)
{
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

fuse_operations kfs_ops()
{
  fuse_operations ops{};
  ops.getattr = kfs_getattr;
  ops.readdir = kfs_readdir;
  ops.open = kfs_open;
  ops.read = kfs_read;
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
  fuse_argv.push_back(const_cast<char *>("ro,kernel_cache,auto_cache,max_readahead=4194304"));
  fuse_argv.push_back(nullptr);

  auto ops = kfs_ops();
  return fuse_main(static_cast<int>(fuse_argv.size() - 1), fuse_argv.data(),
                   &ops, nullptr);
}
