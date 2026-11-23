//------------------------------------------------------------------------------
// POSIX directory exported through XioFS::Fs.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_POSIXFS_HH
#define XIOFS_POSIXFS_HH

#include "XioFs.hh"

#include <string>

namespace XioFS {

class PosixFs : public Fs
{
public:
  // `root` is the directory clients see as "/". It must already exist.
  explicit PosixFs(std::string root);

  const std::string &root() const { return root_; }

  void handle(const FsRequest &req, FsResult &out) override;

private:
  std::string root_;
};

} // namespace XioFS

#endif
