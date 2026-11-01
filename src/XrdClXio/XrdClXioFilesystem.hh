//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDCLXIO_FILESYSTEM_HH
#define XRDCLXIO_FILESYSTEM_HH

#include "XioUrl.hh"

#include <XrdCl/XrdClPlugInInterface.hh>
#include <XrdCl/XrdClURL.hh>

#include <memory>
#include <mutex>
#include <string>

namespace XioFS {
class Http2Session;
}

namespace XrdClXio {

class Filesystem final : public XrdCl::FileSystemPlugIn {
public:
  explicit Filesystem(const std::string &url);
  ~Filesystem() override;

  XrdCl::XRootDStatus DirList(const std::string &path,
                              XrdCl::DirListFlags::Flags flags,
                              XrdCl::ResponseHandler *handler,
                              time_t timeout) override;

  XrdCl::XRootDStatus MkDir(const std::string &path,
                            XrdCl::MkDirFlags::Flags flags,
                            XrdCl::Access::Mode mode,
                            XrdCl::ResponseHandler *handler,
                            time_t timeout) override;

  XrdCl::XRootDStatus Rm(const std::string &path,
                         XrdCl::ResponseHandler *handler,
                         time_t timeout) override;

  XrdCl::XRootDStatus RmDir(const std::string &path,
                            XrdCl::ResponseHandler *handler,
                            time_t timeout) override;

  XrdCl::XRootDStatus Stat(const std::string &path,
                           XrdCl::ResponseHandler *handler,
                           time_t timeout) override;

  XrdCl::XRootDStatus Query(XrdCl::QueryCode::Code queryCode,
                            const XrdCl::Buffer &arg,
                            XrdCl::ResponseHandler *handler,
                            time_t timeout) override;

private:
  std::string fullPath(const std::string &path) const;
  std::shared_ptr<XioFS::Http2Session> session(time_t timeout, std::string &err);

  mutable std::mutex mu_;
  XrdCl::URL url_;
  XioFS::Url endpoint_;
  std::shared_ptr<XioFS::Http2Session> sess_;
};

} // namespace XrdClXio

#endif
