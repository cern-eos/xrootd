//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDCLXIO_FILE_HH
#define XRDCLXIO_FILE_HH

#include "XioUrl.hh"

#include <XrdCl/XrdClPlugInInterface.hh>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace XioFS {
class Http2Session;
}

namespace XrdClXio {

class File final : public XrdCl::FilePlugIn {
public:
  File();
  ~File() override;

  XrdCl::XRootDStatus Open(const std::string &url,
                           XrdCl::OpenFlags::Flags flags,
                           XrdCl::Access::Mode mode,
                           XrdCl::ResponseHandler *handler,
                           time_t timeout) override;

  XrdCl::XRootDStatus Close(XrdCl::ResponseHandler *handler,
                            time_t timeout) override;

  XrdCl::XRootDStatus Sync(XrdCl::ResponseHandler *handler,
                           time_t timeout) override;

  XrdCl::XRootDStatus Stat(bool force, XrdCl::ResponseHandler *handler,
                           time_t timeout) override;

  XrdCl::XRootDStatus Read(uint64_t offset, uint32_t size, void *buffer,
                           XrdCl::ResponseHandler *handler,
                           time_t timeout) override;

  XrdCl::XRootDStatus VectorRead(const XrdCl::ChunkList &chunks, void *buffer,
                                 XrdCl::ResponseHandler *handler,
                                 time_t timeout) override;

  XrdCl::XRootDStatus Write(uint64_t offset, uint32_t size, const void *buffer,
                            XrdCl::ResponseHandler *handler,
                            time_t timeout) override;

  XrdCl::XRootDStatus Write(uint64_t offset, XrdCl::Buffer &&buffer,
                            XrdCl::ResponseHandler *handler,
                            time_t timeout) override;

  bool IsOpen() const override;

  bool SetProperty(const std::string &name, const std::string &value) override;
  bool GetProperty(const std::string &name, std::string &value) const override;

private:
  struct State {
    mutable std::mutex mu;
    bool open{false};
    XioFS::Url url;
    std::string path;
    std::shared_ptr<XioFS::Http2Session> sess;
    std::unordered_map<std::string, std::string> props;
  };

  bool snapshot(std::shared_ptr<XioFS::Http2Session> &sess,
                std::string &path) const;

  std::shared_ptr<State> st_;
};

} // namespace XrdClXio

#endif
