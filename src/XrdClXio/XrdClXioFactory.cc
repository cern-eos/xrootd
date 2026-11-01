//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XrdClXioFile.hh"
#include "XrdClXioFilesystem.hh"

#include <XrdCl/XrdClPlugInInterface.hh>
#include <XrdVersion.hh>

namespace {

class Factory final : public XrdCl::PlugInFactory {
public:
  XrdCl::FilePlugIn *CreateFile(const std::string &) override
  {
    return new XrdClXio::File();
  }

  XrdCl::FileSystemPlugIn *CreateFileSystem(const std::string &url) override
  {
    return new XrdClXio::Filesystem(url);
  }
};

} // namespace

XrdVERSIONINFO(XrdClGetPlugIn, XrdClXio)

extern "C" {

void *XrdClGetPlugIn(const void *)
{
  return new Factory();
}

}
