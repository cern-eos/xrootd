//------------------------------------------------------------------------------
// NIXL (Next-generation Inference Xfer Layer / NVIDIA Inference eXchange Library)
// Storage Plugin for XIOFS and libparsocket.
//
// Enables high-throughput direct streaming of model weights, checkpoints,
// and KV caches into GPU VRAM over RDMA RC without CPU memory bounce buffers.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_NIXL_HH
#define XIOFS_NIXL_HH

#include "XioClient.hh"
#include "XioHttp2.hh"

#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace XioFS {

struct NixlConfig {
  std::string url;                       // e.g. spt://storage.example:8443/models/llama3
  std::string bearer;                    // JWT or token authentication
  std::string transport{"rdma"};         // "rdma" or "tcp"
  std::string protection{"authenticated"}; // "authenticated" (plaintext + HMAC) or "confidential"
  int         sockets{4};                // Parallel lanes/queue-pairs
  int         timeout_ms{30000};
  bool        verify_peer{false};
};

struct NixlMemHandle {
  void   *dev_ptr{nullptr};
  size_t  length{0};
  int     dmabuf_fd{-1};
  void   *internal_handle{nullptr};
};

struct NixlFileHandle {
  std::string path;
  Attr        attr;
};

class NixlStoragePlugin {
public:
  NixlStoragePlugin() = default;
  ~NixlStoragePlugin() { Shutdown(); }

  int Init(const NixlConfig &cfg, std::string &err)
  {
    std::lock_guard<std::mutex> lock(mu_);
    cfg_ = cfg;

    Http2Session::Options opt;
    opt.bearer = cfg.bearer;
    opt.parsock = true;
    opt.parsock_transport = cfg.transport;
    opt.parsock_protection = cfg.protection;
    opt.parsock_sockets = cfg.sockets;
    opt.verify_peer = cfg.verify_peer;
    opt.timeout_ms = cfg.timeout_ms;

    client_ = std::make_unique<Client>();
    return client_->open(cfg.url, std::move(opt), err);
  }

  int Open(const std::string &path, NixlFileHandle **handle, std::string &err)
  {
    if (!handle) {
      err = "null handle pointer";
      return -EINVAL;
    }
    *handle = nullptr;

    std::lock_guard<std::mutex> lock(mu_);
    if (!client_) {
      err = "plugin not initialized";
      return -ENOTCONN;
    }

    auto fh = std::make_unique<NixlFileHandle>();
    fh->path = path;
    int rc = client_->getattr(path, fh->attr, err);
    if (rc)
      return rc;

    *handle = fh.release();
    return 0;
  }

  int Close(NixlFileHandle *handle)
  {
    delete handle;
    return 0;
  }

  int RegisterGpuBuffer(void *dev_ptr, size_t length, int dmabuf_fd,
                        NixlMemHandle **mem_out, std::string &err)
  {
    if (!mem_out) {
      err = "null mem_out pointer";
      return -EINVAL;
    }
    *mem_out = nullptr;

    std::lock_guard<std::mutex> lock(mu_);
    if (!client_) {
      err = "plugin not initialized";
      return -ENOTCONN;
    }

    void *ih = nullptr;
    int rc = client_->registerDeviceMem(dev_ptr, length, dmabuf_fd, &ih, err);
    if (rc)
      return rc;

    auto mh = std::make_unique<NixlMemHandle>();
    mh->dev_ptr = dev_ptr;
    mh->length = length;
    mh->dmabuf_fd = dmabuf_fd;
    mh->internal_handle = ih;

    *mem_out = mh.release();
    return 0;
  }

  int DeregisterGpuBuffer(NixlMemHandle *mem, std::string &err)
  {
    if (!mem)
      return 0;

    std::lock_guard<std::mutex> lock(mu_);
    if (!client_) {
      delete mem;
      return 0;
    }

    int rc = client_->deregisterDeviceMem(mem->internal_handle, err);
    delete mem;
    return rc;
  }

  int ReadGpu(NixlFileHandle *handle, uint64_t offset, size_t length,
              NixlMemHandle *mem, size_t &bytes_read, std::string &err)
  {
    bytes_read = 0;
    if (!handle || !mem) {
      err = "invalid handle or mem pointer";
      return -EINVAL;
    }

    std::lock_guard<std::mutex> lock(mu_);
    if (!client_) {
      err = "plugin not initialized";
      return -ENOTCONN;
    }

    return client_->readDevice(handle->path, offset, length, mem->dev_ptr,
                               mem->internal_handle, bytes_read, err);
  }

  int WriteGpu(NixlFileHandle *handle, uint64_t offset, const NixlMemHandle *mem,
               size_t length, std::string &err)
  {
    if (!handle || !mem) {
      err = "invalid handle or mem pointer";
      return -EINVAL;
    }

    std::lock_guard<std::mutex> lock(mu_);
    if (!client_) {
      err = "plugin not initialized";
      return -ENOTCONN;
    }

    return client_->writeDevice(handle->path, offset, mem->dev_ptr,
                                mem->internal_handle, length, err);
  }

  void Shutdown()
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (client_) {
      client_->close();
      client_.reset();
    }
  }

  bool isConnected() const
  {
    std::lock_guard<std::mutex> lock(mu_);
    return client_ && client_->connected();
  }

private:
  NixlConfig cfg_;
  std::unique_ptr<Client> client_;
  mutable std::mutex mu_;
};

} // namespace XioFS

#endif
