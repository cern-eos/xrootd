#undef NDEBUG

#include "XioNixl.hh"
#include "XioClient.hh"

#include <gtest/gtest.h>

using XioFS::NixlConfig;
using XioFS::NixlMemHandle;
using XioFS::NixlStoragePlugin;
using XioFS::Client;

TEST(XioNixl, PluginConfigAndRegistrationLifecycle)
{
  NixlStoragePlugin plugin;
  NixlConfig cfg;
  cfg.url = "spt://127.0.0.1:8443/models";
  cfg.transport = "rdma";
  cfg.protection = "authenticated";
  cfg.sockets = 4;
  cfg.verify_peer = false;

  std::vector<uint8_t> dummy_gpu_buf(65536, 0xaa);
  NixlMemHandle *mem = nullptr;
  std::string err;

  // Before Init, registration should cleanly report error
  EXPECT_NE(0, plugin.RegisterGpuBuffer(dummy_gpu_buf.data(), dummy_gpu_buf.size(), -1, &mem, err));
  EXPECT_FALSE(err.empty());
  EXPECT_EQ(nullptr, mem);

  // Null pointer checks
  EXPECT_EQ(-EINVAL, plugin.RegisterGpuBuffer(nullptr, 0, -1, nullptr, err));
  EXPECT_EQ(0, plugin.DeregisterGpuBuffer(nullptr, err));
}

TEST(XioNixl, ClientDeviceBufferReadAccumulation)
{
  Client client;
  std::vector<uint8_t> target(1024, 0);
  size_t bytes_read = 0;
  std::string err;

  // Zero-length read should return 0 immediately without I/O
  EXPECT_EQ(0, client.readDevice("/test", 0, 0, target.data(), nullptr, bytes_read, err));
  EXPECT_EQ(0u, bytes_read);

  // Null target should return -EINVAL
  EXPECT_EQ(-EINVAL, client.readDevice("/test", 0, 100, nullptr, nullptr, bytes_read, err));
}
