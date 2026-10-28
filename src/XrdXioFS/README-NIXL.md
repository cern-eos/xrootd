# NIXL Storage Plugin for XIOFS

High-throughput, zero-copy GPU storage connector for AI inference and training
frameworks (vLLM, TensorRT-LLM, KServe, PyTorch).

The **NIXL (Next-generation Inference Xfer Layer)** plugin connects AI runtimes
directly to XRootD / XrdHttp storage over HTTP/2 and **Secure Parallel Transport
(libparsocket)**, streaming model weights (`.safetensors`, `.bin`), checkpoints,
and KV caches directly into GPU High Bandwidth Memory (VRAM) without intermediate
CPU DRAM bounce buffers.

---

## Architecture

In standard POSIX or FUSE clients, every read incurs two memory copies across the
host PCIe bus:

```
Standard Storage Path (Two Memory Bounces):
[ Remote XrdHttp ]
       │  (HTTP/2 over TCP)
       ▼
[ Host CPU RAM (Kernel Page Cache / FUSE) ]
       │  (Userspace buffer copy)
       ▼
[ Host App DRAM ] ──(cudaMemcpy across PCIe)──► [ GPU VRAM ]
```

With `XioNixl` and `libparsocket`'s GPUDirect RDMA (GDR) backend, tensor ranges
stream directly across the PCIe bus into GPU High Bandwidth Memory:

```
XIOFS NIXL Direct GPU Storage Path (Zero Bounce):
[ Remote XrdHttp2 Server ]
       │  (HTTP/2 Range GET over RDMA RC lanes)
       ▼
[ InfiniBand / RoCE NIC ]
       │
       │  PCIe Peer-to-Peer DMA (nvidia-peermem / KFD)
       ▼
[ GPU Device Memory (cudaMalloc / HBM) ]
       ▲
       │ Header validation only
[ Host CPU (Control Plane) ]
```

---

## Security & Protection Model

Standard AES-GCM payload encryption requires CPU instructions (`VAES`, `VPCLMULQDQ`)
to touch every byte of ciphertext. Running AES-GCM on GPU VRAM forces data back across
PCIe into host CPU registers, eliminating the zero-copy advantage.

To maintain end-to-end security at 100–400 Gbps line rates, the NIXL plugin uses
**selective protection**:

1. **Confidential Control Plane & Headers:**
   - The session handshake runs over standard TLS 1.3.
   - HTTP/2 request headers (`:method`, `:path`, `Range`), cookies, and sensitive
     bearer tokens (`Authorization: Bearer <jwt>`) are always sent with
     `PS_PROTECTION_CONFIDENTIAL` (AEAD encrypted).
2. **Authenticated Plaintext DATA Payloads:**
   - Bulk file content (HTTP/2 `DATA` frames carrying model tensor bytes) is
     transmitted in plaintext protected by a 16-byte HMAC-SHA-256 tag (and RoCE/InfiniBand
     ICRC transport verification).
   - The RDMA NIC DMAs the payload directly into GPU VRAM. The host CPU only verifies
     the record header and authentication tag.

---

## Prerequisites

1. **GPU Host:**
   - Linux kernel with `nvidia-peermem` (NVIDIA) or `amdgpu` KFD (AMD) loaded.
   - InfiniBand / RoCE network interface with `libibverbs` and `librdmacm`.
   - CUDA Toolkit (11.x or 12.x) or ROCm.
2. **Server (`XrdHttp`):**
   - Built with `ENABLE_HTTP2=ON` and `ENABLE_PARSOCKET=ON`.
   - Configure `http.parsock` in `xrootd-http.cf`:
     ```cfg
     http.parsock port 8443 transport rdma sockets 4 protection authenticated
     ```

---

## C++ API Reference (`XioNixl.hh`)

The plugin header lives at `<xrootd/XrdXioFS/XioNixl.hh>` (or `src/XrdXioFS/include/XioNixl.hh`).

### Configuration (`NixlConfig`)

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `url` | `std::string` | `""` | Target endpoint, e.g. `spt://storage.example:8443/models/llama3` |
| `bearer` | `std::string` | `""` | Optional WLCG token, SciToken, or JWT bearer credential |
| `transport` | `std::string` | `"rdma"` | Transport data plane: `"rdma"` (RDMA RC) or `"tcp"` |
| `protection` | `std::string` | `"authenticated"` | Record profile: `"authenticated"` (plaintext + HMAC) or `"confidential"` |
| `sockets` | `int` | `4` | Number of parallel RDMA queue-pairs / TCP subflows |
| `timeout_ms` | `int` | `30000` | Connection and I/O timeout in milliseconds |
| `verify_peer` | `bool` | `false` | Verify server TLS certificate |

### Class `NixlStoragePlugin`

```cpp
class NixlStoragePlugin {
public:
  // Initialize storage client and connect to the remote XrdHttp parsock endpoint
  int Init(const NixlConfig &cfg, std::string &err);

  // Open a remote file (e.g. "model-00001-of-00004.safetensors") and stat attributes
  int Open(const std::string &path, NixlFileHandle **handle, std::string &err);

  // Close an open file handle
  int Close(NixlFileHandle *handle);

  // Register pre-allocated GPU device memory with the RDMA protection domain
  int RegisterGpuBuffer(void *dev_ptr, size_t length, int dmabuf_fd,
                        NixlMemHandle **mem_out, std::string &err);

  // Deregister GPU memory region
  int DeregisterGpuBuffer(NixlMemHandle *mem, std::string &err);

  // Stream byte range directly from XrdHttp into registered GPU memory
  int ReadGpu(NixlFileHandle *handle, uint64_t offset, size_t length,
              NixlMemHandle *mem, size_t &bytes_read, std::string &err);

  // Write byte range directly from registered GPU memory
  int WriteGpu(NixlFileHandle *handle, uint64_t offset, const NixlMemHandle *mem,
               size_t length, std::string &err);

  // Terminate connection and clean up resources
  void Shutdown();

  bool isConnected() const;
};
```

---

## Integration Example

```cpp
#include "XioNixl.hh"

#include <cuda_runtime.h>
#include <iostream>

int main()
{
  XioFS::NixlStoragePlugin plugin;
  XioFS::NixlConfig cfg;
  cfg.url = "spt://storage.cluster.local:8443/models/llama3-70b";
  cfg.transport = "rdma";
  cfg.protection = "authenticated";
  cfg.sockets = 4;
  cfg.bearer = "ey..."; // Optional JWT / SciToken

  std::string err;
  if (plugin.Init(cfg, err) != 0) {
    std::cerr << "Plugin init failed: " << err << "\n";
    return 1;
  }

  // 1. Allocate GPU VRAM buffer for tensor weights (e.g. 512 MiB layer)
  const size_t tensor_bytes = 512ULL * 1024ULL * 1024ULL;
  void *d_weights = nullptr;
  cudaMalloc(&d_weights, tensor_bytes);

  // 2. Register GPU pointer with the InfiniBand/RoCE protection domain
  XioFS::NixlMemHandle *mem = nullptr;
  if (plugin.RegisterGpuBuffer(d_weights, tensor_bytes, -1, &mem, err) != 0) {
    std::cerr << "GPU registration failed: " << err << "\n";
    cudaFree(d_weights);
    return 1;
  }

  // 3. Open remote safetensors file
  XioFS::NixlFileHandle *fh = nullptr;
  if (plugin.Open("model-00001-of-00004.safetensors", &fh, err) != 0) {
    std::cerr << "Open failed: " << err << "\n";
    plugin.DeregisterGpuBuffer(mem, err);
    cudaFree(d_weights);
    return 1;
  }

  // 4. Zero-copy stream tensor bytes directly into GPU VRAM
  uint64_t tensor_offset = 0x10000; // Offset inside safetensors file
  size_t bytes_read = 0;
  if (plugin.ReadGpu(fh, tensor_offset, tensor_bytes, mem, bytes_read, err) != 0) {
    std::cerr << "ReadGpu failed: " << err << "\n";
  } else {
    std::cout << "Streamed " << bytes_read << " bytes directly to GPU VRAM!\n";
  }

  // 5. Cleanup
  plugin.Close(fh);
  plugin.DeregisterGpuBuffer(mem, err);
  cudaFree(d_weights);
  plugin.Shutdown();
  return 0;
}
```

---

## Verifying the Setup

You can run the NIXL integration and unit tests using `ctest`:

```bash
ctest --test-dir build -R "XioNixl" --output-on-failure
```

Or run the test binary directly:

```bash
./build/bin/xrdxiofs-unit-tests --gtest_filter="*Nixl*"
```
