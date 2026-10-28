//------------------------------------------------------------------------------
// Secure Parallel Transport (libparsocket) listener for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTPPARSOCKLISTENER_HH
#define XRDHTTPPARSOCKLISTENER_HH

#include <atomic>
#include <pthread.h>
#include <string>

#ifdef HAVE_PARSOCKET
#include "parsock.h"
#endif

struct XrdHttpParsockConfig {
  bool        enabled{false};
  int         port{0};
  std::string bind_addr{"0.0.0.0"};
  std::string transport{"tcp"}; // tcp, rdma, quic
  int         min_sockets{1};
  int         max_sockets{4};
  int         initial_sockets{4};
  std::string protection{"confidential"}; // confidential, authenticated
  std::string cert_file;
  std::string key_file;
  std::string ca_file;
  std::string rdma_device;
  int         connect_timeout_ms{10000};
  int         io_timeout_ms{120000};
};

class XrdHttpParsockListener {
public:
  XrdHttpParsockListener();
  ~XrdHttpParsockListener();

  bool Start(const XrdHttpParsockConfig &cfg);
  void Stop();

  bool isRunning() const { return running_; }
  int port() const { return port_; }
  int dataPort() const { return data_port_; }

private:
  static void *AcceptThreadHelper(void *arg);
  void acceptLoop();

  static void *WorkerThreadHelper(void *arg);
  static void runWorker(void *prot_ptr);

  std::atomic<bool> running_{false};
  int port_{0};
  int data_port_{0};

#ifdef HAVE_PARSOCKET
  ps_listener_t *listener_{nullptr};
#else
  void          *listener_{nullptr};
#endif

  pthread_t acceptThread_{0};
  XrdHttpParsockConfig config_;
  std::string temp_cert_file_;
  std::string temp_key_file_;
};

#endif
