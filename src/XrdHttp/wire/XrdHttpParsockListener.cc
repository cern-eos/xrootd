//------------------------------------------------------------------------------
// Secure Parallel Transport (libparsocket) listener for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XrdHttpParsockListener.hh"
#include "XrdHttpProtocol.hh"
#include "XrdSys/XrdSysPthread.hh"

#include <cstdio>
#include <cstring>
#include <unistd.h>

#ifdef HAVE_PARSOCKET
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace
{
bool generateSelfSigned(std::string &cert_path, std::string &key_path)
{
  char cpath[] = "/tmp/xrd_parsock_cert_XXXXXX.pem";
  char kpath[] = "/tmp/xrd_parsock_key_XXXXXX.pem";
  int cfd = mkstemps(cpath, 4);
  int kfd = mkstemps(kpath, 4);
  if (cfd < 0 || kfd < 0) {
    if (cfd >= 0) { close(cfd); unlink(cpath); }
    if (kfd >= 0) { close(kfd); unlink(kpath); }
    return false;
  }
  close(cfd);
  close(kfd);

  EVP_PKEY *pkey = nullptr;
  EVP_PKEY_CTX *kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  if (!kctx || EVP_PKEY_keygen_init(kctx) != 1 ||
      EVP_PKEY_CTX_set_rsa_keygen_bits(kctx, 2048) != 1 ||
      EVP_PKEY_keygen(kctx, &pkey) != 1) {
    if (kctx) EVP_PKEY_CTX_free(kctx);
    unlink(cpath); unlink(kpath);
    return false;
  }
  EVP_PKEY_CTX_free(kctx);

  X509 *x509 = X509_new();
  if (!x509) {
    EVP_PKEY_free(pkey);
    unlink(cpath); unlink(kpath);
    return false;
  }

  ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
  X509_gmtime_adj(X509_getm_notBefore(x509), 0);
  X509_gmtime_adj(X509_getm_notAfter(x509), 31536000L); // 1 year
  X509_set_pubkey(x509, pkey);

  X509_NAME *name = X509_NAME_new();
  if (!name) {
    X509_free(x509);
    EVP_PKEY_free(pkey);
    unlink(cpath); unlink(kpath);
    return false;
  }
  X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC, (const unsigned char *)"CH", -1, -1, 0);
  X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC, (const unsigned char *)"XRootD", -1, -1, 0);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (const unsigned char *)"127.0.0.1", -1, -1, 0);
  X509_set_subject_name(x509, name);
  X509_set_issuer_name(x509, name);
  X509_NAME_free(name);

  X509V3_CTX ctx;
  X509V3_set_ctx_nodb(&ctx);
  X509V3_set_ctx(&ctx, x509, x509, nullptr, nullptr, 0);
  X509_EXTENSION *san = X509V3_EXT_conf_nid(nullptr, &ctx, NID_subject_alt_name,
                                           (char *)"DNS:localhost,IP:127.0.0.1,IP:::1");
  if (san) {
    X509_add_ext(x509, san, -1);
    X509_EXTENSION_free(san);
  }

  if (X509_sign(x509, pkey, EVP_sha256()) == 0) {
    X509_free(x509);
    EVP_PKEY_free(pkey);
    unlink(cpath); unlink(kpath);
    return false;
  }

  FILE *f = fopen(kpath, "wb");
  if (!f) {
    X509_free(x509);
    EVP_PKEY_free(pkey);
    unlink(cpath); unlink(kpath);
    return false;
  }
  PEM_write_PrivateKey(f, pkey, nullptr, nullptr, 0, nullptr, nullptr);
  fclose(f);

  f = fopen(cpath, "wb");
  if (!f) {
    X509_free(x509);
    EVP_PKEY_free(pkey);
    unlink(cpath); unlink(kpath);
    return false;
  }
  PEM_write_X509(f, x509);
  fclose(f);

  X509_free(x509);
  EVP_PKEY_free(pkey);

  cert_path = cpath;
  key_path = kpath;
  return true;
}
} // namespace
#endif

XrdHttpParsockListener::XrdHttpParsockListener()
{
}

XrdHttpParsockListener::~XrdHttpParsockListener()
{
  Stop();
}

bool XrdHttpParsockListener::Start(const XrdHttpParsockConfig &cfg)
{
#ifdef HAVE_PARSOCKET
  if (running_)
    return true;

  config_ = cfg;
  struct ps_options opt;
  ps_options_init(&opt);

  if (cfg.transport == "rdma") {
    opt.transport = PS_TRANSPORT_RDMA_RC;
  } else if (cfg.transport == "quic") {
    opt.transport = PS_TRANSPORT_QUIC;
  } else {
    opt.transport = PS_TRANSPORT_TCP;
  }

  if (cfg.protection == "authenticated" || cfg.protection == "auth") {
    opt.min_protection = PS_PROTECTION_AUTHENTICATED;
  } else {
    opt.min_protection = PS_PROTECTION_CONFIDENTIAL;
  }

  if (cfg.min_sockets > 0)
    opt.min_sockets = static_cast<unsigned>(cfg.min_sockets);
  if (cfg.max_sockets > 0)
    opt.max_sockets = static_cast<unsigned>(cfg.max_sockets);
  if (cfg.initial_sockets > 0)
    opt.initial_sockets = static_cast<unsigned>(cfg.initial_sockets);
  if (cfg.connect_timeout_ms > 0)
    opt.connect_timeout_ms = static_cast<uint32_t>(cfg.connect_timeout_ms);
  if (cfg.io_timeout_ms > 0)
    opt.io_timeout_ms = static_cast<uint32_t>(cfg.io_timeout_ms);
  if (!cfg.rdma_device.empty())
    opt.rdma_device = cfg.rdma_device.c_str();

  std::string cert = cfg.cert_file;
  std::string key = cfg.key_file;
  if (cert.empty() || key.empty()) {
    if (!generateSelfSigned(cert, key))
      return false;
    temp_cert_file_ = cert;
    temp_key_file_ = key;
  }

  struct ps_tls_config tls;
  memset(&tls, 0, sizeof(tls));
  tls.cert_file = cert.c_str();
  tls.key_file = key.c_str();
  if (!cfg.ca_file.empty())
    tls.ca_file = cfg.ca_file.c_str();
  opt.tls = &tls;

  listener_ = ps_listen(cfg.bind_addr.c_str(), static_cast<uint16_t>(cfg.port), &opt);
  if (!listener_)
    return false;

  uint16_t cport = 0, dport = 0;
  if (ps_listener_port(listener_, &cport, &dport) == 0) {
    port_ = static_cast<int>(cport);
    data_port_ = static_cast<int>(dport);
  } else {
    port_ = cfg.port;
  }

  running_ = true;
  if (XrdSysThread::Run(&acceptThread_, AcceptThreadHelper, this, 0, "Parsock Acceptor") != 0) {
    running_ = false;
    ps_listener_close(listener_);
    listener_ = nullptr;
    return false;
  }
  return true;
#else
  (void)cfg;
  return false;
#endif
}

void XrdHttpParsockListener::Stop()
{
#ifdef HAVE_PARSOCKET
  if (!running_.exchange(false))
    return;

  if (listener_) {
    ps_listener_close(listener_);
    listener_ = nullptr;
  }

  if (acceptThread_) {
    XrdSysThread::Join(acceptThread_, nullptr);
    acceptThread_ = 0;
  }

  if (!temp_cert_file_.empty()) {
    unlink(temp_cert_file_.c_str());
    temp_cert_file_.clear();
  }
  if (!temp_key_file_.empty()) {
    unlink(temp_key_file_.c_str());
    temp_key_file_.clear();
  }
#endif
}

void *XrdHttpParsockListener::AcceptThreadHelper(void *arg)
{
  auto *self = static_cast<XrdHttpParsockListener *>(arg);
  if (self)
    self->acceptLoop();
  return nullptr;
}

void XrdHttpParsockListener::acceptLoop()
{
#ifdef HAVE_PARSOCKET
  while (running_ && listener_) {
    ps_connection_t *conn = ps_accept(listener_);
    if (!conn) {
      if (!running_)
        break;
      continue;
    }

    XrdHttpProtocol *prot = XrdHttpProtocol::Create(true);
    if (!prot) {
      ps_close(conn);
      continue;
    }

    prot->attachParsock(conn, "parsock-client");

    pthread_t workerTh;
    if (XrdSysThread::Run(&workerTh, WorkerThreadHelper, prot, XRDSYSTHREAD_BIND,
                          "Parsock HTTP/2 Worker") != 0) {
      prot->Recycle(nullptr, 0, "thread spawn failed");
    }
  }
#endif
}

void *XrdHttpParsockListener::WorkerThreadHelper(void *arg)
{
  runWorker(arg);
  return nullptr;
}

void XrdHttpParsockListener::runWorker(void *prot_ptr)
{
#ifdef HAVE_PARSOCKET
  auto *prot = static_cast<XrdHttpProtocol *>(prot_ptr);
  if (!prot)
    return;

  ps_connection_t *conn = prot->getParsock();
  if (!conn) {
    prot->Recycle(nullptr, 0, "null parsock connection");
    return;
  }

  while (ps_connection_state(conn) == PS_STATE_OPEN) {
    int rc = prot->processHttp2(nullptr);
    if (rc < 0)
      break;
    if (rc == 1) {
      char buf[16384];
      ssize_t n = ps_recv(conn, buf, sizeof(buf));
      if (n <= 0)
        break;
      if (prot->feedHttp2Wire(buf, static_cast<size_t>(n)) < 0)
        break;
    }
  }

  prot->Recycle(nullptr, 0, "parsock connection closed");
#endif
}
