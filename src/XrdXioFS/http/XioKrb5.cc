//------------------------------------------------------------------------------
// Kerberos SPNEGO helper for XIOFS (Unix GSS-API / Windows SSPI).
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioKrb5.hh"

#include <cstdlib>
#include <cstring>
#include <vector>

#if defined(HAVE_KRB5) || defined(__APPLE__)
extern "C" {
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
}
#elif defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define SECURITY_WIN32
#include <windows.h>
#include <security.h>
#include <secext.h>
#endif

namespace {

constexpr char kB64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string b64enc(const void *data, size_t len)
{
  const auto *p = static_cast<const unsigned char *>(data);
  std::string out;
  out.reserve(((len + 2) / 3) * 4);
  for (size_t i = 0; i < len; i += 3) {
    unsigned n = p[i] << 16;
    if (i + 1 < len)
      n |= p[i + 1] << 8;
    if (i + 2 < len)
      n |= p[i + 2];
    out.push_back(kB64[(n >> 18) & 63]);
    out.push_back(kB64[(n >> 12) & 63]);
    out.push_back(i + 1 < len ? kB64[(n >> 6) & 63] : '=');
    out.push_back(i + 2 < len ? kB64[n & 63] : '=');
  }
  return out;
}

} // namespace

namespace XioFS {

#if defined(HAVE_KRB5) || defined(__APPLE__)

bool getSpnegoToken(const std::string &host,
                    const std::string &principal,
                    std::string &outTokenB64,
                    std::string &err)
{
  std::string target = "HTTP@" + host;
  gss_buffer_desc nameBuf = {target.size(), const_cast<char *>(target.data())};
  gss_name_t targetName = GSS_C_NO_NAME;
  OM_uint32 min = 0;
  OM_uint32 maj = gss_import_name(&min, &nameBuf,
                                  GSS_C_NT_HOSTBASED_SERVICE,
                                  &targetName);
  if (GSS_ERROR(maj)) {
    err = "gss_import_name failed for service " + target;
    return false;
  }

  gss_cred_id_t cred = GSS_C_NO_CREDENTIAL;
  if (!principal.empty()) {
    gss_buffer_desc princBuf = {principal.size(), const_cast<char *>(principal.data())};
    gss_name_t princName = GSS_C_NO_NAME;
    maj = gss_import_name(&min, &princBuf, GSS_C_NT_USER_NAME, &princName);
    if (!GSS_ERROR(maj)) {
      gss_acquire_cred(&min, princName, GSS_C_INDEFINITE, GSS_C_NO_OID_SET,
                       GSS_C_INITIATE, &cred, nullptr, nullptr);
      gss_release_name(&min, &princName);
    }
  }

  static char spnego_oid_bytes[] = "\x2b\x06\x01\x05\x05\x02";
  static gss_OID_desc spnego_oid_desc = {6, spnego_oid_bytes};
  gss_OID mech = &spnego_oid_desc;

  gss_ctx_id_t ctx = GSS_C_NO_CONTEXT;
  gss_buffer_desc outBuf = GSS_C_EMPTY_BUFFER;
  OM_uint32 flags = 0;

  maj = gss_init_sec_context(&min, cred, &ctx, targetName, mech,
                             GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG,
                             GSS_C_INDEFINITE, GSS_C_NO_CHANNEL_BINDINGS,
                             GSS_C_NO_BUFFER, nullptr, &outBuf, &flags, nullptr);
  if (GSS_ERROR(maj)) {
    if (outBuf.length)
      gss_release_buffer(&min, &outBuf);
    outBuf = GSS_C_EMPTY_BUFFER;
    maj = gss_init_sec_context(&min, cred, &ctx, targetName, GSS_C_NO_OID,
                               GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG,
                               GSS_C_INDEFINITE, GSS_C_NO_CHANNEL_BINDINGS,
                               GSS_C_NO_BUFFER, nullptr, &outBuf, &flags, nullptr);
  }

  if (targetName != GSS_C_NO_NAME)
    gss_release_name(&min, &targetName);
  if (cred != GSS_C_NO_CREDENTIAL)
    gss_release_cred(&min, &cred);
  if (ctx != GSS_C_NO_CONTEXT)
    gss_delete_sec_context(&min, &ctx, GSS_C_NO_BUFFER);

  if (GSS_ERROR(maj) || outBuf.length == 0) {
    err = "GSSAPI failed to establish SPNEGO security context for " + target;
    if (outBuf.length)
      gss_release_buffer(&min, &outBuf);
    return false;
  }

  outTokenB64 = b64enc(outBuf.value, outBuf.length);
  gss_release_buffer(&min, &outBuf);
  return true;
}

#elif defined(_WIN32)

bool getSpnegoToken(const std::string &host,
                    const std::string &principal,
                    std::string &outTokenB64,
                    std::string &err)
{
  CredHandle cred{};
  TimeStamp lifetime{};
  SECURITY_STATUS ss = AcquireCredentialsHandleA(
      nullptr, const_cast<char *>("Negotiate"),
      SECPKG_CRED_OUTBOUND, nullptr, nullptr, nullptr, nullptr,
      &cred, &lifetime);
  if (ss != SEC_E_OK) {
    err = "AcquireCredentialsHandle failed (error " + std::to_string(ss) + ")";
    return false;
  }

  std::string spn = "HTTP/" + host;
  CtxtHandle ctx{};
  ULONG contextAttrs = 0;
  SecBuffer outSecBuf{0, SECBUFFER_TOKEN, nullptr};
  SecBufferDesc outSecDesc{SECBUFFER_VERSION, 1, &outSecBuf};

  std::vector<char> tokBuf(16384);
  outSecBuf.pvBuffer = tokBuf.data();
  outSecBuf.cbBuffer = static_cast<unsigned long>(tokBuf.size());

  ss = InitializeSecurityContextA(
      &cred, nullptr, const_cast<char *>(spn.c_str()),
      ISC_REQ_CONFIDENTIALITY | ISC_REQ_REPLAY_DETECT | ISC_REQ_MUTUAL_AUTH,
      0, SECURITY_NATIVE_DREP, nullptr, 0,
      &ctx, &outSecDesc, &contextAttrs, &lifetime);

  if (ss != SEC_E_OK && ss != SEC_I_CONTINUE_NEEDED) {
    FreeCredentialsHandle(&cred);
    err = "InitializeSecurityContext failed for " + spn + " (error " + std::to_string(ss) + ")";
    return false;
  }

  outTokenB64 = b64enc(outSecBuf.pvBuffer, outSecBuf.cbBuffer);
  DeleteSecurityContext(&ctx);
  FreeCredentialsHandle(&cred);
  return true;
}

#else

bool getSpnegoToken(const std::string &,
                    const std::string &,
                    std::string &,
                    std::string &err)
{
  err = "Kerberos SPNEGO is not available on this platform";
  return false;
}

#endif

} // namespace XioFS
