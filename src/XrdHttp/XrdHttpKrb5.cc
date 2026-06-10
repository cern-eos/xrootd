//------------------------------------------------------------------------------
// Kerberos (GSS-API SPNEGO) authentication for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------

#include "XrdHttpKrb5.hh"
#include "XrdHttpUtils.hh"
#include "XrdNet/XrdNetUtils.hh"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <vector>

extern "C" {
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
}

namespace
{
const char *TraceID = "HttpKrb5";

bool        gssInitialized = false;
gss_cred_id_t gssCreds     = GSS_C_NO_CREDENTIAL;
XrdSysMutex   gssInitMutex;

std::string gssErrMsg(OM_uint32 maj, OM_uint32 min)
{
  OM_uint32 msg_ctx = 0;
  OM_uint32 lmaj, lmin;
  gss_buffer_desc buf = GSS_C_EMPTY_BUFFER;
  std::string out;

  lmaj = gss_display_status(&lmin, maj, GSS_C_GSS_CODE, GSS_C_NO_OID,
                            &msg_ctx, &buf);
  if (!lmaj && buf.value && buf.length)
    out.assign((const char *)buf.value, buf.length);
  gss_release_buffer(&lmin, &buf);

  msg_ctx = 0;
  lmaj = gss_display_status(&lmin, min, GSS_C_MECH_CODE, GSS_C_NO_OID,
                            &msg_ctx, &buf);
  if (!lmaj && buf.value && buf.length) {
    if (!out.empty()) out += "; ";
    out += (const char *)buf.value;
  }
  gss_release_buffer(&lmin, &buf);

  return out.empty() ? "GSS error" : out;
}

std::string expandHostKeyword(const char *principal)
{
  if (!principal || !*principal) return std::string();

  std::string result(principal);
  auto pos = result.find("<host>");
  if (pos == std::string::npos) return result;

  char *hn = XrdNetUtils::MyHostName();
  if (!hn) return std::string();

  result.replace(pos, 6, hn);
  free(hn);
  return result;
}

bool extractNegotiateToken(const char *authHdr, std::vector<uint8_t> &token)
{
  token.clear();
  if (!authHdr || !*authHdr) return false;

  while (*authHdr && std::isspace(static_cast<unsigned char>(*authHdr)))
    ++authHdr;

  static const char kNegotiate[] = "Negotiate";
  if (strncasecmp(authHdr, kNegotiate, sizeof(kNegotiate) - 1))
    return false;

  authHdr += sizeof(kNegotiate) - 1;
  while (*authHdr && std::isspace(static_cast<unsigned char>(*authHdr)))
    ++authHdr;

  if (!*authHdr) return false;

  std::string b64(authHdr);
  while (!b64.empty() &&
         (b64.back() == '\r' || b64.back() == '\n' ||
          std::isspace(static_cast<unsigned char>(b64.back()))))
    b64.pop_back();

  if (b64.empty()) return false;

  base64ToBytes(b64, token);
  return !token.empty();
}
}

bool XrdHttpKrb5::enabled = false;
void *XrdHttpKrb5::creds_ = 0;
XrdSysMutex XrdHttpKrb5::initMutex;

bool XrdHttpKrb5::Init(XrdSysError &eDest, const char *keytab,
                       const char *principal)
{
  XrdSysMutexHelper m(initMutex);

  if (!keytab || !*keytab || !principal || !*principal) {
    eDest.Emsg(TraceID, "http.auth krb5 requires a keytab and principal.");
    return false;
  }

  std::string kprinc = expandHostKeyword(principal);
  if (kprinc.empty()) {
    eDest.Emsg(TraceID, "Unable to expand <host> in Kerberos principal.");
    return false;
  }

  if (krb5_gss_register_acceptor_identity(keytab) != 0) {
    eDest.Emsg(TraceID, "Unable to register Kerberos acceptor keytab:", keytab);
    return false;
  }

  gss_buffer_desc nameBuf;
  nameBuf.length = kprinc.size();
  nameBuf.value  = (void *)kprinc.c_str();

  gss_name_t gssName = GSS_C_NO_NAME;
  OM_uint32 maj, min;
  maj = gss_import_name(&min, &nameBuf, GSS_C_NT_USER_NAME, &gssName);
  if (maj != GSS_S_COMPLETE) {
    eDest.Emsg(TraceID, "Unable to import Kerberos principal:",
               kprinc.c_str(), gssErrMsg(maj, min).c_str());
    return false;
  }

  maj = gss_acquire_cred(&min, gssName, GSS_C_INDEFINITE, GSS_C_NO_OID_SET,
                         GSS_C_ACCEPT, &gssCreds, nullptr, nullptr);
  gss_release_name(&min, &gssName);

  if (maj != GSS_S_COMPLETE) {
    eDest.Emsg(TraceID, "Unable to acquire Kerberos acceptor credentials for",
               kprinc.c_str(), gssErrMsg(maj, min).c_str());
    return false;
  }

  creds_ = gssCreds;
  enabled = true;
  gssInitialized = true;

  eDest.Say("Config http.auth krb5 enabled for principal ", kprinc.c_str());
  return true;
}

XrdHttpKrb5::XrdHttpKrb5()
: ctx_(GSS_C_NO_CONTEXT), complete(false)
{
}

XrdHttpKrb5::~XrdHttpKrb5()
{
  Reset();
}

void XrdHttpKrb5::Reset()
{
  if (ctx_ != GSS_C_NO_CONTEXT) {
    OM_uint32 min;
    gss_delete_sec_context(&min, (gss_ctx_id_t *)&ctx_, GSS_C_NO_BUFFER);
    ctx_ = GSS_C_NO_CONTEXT;
  }
  complete = false;
}

XrdHttpKrb5::AuthResult XrdHttpKrb5::Accept(const char *authHdr,
                                            std::string &outToken,
                                            std::string &principal,
                                            std::string &errMsg)
{
  outToken.clear();
  principal.clear();
  errMsg.clear();

  if (!enabled || !gssInitialized)
    {errMsg = "Kerberos authentication is not configured.";
     return kFailed;}

  if (complete)
    return kComplete;

  std::vector<uint8_t> inBytes;
  const bool hasToken = extractNegotiateToken(authHdr, inBytes);
  if (!hasToken)
    return kChallenge;

  gss_buffer_desc inBuf = GSS_C_EMPTY_BUFFER;
  inBuf.length = inBytes.size();
  inBuf.value  = inBytes.data();

  gss_buffer_desc outBuf = GSS_C_EMPTY_BUFFER;
  gss_name_t clientName = GSS_C_NO_NAME;
  gss_OID mechType = GSS_C_NO_OID;
  OM_uint32 retFlags = 0;
  OM_uint32 maj, min;

  maj = gss_accept_sec_context(&min, (gss_ctx_id_t *)&ctx_, gssCreds,
                               &inBuf, GSS_C_NO_CHANNEL_BINDINGS,
                               &clientName, &mechType, &outBuf, &retFlags,
                               nullptr, nullptr);

  if (outBuf.length) {
    std::vector<uint8_t> outVec((uint8_t *)outBuf.value,
                                (uint8_t *)outBuf.value + outBuf.length);
    Tobase64(outVec, outToken);
    gss_release_buffer(&min, &outBuf);
  }

  if (maj == GSS_S_CONTINUE_NEEDED)
    return kContinue;

  if (maj != GSS_S_COMPLETE) {
    errMsg = gssErrMsg(maj, min);
    Reset();
    return kFailed;
  }

  gss_buffer_desc dispBuf = GSS_C_EMPTY_BUFFER;
  maj = gss_display_name(&min, clientName, &dispBuf, nullptr);
  gss_release_name(&min, &clientName);

  if (maj != GSS_S_COMPLETE || !dispBuf.value || !dispBuf.length) {
    errMsg = "Unable to obtain Kerberos client principal.";
    gss_release_buffer(&min, &dispBuf);
    Reset();
    return kFailed;
  }

  principal.assign((const char *)dispBuf.value, dispBuf.length);
  gss_release_buffer(&min, &dispBuf);

  complete = true;
  return kComplete;
}
