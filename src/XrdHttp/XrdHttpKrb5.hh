//------------------------------------------------------------------------------
// Kerberos (GSS-API SPNEGO) authentication for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef __XRDHTTPKRB5_HH__
#define __XRDHTTPKRB5_HH__

#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPthread.hh"

#include <string>

class XrdHttpKrb5
{
public:

  enum AuthResult
  {
    kComplete  = 0,
    kContinue  = 1,
    kChallenge = 2,
    kFailed    = 3
  };

  static bool Init(XrdSysError &eDest, const char *keytab, const char *principal);
  static bool IsEnabled() { return enabled; }

  XrdHttpKrb5();
  ~XrdHttpKrb5();

  AuthResult Accept(const char *authHdr,
                    std::string &outToken,
                    std::string &principal,
                    std::string &errMsg);

  bool IsComplete() const { return complete; }

  void Reset();

private:

  void *ctx_;      // gss_ctx_id_t stored opaquely
  bool  complete;

  static bool        enabled;
  static void       *creds_;   // gss_cred_id_t stored opaquely
  static XrdSysMutex initMutex;
};

#endif
