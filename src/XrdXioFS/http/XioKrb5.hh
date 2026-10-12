//------------------------------------------------------------------------------
// Kerberos SPNEGO helper for XIOFS (Unix GSS-API / Windows SSPI).
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_KRB5_HH
#define XIOFS_KRB5_HH

#include <string>

namespace XioFS {

// Obtains a Kerberos SPNEGO Negotiate authorization token (base64)
// for the given host and optional principal.
// Returns true on success, false on failure with err populated.
bool getSpnegoToken(const std::string &host,
                    const std::string &principal,
                    std::string &outTokenB64,
                    std::string &err);

} // namespace XioFS

#endif // XIOFS_KRB5_HH
