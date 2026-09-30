//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_BEARER_HH
#define XIOFS_BEARER_HH

#include <string>
#include <sys/types.h>

// Must match XIOFS_BEARER_MAX in kernel/xiofs_uapi.h (WLCG default max).
#ifndef XIOFS_BEARER_MAX
#define XIOFS_BEARER_MAX 4096
#endif

namespace XioFS {

// WLCG bearer-token discovery for uid (see XrdSecztn / bt_u<uid>).
// Does not parse the JWT. The file must be a regular file owned by uid
// with no group or world access bits. Tries:
//   /run/user/<uid>/bt_u<uid>
//   /tmp/bt_u<uid>
// Returns true and sets token on success. missing is true when neither
// path exists (errno ENOENT). Other failures set err and errno (EPERM,
// EMSGSIZE, ...).
bool loadBearerToken(uid_t uid, std::string &token, std::string &err,
                     bool *missing = nullptr);

} // namespace XioFS

#endif
