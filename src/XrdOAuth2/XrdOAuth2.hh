/******************************************************************************/
/*                                                                            */
/*                         X r d O A u t h 2 . h h                            */
/*                                                                            */
/* (c) 2026 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/******************************************************************************/

#ifndef XRDOAUTH2_HH
#define XRDOAUTH2_HH

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSec/XrdSecInterface.hh"

class XrdOucErrInfo;
class XrdSysLogger;

namespace XrdOAuth2
{

inline constexpr std::string_view kProtocolId = "oauth2";
inline constexpr std::size_t kProtocolPrefixSize = kProtocolId.size() + 1;

inline char *dupString(std::string_view s)
{
   if (s.empty()) return nullptr;
   auto *p = static_cast<char *>(std::malloc(s.size() + 1));
   if (!p) return nullptr;
   std::memcpy(p, s.data(), s.size());
   p[s.size()] = '\0';
   return p;
}

inline void resetEntityField(char *&field) noexcept
{
   std::free(field);
   field = nullptr;
}

inline void setEntityField(char *&field, std::string_view value)
{
   resetEntityField(field);
   field = dupString(value);
}

inline void setEntityFieldOptional(char *&field, std::string_view value)
{
   resetEntityField(field);
   if (!value.empty()) field = dupString(value);
}

inline void copyEntityField(char *&dest, const char *src)
{
   resetEntityField(dest);
   if (src && *src) dest = dupString(src);
}

inline void fillCredentialBuffer(std::vector<char> &buf, std::string_view token)
{
   const std::size_t bsz = kProtocolPrefixSize + token.size() + 1;
   buf.resize(bsz);
   std::memcpy(buf.data(), kProtocolId.data(), kProtocolId.size());
   buf[kProtocolId.size()] = '\0';
   if (!token.empty())
      std::memcpy(buf.data() + kProtocolPrefixSize, token.data(), token.size());
   buf[kProtocolPrefixSize + token.size()] = '\0';
}

inline void copyProtocolId(char (&prot)[XrdSecPROTOIDSIZE])
{
   static_assert(XrdSecPROTOIDSIZE > kProtocolId.size());
   std::memcpy(prot, kProtocolId.data(), kProtocolId.size() + 1);
}

inline bool hasPrefix(std::string_view s, std::string_view prefix)
{
   return s.size() >= prefix.size() && s.substr(0, prefix.size()) == prefix;
}

inline bool hasSuffix(std::string_view s, std::string_view suffix)
{
   return s.size() >= suffix.size()
       && s.substr(s.size() - suffix.size()) == suffix;
}

inline std::unique_ptr<XrdSecCredentials>
makeCredentials(std::string_view token)
{
   const std::size_t bsz = kProtocolPrefixSize + token.size() + 1;
   // XrdSecBuffer::~XrdSecBuffer() releases buffer with free(), so the
   // allocation must come from malloc(), not new[].
   auto *buf = static_cast<char *>(std::malloc(bsz));
   if (!buf) return nullptr;
   std::memcpy(buf, kProtocolId.data(), kProtocolId.size());
   buf[kProtocolId.size()] = '\0';
   if (!token.empty())
      std::memcpy(buf + kProtocolPrefixSize, token.data(), token.size());
   buf[kProtocolPrefixSize + token.size()] = '\0';
   return std::make_unique<XrdSecCredentials>(buf, static_cast<int>(bsz));
}

inline std::optional<std::string_view>
tokenFromCredentialBuffer(const char *buffer, int size)
{
   if (!buffer || size <= static_cast<int>(kProtocolPrefixSize)) return std::nullopt;
   const std::string_view cred(buffer, static_cast<std::size_t>(size));
   if (cred.find('\0') == std::string_view::npos) return std::nullopt;
   if (std::string_view(buffer) != kProtocolId) return std::nullopt;
   const std::size_t tokenOff = kProtocolPrefixSize;
   if (tokenOff >= cred.size()) return std::nullopt;
   const std::size_t tokenEnd = cred.find('\0', tokenOff);
   if (tokenEnd == std::string_view::npos || tokenEnd <= tokenOff) return std::nullopt;
   return cred.substr(tokenOff, tokenEnd - tokenOff);
}

//------------------------------------------------------------------------------
//! Initialize OAuth2 bearer JWT validation using the same options as
//! sec.protocol oauth2.
//! @return true on success; false with emsg set on failure.
//------------------------------------------------------------------------------
bool Init(XrdSysLogger *logger, const char *parms, std::string &emsg);

//------------------------------------------------------------------------------
//! sec.protocol-compatible initializer. Returns a malloc'd protocol parameter
//! string on success, or nullptr on failure (details in erp).
//------------------------------------------------------------------------------
char *InitSecProtocol(const char *parms, XrdOucErrInfo *erp);

//------------------------------------------------------------------------------
//! True after a successful Init with at least one issuer configured.
//------------------------------------------------------------------------------
bool IsConfigured();

//------------------------------------------------------------------------------
//! Strip whitespace and an optional Bearer prefix from a token buffer.
//! When maxLen >= 0 the input is treated as length-bounded untrusted data.
//------------------------------------------------------------------------------
const char *StripToken(const char *bTok, int &sz, int maxLen = -1);

//------------------------------------------------------------------------------
//! Validate a JWT and return the mapped local username in identity.
//! Uses the shared validated-token cache when enabled.
//! Optional roleOut/grpsOut are filled when -role-claim / -grps-claim are set.
//------------------------------------------------------------------------------
bool ValidateToken(const char *rawTok, std::string &identity, std::string &emsg,
                   uint64_t *expTime = nullptr,
                   std::map<std::string, std::string> *entityAttrs = nullptr,
                   std::string *roleOut = nullptr,
                   std::string *grpsOut = nullptr);

//------------------------------------------------------------------------------
//! Return the cached expiry (unix seconds) of an already-validated token, or 0
//! if it is not in the validated-token cache. Cheap: hashes the token and does a
//! cache lookup only (no signature/JWKS/JSON work). When maxLen >= 0 the input
//! is treated as length-bounded untrusted data.
//------------------------------------------------------------------------------
uint64_t CachedTokenExpiry(const char *rawTok, int maxLen = -1);

} // namespace XrdOAuth2

#endif
