/******************************************************************************/
/*                                                                            */
/*                         X r d O A u t h 2 . c c                          */
/*                                                                            */
/* (c) 2026 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/******************************************************************************/

#define __STDC_FORMAT_MACROS 1

#include <cctype>
#include <cerrno>
#include <charconv>
#include <algorithm>
#include <cinttypes>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <atomic>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <curl/curl.h>
#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
#include <openssl/param_build.h>
#include <openssl/params.h>
#endif

#include <fcntl.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdOuc/XrdOucJson.hh"
#include "XrdOAuth2/XrdOAuth2.hh"
#include "XrdOAuth2/XrdOAuth2Detail.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdSys/XrdSysE2T.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"

#ifndef EAUTH
#define EAUTH EBADE
#endif

namespace XrdOAuth2
{
namespace detail
{
struct CurlDeleter {void operator()(CURL *p) const noexcept {curl_easy_cleanup(p);}};
using CurlPtr = std::unique_ptr<CURL, CurlDeleter>;

#if OPENSSL_VERSION_NUMBER >= 0x30000000L
struct OsslParamBldDeleter {void operator()(OSSL_PARAM_BLD *p) const noexcept {OSSL_PARAM_BLD_free(p);}};
struct OsslParamDeleter    {void operator()(OSSL_PARAM *p)     const noexcept {OSSL_PARAM_free(p);}};
using OsslParamBldPtr = std::unique_ptr<OSSL_PARAM_BLD, OsslParamBldDeleter>;
using OsslParamPtr    = std::unique_ptr<OSSL_PARAM, OsslParamDeleter>;
#else
struct RsaDeleter {void operator()(RSA *p) const noexcept {RSA_free(p);}};
using RsaPtr = std::unique_ptr<RSA, RsaDeleter>;
#endif

/******************************************************************************/
/*                                F a t a l                                 */
/******************************************************************************/

void Fatal(XrdOucErrInfo *erp, const char *eMsg, int rc, bool hdr=true)
{
   if (!erp) std::cerr <<(hdr ? "Secoauth2: " : "") <<eMsg <<"\n" <<std::flush;
      else {const char *eVec[2] = {(hdr ? "Secoauth2: " : ""), eMsg};
            erp->setErrInfo(rc, eVec, 2);
           }
}

/******************************************************************************/
/*                             t r i m C o p y                              */
/******************************************************************************/

std::string trimCopy(const std::string &str)
{
   size_t b = 0, e = str.size();
   while (b < e && isspace(static_cast<unsigned char>(str[b]))) b++;
   while (e > b && isspace(static_cast<unsigned char>(str[e - 1]))) e--;
   return str.substr(b, e - b);
}

/******************************************************************************/
/*                          t o L o w e r C o p y                           */
/******************************************************************************/

std::string toLowerCopy(std::string str)
{
   std::transform(str.begin(), str.end(), str.begin(),
      [](unsigned char c) {
         return static_cast<char>(std::tolower(c));
      });
   return str;
}

/******************************************************************************/
/*                            p a r s e B o o l                             */
/******************************************************************************/

bool parseBool(const std::string &str, bool &out)
{
   std::string v = toLowerCopy(trimCopy(str));
   if (v.empty() || v == "1" || v == "true" || v == "yes" || v == "on")
      {out = true; return true;}
   if (v == "0" || v == "false" || v == "no" || v == "off")
      {out = false; return true;}
   return false;
}

/******************************************************************************/
/*                             b 6 4 V a l u e                              */
/******************************************************************************/

int b64Value(unsigned char c)
{
   if (c >= 'A' && c <= 'Z') return c - 'A';
   if (c >= 'a' && c <= 'z') return c - 'a' + 26;
   if (c >= '0' && c <= '9') return c - '0' + 52;
   if (c == '+') return 62;
   if (c == '/') return 63;
   return -1;
}

/******************************************************************************/
/*                      d e c o d e B a s e 6 4 U R L                       */
/******************************************************************************/

bool decodeBase64URL(const std::string &in, std::string &out)
{
   out.clear();
   if (in.empty()) return false;

   std::string b64 = in;
   for (char &c : b64)
      {if (c == '-') c = '+';
       else if (c == '_') c = '/';
      }
   while ((b64.size() % 4) != 0) b64.push_back('=');

   out.reserve((b64.size() / 4) * 3);
   int val = 0;
   int bits = -8;
   for (unsigned char c : b64)
      {if (isspace(c)) continue;
       if (c == '=') break;
       int d = b64Value(c);
       if (d < 0) return false;
       val = (val << 6) | d;
       bits += 6;
       if (bits >= 0)
          {out.push_back(char((val >> bits) & 0xFF));
           bits -= 8;
          }
      }
   return !out.empty();
}

/******************************************************************************/
/*                      e n c o d e B a s e 6 4 U R L                       */
/******************************************************************************/

std::string encodeBase64URL(const std::string &in)
{
   static const char tbl[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
   std::string out;
   out.reserve(((in.size() + 2) / 3) * 4);
   size_t i = 0;
   while (i + 2 < in.size())
      {uint32_t v = (uint32_t((unsigned char)in[i]) << 16)
                  | (uint32_t((unsigned char)in[i + 1]) << 8)
                  |  uint32_t((unsigned char)in[i + 2]);
       out.push_back(tbl[(v >> 18) & 0x3F]);
       out.push_back(tbl[(v >> 12) & 0x3F]);
       out.push_back(tbl[(v >> 6) & 0x3F]);
       out.push_back(tbl[v & 0x3F]);
       i += 3;
      }
   if (i < in.size())
      {uint32_t v = uint32_t((unsigned char)in[i]) << 16;
       out.push_back(tbl[(v >> 18) & 0x3F]);
       if (i + 1 < in.size())
          {v |= uint32_t((unsigned char)in[i + 1]) << 8;
           out.push_back(tbl[(v >> 12) & 0x3F]);
           out.push_back(tbl[(v >> 6) & 0x3F]);
          } else out.push_back(tbl[(v >> 12) & 0x3F]);
      }
   return out;
}

/******************************************************************************/
/*                            s h a 2 5 6 h e x                             */
/******************************************************************************/

std::string sha256hex(const char *data, size_t len)
{
   unsigned char md[EVP_MAX_MD_SIZE];
   unsigned int mdLen = 0;
   // Fail closed: return an empty string rather than fall back to using the raw
   // token bytes as a cache key (callers skip caching on an empty key).
   if (!EVP_Digest(data, len, md, &mdLen, EVP_sha256(), nullptr))
      return std::string();
   static const char hex[] = "0123456789abcdef";
   std::string out;
   out.reserve(mdLen * 2);
   for (unsigned int i = 0; i < mdLen; ++i)
      {out.push_back(hex[md[i] >> 4]);
       out.push_back(hex[md[i] & 0xf]);
      }
   return out;
}

/******************************************************************************/
/*                      p a r s e J s o n O b j e c t                       */
/******************************************************************************/

bool parseJsonObject(const std::string &json, nlohmann::json &obj)
{
   obj = nlohmann::json::parse(json, 0, false);
   return !obj.is_discarded() && obj.is_object();
}

/******************************************************************************/
/*                       g e t S t r i n g C l a i m                        */
/******************************************************************************/

bool getStringClaim(const nlohmann::json &obj, const char *claim, std::string &val)
{
   if (!obj.is_object()) return false;
   auto it = obj.find(claim);
   if (it == obj.end() || !it->is_string()) return false;
   val = it->get<std::string>();
   return !val.empty();
}

/******************************************************************************/
/*                       g e t S t r i n g C l a i m                        */
/******************************************************************************/

bool getStringClaim(const std::string &json, const char *claim, std::string &val)
{
   nlohmann::json obj;
   if (!parseJsonObject(json, obj)) return false;
   return getStringClaim(obj, claim, val);
}

/******************************************************************************/
/*                  g e t E n t i t y C l a i m V a l u e                   */
/******************************************************************************/

bool getEntityClaimValue(const nlohmann::json &obj, const char *claim,
                         std::string &val)
{
   if (!obj.is_object()) return false;
   auto it = obj.find(claim);
   if (it == obj.end()) return false;
   if (it->is_string())
      {
       val = it->get<std::string>();
       return !val.empty();
      }
   if (it->is_array())
      {
       val.clear();
       for (const auto &elem : *it)
          {
           if (!elem.is_string()) continue;
           const std::string one = elem.get<std::string>();
           if (one.empty()) continue;
           if (!val.empty()) val.push_back(' ');
           val += one;
          }
       return !val.empty();
      }
   return false;
}

std::vector<EntityClaimMapping> EntityClaimMappings;
bool customEntityClaims = false;

/******************************************************************************/
/*       i n i t D e f a u l t E n t i t y C l a i m M a p p i n g s        */
/******************************************************************************/

void initDefaultEntityClaimMappings()
{
   EntityClaimMappings.push_back({"preferred_username", "token.preferred_username"});
   EntityClaimMappings.push_back({"name", "token.name"});
}

/******************************************************************************/
/*                  i s V a l i d J w t C l a i m N a m e                   */
/******************************************************************************/

bool isValidJwtClaimName(const std::string &claim)
{
   if (claim.empty()) return false;
   for (unsigned char c : claim)
      if (!isalnum(c) && c != '_' && c != '-' && c != '.') return false;
   return true;
}

/******************************************************************************/
/*                  i s S a f e E n t i t y A t t r K e y                   */
/******************************************************************************/

bool isSafeEntityAttrKey(const std::string &key)
{
   if (key.empty()) return false;
   for (unsigned char c : key)
      if (!isalnum(c) && c != '.' && c != '_' && c != '-') return false;
   return true;
}

/******************************************************************************/
/*                i s S a f e E n t i t y A t t r V a l u e                 */
/******************************************************************************/

bool isSafeEntityAttrValue(const std::string &val)
{
   if (val.empty() || val.size() > 4096) return false;
   for (unsigned char c : val)
      if (c < 0x20 || c == 0x7f) return false;
   return true;
}

// Validate a resolved identity before it becomes XrdSecEntity.name. The value
// is used for authorization, mapping and logging, so reject control characters,
// whitespace, path separators and the "." / ".." path components. Opaque
// identifiers (UUIDs, emails, preferred usernames) remain acceptable.
/******************************************************************************/
/*                       i s S a f e I d e n t i t y                        */
/******************************************************************************/

bool isSafeIdentity(const std::string &id)
{
   if (id.empty() || id.size() > 256) return false;
   for (unsigned char c : id)
      {if (c < 0x20 || c == 0x7f) return false; // control characters
       if (c == ' ' || c == '\t') return false; // embedded whitespace
       if (c == '/' || c == '\\') return false; // path separators
      }
   if (id == "." || id == "..") return false;
   return true;
}

/******************************************************************************/
/*                 p a r s e E n t i t y C l a i m S p e c                  */
/******************************************************************************/

bool parseEntityClaimSpec(const std::string &specIn, EntityClaimMapping &out,
                          std::string &emsg)
{
   std::string spec = trimCopy(specIn);
   if (spec.empty())
      {emsg = "empty entity-claim entry";
       return false;
      }
   size_t eq = spec.find('=');
   if (eq != std::string::npos)
      {out.jwtClaim = trimCopy(spec.substr(0, eq));
       out.attrKey = trimCopy(spec.substr(eq + 1));
      }
   else
      {out.jwtClaim = spec;
       out.attrKey = "token." + spec;
      }
   if (!isValidJwtClaimName(out.jwtClaim))
      {emsg = "invalid JWT claim name in entity-claim: " + out.jwtClaim;
       return false;
      }
   if (!isSafeEntityAttrKey(out.attrKey))
      {emsg = "invalid entity attribute key in entity-claim: " + out.attrKey;
       return false;
      }
   return true;
}

/******************************************************************************/
/*                s t o r e E n t i t y C l a i m E n t r y                 */
/******************************************************************************/

bool storeEntityClaimEntry(const std::string &spec, std::string &emsg)
{
   if (!customEntityClaims)
      {EntityClaimMappings.clear();
       customEntityClaims = true;
      }
   EntityClaimMapping mapping;
   if (!parseEntityClaimSpec(spec, mapping, emsg)) return false;
   EntityClaimMappings.push_back(mapping);
   return true;
}

bool splitCSV(const std::string &val, std::vector<std::string> &out);

/******************************************************************************/
/*                c a n o n i c a l A b s o l u t e P a t h                 */
/******************************************************************************/

bool canonicalAbsolutePath(const std::string &path, std::string &result)
{
   if (path.empty() || path[0] != '/') return false;

   size_t pos = 0;
   std::vector<std::string> components;
   do
      {
       while (pos < path.size() && path[pos] == '/') pos++;
       size_t nextPos = path.find('/', pos);
       std::string component = path.substr(pos, nextPos - pos);
       pos = nextPos;
       if (component.empty() || component == ".") continue;
       if (component == "..")
          {if (!components.empty()) components.pop_back();
          }
       else components.push_back(component);
      }
   while (pos != std::string::npos);

   if (components.empty())
      {result = "/";
       return true;
      }
   result.clear();
   for (const auto &component : components) result += "/" + component;
   return true;
}

/******************************************************************************/
/*                    s e t I s s u e r B a s e P a t h                     */
/******************************************************************************/

bool setIssuerBasePath(const std::string &val, std::string &target,
                       std::string &emsg)
{
   if (!canonicalAbsolutePath(val, target))
      {emsg = "invalid absolute path: " + val;
       return false;
      }
   return true;
}

/******************************************************************************/
/*               a p p e n d I s s u e r P a t h O p t i o n                */
/******************************************************************************/

bool appendIssuerPathOption(const std::string &val,
                            std::vector<std::string> &target,
                            std::string &emsg)
{
   std::string normalized;
   if (!canonicalAbsolutePath(val, normalized))
      {emsg = "invalid absolute path: " + val;
       return false;
      }
   target.push_back(normalized);
   return true;
}

/******************************************************************************/
/*                     p a t h s T o J s o n A r r a y                      */
/******************************************************************************/

std::string pathsToJsonArray(const std::vector<std::string> &paths)
{
   nlohmann::json arr = nlohmann::json::array();
   for (const auto &path : paths) arr.push_back(path);
   return arr.dump();
}

/******************************************************************************/
/*                  c o l l e c t E n t i t y C l a i m s                   */
/******************************************************************************/

void collectEntityClaims(const std::string &payloadJSON,
                         std::map<std::string, std::string> &out)
{
   if (EntityClaimMappings.empty()) return;
   nlohmann::json payloadObj;
   if (!parseJsonObject(payloadJSON, payloadObj)) return;
   for (const auto &mapping : EntityClaimMappings)
      {std::string val;
       if (getEntityClaimValue(payloadObj, mapping.jwtClaim.c_str(), val)
       &&  isSafeEntityAttrValue(val))
          out[mapping.attrKey] = val;
      }
}

// Strip whitespace and optional Bearer prefix from a length-bounded buffer.
// maxLen must be the number of usable bytes at bTok; strlen is never called.
/******************************************************************************/
/*                                S t r i p                                 */
/******************************************************************************/

const char *Strip(const char *bTok, int &sz, int maxLen)
{
   const char *sTok = bTok;
   sz = 0;
   if (!sTok || maxLen == 0) return 0;

   // Determine safe end pointer without calling strlen on untrusted data.
   const char *endPtr;
   if (maxLen < 0)
      {// Legacy call-site that already guarantees NUL termination.
       endPtr = sTok + std::strlen(sTok);
      }
   else
      {// Server-side path: bound by explicit length; find NUL within range.
       endPtr = sTok + maxLen;
       const void *nul = std::memchr(sTok, '\0', static_cast<std::size_t>(maxLen));
       if (nul) endPtr = static_cast<const char *>(nul);
      }

   while (sTok < endPtr && isspace(static_cast<unsigned char>(*sTok))) sTok++;
   if (sTok >= endPtr) return 0;

   const std::string_view slice(sTok, static_cast<std::size_t>(endPtr - sTok));
   if (slice.size() >= 9 && hasPrefix(slice, "Bearer%20")) sTok += 9;
   else if (slice.size() >= 7 && hasPrefix(slice, "Bearer ")) sTok += 7;

   while (sTok < endPtr && isspace(static_cast<unsigned char>(*sTok))) sTok++;
   if (sTok >= endPtr) return 0;

   while (endPtr > sTok && isspace(static_cast<unsigned char>(*(endPtr-1)))) endPtr--;
   sz = static_cast<int>(endPtr - sTok);
   return (sz > 0 ? sTok : 0);
}

std::atomic<int> expiry{1}; // 1=require, 0=ignore, -1=optional
std::atomic<int> MaxTokSize{8192};
std::atomic<int> ClockSkew{60};
std::atomic<int> JwksRefresh{300};
bool customIdentityClaims = false;
std::atomic<bool> DebugToken{false};
std::atomic<bool> DebugTokenClaims{false};
std::atomic<int> TokenCacheMax{10000};
std::atomic<int> TokenCacheNoExpTTL{60};
std::string JwksCacheFile;
int JwksCacheTTL = 0;
std::vector<std::string> IdentityClaims = {
   "sub", "username", "upn"
};
std::string RoleClaim;
std::string GrpsClaim;

/******************************************************************************/
/*                  s e t N a t i v e F i e l d C l a i m                   */
/******************************************************************************/

bool setNativeFieldClaim(const std::string &claim, std::string &target,
                         const char *optName, std::string &emsg)
{
   if (claim.empty())
      {emsg = std::string("empty ") + optName + " value";
       return false;
      }
   if (!isValidJwtClaimName(claim))
      {emsg = std::string("invalid JWT claim name in ") + optName + ": " + claim;
       return false;
      }
   target = claim;
   return true;
}

/******************************************************************************/
/*           p o p u l a t e E n t i t y N a t i v e F i e l d s            */
/******************************************************************************/

void populateEntityNativeFields(const std::string &payloadJSON,
                                std::string *roleOut,
                                std::string *grpsOut)
{
   if ((!roleOut || RoleClaim.empty()) && (!grpsOut || GrpsClaim.empty())) return;
   nlohmann::json payloadObj;
   if (!parseJsonObject(payloadJSON, payloadObj)) return;
   if (roleOut && !RoleClaim.empty())
      {std::string val;
       if (getEntityClaimValue(payloadObj, RoleClaim.c_str(), val)
       &&  isSafeEntityAttrValue(val))
          *roleOut = val;
      }
   if (grpsOut && !GrpsClaim.empty())
      {std::string val;
       if (getEntityClaimValue(payloadObj, GrpsClaim.c_str(), val)
       &&  isSafeEntityAttrValue(val))
          *grpsOut = val;
      }
}

void freeKeys(std::map<std::string, EvpPkeyPtr> &keys);

/******************************************************************************/
/*                        ~ I s s u e r P o l i c y                         */
/******************************************************************************/

IssuerPolicy::~IssuerPolicy()
{
   std::scoped_lock lock(keysMtx);
   freeKeys(jwksKeys);
}

std::vector<std::shared_ptr<IssuerPolicy>> IssuerPolicies;
std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> IssuerPolicyByIssuer;
std::unordered_map<std::string, std::string> EmailIdentityMap;
std::mutex ConfigMtx;
/******************************************************************************/
/*           a d d I s s u e r P o l i c y E n t i t y A t t r s            */
/******************************************************************************/

void addIssuerPolicyEntityAttrs(const std::shared_ptr<IssuerPolicy> &policy,
                                std::map<std::string, std::string> &out)
{
   if (!policy) return;
   if (!policy->basePath.empty() && isSafeEntityAttrValue(policy->basePath))
      out["base_path"] = policy->basePath;
   if (!policy->restrictedPaths.empty())
      {std::string json = pathsToJsonArray(policy->restrictedPaths);
       if (isSafeEntityAttrValue(json)) out["restricted_path"] = json;
      }
}

/******************************************************************************/
/*        l o o k u p I s s u e r P o l i c y F r o m P a y l o a d         */
/******************************************************************************/

bool lookupIssuerPolicyFromPayload(const std::string &payloadJSON,
                                   std::shared_ptr<IssuerPolicy> &policy)
{
   nlohmann::json payloadObj;
   if (!parseJsonObject(payloadJSON, payloadObj)) return false;
   std::string iss;
   if (!getStringClaim(payloadObj, "iss", iss)) return false;
   std::scoped_lock lock(ConfigMtx);
   auto pIt = IssuerPolicyByIssuer.find(iss);
   if (pIt == IssuerPolicyByIssuer.end()) return false;
   policy = pIt->second;
   return true;
}

/******************************************************************************/
/*                  p o p u l a t e E n t i t y A t t r s                   */
/******************************************************************************/

void populateEntityAttrs(const std::string &payloadJSON,
                         std::map<std::string, std::string> &out)
{
   out.clear();
   collectEntityClaims(payloadJSON, out);
   std::shared_ptr<IssuerPolicy> policy;
   if (lookupIssuerPolicyFromPayload(payloadJSON, policy))
      addIssuerPolicyEntityAttrs(policy, out);
}

std::string OAuth2ConfigPath = "/etc/xrootd/oauth2.cfg";
bool OAuth2ConfigWatch = false;
bool OAuth2ConfigStatValid = false;
ino_t OAuth2ConfigIno = 0;
time_t OAuth2ConfigMTime = 0;
std::string InitProtocolParams;

XrdSysLogger OAuth2Logger;
XrdSysError  OAuth2Log(0, "secoauth2_");

std::mutex TokenCacheMtx;
std::unordered_map<std::string, CachedTokenEntry> TokenCache;
std::atomic<uint64_t> TokenCacheHits(0);
std::atomic<uint64_t> TokenCacheMisses(0);

// Short-lived negative cache of token hashes that failed validation. It bounds
// the cost of an attacker (or a misconfigured client) replaying the same
// invalid token: repeated identical failures are rejected cheaply instead of
// re-parsing/re-verifying (and possibly triggering JWKS work) every time.
constexpr size_t kNegTokenCacheMax = 10000;  // hard cap on entries
std::mutex TokenNegCacheMtx;
std::unordered_map<std::string, uint64_t> TokenNegCache; // hash -> expiresAt

std::mutex JwksDiskCacheMtx;

struct JwksDiskEntry {
   std::string jwksUrl;
   time_t fetchedAt = 0;
   std::string jwksB64;
};

/******************************************************************************/
/*                             f r e e K e y s                              */
/******************************************************************************/

void freeKeys(std::map<std::string, EvpPkeyPtr> &keys)
{
   // The map now owns each EVP_PKEY through a unique_ptr, so clearing it
   // releases every key without manual EVP_PKEY_free() calls.
   keys.clear();
}

/******************************************************************************/
/*                  c l e a r I s s u e r P o l i c i e s                   */
/******************************************************************************/

void clearIssuerPolicies()
{
   for (auto &p : IssuerPolicies)
      {
       if (!p) continue;
       std::scoped_lock lock(p->keysMtx);
       freeKeys(p->jwksKeys);
      }
   IssuerPolicies.clear();
   IssuerPolicyByIssuer.clear();
}

/******************************************************************************/
/*                              j o i n U R L                               */
/******************************************************************************/

std::string joinURL(const std::string &base, const char *sfx)
{
   if (base.empty()) return std::string();
   if (hasSuffix(base, "/")) return base + (sfx[0] == '/' ? sfx + 1 : sfx);
   return base + (sfx[0] == '/' ? sfx : std::string("/") + sfx);
}

/******************************************************************************/
/*                    n o r m a l i z e E m a i l K e y                     */
/******************************************************************************/

std::string normalizeEmailKey(const std::string &email)
{
   return toLowerCopy(trimCopy(email));
}

/******************************************************************************/
/*                         a p p e n d C l i O p t                          */
/******************************************************************************/

void appendCliOpt(std::string &opts, const std::string &key, const std::string *val = nullptr)
{
   if (!opts.empty()) opts.push_back(' ');
   opts += key;
   if (val)
      {opts.push_back(' ');
       opts += *val;
      }
}

/******************************************************************************/
/*              s t a r t s W i t h I s s u e r S e c t i o n               */
/******************************************************************************/

bool startsWithIssuerSection(const std::string &name)
{
   if (name.size() < 6) return false;
   if (toLowerCopy(name.substr(0, 6)) != "issuer") return false;
   return name.size() == 6 || isspace(static_cast<unsigned char>(name[6]));
}

/******************************************************************************/
/*                          s t r i p Q u o t e s                           */
/******************************************************************************/

std::string stripQuotes(const std::string &s)
{
   if (s.size() >= 2
   && ((s.front() == '"' && s.back() == '"')
    || (s.front() == '\'' && s.back() == '\'')))
      return s.substr(1, s.size() - 2);
   return s;
}

// Parse "email=username" and store in target (email key is normalized).
/******************************************************************************/
/*                     a d d E m a i l M a p E n t r y                      */
/******************************************************************************/

bool addEmailMapEntry(const std::string &spec,
                      std::unordered_map<std::string, std::string> &target,
                      std::string &emsg)
{
   size_t eq = spec.find('=');
   if (eq == std::string::npos)
      {emsg = "email-map entry must be email=username";
       return false;
      }
   std::string email = normalizeEmailKey(stripQuotes(trimCopy(spec.substr(0, eq))));
   std::string uname = trimCopy(stripQuotes(spec.substr(eq + 1)));
   if (email.empty() || uname.empty())
      {emsg = "invalid email-map entry";
       return false;
      }
   target[email] = uname;
   return true;
}

/******************************************************************************/
/*                   s t o r e E m a i l M a p E n t r y                    */
/******************************************************************************/

bool storeEmailMapEntry(const std::string &spec, std::string &emsg)
{
   std::scoped_lock lock(ConfigMtx);
   return addEmailMapEntry(spec, EmailIdentityMap, emsg);
}

/******************************************************************************/
/*                             a d d I n i K V                              */
/******************************************************************************/

bool addIniKV(const std::string &keyIn, const std::string &valIn, bool inIssuer,
              std::string &opts, std::string &emsg)
{
   std::string key = toLowerCopy(trimCopy(keyIn));
   std::string val = trimCopy(stripQuotes(trimCopy(valIn)));
   if (key.empty())
      {emsg = "empty key in config file";
       return false;
      }

   if (key == "issuer")
      {if (val.empty()) {emsg = "issuer value is empty"; return false;}
       appendCliOpt(opts, "-issuer", &val);
       return true;
      }

   if (key == "audience")
      {
       if (!inIssuer) {emsg = "audience requires an [issuer ...] section or issuer=..."; return false;}
       size_t pos = 0;
       while (pos <= val.size())
          {
           size_t comma = val.find(',', pos);
           std::string one = trimCopy(val.substr(pos, comma == std::string::npos ? std::string::npos
                                                                                  : comma - pos));
           if (!one.empty()) appendCliOpt(opts, "-audience", &one);
           if (comma == std::string::npos) break;
           pos = comma + 1;
          }
       return true;
      }

   if (key == "oidc-config-url")      {appendCliOpt(opts, "-oidc-config-url", &val); return true;}
   if (key == "jwks-url")             {appendCliOpt(opts, "-jwks-url", &val); return true;}
   if (key == "forced-identity-claim"){appendCliOpt(opts, "-forced-identity-claim", &val); return true;}
   if (key == "base-path" || key == "base_path")
      {
       if (!inIssuer) {emsg = "base_path requires an [issuer ...] section or issuer=..."; return false;}
       std::string normalized;
       if (!canonicalAbsolutePath(val, normalized))
          {emsg = "invalid absolute path: " + val;
           return false;
          }
       appendCliOpt(opts, "-base-path", &normalized);
       return true;
      }
   if (key == "restricted-path" || key == "restricted_path")
      {
       if (!inIssuer) {emsg = "restricted_path requires an [issuer ...] section or issuer=..."; return false;}
       std::string normalized;
       if (!canonicalAbsolutePath(val, normalized))
          {emsg = "invalid absolute path: " + val;
           return false;
          }
       appendCliOpt(opts, "-restricted-path", &normalized);
       return true;
      }
   if (key == "maxsz")                {appendCliOpt(opts, "-maxsz", &val); return true;}
   if (key == "expiry")               {appendCliOpt(opts, "-expiry", &val); return true;}
   if (key == "jwks-refresh")         {appendCliOpt(opts, "-jwks-refresh", &val); return true;}
   if (key == "jwks-cache-file")      {appendCliOpt(opts, "-jwks-cache-file", &val); return true;}
   if (key == "jwks-cache-ttl")       {appendCliOpt(opts, "-jwks-cache-ttl", &val); return true;}
   if (key == "clock-skew")           {appendCliOpt(opts, "-clock-skew", &val); return true;}
   if (key == "token-cache-max")      {appendCliOpt(opts, "-token-cache-max", &val); return true;}
   if (key == "token-cache-noexp-ttl"){appendCliOpt(opts, "-token-cache-noexp-ttl", &val); return true;}

   if (key == "identity-claim")
      {
       size_t pos = 0;
       while (pos <= val.size())
          {
           size_t comma = val.find(',', pos);
           std::string one = trimCopy(val.substr(pos, comma == std::string::npos ? std::string::npos
                                                                                  : comma - pos));
           if (!one.empty()) appendCliOpt(opts, "-identity-claim", &one);
           if (comma == std::string::npos) break;
           pos = comma + 1;
          }
       return true;
      }

   if (key == "entity-claim")
      {
       size_t pos = 0;
       while (pos <= val.size())
          {
           size_t comma = val.find(',', pos);
           std::string one = trimCopy(val.substr(pos, comma == std::string::npos ? std::string::npos
                                                                                  : comma - pos));
           if (!one.empty()) appendCliOpt(opts, "-entity-claim", &one);
           if (comma == std::string::npos) break;
           pos = comma + 1;
          }
       return true;
      }

   if (key == "role-claim")           {appendCliOpt(opts, "-role-claim", &val); return true;}
   if (key == "grps-claim")           {appendCliOpt(opts, "-grps-claim", &val); return true;}

   if (key == "debug-token" || key == "show-token-claims")
      {
       bool enabled = false;
       if (!parseBool(val, enabled))
          {emsg = "invalid boolean for " + key + ": " + val;
           return false;
          }
       if (enabled)
          appendCliOpt(opts, key == "debug-token" ? "-debug-token" : "-show-token-claims");
       return true;
      }

   emsg = "unsupported key in config file: " + key;
   return false;
}

struct SafeFileResult {
   std::string contents;
   ino_t ino;
   time_t mtime;
   bool found;
};

/******************************************************************************/
/*                   s a f e R e a d C o n f i g F i l e                    */
/******************************************************************************/

bool safeReadConfigFile(const char *path, SafeFileResult &result, std::string &emsg)
{
   result.found = false;
   result.contents.clear();
   result.ino = 0;
   result.mtime = 0;

   int flags = O_RDONLY;
#ifdef O_NOFOLLOW
   flags |= O_NOFOLLOW;
#endif
   int fd = open(path, flags);
   if (fd < 0)
      {
       if (errno == ENOENT) return true;
       emsg = std::string("unable to open ") + path + ": " + strerror(errno);
       return false;
      }

   struct stat st;
   if (fstat(fd, &st) != 0)
      {int rc = errno; close(fd);
       emsg = std::string("unable to stat ") + path + ": " + strerror(rc);
       return false;
      }

   if (!S_ISREG(st.st_mode))
      {close(fd);
       emsg = std::string(path) + ": config path must be a regular file";
       return false;
      }

   uid_t euid = geteuid();
   if (st.st_uid != euid)
      {close(fd);
       emsg = std::string(path) + ": config file owner uid "
            + std::to_string(static_cast<unsigned long long>(st.st_uid))
            + " does not match process euid "
            + std::to_string(static_cast<unsigned long long>(euid));
       return false;
      }

   if (st.st_mode & (S_IWGRP | S_IWOTH))
      {close(fd);
       emsg = std::string(path) + ": must not be writable by group/other";
       return false;
      }

   if (st.st_size < 0 || st.st_size > 10 * 1024 * 1024)
      {close(fd);
       emsg = std::string(path) + ": config file too large";
       return false;
      }

   size_t fsize = static_cast<size_t>(st.st_size);
   result.contents.resize(fsize);
   size_t got = 0;
   while (got < fsize)
      {ssize_t rd = read(fd, &result.contents[got], fsize - got);
       if (rd < 0)
          {if (errno == EINTR) continue;
           int rc = errno; close(fd);
           emsg = std::string("read error on ") + path + ": " + strerror(rc);
           return false;
          }
       if (rd == 0) break;
       got += static_cast<size_t>(rd);
      }
   close(fd);
   result.contents.resize(got);
   result.ino = st.st_ino;
   result.mtime = st.st_mtime;
   result.found = true;
   return true;
}

/******************************************************************************/
/*                  l o a d O A u t h 2 I n i A s A r g s                   */
/******************************************************************************/

bool loadOAuth2IniAsArgs(const char *path, std::string &opts, bool &found,
                       std::string &emsg, ino_t *inoOut,
                       time_t *mtimeOut)
{
   opts.clear();
   emsg.clear();
   found = false;

   SafeFileResult sfr;
   if (!safeReadConfigFile(path, sfr, emsg)) return false;
   if (!sfr.found) return true;
   found = true;
   if (inoOut) *inoOut = sfr.ino;
   if (mtimeOut) *mtimeOut = sfr.mtime;

   std::istringstream in(sfr.contents);

   bool inIssuer = false;
   bool inEmailMap = false;
   std::string line;
   int lineNo = 0;
   while (std::getline(in, line))
      {
       ++lineNo;
       std::string t = trimCopy(line);
       if (t.empty() || t[0] == '#' || t[0] == ';') continue;

       if (t.front() == '[' && t.back() == ']')
          {
           std::string sec = trimCopy(t.substr(1, t.size() - 2));
           if (toLowerCopy(sec) == "global")
              {inIssuer = false;
               inEmailMap = false;
               continue;
              }
           if (toLowerCopy(sec) == "email-map")
              {inIssuer = false;
               inEmailMap = true;
               continue;
              }
           if (startsWithIssuerSection(sec))
              {
               std::string val = trimCopy(sec.substr(6));
               if (val.empty())
                  {inIssuer = false;
                   inEmailMap = false;
                   continue;
                  }
               val = stripQuotes(val);
               std::string localErr;
               if (!addIniKV("issuer", val, false, opts, localErr))
                  {emsg = std::string(path) + ":" + std::to_string(lineNo) + ": " + localErr;
                   return false;
                  }
               inIssuer = true;
               inEmailMap = false;
               continue;
              }
           emsg = std::string(path) + ":" + std::to_string(lineNo)
                + ": unsupported section '" + sec + "'";
           return false;
          }

       size_t eq = t.find('=');
       if (inEmailMap)
          {
           if (eq == std::string::npos)
              {emsg = std::string(path) + ":" + std::to_string(lineNo)
                    + ": email-map entry must be key=value";
               return false;
              }
           std::string localErr;
           if (!addEmailMapEntry(t, EmailIdentityMap, localErr))
              {emsg = std::string(path) + ":" + std::to_string(lineNo)
                    + ": " + localErr;
               return false;
              }
           continue;
          }

       std::string key = (eq == std::string::npos ? t : t.substr(0, eq));
       std::string val = (eq == std::string::npos ? "true" : t.substr(eq + 1));
       std::string localErr;
       if (!addIniKV(key, val, inIssuer, opts, localErr))
          {emsg = std::string(path) + ":" + std::to_string(lineNo) + ": " + localErr;
           return false;
          }
       if (toLowerCopy(trimCopy(key)) == "issuer") inIssuer = true;
      }
   return true;
}

bool refreshJWKSForPolicy(std::shared_ptr<IssuerPolicy> policy, bool force,
                          std::string &emsg);

/******************************************************************************/
/*                             s p l i t C S V                              */
/******************************************************************************/

bool splitCSV(const std::string &val, std::vector<std::string> &out)
{
   out.clear();
   size_t pos = 0;
   while (pos <= val.size())
      {
       size_t comma = val.find(',', pos);
       if (std::string one = trimCopy(val.substr(pos, comma == std::string::npos ? std::string::npos
                                                                                 : comma - pos));
           !one.empty())
          out.push_back(one);
       if (comma == std::string::npos) break;
       pos = comma + 1;
      }
   return true;
}

/******************************************************************************/
/*           p a r s e R e l o a d a b l e I n i S e c t i o n s            */
/******************************************************************************/

bool parseReloadableIniSections(const char *path,
                                const std::string &fileContents,
                                std::vector<std::shared_ptr<IssuerPolicy>> &outPolicies,
                                std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> &outByIssuer,
                                std::unordered_map<std::string, std::string> &outEmailMap,
                                std::string &emsg)
{
   outPolicies.clear();
   outByIssuer.clear();
   outEmailMap.clear();

   std::istringstream in(fileContents);

   bool inIssuer = false;
   bool inEmailMap = false;
   std::shared_ptr<IssuerPolicy> curPolicy;
   std::string line;
   int lineNo = 0;
   while (std::getline(in, line))
      {
       ++lineNo;
       std::string t = trimCopy(line);
       if (t.empty() || t[0] == '#' || t[0] == ';') continue;

       if (t.front() == '[' && t.back() == ']')
          {
           inIssuer = false;
           inEmailMap = false;
           curPolicy.reset();
           std::string sec = trimCopy(t.substr(1, t.size() - 2));
           if (toLowerCopy(sec) == "global") continue;
           if (toLowerCopy(sec) == "email-map")
              {inEmailMap = true; continue;}
           if (startsWithIssuerSection(sec))
              {
               std::string iss = trimCopy(sec.substr(6));
               if (!iss.empty())
                  iss = trimCopy(stripQuotes(iss));
               if (iss.empty()) {inIssuer = true; continue;}
               auto it = outByIssuer.find(iss);
               if (it != outByIssuer.end()) curPolicy = it->second;
               else
                  {
                   curPolicy = std::make_shared<IssuerPolicy>();
                   curPolicy->issuer = iss;
                   outPolicies.push_back(curPolicy);
                   outByIssuer[iss] = curPolicy;
                  }
               inIssuer = true;
               continue;
              }
           emsg = std::string(path) + ":" + std::to_string(lineNo)
                + ": unsupported section '" + sec + "'";
           return false;
          }

       size_t eq = t.find('=');
       std::string key = toLowerCopy(trimCopy(eq == std::string::npos ? t : t.substr(0, eq)));
       std::string val = trimCopy(stripQuotes(eq == std::string::npos ? "true" : t.substr(eq + 1)));

       if (inEmailMap)
          {
           std::string localErr;
           if (!addEmailMapEntry(t, outEmailMap, localErr))
              {emsg = std::string(path) + ":" + std::to_string(lineNo)
                    + ": " + localErr;
               return false;
              }
           continue;
          }

       if (!inIssuer) continue; // reloadable sections only

       if (key == "issuer")
          {
           if (val.empty())
              {emsg = std::string(path) + ":" + std::to_string(lineNo) + ": issuer value is empty";
               return false;
              }
           auto it = outByIssuer.find(val);
           if (it != outByIssuer.end()) curPolicy = it->second;
           else
              {
               curPolicy = std::make_shared<IssuerPolicy>();
               curPolicy->issuer = val;
               outPolicies.push_back(curPolicy);
               outByIssuer[val] = curPolicy;
              }
           continue;
          }
       if (!curPolicy)
          {
           emsg = std::string(path) + ":" + std::to_string(lineNo)
                + ": issuer-scoped key requires issuer to be set";
           return false;
          }
       if (key == "audience")
          {
           std::vector<std::string> items;
           splitCSV(val, items);
           for (const auto &one : items) curPolicy->audiences.push_back(one);
           continue;
          }
       if (key == "oidc-config-url")
          {curPolicy->oidcConfigURL = val; continue;}
       if (key == "jwks-url")
          {curPolicy->jwksURL = val; continue;}
       if (key == "forced-identity-claim")
          {curPolicy->forcedIdentityClaim = val; continue;}
       if (key == "base-path" || key == "base_path")
          {
           std::string localErr;
           if (!setIssuerBasePath(val, curPolicy->basePath, localErr))
              {emsg = std::string(path) + ":" + std::to_string(lineNo)
                    + ": " + localErr;
               return false;
              }
           continue;
          }
       if (key == "restricted-path" || key == "restricted_path")
          {
           std::string localErr;
           if (!appendIssuerPathOption(val, curPolicy->restrictedPaths, localErr))
              {emsg = std::string(path) + ":" + std::to_string(lineNo)
                    + ": " + localErr;
               return false;
              }
           continue;
          }
       // Ignore unsupported/global keys in reload mode by design.
      }

   return true;
}

/******************************************************************************/
/*      v a l i d a t e A n d W a r m R e l o a d a b l e C o n f i g       */
/******************************************************************************/

bool validateAndWarmReloadableConfig(std::vector<std::shared_ptr<IssuerPolicy>> &policies,
                                     std::string &emsg)
{
   if (policies.empty())
      {emsg = "At least one issuer must be configured in reloadable config";
       return false;
      }
   for (auto &policy : policies)
      {
       if (policy->oidcConfigURL.empty() && !policy->issuer.empty())
          policy->oidcConfigURL = joinURL(policy->issuer, "/.well-known/openid-configuration");
       if (policy->oidcConfigURL.empty() && policy->jwksURL.empty())
          {emsg = "issuer '" + policy->issuer + "' requires oidc-config-url or jwks-url";
           return false;
          }
       if ((!policy->oidcConfigURL.empty() && !hasPrefix(policy->oidcConfigURL, "https://"))
       ||  (!policy->jwksURL.empty() && !hasPrefix(policy->jwksURL, "https://")))
          {emsg = "issuer '" + policy->issuer + "' has non-https OIDC/JWKS URL";
           return false;
          }
       if (!refreshJWKSForPolicy(policy, true, emsg))
          {emsg = "issuer '" + policy->issuer + "': " + emsg;
           return false;
          }
       if (policy->audiences.empty())
          OAuth2Log.Emsg("Config", "oauth2",
             ("WARNING: issuer '" + policy->issuer + "' has no audience "
              "configured; any signed, unexpired token from this issuer will be "
              "accepted regardless of its 'aud' claim.").c_str());
      }
   return true;
}

/******************************************************************************/
/*                      c l e a r T o k e n C a c h e                       */
/******************************************************************************/

void clearTokenCache()
{
   {
      std::scoped_lock lock(TokenCacheMtx);
      TokenCache.clear();
   }
   std::scoped_lock lock(TokenNegCacheMtx);
   TokenNegCache.clear();
}

/******************************************************************************/
/*                  n e g T o k e n C a c h e L o o k u p                   */
/******************************************************************************/

bool negTokenCacheLookup(const std::string &tokKey, uint64_t now)
{
   if (tokKey.empty()) return false;
   std::scoped_lock lock(TokenNegCacheMtx);
   auto it = TokenNegCache.find(tokKey);
   if (it == TokenNegCache.end()) return false;
   if (it->second <= now)
      {TokenNegCache.erase(it);
       return false;
      }
   return true;
}

/******************************************************************************/
/*                   n e g T o k e n C a c h e S t o r e                    */
/******************************************************************************/

void negTokenCacheStore(const std::string &tokKey, uint64_t now)
{
   if (tokKey.empty()) return;
   std::scoped_lock lock(TokenNegCacheMtx);
   if (TokenNegCache.size() >= kNegTokenCacheMax
   &&  TokenNegCache.find(tokKey) == TokenNegCache.end())
      {// Drop entries that have already expired before considering the cap.
       for (auto it = TokenNegCache.begin(); it != TokenNegCache.end(); )
          {if (it->second <= now) it = TokenNegCache.erase(it);
           else ++it;
          }
       // Still full of live entries: skip the insert rather than grow unbounded.
       if (TokenNegCache.size() >= kNegTokenCacheMax) return;
      }
   TokenNegCache[tokKey] = now + static_cast<uint64_t>(kNegTokenCacheTTL);
}

// Lightweight metadata probe used on the authentication hot path: it does not
// read the file contents, so an unchanged config costs only open()+fstat().
/******************************************************************************/
/*                   s t a t C o n f i g F i l e M e t a                    */
/******************************************************************************/

bool statConfigFileMeta(const char *path, ino_t &ino, time_t &mtime,
                        bool &found, std::string &emsg)
{
   ino = 0;
   mtime = 0;
   found = false;

   int flags = O_RDONLY;
#ifdef O_NOFOLLOW
   flags |= O_NOFOLLOW;
#endif
   int fd = open(path, flags);
   if (fd < 0)
      {if (errno == ENOENT) return true;
       emsg = std::string("unable to open ") + path + ": " + strerror(errno);
       return false;
      }
   struct stat st;
   if (fstat(fd, &st) != 0)
      {int rc = errno; close(fd);
       emsg = std::string("unable to stat ") + path + ": " + strerror(rc);
       return false;
      }
   close(fd);
   if (!S_ISREG(st.st_mode))
      {emsg = std::string(path) + ": config path must be a regular file";
       return false;
      }
   ino = st.st_ino;
   mtime = st.st_mtime;
   found = true;
   return true;
}

/******************************************************************************/
/*          m a y b e R e l o a d O A u t h 2 F i l e C o n f i g           */
/******************************************************************************/

void maybeReloadOAuth2FileConfig()
{
   std::string cfgPath;
   {
      std::scoped_lock lock(ConfigMtx);
      if (!OAuth2ConfigWatch || OAuth2ConfigPath.empty()) return;
      cfgPath = OAuth2ConfigPath;
   }
   std::string emsg;

   // Fast path: stat only. Avoid reading the (potentially large) file on every
   // authentication; the full read+parse happens only when ino/mtime changes.
   ino_t curIno = 0;
   time_t curMtime = 0;
   bool curFound = false;
   if (!statConfigFileMeta(cfgPath.c_str(), curIno, curMtime, curFound, emsg))
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("config stat failed: " + emsg).c_str());
       return;
      }
   if (!curFound)
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("config file disappeared: " + cfgPath).c_str());
       return;
      }
   {
      std::scoped_lock lock(ConfigMtx);
      if (OAuth2ConfigStatValid && curIno == OAuth2ConfigIno && curMtime == OAuth2ConfigMTime) return;
   }

   // Single-flight: only one thread performs the (network-touching) reload at a
   // time. Concurrent authenticators that observe the same change return and
   // keep serving with the current config instead of all re-parsing and
   // re-fetching JWKS in a thundering herd.
   static std::atomic<bool> reloadInProgress{false};
   bool expected = false;
   if (!reloadInProgress.compare_exchange_strong(expected, true)) return;
   struct ReloadGuard {
      std::atomic<bool> &flag;
      ~ReloadGuard() {flag.store(false);}
   } reloadGuard{reloadInProgress};

   // Re-check under the lock: another thread may have just finished reloading
   // this exact version while we were waiting to win the single-flight flag.
   {
      std::scoped_lock lock(ConfigMtx);
      if (OAuth2ConfigStatValid && curIno == OAuth2ConfigIno && curMtime == OAuth2ConfigMTime) return;
   }

   // Config changed (or first observation): read and parse the full contents.
   SafeFileResult sfr;
   if (!safeReadConfigFile(cfgPath.c_str(), sfr, emsg))
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("config read failed: " + emsg).c_str());
       return;
      }
   if (!sfr.found)
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("config file disappeared: " + cfgPath).c_str());
       return;
      }

   std::vector<std::shared_ptr<IssuerPolicy>> newPolicies;
   std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> newByIssuer;
   std::unordered_map<std::string, std::string> newEmailMap;
   if (!parseReloadableIniSections(cfgPath.c_str(), sfr.contents, newPolicies, newByIssuer, newEmailMap, emsg))
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("config reload parse failed: " + emsg).c_str());
       return;
      }
   if (!validateAndWarmReloadableConfig(newPolicies, emsg))
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("config reload validation failed: " + emsg).c_str());
       return;
      }

   std::scoped_lock lock(ConfigMtx);
   IssuerPolicies.swap(newPolicies);
   IssuerPolicyByIssuer.swap(newByIssuer);
   EmailIdentityMap.swap(newEmailMap);
   OAuth2ConfigIno = sfr.ino;
   OAuth2ConfigMTime = sfr.mtime;
   OAuth2ConfigStatValid = true;
   clearTokenCache();
   OAuth2Log.Emsg("Auth", "oauth2", ("config reloaded from " + cfgPath).c_str());
}

// Maximum bytes we will buffer from any single HTTP response (4 MiB).
const size_t kFetchBodyLimit = 4 * 1024 * 1024;

struct FetchSink {
   std::string *dst;
   bool truncated;
};

/******************************************************************************/
/*                          c u r l W r i t e C B                           */
/******************************************************************************/

size_t curlWriteCB(char *ptr, size_t sz, size_t nmemb, void *ud)
{
   FetchSink *sink = static_cast<FetchSink *>(ud);
   if (nmemb != 0 && sz > SIZE_MAX / nmemb)
      {sink->truncated = true; return 0;}
   size_t incoming = sz * nmemb;
   if (sink->dst->size() + incoming > kFetchBodyLimit)
      {// Returning 0 aborts the transfer with CURLE_WRITE_ERROR.
       sink->truncated = true;
       return 0;
      }
   sink->dst->append(ptr, incoming);
   return incoming;
}

/******************************************************************************/
/*                             f e t c h U R L                              */
/******************************************************************************/

bool fetchURL(const std::string &url, std::string &body, std::string &emsg)
{
   body.clear();
   CurlPtr c(curl_easy_init());
   if (!c) {emsg = "curl init failed"; return false;}
   FetchSink sink;
   sink.dst = &body;
   sink.truncated = false;
   curl_easy_setopt(c.get(), CURLOPT_URL, url.c_str());
   // Follow redirects but only to HTTPS targets; cap at 3 hops.
   curl_easy_setopt(c.get(), CURLOPT_FOLLOWLOCATION, 1L);
   curl_easy_setopt(c.get(), CURLOPT_MAXREDIRS, 3L);
#if CURL_AT_LEAST_VERSION(7, 85, 0)
   curl_easy_setopt(c.get(), CURLOPT_REDIR_PROTOCOLS_STR, "https");
#else
   curl_easy_setopt(c.get(), CURLOPT_REDIR_PROTOCOLS, CURLPROTO_HTTPS);
#endif
   curl_easy_setopt(c.get(), CURLOPT_TIMEOUT, 15L);
   curl_easy_setopt(c.get(), CURLOPT_CONNECTTIMEOUT, 5L);
   curl_easy_setopt(c.get(), CURLOPT_WRITEFUNCTION, curlWriteCB);
   curl_easy_setopt(c.get(), CURLOPT_WRITEDATA, &sink);
   curl_easy_setopt(c.get(), CURLOPT_SSL_VERIFYPEER, 1L);
   curl_easy_setopt(c.get(), CURLOPT_SSL_VERIFYHOST, 2L);
   CURLcode rc = curl_easy_perform(c.get());
   long httpCode = 0;
   curl_easy_getinfo(c.get(), CURLINFO_RESPONSE_CODE, &httpCode);
   if (sink.truncated)
      {emsg = "JWKS/OIDC response body too large (> 4 MiB), aborting";
       return false;
      }
   if (rc != CURLE_OK)
      {emsg = curl_easy_strerror(rc);
       return false;
      }
   if (httpCode < 200 || httpCode >= 300)
      {emsg = "HTTP status " + std::to_string(httpCode);
       return false;
      }
   return true;
}

/******************************************************************************/
/*                         g e t U i n t C l a i m                          */
/******************************************************************************/

bool getUintClaim(const nlohmann::json &obj, const char *claim, uint64_t &out)
{
   if (!obj.is_object()) return false;
   auto it = obj.find(claim);
   if (it == obj.end()) return false;
   if (it->is_number_unsigned())
      {out = it->get<uint64_t>();
       return true;
      }
   if (it->is_number_integer())
      {int64_t v = it->get<int64_t>();
       if (v < 0) return false;
       out = static_cast<uint64_t>(v);
       return true;
      }
   // Some IdPs emit NumericDate claims (exp/nbf/iat) as JSON floats; accept a
   // finite, non-negative value by truncating to whole seconds.
   if (it->is_number_float())
      {double d = it->get<double>();
       if (!std::isfinite(d) || d < 0) return false;
       out = static_cast<uint64_t>(d);
       return true;
      }
   return false;
}

/******************************************************************************/
/*                h a s S t r i n g I n A r r a y C l a i m                 */
/******************************************************************************/

bool hasStringInArrayClaim(const nlohmann::json &obj, const char *claim,
                           const std::string &want)
{
   if (!obj.is_object() || !obj.contains(claim)) return false;
   const nlohmann::json &arr = obj.at(claim);
   if (!arr.is_array()) return false;
   for (size_t i = 0; i < arr.size(); ++i)
      {
       if (arr[i].is_string() && arr[i].get<std::string>() == want) return true;
      }
   return false;
}

/******************************************************************************/
/*                          v e r i f y R S 2 5 6                           */
/******************************************************************************/

bool verifyRS256(EVP_PKEY *pkey, std::string_view signedData,
                 std::string_view sig)
{
   EvpMdCtxPtr ctx(EVP_MD_CTX_new());
   if (!ctx) return false;
   return EVP_DigestVerifyInit(ctx.get(), nullptr, EVP_sha256(), nullptr, pkey) == 1
       && EVP_DigestVerifyUpdate(ctx.get(), signedData.data(), signedData.size()) == 1
       && EVP_DigestVerifyFinal(ctx.get(),
            reinterpret_cast<const unsigned char *>(sig.data()), sig.size()) == 1;
}

// Reject RSA signing keys weaker than this; sub-2048-bit moduli are considered
// factorable and must not be trusted even if served by a configured issuer.
constexpr int kMinRSAKeyBits = 2048;

/******************************************************************************/
/*                     m a k e R S A P u b l i c K e y                      */
/******************************************************************************/

EvpPkeyPtr makeRSAPublicKey(std::string_view modulus,
                            std::string_view exponent)
{
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
   EvpPkeyCtxPtr ctx(EVP_PKEY_CTX_new_from_name(nullptr, "RSA", nullptr));
   if (!ctx) return nullptr;
   BignumPtr bnN(BN_bin2bn(reinterpret_cast<const unsigned char *>(modulus.data()),
                           modulus.size(), nullptr));
   BignumPtr bnE(BN_bin2bn(reinterpret_cast<const unsigned char *>(exponent.data()),
                           exponent.size(), nullptr));
   OsslParamBldPtr bld(OSSL_PARAM_BLD_new());
   if (!bnN || !bnE || !bld) return nullptr;
   if (OSSL_PARAM_BLD_push_BN(bld.get(), "n", bnN.get()) <= 0
   ||  OSSL_PARAM_BLD_push_BN(bld.get(), "e", bnE.get()) <= 0)
      return nullptr;
   OsslParamPtr params(OSSL_PARAM_BLD_to_param(bld.get()));
   if (!params) return nullptr;
   EVP_PKEY *raw = nullptr;
   if (EVP_PKEY_fromdata_init(ctx.get()) <= 0
   ||  EVP_PKEY_fromdata(ctx.get(), &raw, EVP_PKEY_PUBLIC_KEY, params.get()) <= 0)
      return nullptr;
   EvpPkeyPtr key(raw);
   if (EVP_PKEY_bits(key.get()) < kMinRSAKeyBits) return nullptr;
   return key;
#else
   BignumPtr bnN(BN_bin2bn(reinterpret_cast<const unsigned char *>(modulus.data()),
                           modulus.size(), nullptr));
   BignumPtr bnE(BN_bin2bn(reinterpret_cast<const unsigned char *>(exponent.data()),
                           exponent.size(), nullptr));
   if (!bnN || !bnE) return nullptr;

   RsaPtr rsa(RSA_new());
   if (!rsa) return nullptr;
   if (RSA_set0_key(rsa.get(), bnN.get(), bnE.get(), nullptr) != 1) return nullptr;
   // Ownership of the bignums has transferred to rsa.
   bnN.release();
   bnE.release();

   EvpPkeyPtr pkey(EVP_PKEY_new());
   if (!pkey) return nullptr;
   if (EVP_PKEY_assign_RSA(pkey.get(), rsa.get()) != 1) return nullptr;
   // Ownership of rsa has transferred to pkey.
   rsa.release();
   if (EVP_PKEY_bits(pkey.get()) < kMinRSAKeyBits) return nullptr;
   return pkey;
#endif
}

/******************************************************************************/
/*                             l o a d J W K S                              */
/******************************************************************************/

bool loadJWKS(const std::string &json, std::map<std::string, EvpPkeyPtr> &keys,
              std::string &emsg)
{
   keys.clear();
   nlohmann::json root;
   if (!parseJsonObject(json, root))
      {
       emsg = "invalid JWKS JSON";
       return false;
      }
   auto kIt = root.find("keys");
   if (kIt == root.end() || !kIt->is_array())
      {
       emsg = "invalid JWKS JSON: missing keys array";
       return false;
      }
   for (const auto &kobj : *kIt)
      {
       std::string kty, kid, n, e;
       if (!getStringClaim(kobj, "kty", kty)
       ||  !getStringClaim(kobj, "kid", kid)
       ||  !getStringClaim(kobj, "n", n)
       ||  !getStringClaim(kobj, "e", e))
          continue;
       if (kty != "RSA") continue;
       std::string use;
       if (getStringClaim(kobj, "use", use) && use != "sig") continue;
       std::string nb, eb;
       if (!decodeBase64URL(n, nb) || !decodeBase64URL(e, eb))
          continue;
       EvpPkeyPtr pkey = makeRSAPublicKey(nb, eb);
       if (!pkey) continue;
       // Assigning into the owning map releases any previous key for this kid.
       keys[kid] = std::move(pkey);
      }
   if (keys.empty())
      {emsg = "no usable RSA keys in JWKS";
       return false;
      }
   return true;
}

/******************************************************************************/
/*                j w k s C a c h e E f f e c t i v e T T L                 */
/******************************************************************************/

int jwksCacheEffectiveTTL()
{
   return (JwksCacheTTL > 0 ? JwksCacheTTL : JwksRefresh.load());
}

// Return true iff path is a regular file owned by the effective UID and not
// writable by group or other (same policy as the main config file). The check
// is performed on an O_NOFOLLOW-opened descriptor (fstat) so a symlink at the
// path is rejected and cannot redirect the inspection to another file.
/******************************************************************************/
/*                  c h e c k C a c h e F i l e P e r m s                   */
/******************************************************************************/

bool checkCacheFilePerms(const char *path, std::string &emsg)
{
   int flags = O_RDONLY;
#ifdef O_NOFOLLOW
   flags |= O_NOFOLLOW;
#endif
   int fd = open(path, flags);
   if (fd < 0)
      {if (errno == ENOENT) return true; // not yet created – OK
       emsg = std::string("open JWKS cache file failed: ") + strerror(errno);
       return false;
      }
   struct stat st;
   if (fstat(fd, &st) != 0)
      {int rc = errno; close(fd);
       emsg = std::string("fstat JWKS cache file failed: ") + strerror(rc);
       return false;
      }
   close(fd);
   if (!S_ISREG(st.st_mode))
      {emsg = std::string("JWKS cache file is not a regular file: ") + path;
       return false;
      }
   if (st.st_uid != geteuid())
      {emsg = std::string("JWKS cache file not owned by the running UID: ") + path;
       return false;
      }
   if (st.st_mode & (S_IWGRP | S_IWOTH))
      {emsg = std::string("JWKS cache file must not be group/other writable: ") + path;
       return false;
      }
   return true;
}

/******************************************************************************/
/*                     l o a d J W K S C a c h e M a p                      */
/******************************************************************************/

bool loadJWKSCacheMap(std::unordered_map<std::string, JwksDiskEntry> &out,
                      std::string &emsg)
{
   out.clear();
   emsg.clear();
   if (JwksCacheFile.empty()) return true;

   // Security: open with O_NOFOLLOW and verify ownership/permissions on the same
   // descriptor we read from. The contents are decoded into trusted signature
   // keys, so a swapped or symlinked file must never be honoured (no TOCTOU
   // between the permission check and the read).
   SafeFileResult sfr;
   if (!safeReadConfigFile(JwksCacheFile.c_str(), sfr, emsg)) return false;
   if (!sfr.found) return true; // not yet created – OK

   std::istringstream in(sfr.contents);
   std::string line;
   std::string curIssuer;
   int lineNo = 0;
   while (std::getline(in, line))
      {
       ++lineNo;
       std::string t = trimCopy(line);
       if (t.empty() || t[0] == '#' || t[0] == ';') continue;
       if (t.front() == '[' && t.back() == ']')
          {
           std::string sec = trimCopy(t.substr(1, t.size() - 2));
           if (!startsWithIssuerSection(sec))
              {emsg = "invalid JWKS cache section at line " + std::to_string(lineNo);
               return false;
              }
           curIssuer = stripQuotes(trimCopy(sec.substr(6)));
           if (!curIssuer.empty()) out[curIssuer];
           continue;
          }
       if (curIssuer.empty()) continue;
       size_t eq = t.find('=');
       if (eq == std::string::npos) continue;
       std::string key = toLowerCopy(trimCopy(t.substr(0, eq)));
       std::string val = trimCopy(stripQuotes(t.substr(eq + 1)));
       auto &e = out[curIssuer];
       if (key == "jwks_url") e.jwksUrl = val;
       else if (key == "fetched_at")
          {
           long long vv = 0;
           const auto [ptr, ec] = std::from_chars(val.data(),
                                                  val.data() + val.size(), vv);
           if (ec == std::errc() && ptr == val.data() + val.size() && vv > 0)
              e.fetchedAt = static_cast<time_t>(vv);
          }
       else if (key == "jwks_b64") e.jwksB64 = val;
      }
   return true;
}

/******************************************************************************/
/*                    w r i t e J W K S C a c h e M a p                     */
/******************************************************************************/

bool writeJWKSCacheMap(const std::unordered_map<std::string, JwksDiskEntry> &cache,
                       std::string &emsg)
{
   emsg.clear();
   if (JwksCacheFile.empty()) return true;

   if (!checkCacheFilePerms(JwksCacheFile.c_str(), emsg)) return false;

   std::string tmp = JwksCacheFile + ".tmp";
   int tfd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
   if (tfd < 0)
      {emsg = std::string("unable to create JWKS cache temp file: ") + strerror(errno);
       return false;
      }
   if (fchmod(tfd, 0600) != 0)
      {emsg = std::string("unable to set JWKS cache temp file permissions: ") + strerror(errno);
       close(tfd); unlink(tmp.c_str());
       return false;
      }

   std::string content;
   for (const auto &it : cache)
      {
       if (it.first.empty() || it.second.jwksB64.empty()) continue;
       content += "[issuer \"" + it.first + "\"]\n";
       content += "fetched_at=" + std::to_string(static_cast<long long>(it.second.fetchedAt)) + "\n";
       if (!it.second.jwksUrl.empty())
          content += "jwks_url=" + it.second.jwksUrl + "\n";
       content += "jwks_b64=" + it.second.jwksB64 + "\n\n";
      }

   const char *ptr = content.data();
   size_t remaining = content.size();
   while (remaining > 0)
      {ssize_t wr = write(tfd, ptr, remaining);
       if (wr < 0)
          {if (errno == EINTR) continue;
           int rc = errno;
           close(tfd); unlink(tmp.c_str());
           emsg = std::string("write error on JWKS cache temp: ") + strerror(rc);
           return false;
          }
       ptr += wr;
       remaining -= static_cast<size_t>(wr);
      }
   close(tfd);
   if (rename(tmp.c_str(), JwksCacheFile.c_str()) != 0)
      {
       emsg = std::string("unable to replace JWKS cache file: ") + strerror(errno);
       unlink(tmp.c_str());
       return false;
      }
   return true;
}

/******************************************************************************/
/*       l o a d J W K S F r o m D i s k C a c h e F o r P o l i c y        */
/******************************************************************************/

bool loadJWKSFromDiskCacheForPolicy(std::shared_ptr<IssuerPolicy> policy, time_t now,
                                    std::map<std::string, EvpPkeyPtr> &keysOut,
                                    std::string &emsg)
{
   keysOut.clear();
   if (!policy || JwksCacheFile.empty()) return false;
   std::scoped_lock lock(JwksDiskCacheMtx);
   std::unordered_map<std::string, JwksDiskEntry> cache;
   if (!loadJWKSCacheMap(cache, emsg)) return false;
   auto it = cache.find(policy->issuer);
   if (it == cache.end() || it->second.jwksB64.empty()) return false;
   if (int ttl = jwksCacheEffectiveTTL();
       ttl > 0 && (now - it->second.fetchedAt) > ttl) return false;

   std::string jwksJson;
   if (!decodeBase64URL(it->second.jwksB64, jwksJson))
      {emsg = "cached JWKS decode failed";
       return false;
      }
   if (!loadJWKS(jwksJson, keysOut, emsg)) return false;
   return true;
}

/******************************************************************************/
/*        s t o r e J W K S I n D i s k C a c h e F o r P o l i c y         */
/******************************************************************************/

void storeJWKSInDiskCacheForPolicy(std::shared_ptr<IssuerPolicy> policy,
                                   const std::string &jwksUrl,
                                   const std::string &jwksJson,
                                   time_t fetchedAt)
{
   if (!policy || JwksCacheFile.empty() || jwksJson.empty()) return;
   std::scoped_lock lock(JwksDiskCacheMtx);
   std::unordered_map<std::string, JwksDiskEntry> cache;
   std::string emsg;
   if (!loadJWKSCacheMap(cache, emsg))
      {
       OAuth2Log.Emsg("Auth", "oauth2", ("jwks cache load failed: " + emsg).c_str());
       return;
      }
   JwksDiskEntry &e = cache[policy->issuer];
   e.jwksUrl = jwksUrl;
   e.fetchedAt = fetchedAt;
   e.jwksB64 = encodeBase64URL(jwksJson);
   if (!writeJWKSCacheMap(cache, emsg))
      OAuth2Log.Emsg("Auth", "oauth2", ("jwks cache write failed: " + emsg).c_str());
}

/******************************************************************************/
/*                 r e f r e s h J W K S F o r P o l i c y                  */
/******************************************************************************/

bool refreshJWKSForPolicy(std::shared_ptr<IssuerPolicy> policy, bool force,
                          std::string &emsg)
{
   if (!policy)
      {emsg = "missing issuer policy";
       return false;
      }
   std::scoped_lock lock(policy->keysMtx);
   time_t now = time(nullptr);
   if (!force && !policy->jwksKeys.empty()
   && (now - policy->lastJwksLoad) < JwksRefresh) return true;

   if (!force && JwksCacheFile.size())
      {
       std::map<std::string, EvpPkeyPtr> cachedKeys;
       std::string cmsg;
       if (loadJWKSFromDiskCacheForPolicy(policy, now, cachedKeys, cmsg))
          {
           freeKeys(policy->jwksKeys);
           policy->jwksKeys.swap(cachedKeys);
           policy->lastJwksLoad = now;
           return true;
          }
      }

   std::string useJwks = policy->jwksURL;
   if (useJwks.empty())
      {if (policy->oidcConfigURL.empty())
          {emsg = "OIDC config URL not set";
           return false;
          }
       std::string cfg;
       if (!fetchURL(policy->oidcConfigURL, cfg, emsg))
          {emsg = "failed OIDC discovery fetch: " + emsg;
           return false;
          }
       if (!getStringClaim(cfg, "jwks_uri", useJwks))
          {emsg = "jwks_uri missing or empty in OIDC discovery";
           return false;
          }
       if (!hasPrefix(useJwks, "https://"))
          {emsg = "jwks_uri in discovery must use https://";
           return false;
          }
      }

   std::string jwks;
   if (!fetchURL(useJwks, jwks, emsg))
      {
       std::string fetchErr = "failed JWKS fetch: " + emsg;
       std::map<std::string, EvpPkeyPtr> cachedKeys;
       std::string cmsg;
       if (JwksCacheFile.size()
       &&  loadJWKSFromDiskCacheForPolicy(policy, now, cachedKeys, cmsg))
          {
           freeKeys(policy->jwksKeys);
           policy->jwksKeys.swap(cachedKeys);
           policy->lastJwksLoad = now;
           emsg.clear();
           return true;
          }
       emsg = fetchErr;
       return false;
      }

   std::map<std::string, EvpPkeyPtr> newKeys;
   if (!loadJWKS(jwks, newKeys, emsg)) return false;
   freeKeys(policy->jwksKeys);
   policy->jwksKeys.swap(newKeys);
   policy->lastJwksLoad = now;
   storeJWKSInDiskCacheForPolicy(policy, useJwks, jwks, now);
   return true;
}

/******************************************************************************/
/*                      a u d i e n c e M a t c h e s                       */
/******************************************************************************/

bool audienceMatches(std::shared_ptr<IssuerPolicy> policy,
                     const nlohmann::json &payloadObj)
{
   if (!policy || policy->audiences.empty()) return true;
   std::string tokAud;
   bool haveSingleAud = getStringClaim(payloadObj, "aud", tokAud);
   for (const auto &aud : policy->audiences)
      {
       if ((haveSingleAud && tokAud == aud)
       ||   hasStringInArrayClaim(payloadObj, "aud", aud))
          return true;
      }
   return false;
}

/******************************************************************************/
/*                  p a r s e A n d V a l i d a t e J W T                   */
/******************************************************************************/

bool parseAndValidateJWT(const char *rawTok, std::string &payloadJSON,
                         std::string &headerJSON,
                         std::string &identity, uint64_t &expOut,
                         std::string &emsg,
                         std::string *identityMethod)
{
   maybeReloadOAuth2FileConfig();
   payloadJSON.clear();
   headerJSON.clear();
   identity.clear();
   expOut = 0;

   int tlen = 0;
   const char *tok = Strip(rawTok, tlen);
   if (!tok || tlen <= 0) {emsg = "invalid token format"; return false;}
   std::string jwt(tok, tlen);

   size_t dot1 = jwt.find('.');
   size_t dot2 = (dot1 == std::string::npos ? std::string::npos : jwt.find('.', dot1+1));
   if (dot1 == std::string::npos || dot2 == std::string::npos)
      {emsg = "token is not JWT"; return false;}
   std::string h64 = jwt.substr(0, dot1);
   std::string p64 = jwt.substr(dot1 + 1, dot2 - dot1 - 1);
   std::string s64 = jwt.substr(dot2 + 1);
   std::string hdr, sig;
   if (!decodeBase64URL(h64, hdr) || !decodeBase64URL(p64, payloadJSON)
   ||  !decodeBase64URL(s64, sig))
      {emsg = "JWT decode failed"; return false;}
   headerJSON = hdr;
   nlohmann::json hdrObj, payloadObj;
   if (!parseJsonObject(headerJSON, hdrObj) || !parseJsonObject(payloadJSON, payloadObj))
      {emsg = "JWT JSON decode failed"; return false;}

   std::string alg;
   std::string kid;
   if (!getStringClaim(hdrObj, "alg", alg)) {emsg = "JWT alg missing"; return false;}
   if (alg != "RS256") {emsg = "unsupported JWT alg"; return false;}
   getStringClaim(hdrObj, "kid", kid);

   std::string tokIss;
   if (!getStringClaim(payloadObj, "iss", tokIss))
      {emsg = "token issuer missing"; return false;}
   std::shared_ptr<IssuerPolicy> policy;
   {
      std::scoped_lock lock(ConfigMtx);
      auto pIt = IssuerPolicyByIssuer.find(tokIss);
      if (pIt == IssuerPolicyByIssuer.end())
         {emsg = "token issuer not configured";
          return false;
         }
      policy = pIt->second;
   }

   if (!audienceMatches(policy, payloadObj))
      {emsg = "token audience mismatch"; return false;}

   uint64_t exp = 0;
   bool haveExp = getUintClaim(payloadObj, "exp", exp);
   if (expiry > 0 && !haveExp) {emsg = "token expiry missing"; return false;}
   time_t now = time(nullptr);
   // expiry == 0 means "ignore": a present-but-expired exp is not enforced.
   if (expiry != 0 && haveExp && exp + ClockSkew < static_cast<uint64_t>(now))
      {emsg = "token expired"; return false;}
   uint64_t nbf = 0;
   if (getUintClaim(payloadObj, "nbf", nbf)
   &&  nbf > static_cast<uint64_t>(now) + ClockSkew)
      {emsg = "token not yet valid"; return false;}
   if (haveExp) expOut = exp;

   if (!refreshJWKSForPolicy(policy, false, emsg))
      return false;

   const std::string signedData = jwt.substr(0, dot2);

   // Attempt verification against the currently loaded keyset. Also reports
   // whether the token's kid was found, so the caller can distinguish "key may
   // have rotated" (kid unknown) from "signature is simply invalid" (kid known).
   auto attemptVerify = [&](bool &kidKnown) -> bool
      {
       std::scoped_lock lock(policy->keysMtx);
       kidKnown = false;
       if (!kid.empty())
          {auto it = policy->jwksKeys.find(kid);
           if (it == policy->jwksKeys.end()) return false;
           kidKnown = true;
           return verifyRS256(it->second.get(), signedData, sig);
          }
       for (auto &it : policy->jwksKeys)
          if (verifyRS256(it.second.get(), signedData, sig)) return true;
       return false;
      };

   bool kidKnown = false;
   bool verified = attemptVerify(kidKnown);
   if (!verified)
      {
       const bool haveRefreshSrc =
          !(policy->jwksURL.empty() && policy->oidcConfigURL.empty());
       // Only hit the network when the signing key might genuinely be new: a
       // known kid with a bad signature is just an invalid token and must not
       // trigger a JWKS re-fetch (otherwise junk tokens become a DoS/refresh
       // amplification vector against this server and the issuer's JWKS
       // endpoint). Forced refreshes are additionally rate-limited so a burst
       // of unknown-kid tokens cannot hammer the endpoint.
       constexpr int kForcedJwksMinInterval = 5; // seconds
       bool keyMaybeRotated = !kidKnown;
       bool cooldownElapsed = false;
       if (keyMaybeRotated)
          {std::scoped_lock lock(policy->keysMtx);
           cooldownElapsed =
              (time(nullptr) - policy->lastJwksLoad) >= kForcedJwksMinInterval;
          }
       if (haveRefreshSrc && keyMaybeRotated && cooldownElapsed)
          {if (!refreshJWKSForPolicy(policy, true, emsg))
              return false;
           verified = attemptVerify(kidKnown);
          }
       if (!verified) {emsg = "JWT signature validation failed"; return false;}
      }

   if (!policy->forcedIdentityClaim.empty())
      {
       std::string forced = policy->forcedIdentityClaim;
       if (!getStringClaim(payloadObj, forced.c_str(), identity)
       ||  identity.empty())
          {emsg = "token identity claim missing: " + forced;
           return false;
          }
       if (forced == "email")
          {
           std::string rawEmail = identity;
           std::string emailKey = normalizeEmailKey(rawEmail);
           std::string mappedUser;
           {
              std::scoped_lock lock(ConfigMtx);
              auto mIt = EmailIdentityMap.find(emailKey);
              if (mIt != EmailIdentityMap.end()) mappedUser = mIt->second;
           }
           if (mappedUser.empty())
              {emsg = "token email is not mapped to a username";
               return false;
              }
           identity = mappedUser;
           if (identityMethod)
              *identityMethod = "email-map:" + rawEmail;
          }
       else
          {
           if (identityMethod)
              *identityMethod = "claim:" + forced;
          }
      } else {
       std::string usedClaim;
       for (const auto &claim : IdentityClaims)
          {if (getStringClaim(payloadObj, claim.c_str(), identity) && !identity.empty())
              {usedClaim = claim;
               break;
              }
          }
       if (identity.empty())
          {emsg = "token identity claim missing";
           return false;
          }
       if (identityMethod)
          *identityMethod = "default:" + usedClaim;
      }
   if (!isSafeIdentity(identity))
      {emsg = "resolved identity contains unacceptable characters";
       return false;
      }
   return true;
}

/******************************************************************************/
/*                p r u n e T o k e n C a c h e L o c k e d                 */
/******************************************************************************/

void pruneTokenCacheLocked(uint64_t now)
{
   for (auto it = TokenCache.begin(); it != TokenCache.end(); )
      {
       if (it->second.expiresAt <= now) it = TokenCache.erase(it);
       else ++it;
      }
}

/******************************************************************************/
/*                     t o k e n C a c h e L o o k u p                      */
/******************************************************************************/

bool tokenCacheLookup(const std::string &tok, uint64_t now, CachedTokenEntry &out)
{
   if (tok.empty() || TokenCacheMax == 0) return false;
   std::scoped_lock lock(TokenCacheMtx);
   auto it = TokenCache.find(tok);
   if (it == TokenCache.end()) return false;
   // Lazily drop a single expired entry instead of scanning the whole cache.
   if (it->second.expiresAt <= now)
      {TokenCache.erase(it);
       return false;
      }
   out = it->second;
   return true;
}

/******************************************************************************/
/*                       t o k e n C a c h e S i z e                        */
/******************************************************************************/

size_t tokenCacheSize()
{
   std::scoped_lock lock(TokenCacheMtx);
   return TokenCache.size();
}

/******************************************************************************/
/*                      t o k e n C a c h e S t o r e                       */
/******************************************************************************/

void tokenCacheStore(const std::string &tok, const CachedTokenEntry &entry, uint64_t now)
{
   if (tok.empty() || TokenCacheMax == 0) return;
   std::scoped_lock lock(TokenCacheMtx);
   // Only pay for a full scan when we are about to exceed the bound. The common
   // case (cache below capacity) is a single insert with no O(n) work.
   if (TokenCacheMax > 0 && TokenCache.size() >= static_cast<size_t>(TokenCacheMax)
   &&  TokenCache.find(tok) == TokenCache.end())
      {
       pruneTokenCacheLocked(now);
       if (TokenCache.size() >= static_cast<size_t>(TokenCacheMax))
          {
           // Still full of unexpired entries: evict the soonest-to-expire one
           // so the entry we drop is the closest to becoming useless anyway.
           auto victim = TokenCache.begin();
           for (auto it = TokenCache.begin(); it != TokenCache.end(); ++it)
              if (it->second.expiresAt < victim->second.expiresAt) victim = it;
           if (victim != TokenCache.end()) TokenCache.erase(victim);
          }
      }
   TokenCache[tok] = entry;
}

/******************************************************************************/
/*                      d e b u g P r i n t T o k e n                       */
/******************************************************************************/

void debugPrintToken(const char *tident, const char *mappedName,
                     const std::string &headerJSON,
                     const std::string &payloadJSON, bool cacheHit,
                     const std::string &identityMethod = std::string())
{
   if (!DebugToken && !DebugTokenClaims) return;
   const char *tid = (tident && *tident ? tident : "oauth2");
   uint64_t hits = TokenCacheHits.load();
   uint64_t misses = TokenCacheMisses.load();
   size_t csize = tokenCacheSize();

   if (DebugToken)
      {std::string hMsg = std::string("oauth2.jwt.header ") + headerJSON;
       std::string pMsg = std::string("oauth2.jwt.payload ") + payloadJSON;
       std::string sMsg = "oauth2.cache.stats"
                          " cache_hit=" + std::string(cacheHit ? "1" : "0") +
                          " hits=" + std::to_string(hits) +
                          " misses=" + std::to_string(misses) +
                          " size=" + std::to_string(csize);
       OAuth2Log.Emsg("Auth", tid, hMsg.c_str());
       OAuth2Log.Emsg("Auth", tid, pMsg.c_str());
       OAuth2Log.Emsg("Auth", tid, sMsg.c_str());
       return;
      }

   // Claims-only mode: log selected fields, avoid full token payload output.
   nlohmann::json hObj, pObj;
   parseJsonObject(headerJSON, hObj);
   parseJsonObject(payloadJSON, pObj);
   std::string kid, alg, typ;
   std::string iss, aud, sub, prefUser, azp;
   uint64_t iat = 0, nbf = 0, exp = 0;
   bool haveIat = getUintClaim(pObj, "iat", iat);
   bool haveNbf = getUintClaim(pObj, "nbf", nbf);
   bool haveExp = getUintClaim(pObj, "exp", exp);

   getStringClaim(hObj, "kid", kid);
   getStringClaim(hObj, "alg", alg);
   getStringClaim(hObj, "typ", typ);
   getStringClaim(pObj, "iss", iss);
   getStringClaim(pObj, "aud", aud);
   getStringClaim(pObj, "sub", sub);
   getStringClaim(pObj, "preferred_username", prefUser);
   getStringClaim(pObj, "azp", azp);

   std::string msg = "oauth2.jwt.claims";
   msg += " alg='" + alg + "'";
   msg += " kid='" + kid + "'";
   msg += " typ='" + typ + "'";
   msg += " iss='" + iss + "'";
   msg += " aud='" + aud + "'";
   msg += " sub='" + sub + "'";
   msg += " preferred_username='" + prefUser + "'";
   msg += " azp='" + azp + "'";
   if (haveIat) msg += " iat=" + std::to_string(iat);
   if (haveNbf) msg += " nbf=" + std::to_string(nbf);
   if (haveExp) msg += " exp=" + std::to_string(exp);
   msg += " mapped_name='" + std::string(mappedName ? mappedName : "") + "'";
   if (!identityMethod.empty())
      msg += " identity_method='" + identityMethod + "'";
   msg += " cache_hit=" + std::string(cacheHit ? "1" : "0");
   msg += " cache_hits=" + std::to_string(hits);
   msg += " cache_misses=" + std::to_string(misses);
   msg += " cache_size=" + std::to_string(csize);
   OAuth2Log.Emsg("Auth", tid, msg.c_str());
}

// Outcome of trying to apply one Init option token to the global config.
enum class OptResult { NotMine, Ok, Error };

// Consume the next token as an integer in [lo,hi], optionally honouring a
// trailing 'k'/'K' (x1024) multiplier. On failure sets erp and returns false.
/******************************************************************************/
/*                           n e x t I n t A r g                            */
/******************************************************************************/

bool nextIntArg(XrdOucTokenizer &cfg, XrdOucErrInfo *erp, const char *optName,
                long lo, long hi, bool kSuffix, long &out)
{
   char *arg = cfg.GetToken();
   if (!arg)
      {Fatal(erp, (std::string(optName) + " argument missing").c_str(), EINVAL);
       return false;
      }
   const std::string_view sv(arg);
   long v = 0;
   auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), v);
   if (ec != std::errc())
      {Fatal(erp, (std::string(optName) + " argument invalid").c_str(), EINVAL);
       return false;
      }
   if (kSuffix && ptr < sv.data() + sv.size())
      {if (*ptr == 'k' || *ptr == 'K') {v *= 1024; ++ptr;}
       else
          {Fatal(erp, (std::string(optName) + " argument invalid").c_str(), EINVAL);
           return false;
          }
      }
   if (ptr != sv.data() + sv.size() || v < lo || v > hi)
      {Fatal(erp, (std::string(optName) + " argument invalid").c_str(), EINVAL);
       return false;
      }
   out = v;
   return true;
}

// Fetch an argument that must follow optName and requires a current issuer
// policy. Returns nullptr (erp set) on a missing argument or missing issuer.
/******************************************************************************/
/*                        n e x t P o l i c y A r g                         */
/******************************************************************************/

const char *nextPolicyArg(XrdOucTokenizer &cfg, XrdOucErrInfo *erp,
                          const char *optName,
                          const std::shared_ptr<IssuerPolicy> &curPolicy)
{
   char *arg = cfg.GetToken();
   if (!arg)
      {Fatal(erp, (std::string(optName) + " argument missing").c_str(), EINVAL);
       return nullptr;
      }
   if (!curPolicy)
      {Fatal(erp, (std::string(optName) + " requires a prior -issuer").c_str(), EINVAL);
       return nullptr;
      }
   return arg;
}

/******************************************************************************/
/*                  a p p l y N u m e r i c I n i t O p t                   */
/******************************************************************************/

OptResult applyNumericInitOpt(std::string_view val, XrdOucTokenizer &cfg,
                              XrdOucErrInfo *erp)
{
   long v = 0;
   if (val == "-maxsz")
      {if (!nextIntArg(cfg, erp, "-maxsz", 1, 524288, true, v)) return OptResult::Error;
       MaxTokSize = static_cast<int>(v); return OptResult::Ok;
      }
   if (val == "-jwks-refresh")
      {if (!nextIntArg(cfg, erp, "-jwks-refresh", 1, LONG_MAX, false, v)) return OptResult::Error;
       JwksRefresh = static_cast<int>(v); return OptResult::Ok;
      }
   if (val == "-jwks-cache-ttl")
      {if (!nextIntArg(cfg, erp, "-jwks-cache-ttl", 0, LONG_MAX, false, v)) return OptResult::Error;
       JwksCacheTTL = static_cast<int>(v); return OptResult::Ok;
      }
   if (val == "-clock-skew")
      {if (!nextIntArg(cfg, erp, "-clock-skew", 0, 3600, false, v)) return OptResult::Error;
       ClockSkew = static_cast<int>(v); return OptResult::Ok;
      }
   if (val == "-token-cache-max")
      {if (!nextIntArg(cfg, erp, "-token-cache-max", 0, LONG_MAX, false, v)) return OptResult::Error;
       TokenCacheMax = static_cast<int>(v); return OptResult::Ok;
      }
   if (val == "-token-cache-noexp-ttl")
      {if (!nextIntArg(cfg, erp, "-token-cache-noexp-ttl", 0, LONG_MAX, false, v)) return OptResult::Error;
       TokenCacheNoExpTTL = static_cast<int>(v); return OptResult::Ok;
      }
   return OptResult::NotMine;
}

/******************************************************************************/
/*                   a p p l y I s s u e r I n i t O p t                    */
/******************************************************************************/

OptResult applyIssuerInitOpt(std::string_view val, XrdOucTokenizer &cfg,
                             std::shared_ptr<IssuerPolicy> &curPolicy,
                             XrdOucErrInfo *erp)
{
   if (val == "-issuer")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-issuer argument missing", EINVAL); return OptResult::Error;}
       auto pIt = IssuerPolicyByIssuer.find(arg);
       if (pIt != IssuerPolicyByIssuer.end()) curPolicy = pIt->second;
       else
          {curPolicy = std::make_shared<IssuerPolicy>();
           curPolicy->issuer = arg;
           IssuerPolicies.push_back(curPolicy);
           IssuerPolicyByIssuer[curPolicy->issuer] = curPolicy;
          }
       return OptResult::Ok;
      }
   if (val == "-audience")
      {const char *arg = nextPolicyArg(cfg, erp, "-audience", curPolicy);
       if (!arg) return OptResult::Error;
       curPolicy->audiences.push_back(arg); return OptResult::Ok;
      }
   if (val == "-oidc-config-url")
      {const char *arg = nextPolicyArg(cfg, erp, "-oidc-config-url", curPolicy);
       if (!arg) return OptResult::Error;
       curPolicy->oidcConfigURL = arg; return OptResult::Ok;
      }
   if (val == "-jwks-url")
      {const char *arg = nextPolicyArg(cfg, erp, "-jwks-url", curPolicy);
       if (!arg) return OptResult::Error;
       curPolicy->jwksURL = arg; return OptResult::Ok;
      }
   if (val == "-forced-identity-claim")
      {const char *arg = nextPolicyArg(cfg, erp, "-forced-identity-claim", curPolicy);
       if (!arg) return OptResult::Error;
       curPolicy->forcedIdentityClaim = arg; return OptResult::Ok;
      }
   if (val == "-base-path")
      {const char *arg = nextPolicyArg(cfg, erp, "-base-path", curPolicy);
       if (!arg) return OptResult::Error;
       std::string localErr;
       if (!setIssuerBasePath(arg, curPolicy->basePath, localErr))
          {Fatal(erp, localErr.c_str(), EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-restricted-path")
      {const char *arg = nextPolicyArg(cfg, erp, "-restricted-path", curPolicy);
       if (!arg) return OptResult::Error;
       std::string localErr;
       if (!appendIssuerPathOption(arg, curPolicy->restrictedPaths, localErr))
          {Fatal(erp, localErr.c_str(), EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   return OptResult::NotMine;
}

/******************************************************************************/
/*                    a p p l y O t h e r I n i t O p t                     */
/******************************************************************************/

OptResult applyOtherInitOpt(std::string_view val, XrdOucTokenizer &cfg,
                            XrdOucErrInfo *erp)
{
   if (val == "-expiry")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-expiry argument missing", EINVAL); return OptResult::Error;}
       const std::string_view argView(arg);
            if (argView == "ignore")   expiry =  0;
       else if (argView == "optional") expiry = -1;
       else if (argView == "required") expiry =  1;
       else {Fatal(erp, "-expiry argument invalid", EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-config-file")
      {// Already consumed in the pre-scan; just swallow the argument here.
       if (!cfg.GetToken()) {Fatal(erp, "-config-file argument missing", EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-jwks-cache-file")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-jwks-cache-file argument missing", EINVAL); return OptResult::Error;}
       JwksCacheFile = arg; return OptResult::Ok;
      }
   if (val == "-identity-claim")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-identity-claim argument missing", EINVAL); return OptResult::Error;}
       if (!customIdentityClaims) {IdentityClaims.clear(); customIdentityClaims = true;}
       IdentityClaims.push_back(arg); return OptResult::Ok;
      }
   if (val == "-entity-claim")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-entity-claim argument missing", EINVAL); return OptResult::Error;}
       std::string localErr;
       if (!storeEntityClaimEntry(arg, localErr))
          {Fatal(erp, localErr.c_str(), EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-role-claim")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-role-claim argument missing", EINVAL); return OptResult::Error;}
       std::string localErr;
       if (!setNativeFieldClaim(arg, RoleClaim, "role-claim", localErr))
          {Fatal(erp, localErr.c_str(), EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-grps-claim")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-grps-claim argument missing", EINVAL); return OptResult::Error;}
       std::string localErr;
       if (!setNativeFieldClaim(arg, GrpsClaim, "grps-claim", localErr))
          {Fatal(erp, localErr.c_str(), EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-email-map")
      {char *arg = cfg.GetToken();
       if (!arg) {Fatal(erp, "-email-map argument missing", EINVAL); return OptResult::Error;}
       std::string localErr;
       if (!storeEmailMapEntry(arg, localErr))
          {Fatal(erp, localErr.c_str(), EINVAL); return OptResult::Error;}
       return OptResult::Ok;
      }
   if (val == "-debug-token")       {DebugToken = true;       return OptResult::Ok;}
   if (val == "-show-token-claims") {DebugTokenClaims = true; return OptResult::Ok;}
   return OptResult::NotMine;
}

// Pre-scan: pull out -config-file (consumed here) and accumulate the remaining
// tokens into inlineParms. Returns false (erp set) on a malformed -config-file.
/******************************************************************************/
/*                     p r e s c a n I n i t P a r m s                      */
/******************************************************************************/

bool prescanInitParms(const char *parms, std::string &selectedCfgPath,
                      bool &requestedCfgOverride, std::string &inlineParms,
                      XrdOucErrInfo *erp)
{
   if (!parms || !*parms) return true;
   const std::string_view parmView(parms);
   std::vector<char> pbuf(parmView.begin(), parmView.end());
   pbuf.push_back('\0');
   XrdOucTokenizer t(pbuf.data());
   char *tok = nullptr;
   // Multiple sec.protparm lines are joined with newlines by XrdSecServer; each
   // record must be scanned (same pattern as other sec plugins, e.g. gsi).
   while (t.GetLine())
        {
         while ((tok = t.GetToken()))
              {
               if (std::string_view(tok) == "-config-file")
                  {char *path = t.GetToken();
                   if (!path || !*path)
                      {Fatal(erp, "-config-file argument missing", EINVAL);
                       return false;
                      }
                   selectedCfgPath = path;
                   requestedCfgOverride = true;
                   continue;
                  }
               if (!inlineParms.empty()) inlineParms.push_back(' ');
               inlineParms += tok;
              }
        }
   return true;
}

// Parse the effective option string into the global config and issuer policies.
/******************************************************************************/
/*                       p a r s e I n i t P a r m s                        */
/******************************************************************************/

bool parseInitParms(const char *parms, std::shared_ptr<IssuerPolicy> &curPolicy,
                    XrdOucErrInfo *erp)
{
   if (!parms || !*parms) return true;
   const std::string_view parmView(parms);
   std::vector<char> cfgParms(parmView.begin(), parmView.end());
   cfgParms.push_back('\0');
   XrdOucTokenizer cfg(cfgParms.data());
   char *val = nullptr;
   while (cfg.GetLine())
        {
         while ((val = cfg.GetToken()))
              {
               const std::string_view opt(val);
               OptResult r = applyNumericInitOpt(opt, cfg, erp);
               if (r == OptResult::Error) return false;
               if (r == OptResult::Ok) continue;
               r = applyIssuerInitOpt(opt, cfg, curPolicy, erp);
               if (r == OptResult::Error) return false;
               if (r == OptResult::Ok) continue;
               r = applyOtherInitOpt(opt, cfg, erp);
               if (r == OptResult::Error) return false;
               if (r == OptResult::Ok) continue;
               XrdOucString eTxt("Invalid parameter - "); eTxt += val;
               Fatal(erp, eTxt.c_str(), EINVAL);
               return false;
              }
        }
   return true;
}

// Resolve discovery URLs and warm the JWKS cache for every configured issuer.
/******************************************************************************/
/*           v a l i d a t e A n d W a r m I n i t I s s u e r s            */
/******************************************************************************/

bool validateAndWarmInitIssuers(XrdOucErrInfo *erp)
{
   for (auto &policy : IssuerPolicies)
      {
       if (policy->oidcConfigURL.empty() && !policy->issuer.empty())
          policy->oidcConfigURL = joinURL(policy->issuer, "/.well-known/openid-configuration");
       if (policy->oidcConfigURL.empty() && policy->jwksURL.empty())
          {std::string e = "issuer '" + policy->issuer + "' requires -oidc-config-url or -jwks-url";
           Fatal(erp, e.c_str(), EINVAL); return false;
          }
       if ((!policy->oidcConfigURL.empty() && !hasPrefix(policy->oidcConfigURL, "https://"))
       ||  (!policy->jwksURL.empty() && !hasPrefix(policy->jwksURL, "https://")))
          {std::string e = "issuer '" + policy->issuer + "' has non-https OIDC/JWKS URL";
           Fatal(erp, e.c_str(), EINVAL); return false;
          }
       std::string emsg;
       if (!refreshJWKSForPolicy(policy, true, emsg))
          {std::string e = "issuer '" + policy->issuer + "': " + emsg;
           Fatal(erp, e.c_str(), EINVAL); return false;
          }
       if (policy->audiences.empty())
          OAuth2Log.Emsg("Config", "oauth2",
             ("WARNING: issuer '" + policy->issuer + "' has no audience "
              "configured; any signed, unexpired token from this issuer will be "
              "accepted regardless of its 'aud' claim. Configure at least one "
              "audience to bind tokens to this service.").c_str());
      }
   return true;
}

static const uint64_t oauth2Version = 0;

/******************************************************************************/
/*                         i s C o n f i g u r e d                          */
/******************************************************************************/

bool isConfigured()
{
   std::scoped_lock lock(ConfigMtx);
   return !IssuerPolicies.empty();
}

/******************************************************************************/
/*                           s t r i p T o k e n                            */
/******************************************************************************/

const char *stripToken(const char *bTok, int &sz, int maxLen)
{
   return Strip(bTok, sz, maxLen);
}

/******************************************************************************/
/*                        v a l i d a t e T o k e n                         */
/******************************************************************************/

bool validateToken(const char *rawTok, std::string &identity, std::string &emsg,
                   uint64_t *expTime,
                   std::map<std::string, std::string> *entityAttrs,
                   std::string *roleOut,
                   std::string *grpsOut)
{
   if (!rawTok || !*rawTok)
      {emsg = "missing token";
       return false;
      }

   int tlen = 0;
   const char *tok = Strip(rawTok, tlen);
   if (!tok || tlen <= 0)
      {emsg = "token value malformed";
       return false;
      }

   if (tlen > MaxTokSize)
      {emsg = "token too large";
       return false;
      }

   std::string payloadJSON, headerJSON, msgRC, identityMethod;
   // Empty key means the digest failed: skip all caching and validate directly
   // rather than risk using raw token bytes as a key (see sha256hex).
   std::string tokKey = sha256hex(tok, static_cast<size_t>(tlen));
   uint64_t now = static_cast<uint64_t>(time(nullptr));
   if (negTokenCacheLookup(tokKey, now))
      {emsg = "token previously failed validation";
       return false;
      }
   CachedTokenEntry cached;
   if (tokenCacheLookup(tokKey, now, cached))
      {
       TokenCacheHits++;
       identity = cached.identity;
       if (expTime) *expTime = cached.expiresAt;
       if (entityAttrs) populateEntityAttrs(cached.payloadJSON, *entityAttrs);
       populateEntityNativeFields(cached.payloadJSON, roleOut, grpsOut);
       debugPrintToken("oauth2", identity.c_str(), cached.headerJSON,
                       cached.payloadJSON, true, cached.identityMethod);
       return true;
      }
   TokenCacheMisses++;

   uint64_t expOut = 0;
   if (!parseAndValidateJWT(tok, payloadJSON, headerJSON, identity, expOut, msgRC,
                             &identityMethod))
      {emsg = msgRC;
       negTokenCacheStore(tokKey, now);
       return false;
      }

   debugPrintToken("oauth2", identity.c_str(), headerJSON, payloadJSON, false,
                   identityMethod);

   CachedTokenEntry ins;
   ins.identity = identity;
   ins.identityMethod = identityMethod;
   ins.headerJSON = headerJSON;
   ins.payloadJSON = payloadJSON;
   ins.expiresAt = (expOut ? expOut : now + static_cast<uint64_t>(TokenCacheNoExpTTL));
   tokenCacheStore(tokKey, ins, now);
   if (expTime) *expTime = ins.expiresAt;
   if (entityAttrs) populateEntityAttrs(payloadJSON, *entityAttrs);
   populateEntityNativeFields(payloadJSON, roleOut, grpsOut);
   return true;
}

// Cheap expiry peek: hash the token and consult the validated-token cache only.
// Returns the cached expiry (unix seconds) for an already-validated token, or 0
// if it is not (or no longer) in the cache. Performs no signature, JWKS, or JSON
// work, so callers can use it to gate a full re-validation.
/******************************************************************************/
/*                    c a c h e d T o k e n E x p i r y                     */
/******************************************************************************/

uint64_t cachedTokenExpiry(const char *rawTok, int maxLen)
{
   if (!rawTok || !*rawTok) return 0;
   int tlen = 0;
   const char *tok = Strip(rawTok, tlen, maxLen);
   if (!tok || tlen <= 0 || tlen > MaxTokSize) return 0;
   std::string tokKey = sha256hex(tok, static_cast<size_t>(tlen));
   if (tokKey.empty()) return 0;
   uint64_t now = static_cast<uint64_t>(time(nullptr));
   CachedTokenEntry cached;
   if (tokenCacheLookup(tokKey, now, cached)) return cached.expiresAt;
   return 0;
}

/******************************************************************************/
/*                      i n i t S e c P r o t o c o l                       */
/******************************************************************************/

char *initSecProtocol(const char *parms, XrdOucErrInfo *erp)
{
   OAuth2Log.logger(&OAuth2Logger);

   {
      std::scoped_lock lock(ConfigMtx);
      if (!IssuerPolicies.empty())
         return dupString(InitProtocolParams);
      clearIssuerPolicies();
      EmailIdentityMap.clear();
   }
   EntityClaimMappings.clear();
   customEntityClaims = false;
   RoleClaim.clear();
   GrpsClaim.clear();
   JwksCacheFile.clear();
   JwksCacheTTL = 0;
   std::shared_ptr<IssuerPolicy> curPolicy;
   std::string fileBackedParms;
   std::string inlineParms;
   std::string selectedCfgPath = "/etc/xrootd/oauth2.cfg";
   bool requestedCfgOverride = false;
   bool loadedCfgFile = false;
   OAuth2ConfigWatch = false;
   OAuth2ConfigStatValid = false;

   // On any failure after this point, drop whatever issuer/email-map state was
   // partially built so a subsequent init does not mistake a half-configured
   // plugin for a successfully configured one.
   auto failInit = [&]() -> char *
      {
       std::scoped_lock lock(ConfigMtx);
       clearIssuerPolicies();
       EmailIdentityMap.clear();
       return nullptr;
      };

   if (!prescanInitParms(parms, selectedCfgPath, requestedCfgOverride, inlineParms, erp))
      return failInit();

   ino_t cfgIno = 0;
   time_t cfgMtime = 0;
   if (!parms || !*parms || requestedCfgOverride)
      {
       bool cfgFound = false;
       std::string cfgErr;
       if (!loadOAuth2IniAsArgs(selectedCfgPath.c_str(), fileBackedParms, cfgFound, cfgErr,
                              &cfgIno, &cfgMtime))
          {Fatal(erp, cfgErr.c_str(), EINVAL);
           return failInit();
          }
       if (!cfgFound)
          {std::string e = std::string("Missing required OAuth2 config file: ")
                         + selectedCfgPath;
           Fatal(erp, e.c_str(), ENOENT);
           return failInit();
          }
       loadedCfgFile = true;
      }

   std::string effectiveParms;
   if (!fileBackedParms.empty()) effectiveParms += fileBackedParms;
   if (!inlineParms.empty())
      {
       if (!effectiveParms.empty()) effectiveParms.push_back(' ');
       effectiveParms += inlineParms;
      }
   if (!parseInitParms(effectiveParms.empty() ? parms : effectiveParms.c_str(),
                       curPolicy, erp))
      return failInit();

   if (IssuerPolicies.empty())
      {Fatal(erp, "At least one -issuer must be configured.", EINVAL);
       return failInit();
      }

   if (!customEntityClaims)
      initDefaultEntityClaimMappings();

   if (IdentityClaims.empty())
      {Fatal(erp, "At least one identity claim must be configured.", EINVAL);
       return failInit();
      }

   curl_global_init(CURL_GLOBAL_DEFAULT);

   if (!validateAndWarmInitIssuers(erp)) return failInit();

   if (loadedCfgFile)
      {
       OAuth2ConfigPath = selectedCfgPath;
       OAuth2ConfigIno = cfgIno;
       OAuth2ConfigMTime = cfgMtime;
       OAuth2ConfigStatValid = true;
       OAuth2ConfigWatch = true;
      }

   InitProtocolParams =
      "TLS:" + std::to_string(oauth2Version) + ":"
             + std::to_string(MaxTokSize.load()) + ":";
   return dupString(InitProtocolParams);
}

} // namespace detail

/******************************************************************************/
/*                         I s C o n f i g u r e d                          */
/******************************************************************************/

bool IsConfigured()
{
   return detail::isConfigured();
}

/******************************************************************************/
/*                           S t r i p T o k e n                            */
/******************************************************************************/

const char *StripToken(const char *bTok, int &sz, int maxLen)
{
   return detail::stripToken(bTok, sz, maxLen);
}

/******************************************************************************/
/*                        V a l i d a t e T o k e n                         */
/******************************************************************************/

bool ValidateToken(const char *rawTok, std::string &identity, std::string &emsg,
                   uint64_t *expTime,
                   std::map<std::string, std::string> *entityAttrs,
                   std::string *roleOut,
                   std::string *grpsOut)
{
   return detail::validateToken(rawTok, identity, emsg, expTime, entityAttrs,
                                roleOut, grpsOut);
}

/******************************************************************************/
/*                    C a c h e d T o k e n E x p i r y                     */
/******************************************************************************/

uint64_t CachedTokenExpiry(const char *rawTok, int maxLen)
{
   return detail::cachedTokenExpiry(rawTok, maxLen);
}

/******************************************************************************/
/*                      I n i t S e c P r o t o c o l                       */
/******************************************************************************/

char *InitSecProtocol(const char *parms, XrdOucErrInfo *erp)
{
   return detail::initSecProtocol(parms, erp);
}

/******************************************************************************/
/*                                 I n i t                                  */
/******************************************************************************/

bool Init(XrdSysLogger *logger, const char *parms, std::string &emsg)
{
   detail::OAuth2Log.logger(logger ? logger : &detail::OAuth2Logger);
   XrdOucErrInfo erp;
   char *rc = detail::initSecProtocol(parms, &erp);
   if (!rc)
      {int ec = 0;
       const char *et = erp.getErrText(ec);
       emsg = (et && *et ? et : "OAuth2 initialization failed");
       return false;
      }
   std::free(rc);
   return true;
}

} // namespace XrdOAuth2
