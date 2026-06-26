/******************************************************************************/
/*                                                                            */
/*                    X r d O A u t h 2 D e t a i l . h h                     */
/*                                                                            */
/* (c) 2026 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*                                                                            */
/* Internal OAuth2 helpers exported for unit tests. Production code should    */
/* use the public XrdOAuth2.hh API; tests link libXrdUtils once and include   */
/* this header instead of compiling XrdOAuth2.cc again.                       */
/*                                                                            */
/******************************************************************************/

#ifndef XRDOAUTH2DETAIL_HH
#define XRDOAUTH2DETAIL_HH

#include <atomic>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/types.h>

#include <openssl/bn.h>
#include <openssl/evp.h>
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
#include <openssl/param_build.h>
#include <openssl/params.h>
#endif

class XrdOucErrInfo;

namespace XrdOAuth2
{
namespace detail
{

struct EvpPkeyDeleter    {void operator()(EVP_PKEY *p)     const noexcept {EVP_PKEY_free(p);}};
struct EvpMdCtxDeleter   {void operator()(EVP_MD_CTX *p)   const noexcept {EVP_MD_CTX_free(p);}};
struct BignumDeleter     {void operator()(BIGNUM *p)       const noexcept {BN_free(p);}};
struct EvpPkeyCtxDeleter {void operator()(EVP_PKEY_CTX *p) const noexcept {EVP_PKEY_CTX_free(p);}};

using EvpPkeyPtr    = std::unique_ptr<EVP_PKEY, EvpPkeyDeleter>;
using EvpMdCtxPtr   = std::unique_ptr<EVP_MD_CTX, EvpMdCtxDeleter>;
using BignumPtr     = std::unique_ptr<BIGNUM, BignumDeleter>;
using EvpPkeyCtxPtr = std::unique_ptr<EVP_PKEY_CTX, EvpPkeyCtxDeleter>;

struct EntityClaimMapping {
   std::string jwtClaim;
   std::string attrKey;
};

struct IssuerPolicy {
   std::string issuer;
   std::vector<std::string> audiences;
   std::string oidcConfigURL;
   std::string jwksURL;
   std::string forcedIdentityClaim;
   std::string basePath;
   std::vector<std::string> restrictedPaths;

   std::mutex keysMtx;
   std::map<std::string, EvpPkeyPtr> jwksKeys;
   time_t lastJwksLoad = 0;

   ~IssuerPolicy();
};

struct CachedTokenEntry {
   std::string identity;
   std::string identityMethod;
   std::string headerJSON;
   std::string payloadJSON;
   uint64_t    expiresAt = 0;
};

constexpr int kNegTokenCacheTTL = 30;

extern std::atomic<int> expiry;
extern std::atomic<int> MaxTokSize;
extern std::atomic<int> ClockSkew;
extern std::atomic<int> JwksRefresh;
extern bool customIdentityClaims;
extern bool customEntityClaims;
extern std::atomic<bool> DebugToken;
extern std::atomic<bool> DebugTokenClaims;
extern std::atomic<int> TokenCacheMax;
extern std::vector<std::string> IdentityClaims;
extern std::string RoleClaim;
extern std::string GrpsClaim;
extern std::vector<EntityClaimMapping> EntityClaimMappings;
extern std::vector<std::shared_ptr<IssuerPolicy>> IssuerPolicies;
extern std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> IssuerPolicyByIssuer;
extern std::unordered_map<std::string, std::string> EmailIdentityMap;
extern std::unordered_map<std::string, CachedTokenEntry> TokenCache;
extern std::unordered_map<std::string, uint64_t> TokenNegCache;
extern std::atomic<uint64_t> TokenCacheHits;
extern std::atomic<uint64_t> TokenCacheMisses;

const char *Strip(const char *bTok, int &sz, int maxLen = -1);

bool decodeBase64URL(const std::string &in, std::string &out);
std::string encodeBase64URL(const std::string &in);

void freeKeys(std::map<std::string, EvpPkeyPtr> &keys);
void clearIssuerPolicies();
void initDefaultEntityClaimMappings();
bool storeEntityClaimEntry(const std::string &spec, std::string &emsg);
bool appendIssuerPathOption(const std::string &val,
                            std::vector<std::string> &target,
                            std::string &emsg);

bool parseAndValidateJWT(const char *rawTok, std::string &payloadJSON,
                         std::string &headerJSON,
                         std::string &identity, uint64_t &expOut,
                         std::string &emsg,
                         std::string *identityMethod = nullptr);

bool validateToken(const char *rawTok, std::string &identity, std::string &emsg,
                   uint64_t *expTime = nullptr,
                   std::map<std::string, std::string> *entityAttrs = nullptr,
                   std::string *roleOut = nullptr,
                   std::string *grpsOut = nullptr);

bool loadJWKS(const std::string &json, std::map<std::string, EvpPkeyPtr> &keys,
              std::string &emsg);

bool negTokenCacheLookup(const std::string &tokKey, uint64_t now);
void negTokenCacheStore(const std::string &tokKey, uint64_t now);

void tokenCacheStore(const std::string &tok, const CachedTokenEntry &entry, uint64_t now);
bool tokenCacheLookup(const std::string &tok, uint64_t now, CachedTokenEntry &out);
size_t tokenCacheSize();

bool parseReloadableIniSections(const char *path,
                                const std::string &fileContents,
                                std::vector<std::shared_ptr<IssuerPolicy>> &outPolicies,
                                std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> &outByIssuer,
                                std::unordered_map<std::string, std::string> &outEmailMap,
                                std::string &emsg);

bool prescanInitParms(const char *parms, std::string &selectedCfgPath,
                      bool &requestedCfgOverride, std::string &inlineParms,
                      XrdOucErrInfo *erp);

bool parseInitParms(const char *parms, std::shared_ptr<IssuerPolicy> &curPolicy,
                    XrdOucErrInfo *erp);

bool loadOAuth2IniAsArgs(const char *path, std::string &opts, bool &found,
                         std::string &emsg, ino_t *inoOut = nullptr,
                         time_t *mtimeOut = nullptr);

} // namespace detail
} // namespace XrdOAuth2

#endif
