#include <chrono>
#include <ctime>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include "XrdNet/XrdNetAddrInfo.hh"
#include "XrdOAuth2/XrdOAuth2.hh"
#include "XrdOAuth2/XrdOAuth2Detail.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "../../src/XrdSecoauth2/XrdSecProtocoloauth2.cc"

using namespace XrdOAuth2::detail;

namespace {

// Current wall-clock time in epoch seconds, expressed via std::chrono.
time_t NowSeconds()
{
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

std::string Base64URLEncode(const unsigned char *data, size_t len)
{
  static const char tbl[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string out;
  out.reserve(((len + 2) / 3) * 4);
  size_t i = 0;
  while (i + 2 < len) {
    uint32_t v = (uint32_t(data[i]) << 16) | (uint32_t(data[i + 1]) << 8) |
                 uint32_t(data[i + 2]);
    out.push_back(tbl[(v >> 18) & 0x3F]);
    out.push_back(tbl[(v >> 12) & 0x3F]);
    out.push_back(tbl[(v >> 6) & 0x3F]);
    out.push_back(tbl[v & 0x3F]);
    i += 3;
  }
  if (i < len) {
    uint32_t v = uint32_t(data[i]) << 16;
    out.push_back(tbl[(v >> 18) & 0x3F]);
    if (i + 1 < len) {
      v |= uint32_t(data[i + 1]) << 8;
      out.push_back(tbl[(v >> 12) & 0x3F]);
      out.push_back(tbl[(v >> 6) & 0x3F]);
    } else {
      out.push_back(tbl[(v >> 12) & 0x3F]);
    }
  }
  return out;
}

std::string Base64URLEncode(const std::string &s)
{
  return Base64URLEncode(reinterpret_cast<const unsigned char *>(s.data()),
                         s.size());
}

std::string BigNumToBase64URL(const BIGNUM *bn)
{
  int nbytes = BN_num_bytes(bn);
  std::vector<unsigned char> buf(nbytes);
  BN_bn2bin(bn, buf.data());
  return Base64URLEncode(buf.data(), buf.size());
}

struct KeyMaterial {
  EVP_PKEY *pkey{nullptr};
  std::string jwks;
};

KeyMaterial MakeKeyAndJWKS()
{
  KeyMaterial km;
  // 2048-bit RSA test key. The plugin rejects RSA signing keys smaller than
  // 2048 bits (kMinRSAKeyBits), so the fixture must meet that floor too.
  static const char kTestPrivateKeyPem[] =
      "-----BEGIN PRIVATE KEY-----\n"
      "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDLFl+tCqSrLgpk\n"
      "93Di3AMN6LOXGQs/RyOmd+xMNyRKrth32cy00RWOzr7T1LCOzBXRpEKjQDIH5Ecp\n"
      "nXSDYI1qpizqRwxl4iaM76MBIGC9zMY8jBdN+BgVn/rRdmwhouPofIpQAwlznYvb\n"
      "cQIE63eIBAR5NAFNBmJpER16imgXdeMM3WP5qgbaO8bqilsqHWAHb+4k53x2v9yT\n"
      "R6+vp1vuj3FHrU8XzbaBfq0so3i1ELBCvmqoGEPKhTyMubw/SVVzToarJQKGjnBa\n"
      "0EIwdv6RrRE4FSTBV0/25HlUpto949Hhl9oLrWxeSLiBr7v7PalXfzSFbhEnMDn9\n"
      "Dj8CWEBJAgMBAAECggEBAJwqQToT9Bn5ll6bc3/PBL4+UVMAHAj1kDFjCB53TH3t\n"
      "Q2Gt5l6oZLZivpSveXDk+GYztFZKd+5fmGUkwcKAjV4UGHeyWJNDurcnUqX8Gsf4\n"
      "XfuTEhyPiR2f3kQRlwaqdiyBD+6E11DDNhdxJ8dWtZyu0i3NUq7DGQuH8K4ZHf/i\n"
      "8tjOkgNk587QBZ6HK+bbaqp9R70IcYI0o4YhqZOXE0k1U+NjfUD2DjUXt1X9K9rF\n"
      "0VAO5qlpQqBoI/J6GHpilxqAqpdCieR+gfkn33mRqYmkWg1z0JJjzpBNSNLrTNba\n"
      "OB5lprXBMet4okHsF+rLCJ4RATe/1s4Hemm98bBA++0CgYEA/4qQeyDpmTQ83WFd\n"
      "0x3UepqeMZx19dqwUb41BRChCPleq4ITQazjIDyCUGEvdU2+C/qZPb8aqNNEgBw/\n"
      "82RoVHk52NguDeS7+bMcpZVtZ6zk6aEM4aDOK0ngu4f6p/zMdKgoSXXm4V7FX6Tg\n"
      "bNhtqk7+OqxKWwtrpxGzE1rNOeMCgYEAy3O0LyhHBgP0MQg+K/CNWELVncciGWy4\n"
      "fW7LPB9idVEKpcuo/n7rV9EarU1P96OPwySE3cjUlILRgWcvNlv6di3cbdvWqsvK\n"
      "O1LsqRmPoaLUmhS/4lWQAvCbrf1rHox2Ay44emd/mWLPwTsPViwUExU8YtmwFez1\n"
      "5GEHjHH+JOMCgYBGvjZ3T8o7loGPC+hsjKKI+or20wi48jzDtHN7HnpmQJrbwhvQ\n"
      "n6sU/otY8z6vK8GEEXEg7enUeQBKswdlOxPC5viDtn0xbXQ4kURJ9s8d13hb0TD+\n"
      "uYS56S7k26UholN7rB3TEGfFVnYvnzZeC6B4eHvbBF1lTQkVbEn1/ro33wKBgHBo\n"
      "41tS07sdICSfO0qnxFDJzKE6Tzrg+SZEuwHjDVFoj4t/dUX39iw1Gpo6Jz7aHiph\n"
      "2Q95UQslJIBs9IcCVuZI/IuudXM02e3hKWVc/CEAiJsBb+ur/r/BFSMS68abMPEc\n"
      "7pOi77td/w/yg8zG2eiZSR4MzN+wZ0Ph4HvW1+alAoGAfDQoHXrPVupDpwq9G+T/\n"
      "ZZN/edlpoMs1eUD5Ma1hyR3ssJGSbqwLJPVIsfvhSEJdPRVYhJi65vkqi5NYla24\n"
      "slW9qw63OcX9mNmFosugoG8IMp3gwMlTgkLr+wjoxdbUbKR8DL2I+oMkuUGGFSqn\n"
      "4M2T9SYbToxo2XTZt8Q1mJQ=\n"
      "-----END PRIVATE KEY-----\n";

  BIO *bio = BIO_new_mem_buf(kTestPrivateKeyPem, -1);
  if (!bio) {
    ADD_FAILURE() << "failed to allocate PEM BIO";
    return km;
  }
  km.pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  if (!km.pkey) {
    ADD_FAILURE() << "failed to parse embedded private key";
    return km;
  }

  BIGNUM *n = nullptr;
  BIGNUM *e = nullptr;
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
  if (EVP_PKEY_get_bn_param(km.pkey, "n", &n) != 1 ||
      EVP_PKEY_get_bn_param(km.pkey, "e", &e) != 1) {
    ADD_FAILURE() << "failed to extract RSA n/e";
    if (n) BN_free(n);
    if (e) BN_free(e);
    EVP_PKEY_free(km.pkey);
    km.pkey = nullptr;
    return km;
  }
#else
  RSA *rsa = EVP_PKEY_get1_RSA(km.pkey);
  if (!rsa) {
    ADD_FAILURE() << "failed to extract RSA key";
    EVP_PKEY_free(km.pkey);
    km.pkey = nullptr;
    return km;
  }
  const BIGNUM *nRef = nullptr;
  const BIGNUM *eRef = nullptr;
  RSA_get0_key(rsa, &nRef, &eRef, nullptr);
  if (!nRef || !eRef) {
    ADD_FAILURE() << "failed to extract RSA n/e";
    RSA_free(rsa);
    EVP_PKEY_free(km.pkey);
    km.pkey = nullptr;
    return km;
  }
  n = BN_dup(nRef);
  e = BN_dup(eRef);
  RSA_free(rsa);
  if (!n || !e) {
    ADD_FAILURE() << "failed to duplicate RSA n/e";
    if (n) BN_free(n);
    if (e) BN_free(e);
    EVP_PKEY_free(km.pkey);
    km.pkey = nullptr;
    return km;
  }
#endif
  std::string n64 = BigNumToBase64URL(n);
  std::string e64 = BigNumToBase64URL(e);
  km.jwks = "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"k1\",\"n\":\"" + n64 +
            "\",\"e\":\"" + e64 + "\"}]}";
  BN_free(n);
  BN_free(e);
  return km;
}

std::string SignRS256(EVP_PKEY *pkey, const std::string &data)
{
  EvpMdCtxPtr mctx(EVP_MD_CTX_new());
  if (!mctx) {
    ADD_FAILURE() << "failed to allocate signing context";
    return std::string();
  }
  if (EVP_DigestSignInit(mctx.get(), nullptr, EVP_sha256(), nullptr, pkey) != 1 ||
      EVP_DigestSignUpdate(mctx.get(), data.data(), data.size()) != 1) {
    ADD_FAILURE() << "failed to initialize signature";
    return std::string();
  }
  size_t siglen = 0;
  if (EVP_DigestSignFinal(mctx.get(), nullptr, &siglen) != 1) {
    ADD_FAILURE() << "failed to get signature length";
    return std::string();
  }
  std::vector<unsigned char> sig(siglen);
  if (EVP_DigestSignFinal(mctx.get(), sig.data(), &siglen) != 1) {
    ADD_FAILURE() << "failed to sign payload";
    return std::string();
  }
  return Base64URLEncode(sig.data(), siglen);
}

std::string MakeToken(EVP_PKEY *pkey, const std::string &issuer,
                      const std::string &audience, time_t exp,
                      const std::string &sub = "alice")
{
  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload =
      "{\"iss\":\"" + issuer + "\",\"aud\":\"" + audience + "\",\"exp\":" +
      std::to_string(static_cast<long long>(exp)) + ",\"sub\":\"" + sub + "\"}";

  const std::string signedData =
      Base64URLEncode(hdr) + "." + Base64URLEncode(payload);
  const std::string sig = SignRS256(pkey, signedData);
  return signedData + "." + sig;
}

void ResetGlobalsForTest()
{
  expiry = 1;
  MaxTokSize = 8192;
  ClockSkew = 0;
  JwksRefresh = 3600;
  customIdentityClaims = false;
  customEntityClaims = false;
  clearIssuerPolicies();
  auto p = std::make_shared<IssuerPolicy>();
  p->issuer = "https://issuer.example";
  p->audiences.push_back("xrootd");
  IssuerPolicies.push_back(p);
  IssuerPolicyByIssuer[p->issuer] = p;
  IdentityClaims = {"sub", "username", "upn"};
  EmailIdentityMap.clear();
  EntityClaimMappings.clear();
  initDefaultEntityClaimMappings();
  RoleClaim.clear();
  GrpsClaim.clear();
  TokenCache.clear();
  TokenCacheHits = 0;
  TokenCacheMisses = 0;
}

void InstallTestKey(KeyMaterial &km, const char *kid = "k1");
std::string MakeJWT(EVP_PKEY *pkey, const std::string &hdrJson,
                    const std::string &payloadJson);
std::string IssuerClaim();

} // namespace

TEST(XrdSecOAuth2Test, ValidTokenPassesAndExtractsIdentity)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string emsg;
  std::string token = MakeToken(pkey, "https://issuer.example", "xrootd",
                                NowSeconds() + 600, "alice");
  std::string header;
  std::string payload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), payload, header, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, AudienceMismatchFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string emsg;
  std::string token =
      MakeToken(pkey, "https://issuer.example", "other-aud", NowSeconds() + 600);
  std::string header;
  std::string payload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), payload, header, identity, expOut, emsg));
  EXPECT_NE(emsg.find("audience"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ExpiredTokenFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string emsg;
  std::string token = MakeToken(pkey, "https://issuer.example", "xrootd",
                                NowSeconds() - 10);
  std::string header;
  std::string payload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), payload, header, identity, expOut, emsg));
  EXPECT_NE(emsg.find("expired"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, MalformedTokenFails)
{
  ResetGlobalsForTest();
  std::string header;
  std::string payload;
  std::string identity;
  std::string emsg;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT("not.a.jwt", payload, header, identity, expOut, emsg));
}

TEST(XrdSecOAuth2Test, BadSignatureFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string emsg;
  std::string token = MakeToken(pkey, "https://issuer.example", "xrootd",
                                NowSeconds() + 600);
  const size_t dot2 = token.rfind('.');
  ASSERT_NE(dot2, std::string::npos);
  ASSERT_GT(token.size(), dot2 + 3);
  const size_t tamperPos = dot2 + 2; // non-trailing signature char
  token[tamperPos] = (token[tamperPos] == 'A' ? 'B' : 'A');

  std::string payload;
  std::string header;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), payload, header, identity, expOut, emsg));
  EXPECT_NE(emsg.find("signature"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, UnknownIssuerFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string emsg;
  std::string token =
      MakeToken(pkey, "https://unknown-issuer.example", "xrootd", NowSeconds() + 600);
  std::string payload;
  std::string header;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), payload, header, identity, expOut, emsg));
  EXPECT_NE(emsg.find("issuer"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, MultiIssuerAudiencePolicyMismatchFails)
{
  ResetGlobalsForTest();
  auto p2 = std::make_shared<IssuerPolicy>();
  p2->issuer = "https://issuer-b.example";
  p2->audiences.push_back("service-b");
  IssuerPolicies.push_back(p2);
  IssuerPolicyByIssuer[p2->issuer] = p2;

  KeyMaterial kmA = MakeKeyAndJWKS();
  ASSERT_NE(kmA.pkey, nullptr);
  EVP_PKEY *const pkeyA = kmA.pkey;

  std::string emsg;
  std::string token = MakeToken(pkeyA, "https://issuer.example", "service-b", NowSeconds() + 600);
  IssuerPolicies[0]->jwksKeys["k1"] = EvpPkeyPtr(kmA.pkey);
  kmA.pkey = nullptr;
  IssuerPolicies[0]->lastJwksLoad = NowSeconds();

  std::string payload;
  std::string header;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), payload, header, identity, expOut, emsg));
  EXPECT_NE(emsg.find("audience"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ForcedIdentityClaimPerIssuerPasses)
{
  ResetGlobalsForTest();
  IssuerPolicies[0]->forcedIdentityClaim = "cern_upn";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload =
      R"({"iss":"https://issuer.example","aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","cern_upn":"apeters"})";
  const std::string signedData =
      Base64URLEncode(hdr) + "." + Base64URLEncode(payload);
  const std::string token = signedData + "." + SignRS256(pkey, signedData);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "apeters");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ForcedIdentityClaimMissingFails)
{
  ResetGlobalsForTest();
  IssuerPolicies[0]->forcedIdentityClaim = "cern_upn";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string token = MakeToken(pkey, "https://issuer.example", "xrootd",
                                NowSeconds() + 600, "alice");
  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("cern_upn"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ForcedEmailIdentityClaimUsesMap)
{
  ResetGlobalsForTest();
  IssuerPolicies[0]->forcedIdentityClaim = "email";
  EmailIdentityMap["andreas.joachim.peters@cern.ch"] = "apeters";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload =
      R"({"iss":"https://issuer.example","aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"email":"andreas.joachim.peters@cern.ch"})";
  const std::string signedData =
      Base64URLEncode(hdr) + "." + Base64URLEncode(payload);
  const std::string token = signedData + "." + SignRS256(pkey, signedData);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "apeters");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ForcedEmailIdentityClaimMissingMapFails)
{
  ResetGlobalsForTest();
  IssuerPolicies[0]->forcedIdentityClaim = "email";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload =
      R"({"iss":"https://issuer.example","aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"email":"unknown.user@cern.ch"})";
  const std::string signedData =
      Base64URLEncode(hdr) + "." + Base64URLEncode(payload);
  const std::string token = signedData + "." + SignRS256(pkey, signedData);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("not mapped"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

namespace {

// Build a JWT from explicit header/payload JSON. The signature is always a real
// RS256 signature over the encoded header+payload, regardless of the "alg"
// claimed in the header, so we can craft algorithm-confusion test vectors.
std::string MakeJWT(EVP_PKEY *pkey, const std::string &hdrJson,
                    const std::string &payloadJson)
{
  const std::string signedData =
      Base64URLEncode(hdrJson) + "." + Base64URLEncode(payloadJson);
  return signedData + "." + SignRS256(pkey, signedData);
}

// Register the test key under kid in the first issuer policy and mark JWKS as
// freshly loaded so no network refresh is attempted during validation.
// Transfers ownership of km.pkey into the policy keyset.
void InstallTestKey(KeyMaterial &km, const char *kid)
{
  ASSERT_NE(km.pkey, nullptr);
  IssuerPolicies[0]->jwksKeys[kid] = EvpPkeyPtr(km.pkey);
  km.pkey = nullptr;
  IssuerPolicies[0]->lastJwksLoad = NowSeconds();
}

std::string IssuerClaim() { return "\"iss\":\"https://issuer.example\""; }

} // namespace

// --- Algorithm-confusion guards (the classic JWT vulnerability class) --------

TEST(XrdSecOAuth2Test, AlgNoneRejected)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = "{\"alg\":\"none\",\"kid\":\"k1\",\"typ\":\"JWT\"}";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("alg"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, AlgHS256Rejected)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = "{\"alg\":\"HS256\",\"kid\":\"k1\",\"typ\":\"JWT\"}";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("alg"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, MissingAlgRejected)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = "{\"kid\":\"k1\",\"typ\":\"JWT\"}";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("alg"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- Time-based claims -------------------------------------------------------

TEST(XrdSecOAuth2Test, NotYetValidTokenFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 1200)) + ",\"nbf\":" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("not yet valid"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ClockSkewAllowsRecentlyExpiredToken)
{
  ResetGlobalsForTest();
  ClockSkew = 120;
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  std::string token = MakeToken(pkey, "https://issuer.example", "xrootd",
                                NowSeconds() - 60, "alice");
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ExpiryOptionalAllowsMissingExp)
{
  ResetGlobalsForTest();
  expiry = -1;
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() + ",\"aud\":\"xrootd\",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");
  EXPECT_EQ(expOut, 0u);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ExpiryRequiredRejectsMissingExp)
{
  ResetGlobalsForTest();
  expiry = 1;
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() + ",\"aud\":\"xrootd\",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("expiry"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, ExpiryIgnoreAllowsExpiredToken)
{
  ResetGlobalsForTest();
  expiry = 0;
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  // exp is in the past, but "ignore" mode must not enforce it.
  std::string token = MakeToken(pkey, "https://issuer.example", "xrootd",
                                NowSeconds() - 3600, "alice");
  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- Audience as JSON array --------------------------------------------------

TEST(XrdSecOAuth2Test, AudienceArrayMatchPasses)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      ",\"aud\":[\"other-service\",\"xrootd\"],\"exp\":" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, AudienceArrayMismatchFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      ",\"aud\":[\"svc-a\",\"svc-b\"],\"exp\":" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("audience"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// A token that carries no "aud" claim at all must be rejected whenever the
// issuer policy configures an expected audience.
TEST(XrdSecOAuth2Test, AudienceConfiguredButTokenMissingAudFails)
{
  ResetGlobalsForTest(); // ResetGlobalsForTest configures audience "xrootd"
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() + R"(,"exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("audience"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// When an issuer policy configures no audience, audience binding is disabled
// by design: a token minted for an unrelated relying party is still accepted.
TEST(XrdSecOAuth2Test, NoConfiguredAudienceSkipsAudienceCheck)
{
  ResetGlobalsForTest();
  IssuerPolicies[0]->audiences.clear(); // opt out of audience binding
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  std::string token = MakeToken(pkey, "https://issuer.example",
                                "some-other-service", NowSeconds() + 600, "alice");
  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- Key id (kid) handling ---------------------------------------------------

TEST(XrdSecOAuth2Test, KidMismatchFails)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km, "k1");

  const std::string hdr = "{\"alg\":\"RS256\",\"kid\":\"k2\",\"typ\":\"JWT\"}";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_FALSE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg));
  EXPECT_NE(emsg.find("signature"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, KidlessTokenVerifiedAgainstAllKeys)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km, "k1");

  const std::string hdr = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ",\"sub\":\"alice\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string outHdr;
  std::string outPayload;
  std::string identity;
  uint64_t expOut = 0;
  ASSERT_TRUE(parseAndValidateJWT(token.c_str(), outPayload, outHdr, identity, expOut, emsg)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- Default identity-claim fallback order -----------------------------------

TEST(XrdSecOAuth2Test, SubUsedForIdentityPreferredUsernameExported)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      ",\"preferred_username\":\"alice\",\"sub\":\"uuid-1234\"}";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string emsg;
  std::string identity;
  std::map<std::string, std::string> entityAttrs;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, &entityAttrs)) << emsg;
  EXPECT_EQ(identity, "uuid-1234");
  EXPECT_EQ(entityAttrs["token.preferred_username"], "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, EntityClaimsExportScopeJsonArray)
{
  ResetGlobalsForTest();
  std::string emsg;
  ASSERT_TRUE(storeEntityClaimEntry("scope", emsg)) << emsg;

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","scope":["storage.read:/public","storage.modify:/alice"],"wlcg.ver":"1.0"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity;
  std::map<std::string, std::string> entityAttrs;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, &entityAttrs)) << emsg;
  EXPECT_EQ(entityAttrs["token.scope"],
            "storage.read:/public storage.modify:/alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, RoleClaimMapsJwtArrayToEntityRole)
{
  ResetGlobalsForTest();
  RoleClaim = "roles";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","roles":["admin","user"]})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity, emsg, role, grps;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, nullptr,
                            &role, &grps)) << emsg;
  EXPECT_EQ(identity, "alice");
  EXPECT_EQ(role, "admin user");
  EXPECT_TRUE(grps.empty());

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, GrpsClaimMapsJwtArrayToEntityGrps)
{
  ResetGlobalsForTest();
  GrpsClaim = "wlcg.groups";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","wlcg.groups":["/wlcg/it","/wlcg/usatlas"]})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity, emsg, role, grps;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, nullptr,
                            &role, &grps)) << emsg;
  EXPECT_EQ(identity, "alice");
  EXPECT_TRUE(role.empty());
  EXPECT_EQ(grps, "/wlcg/it /wlcg/usatlas");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, NativeRoleGrpsFieldsSetOnAuthenticate)
{
  ResetGlobalsForTest();
  RoleClaim = "roles";
  GrpsClaim = "wlcg.groups";

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","roles":["writer"],"wlcg.groups":["/wlcg/eos"]})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  auto cred = XrdOAuth2::makeCredentials(token);
  ASSERT_NE(cred, nullptr);

  XrdNetAddrInfo addr;
  XrdSecProtocoloauth2 prot("host.example", addr);
  ASSERT_EQ(prot.Authenticate(cred.get(), nullptr, nullptr), 0);
  ASSERT_NE(prot.Entity.name, nullptr);
  EXPECT_STREQ(prot.Entity.name, "alice");
  ASSERT_NE(prot.Entity.role, nullptr);
  EXPECT_STREQ(prot.Entity.role, "writer");
  ASSERT_NE(prot.Entity.grps, nullptr);
  EXPECT_STREQ(prot.Entity.grps, "/wlcg/eos");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, EntityClaimsPopulateMappedAttributes)
{
  ResetGlobalsForTest();
  std::string emsg;
  ASSERT_TRUE(storeEntityClaimEntry("sub=token.subject", emsg)) << emsg;
  ASSERT_TRUE(storeEntityClaimEntry("iss", emsg)) << emsg;

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"uuid-1234","iss":"https://issuer.example"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity;
  std::map<std::string, std::string> entityAttrs;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, &entityAttrs)) << emsg;
  EXPECT_EQ(entityAttrs["token.subject"], "uuid-1234");
  EXPECT_EQ(entityAttrs["token.iss"], "https://issuer.example");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, AppendIssuerPathOptionAcceptsAbsolutePaths)
{
  std::vector<std::string> paths;
  std::string emsg;
  ASSERT_TRUE(appendIssuerPathOption("/tree1/", paths, emsg)) << emsg;
  EXPECT_EQ(paths, std::vector<std::string>{"/tree1"});
  ASSERT_TRUE(appendIssuerPathOption("/tree2/", paths, emsg)) << emsg;
  EXPECT_EQ(paths, (std::vector<std::string>{"/tree1", "/tree2"}));

  EXPECT_FALSE(appendIssuerPathOption("relative/path", paths, emsg));
  EXPECT_FALSE(appendIssuerPathOption("", paths, emsg));
}

TEST(XrdSecOAuth2Test, ParseInitParmsIssuerPathOptions)
{
  clearIssuerPolicies();
  std::shared_ptr<IssuerPolicy> curPolicy;
  const char *parms =
      "-issuer https://issuer.example "
      "-base-path /tree1/ "
      "-restricted-path /restricted/ -restricted-path /shared/";
  ASSERT_TRUE(parseInitParms(parms, curPolicy, nullptr));
  ASSERT_EQ(IssuerPolicies.size(), 1u);
  EXPECT_EQ(IssuerPolicies[0]->basePath, "/tree1");
  EXPECT_EQ(IssuerPolicies[0]->restrictedPaths,
            (std::vector<std::string>{"/restricted", "/shared"}));
}

TEST(XrdSecOAuth2Test, ParseInitParmsIssuerBasePathReplacesPrevious)
{
  clearIssuerPolicies();
  std::shared_ptr<IssuerPolicy> curPolicy;
  ASSERT_TRUE(parseInitParms(
      "-issuer https://issuer.example -base-path /tree1/ -base-path /tree2/",
      curPolicy, nullptr));
  EXPECT_EQ(IssuerPolicies[0]->basePath, "/tree2");
}

TEST(XrdSecOAuth2Test, ParseInitParmsIssuerBasePathAllowsCommaInPath)
{
  clearIssuerPolicies();
  std::shared_ptr<IssuerPolicy> curPolicy;
  ASSERT_TRUE(parseInitParms(
      "-issuer https://issuer.example -base-path /tree,one/",
      curPolicy, nullptr));
  EXPECT_EQ(IssuerPolicies[0]->basePath, "/tree,one");
}

TEST(XrdSecOAuth2Test, ParseInitParmsIssuerPathOptionsRejectRelative)
{
  clearIssuerPolicies();
  std::shared_ptr<IssuerPolicy> curPolicy;
  EXPECT_FALSE(parseInitParms("-issuer https://issuer.example -base-path rel",
                              curPolicy, nullptr));
}

TEST(XrdSecOAuth2Test, IssuerPathOptionsPopulateEntityAttributes)
{
  ResetGlobalsForTest();
  IssuerPolicies[0]->basePath = "/stash";
  IssuerPolicies[0]->restrictedPaths = {"/public", "/shared"};

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","preferred_username":"alice"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity, emsg;
  std::map<std::string, std::string> entityAttrs;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, &entityAttrs)) << emsg;
  EXPECT_EQ(identity, "alice");
  EXPECT_EQ(entityAttrs["base_path"], "/stash");
  EXPECT_EQ(entityAttrs["restricted_path"], "[\"/public\",\"/shared\"]");
  EXPECT_EQ(entityAttrs["token.preferred_username"], "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, EntityClaimsSkipMissingOrUnsafeValues)
{
  ResetGlobalsForTest();
  std::string emsg;
  // "name" carries an unsafe value (control char) that must be skipped; "sub"
  // remains a clean identity so the token still validates.
  ASSERT_TRUE(storeEntityClaimEntry("name", emsg)) << emsg;
  ASSERT_TRUE(storeEntityClaimEntry("azp", emsg)) << emsg;

  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"alice","name":"bad\nvalue","azp":"client-app"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity;
  std::map<std::string, std::string> entityAttrs;
  ASSERT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, &entityAttrs)) << emsg;
  EXPECT_EQ(identity, "alice");
  EXPECT_EQ(entityAttrs.count("token.name"), 0u);
  EXPECT_EQ(entityAttrs["token.azp"], "client-app");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- Strip(): Bearer-prefix and whitespace handling --------------------------

TEST(XrdSecOAuth2Test, StripHandlesBearerAndWhitespace)
{
  int sz = 0;
  const char *r = Strip("  Bearer token123  ", sz);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(std::string(r, sz), "token123");

  r = Strip("Bearer%20abc", sz);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(std::string(r, sz), "abc");

  r = Strip("plain", sz);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(std::string(r, sz), "plain");

  // Length-bounded: only the first maxLen bytes are considered.
  const char buf[] = "abcGARBAGE";
  r = Strip(buf, sz, 3);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(std::string(r, sz), "abc");

  // Whitespace-only input yields nothing.
  r = Strip("     ", sz);
  EXPECT_EQ(r, nullptr);
}

// --- base64url round trip ----------------------------------------------------

TEST(XrdSecOAuth2Test, Base64URLRoundTrip)
{
  std::string data;
  for (int i = 0; i < 256; ++i) data.push_back(static_cast<char>(i));
  const std::string enc = encodeBase64URL(data);
  EXPECT_EQ(enc.find('+'), std::string::npos);
  EXPECT_EQ(enc.find('/'), std::string::npos);
  EXPECT_EQ(enc.find('='), std::string::npos);

  std::string dec;
  ASSERT_TRUE(decodeBase64URL(enc, dec));
  EXPECT_EQ(dec, data);

  std::string bad;
  EXPECT_FALSE(decodeBase64URL("@@@@", bad));
}

// --- JWKS parsing ------------------------------------------------------------

TEST(XrdSecOAuth2Test, LoadJWKSParsesRSAKey)
{
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);

  std::map<std::string, EvpPkeyPtr> keys;
  std::string emsg;
  ASSERT_TRUE(loadJWKS(km.jwks, keys, emsg)) << emsg;
  EXPECT_EQ(keys.size(), 1u);
  EXPECT_NE(keys.find("k1"), keys.end());

  freeKeys(keys);
  EVP_PKEY_free(km.pkey);
}

TEST(XrdSecOAuth2Test, LoadJWKSRejectsNonRSAAndMalformed)
{
  std::map<std::string, EvpPkeyPtr> keys;
  std::string emsg;

  const std::string ecOnly =
      "{\"keys\":[{\"kty\":\"EC\",\"kid\":\"e1\",\"crv\":\"P-256\","
      "\"x\":\"AA\",\"y\":\"BB\"}]}";
  EXPECT_FALSE(loadJWKS(ecOnly, keys, emsg));
  EXPECT_NE(emsg.find("no usable RSA"), std::string::npos);

  EXPECT_FALSE(loadJWKS("{}", keys, emsg));
  EXPECT_FALSE(loadJWKS("not json", keys, emsg));
}

TEST(XrdSecOAuth2Test, LoadJWKSRejectsUndersizedRSAKey)
{
  // Build a JWKS for a 1024-bit RSA key; it must be rejected by the
  // kMinRSAKeyBits (2048-bit) floor even though it is otherwise well formed.
  EVP_PKEY *pkey = nullptr;
  EvpPkeyCtxPtr ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr));
  ASSERT_NE(ctx, nullptr);
  ASSERT_GT(EVP_PKEY_keygen_init(ctx.get()), 0);
  ASSERT_GT(EVP_PKEY_CTX_set_rsa_keygen_bits(ctx.get(), 1024), 0);
  ASSERT_GT(EVP_PKEY_keygen(ctx.get(), &pkey), 0);
  ASSERT_NE(pkey, nullptr);

  BIGNUM *n = nullptr;
  BIGNUM *e = nullptr;
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
  ASSERT_EQ(EVP_PKEY_get_bn_param(pkey, "n", &n), 1);
  ASSERT_EQ(EVP_PKEY_get_bn_param(pkey, "e", &e), 1);
#else
  RSA *rsa = EVP_PKEY_get1_RSA(pkey);
  ASSERT_NE(rsa, nullptr);
  const BIGNUM *nRef = nullptr;
  const BIGNUM *eRef = nullptr;
  RSA_get0_key(rsa, &nRef, &eRef, nullptr);
  n = BN_dup(nRef);
  e = BN_dup(eRef);
  RSA_free(rsa);
#endif
  const std::string jwks =
      "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"small\",\"n\":\"" +
      BigNumToBase64URL(n) + "\",\"e\":\"" + BigNumToBase64URL(e) + "\"}]}";
  BN_free(n);
  BN_free(e);
  EVP_PKEY_free(pkey);

  std::map<std::string, EvpPkeyPtr> keys;
  std::string emsg;
  EXPECT_FALSE(loadJWKS(jwks, keys, emsg));
  EXPECT_NE(emsg.find("no usable RSA"), std::string::npos);
}

// --- Identity sanitization ---------------------------------------------------

TEST(XrdSecOAuth2Test, IdentityWithControlCharRejected)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  // A sub carrying a newline must not become XrdSecEntity.name.
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"bad\nvalue"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity, emsg;
  EXPECT_FALSE(validateToken(token.c_str(), identity, emsg, nullptr, nullptr));
  EXPECT_NE(emsg.find("identity"), std::string::npos);

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

TEST(XrdSecOAuth2Test, IdentityWithPathSeparatorRejected)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) +
      R"(,"sub":"../etc/passwd"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity, emsg;
  EXPECT_FALSE(validateToken(token.c_str(), identity, emsg, nullptr, nullptr));

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- NumericDate (float exp) interop -----------------------------------------

TEST(XrdSecOAuth2Test, FloatExpClaimAccepted)
{
  ResetGlobalsForTest();
  KeyMaterial km = MakeKeyAndJWKS();
  ASSERT_NE(km.pkey, nullptr);
  EVP_PKEY *const pkey = km.pkey;
  InstallTestKey(km);

  const std::string hdr = R"({"alg":"RS256","kid":"k1","typ":"JWT"})";
  // exp expressed as a JSON float (NumericDate); must be accepted/truncated.
  const std::string payload = "{" + IssuerClaim() +
      R"(,"aud":"xrootd","exp":)" +
      std::to_string(static_cast<long long>(NowSeconds() + 600)) + ".5" +
      R"(,"sub":"alice"})";
  const std::string token = MakeJWT(pkey, hdr, payload);

  std::string identity, emsg;
  EXPECT_TRUE(validateToken(token.c_str(), identity, emsg, nullptr, nullptr)) << emsg;
  EXPECT_EQ(identity, "alice");

  freeKeys(IssuerPolicies[0]->jwksKeys);
}

// --- Negative token cache ----------------------------------------------------

TEST(XrdSecOAuth2Test, NegativeTokenCacheShortCircuitsRepeatFailures)
{
  ResetGlobalsForTest();
  TokenCache.clear();
  TokenNegCache.clear();
  const uint64_t now = static_cast<uint64_t>(NowSeconds());

  const std::string key = "deadbeef";
  EXPECT_FALSE(negTokenCacheLookup(key, now));
  negTokenCacheStore(key, now);
  EXPECT_TRUE(negTokenCacheLookup(key, now));
  // Entry expires after its TTL.
  EXPECT_FALSE(negTokenCacheLookup(key, now + kNegTokenCacheTTL + 1));
}

// --- Server-side token cache -------------------------------------------------

TEST(XrdSecOAuth2Test, TokenCacheStoreLookupAndExpiry)
{
  ResetGlobalsForTest();
  TokenCache.clear();
  TokenCacheMax = 10;
  const uint64_t now = static_cast<uint64_t>(NowSeconds());

  CachedTokenEntry e;
  e.identity = "alice";
  e.expiresAt = now + 100;
  tokenCacheStore("key-a", e, now);

  CachedTokenEntry out;
  ASSERT_TRUE(tokenCacheLookup("key-a", now, out));
  EXPECT_EQ(out.identity, "alice");

  // An entry past its expiry is treated as a miss and dropped on lookup.
  CachedTokenEntry e2;
  e2.identity = "bob";
  e2.expiresAt = now + 10;
  tokenCacheStore("key-b", e2, now);
  EXPECT_FALSE(tokenCacheLookup("key-b", now + 20, out));
  EXPECT_EQ(tokenCacheSize(), 1u); // key-b removed, key-a remains

  // Caching disabled => never stores nor returns.
  TokenCacheMax = 0;
  tokenCacheStore("key-c", e, now);
  EXPECT_FALSE(tokenCacheLookup("key-c", now, out));

  TokenCacheMax = 10;
  TokenCache.clear();
}

TEST(XrdSecOAuth2Test, TokenCacheEvictsSoonestToExpireWhenFull)
{
  ResetGlobalsForTest();
  TokenCache.clear();
  TokenCacheMax = 2;
  const uint64_t now = static_cast<uint64_t>(NowSeconds());

  CachedTokenEntry a; a.identity = "a"; a.expiresAt = now + 10;   // soonest
  CachedTokenEntry b; b.identity = "b"; b.expiresAt = now + 1000;
  CachedTokenEntry c; c.identity = "c"; c.expiresAt = now + 2000;
  tokenCacheStore("a", a, now);
  tokenCacheStore("b", b, now);
  ASSERT_EQ(tokenCacheSize(), 2u);

  tokenCacheStore("c", c, now); // at capacity: evict soonest-to-expire ("a")
  EXPECT_EQ(tokenCacheSize(), 2u);

  CachedTokenEntry out;
  EXPECT_FALSE(tokenCacheLookup("a", now, out));
  EXPECT_TRUE(tokenCacheLookup("b", now, out));
  EXPECT_TRUE(tokenCacheLookup("c", now, out));

  TokenCache.clear();
  TokenCacheMax = 10;
}

// --- INI config parsing / reload ---------------------------------------------

TEST(XrdSecOAuth2Test, ParseReloadableIniSectionsParsesIssuersAndEmailMap)
{
  const std::string cfg =
      "[global]\n"
      "expiry = required\n"               // global keys are ignored on reload
      "[issuer \"https://issuer-a.example\"]\n"
      "audience = xrootd, xrootd-admin\n"
      "forced-identity-claim = preferred_username\n"
      "[email-map]\n"
      "Alice@Example.ORG = alice\n";

  std::vector<std::shared_ptr<IssuerPolicy>> pols;
  std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> byIss;
  std::unordered_map<std::string, std::string> emap;
  std::string emsg;
  ASSERT_TRUE(parseReloadableIniSections("test.cfg", cfg, pols, byIss, emap, emsg)) << emsg;

  ASSERT_EQ(pols.size(), 1u);
  EXPECT_EQ(pols[0]->issuer, "https://issuer-a.example");
  ASSERT_EQ(pols[0]->audiences.size(), 2u);
  EXPECT_EQ(pols[0]->audiences[0], "xrootd");
  EXPECT_EQ(pols[0]->audiences[1], "xrootd-admin");
  EXPECT_EQ(pols[0]->forcedIdentityClaim, "preferred_username");

  // Email keys are normalized (trimmed + lowercased).
  ASSERT_EQ(emap.count("alice@example.org"), 1u);
  EXPECT_EQ(emap["alice@example.org"], "alice");
}

TEST(XrdSecOAuth2Test, ParseReloadableIniSectionsRejectsUnknownSection)
{
  const std::string cfg = "[bogus]\nfoo = bar\n";
  std::vector<std::shared_ptr<IssuerPolicy>> pols;
  std::unordered_map<std::string, std::shared_ptr<IssuerPolicy>> byIss;
  std::unordered_map<std::string, std::string> emap;
  std::string emsg;
  ASSERT_FALSE(parseReloadableIniSections("test.cfg", cfg, pols, byIss, emap, emsg));
  EXPECT_NE(emsg.find("unsupported section"), std::string::npos);
}

// XrdSecServer joins multiple sec.protparm lines with '\n'. Init must parse every
// record, not only the first line.
TEST(XrdSecOAuth2Test, ParseInitParmsMultiLineProtparm)
{
  clearIssuerPolicies();
  expiry = 1;
  DebugToken = false;
  DebugTokenClaims = false;
  customIdentityClaims = false;
  IdentityClaims = {"sub"};

  const char *parms =
      " -issuer https://auth.cern.ch/auth/realms/cern\n"
      " -audience eos-service -expiry required -show-token-claims";

  std::shared_ptr<IssuerPolicy> curPolicy;
  ASSERT_TRUE(parseInitParms(parms, curPolicy, nullptr));

  ASSERT_EQ(IssuerPolicies.size(), 1u);
  EXPECT_EQ(IssuerPolicies[0]->issuer, "https://auth.cern.ch/auth/realms/cern");
  ASSERT_EQ(IssuerPolicies[0]->audiences.size(), 1u);
  EXPECT_EQ(IssuerPolicies[0]->audiences[0], "eos-service");
  EXPECT_EQ(expiry.load(), 1);
  EXPECT_TRUE(DebugTokenClaims.load());

  clearIssuerPolicies();
}

TEST(XrdSecOAuth2Test, ParseInitParmsEmailMapFromProtparm)
{
  clearIssuerPolicies();
  EmailIdentityMap.clear();
  IdentityClaims = {"sub"};

  const char *parms =
      "-email-map foo.bar@gmail.com=foo\n"
      "-email-map Bob@Example.org=bobby";

  std::shared_ptr<IssuerPolicy> curPolicy;
  ASSERT_TRUE(parseInitParms(parms, curPolicy, nullptr));
  EXPECT_EQ(EmailIdentityMap["foo.bar@gmail.com"], "foo");
  EXPECT_EQ(EmailIdentityMap["bob@example.org"], "bobby");
}

TEST(XrdSecOAuth2Test, ParseInitParmsEmailMapRejectsMalformed)
{
  clearIssuerPolicies();
  EmailIdentityMap.clear();
  IdentityClaims = {"sub"};

  std::shared_ptr<IssuerPolicy> curPolicy;
  ASSERT_FALSE(parseInitParms("-email-map not-an-email", curPolicy, nullptr));
}

TEST(XrdSecOAuth2Test, PrescanInitParmsMultiLineProtparm)
{
  std::string selectedCfgPath = "/etc/xrootd/oauth2.cfg";
  bool requestedCfgOverride = false;
  std::string inlineParms;
  const char *parms =
      " -config-file /custom/oauth2.cfg\n"
      " -issuer https://issuer.example -audience xrootd";

  ASSERT_TRUE(prescanInitParms(parms, selectedCfgPath, requestedCfgOverride,
                               inlineParms, nullptr));
  EXPECT_TRUE(requestedCfgOverride);
  EXPECT_EQ(selectedCfgPath, "/custom/oauth2.cfg");
  EXPECT_NE(inlineParms.find("-issuer"), std::string::npos);
  EXPECT_NE(inlineParms.find("-audience"), std::string::npos);
  EXPECT_EQ(inlineParms.find("-config-file"), std::string::npos);
}

TEST(XrdSecOAuth2Test, LoadOAuth2IniAsArgsBuildsOptsFromFile)
{
  ResetGlobalsForTest();
  EmailIdentityMap.clear();

  std::string tmpl = "/tmp/oauth2_test_cfg_XXXXXX";
  int fd = mkstemp(tmpl.data());
  ASSERT_GE(fd, 0);
  ASSERT_EQ(fchmod(fd, 0600), 0); // owner-only, as the loader requires

  const std::string content =
      "[global]\n"
      "expiry = required\n"
      "identity-claim = preferred_username,sub\n"
      "[issuer \"https://issuer-a.example\"]\n"
      "audience = xrootd, adminaud\n"
      "forced-identity-claim = email\n"
      "base_path = /tree1/\n"
      "restricted_path = /restricted/\n"
      "restricted_path = /shared/\n"
      "[email-map]\n"
      "Bob@Example.org = bobby\n";
  ASSERT_EQ(write(fd, content.data(), content.size()),
            static_cast<ssize_t>(content.size()));
  close(fd);

  std::string opts, emsg;
  bool found = false;
  ASSERT_TRUE(loadOAuth2IniAsArgs(tmpl.c_str(), opts, found, emsg)) << emsg;
  EXPECT_TRUE(found);
  EXPECT_NE(opts.find("-issuer https://issuer-a.example"), std::string::npos);
  EXPECT_NE(opts.find("-expiry required"), std::string::npos);
  EXPECT_NE(opts.find("-audience xrootd"), std::string::npos);
  EXPECT_NE(opts.find("-audience adminaud"), std::string::npos);
  EXPECT_NE(opts.find("-forced-identity-claim email"), std::string::npos);
  EXPECT_NE(opts.find("-base-path /tree1"), std::string::npos);
  EXPECT_NE(opts.find("-restricted-path /restricted"), std::string::npos);
  EXPECT_NE(opts.find("-restricted-path /shared"), std::string::npos);

  // The [email-map] section is applied directly to the global map (normalized).
  EXPECT_EQ(EmailIdentityMap.count("bob@example.org"), 1u);
  EXPECT_EQ(EmailIdentityMap["bob@example.org"], "bobby");

  unlink(tmpl.c_str());
}

// --- Server Authenticate() credential framing --------------------------------

TEST(XrdSecOAuth2Test, AuthenticateRejectsMalformedFraming)
{
  ResetGlobalsForTest();
  XrdNetAddrInfo addr;
  XrdSecProtocoloauth2 prot("host.example", addr);

  // Too small (size <= 8) is rejected outright.
  {
    char *b = static_cast<char *>(malloc(4));
    memcpy(b, "oid", 4);
    XrdSecCredentials c(b, 4);
    EXPECT_EQ(prot.Authenticate(&c, nullptr, nullptr), -1);
  }

  // Correct size but wrong protocol prefix.
  {
    char *b = static_cast<char *>(malloc(13));
    memcpy(b, "xxxxxx", 6);
    b[6] = '\0';
    memcpy(b + 7, "token", 5);
    b[12] = '\0';
    XrdSecCredentials c(b, 13);
    EXPECT_EQ(prot.Authenticate(&c, nullptr, nullptr), -1);
  }

  // No NUL within the declared size (hostile, non-terminated buffer).
  {
    char *b = static_cast<char *>(malloc(10));
    memset(b, 'a', 10);
    XrdSecCredentials c(b, 10);
    EXPECT_EQ(prot.Authenticate(&c, nullptr, nullptr), -1);
  }

  // Valid "oauth2" prefix but an empty token.
  {
    char *b = static_cast<char *>(malloc(8));
    memcpy(b, "oauth2", 7); // "oauth2\0"
    b[7] = '\0';
    XrdSecCredentials c(b, 8);
    EXPECT_EQ(prot.Authenticate(&c, nullptr, nullptr), -1);
  }
}
