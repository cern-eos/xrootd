//------------------------------------------------------------------------------
// This file is part of XrdHTTP: A pragmatic implementation of the
// HTTP/WebDAV protocol for the Xrootd framework
//
// Copyright (c) 2020 by European Organization for Nuclear Research (CERN)
// Author: Fabrizio Furano <furano@cern.ch>
//------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <openssl/evp.h>

#include "XrdHttpProtocol.hh"
#include "XrdHttpTrace.hh"
#include "XrdHttpSecXtractor.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSec/XrdSecLoadSecurity.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdOAuth2/XrdOAuth2.hh"
#include "Xrd/XrdLink.hh"
#include "XrdCrypto/XrdCryptoX509Chain.hh"
#include "XrdCrypto/XrdCryptosslAux.hh"
#include "XrdCrypto/XrdCryptoFactory.hh"
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdTls/XrdTlsPeerCerts.hh"
#include "XrdTls/XrdTlsContext.hh"
#include "XrdOuc/XrdOucGMap.hh"

namespace XrdHttpProtoInfo
{
  extern XrdTlsContext *xrdctx;
}

XrdOucGMap *XrdHttpProtocol::servGMap = 0;  // Grid mapping service
XrdCryptoFactory *XrdHttpProtocol::myCryptoFactory = 0;

// Static definitions
#define TRACELINK lp

namespace
{
const char *TraceID = "Security";
}

using namespace XrdHttpProtoInfo;

namespace
{

std::string sha256Hex(std::string_view data)
{
   unsigned char md[EVP_MAX_MD_SIZE];
   unsigned int mdLen = 0;
   if (data.empty()
   ||  !EVP_Digest(data.data(), data.size(), md, &mdLen, EVP_sha256(), nullptr))
      return {};
   static constexpr char kHex[] = "0123456789abcdef";
   std::string out(mdLen * 2, '\0');
   for (unsigned int i = 0; i < mdLen; ++i)
      {out[i * 2]     = kHex[(md[i] >> 4) & 0x0f];
       out[i * 2 + 1] = kHex[md[i] & 0x0f];
      }
   return out;
}

bool iequals(std::string_view a, std::string_view b)
{
   return a.size() == b.size()
       && std::equal(a.begin(), a.end(), b.begin(), b.end(),
            [](unsigned char x, unsigned char y) {
               return std::tolower(x) == std::tolower(y);
            });
}

void copyEntityAttrs(XrdSecEntity &dst, const XrdSecEntity &src)
{
   for (const auto &key : src.eaAPI->Keys())
      {std::string val;
       if (src.eaAPI->Get(key, val)) dst.eaAPI->Add(key, val, true);
      }
}

// RFC 6750 WWW-Authenticate challenges returned with 4xx responses so that
// clients can detect a bearer-protected resource and drive token refresh.
constexpr std::string_view kBearerChallenge =
   "WWW-Authenticate: Bearer realm=\"XRootD\"";
constexpr std::string_view kBearerChallengeInvalid =
   "WWW-Authenticate: Bearer realm=\"XRootD\", error=\"invalid_token\", "
   "error_description=\"The access token is missing, invalid, or expired\"";
constexpr std::string_view kBearerChallengeBadRequest =
   "WWW-Authenticate: Bearer realm=\"XRootD\", error=\"invalid_request\", "
   "error_description=\"Malformed or duplicate Authorization credentials\"";

const char *oauth2ErrText(XrdOucErrInfo &eMsg)
{
   int ec = 0;
   const char *et = eMsg.getErrText(ec);
   return (et && *et) ? et : "unknown";
}

uint64_t nowUnixSeconds()
{
   return static_cast<uint64_t>(std::chrono::system_clock::to_time_t(
       std::chrono::system_clock::now()));
}

struct SecProtocolHolder {
   XrdSecProtocol *prot{nullptr};
   ~SecProtocolHolder() { if (prot) prot->Delete(); }
   explicit operator bool() const { return prot != nullptr; }
   XrdSecProtocol *get() { return prot; }
};

// Extract the bearer token from the request. Returns true and sets token when a
// single credential is found. Returns false otherwise; malformed is set true
// when the request is ambiguous (e.g. more than one Authorization header) and
// must be rejected rather than treated as token-less.
bool extractBearerToken(const XrdHttpReq &req, std::string &token, bool &malformed)
{
   malformed = false;
   const std::string &cgi = req.hdr2cgistr;
   constexpr std::string_view kAuthzKey = "authz=";
   if (const auto pos = cgi.find(kAuthzKey); pos != std::string::npos)
      {const auto start = pos + kAuthzKey.size();
       const auto end = cgi.find('&', start);
       token = (end == std::string::npos ? cgi.substr(start)
                                         : cgi.substr(start, end - start));
       return !token.empty();
      }

   // Reject more than one Authorization header. Case-variant duplicates
   // (e.g. "Authorization" and "authorization") survive header parsing as
   // distinct map entries, so a case-insensitive count is required; ambiguous
   // credentials are a hallmark of request-smuggling attempts.
   const std::string *found = nullptr;
   int count = 0;
   constexpr std::string_view kAuthorization = "authorization";
   for (const auto &hdr : req.allheaders)
      {if (iequals(hdr.first, kAuthorization))
          {++count;
           found = &hdr.second;
          }
      }
   if (count > 1) {malformed = true; return false;}
   if (count == 1 && found)
      {token = *found;
       return !token.empty();
      }
   return false;
}

} // namespace

/******************************************************************************/
/*                          I n i t S e c u r i t y                           */
/******************************************************************************/

bool XrdHttpProtocol::InitSecurity() {
  // Borrow the initialization of XrdCryptossl, in order to share the
  // OpenSSL threading bits
  if (!(myCryptoFactory = XrdCryptoFactory::GetCryptoFactory("ssl"))) {
    eDest.Say("Error instantiating crypto factory ssl", "");
    return false;
  }

// If GRID map file was specified, load the plugin for it
//
   if (gridmap) {
     XrdOucString pars;
     if (XrdHttpTrace.What & TRACE_DEBUG) pars += "dbg|";

     if (!(servGMap = XrdOucgetGMap(&eDest, gridmap, pars.c_str()))) {
       eDest.Say("Error loading grid map file:", gridmap);
       return false;
     }
     TRACE(ALL, "using grid map file: "<< gridmap);
   }

// If a secxtractor was specified, load that too.
//
   if (secxtractor)
      {SSL_CTX *sslctx = (SSL_CTX*)xrdctx->Context(); // Need to avoid this!
       secxtractor->Init(sslctx, XrdHttpTrace.What);
      }

// Load the security framework when HTTP bearer OAuth2 is enabled.
//
   if (oauth2HttpMode != OAuth2HttpMode::Off && !CIA)
      {if (oauth2ConfigFN.empty())
          {eDest.Say("Error: http.oauth2 requires a configuration file path");
           return false;
          }
       if (configEnv)
          CIA = static_cast<XrdSecService *>(configEnv->GetPtr("XrdSecService*"));
       if (!CIA)
          {XrdSecGetProt_t getProt = nullptr;
           XrdSecProtector *dhs = nullptr;
           CIA = XrdSecLoadSecService(&eDest, oauth2ConfigFN.data(), nullptr,
                                      &getProt, &dhs);
           if (CIA && configEnv)
              {configEnv->PutPtr("XrdSecService*", CIA);
               if (getProt) configEnv->PutPtr("XrdSecGetProtocol*", (void *)getProt);
               if (dhs)     configEnv->PutPtr("XrdSecProtector*", (void *)dhs);
              }
          }
       else if (configEnv)
          configEnv->PutPtr("XrdSecService*", CIA);
       if (!CIA)
          {eDest.Say("Error loading security framework for http.oauth2");
           return false;
          }
      }

// All done
//
   return true;
}

/******************************************************************************/
/*          H a n d l e O A u t h 2 A u t h e n t i c a t i o n               */
/******************************************************************************/

int
XrdHttpProtocol::HandleOAuth2Authentication()
{
#undef  TRACELINK
#define TRACELINK Link

  if (oauth2HttpMode == OAuth2HttpMode::Off || !CIA) return 0;

  // Client-certificate identity is fixed for the TLS connection.
  if (SecEntity.name
  &&  std::string_view(SecEntity.prot) != XrdOAuth2::kProtocolId) return 0;

  std::string bearer;
  bool tokMalformed = false;
  if (!extractBearerToken(CurrentReq, bearer, tokMalformed))
     {if (tokMalformed)
         {TRACEI(REQ, " OAuth2 ambiguous Authorization credentials; rejecting.");
          SendSimpleResp(400, nullptr, kBearerChallengeBadRequest.data(),
                         "Malformed authorization", 0, false);
          return 1;
         }
      // A bearer token must be presented on *every* request in require mode;
      // a token validated earlier on this keep-alive connection does not
      // authorize subsequent token-less requests (mTLS identity, handled
      // above, still satisfies the requirement).
      if (oauth2HttpMode == OAuth2HttpMode::Require)
         {TRACEI(REQ, " OAuth2 bearer token required but not provided.");
          SendSimpleResp(401, nullptr, kBearerChallenge.data(),
                         "Authentication required", 0, false);
          return 1;
         }
      return 0;
     }

  const int rawLen = static_cast<int>(bearer.size());
  int tlen = rawLen;
  const char *tok = XrdOAuth2::StripToken(bearer.data(), tlen, rawLen);
  if (!tok || tlen <= 0)
     {TRACEI(REQ, " OAuth2 bearer token malformed.");
      SendSimpleResp(401, nullptr, kBearerChallengeInvalid.data(),
                     "Authentication failed", 0, false);
      return 1;
     }

  const std::string_view tokenView(tok, static_cast<std::size_t>(tlen));
  const std::string tokKey = sha256Hex(tokenView);
  if (tokKey.empty())
     {TRACEI(REQ, " OAuth2 bearer token fingerprint failed.");
      SendSimpleResp(500, nullptr, nullptr, "Authentication failed", 0, false);
      return 1;
     }

  // Fast path: the same token presented again on this connection is accepted
  // without re-validation only while it has not yet expired. This is a pure
  // clock comparison (no crypto/JSON/allocation). Once the recorded expiry has
  // passed we fall through and re-validate, so an expired token can never be
  // reused for the lifetime of a keep-alive connection.
  if (!oauth2BearerTokKey.empty() && tokKey == oauth2BearerTokKey
  &&  oauth2BearerTokExp != 0 && nowUnixSeconds() < oauth2BearerTokExp)
     return 0;

  // Validate the presented token. The shared validated-token cache keeps an
  // unexpired token cheap (no signature/JWKS work); expiry, JWKS rotation and
  // config reloads are all still enforced here.
  auto cred = XrdOAuth2::makeCredentials(tokenView);
  if (!cred)
     {TRACEI(REQ, " OAuth2 credential allocation failed.");
      SendSimpleResp(500, nullptr, nullptr, "Authentication failed", 0, false);
      return 1;
     }

  XrdOucErrInfo eMsg;
  SecProtocolHolder authProt{CIA->getProtocol(Link->Host(), *(Link->AddrInfo()),
                                             cred.get(), eMsg)};
  if (!authProt)
     {TRACEI(REQ, " OAuth2 protocol unavailable: " << oauth2ErrText(eMsg));
      SendSimpleResp(401, nullptr, kBearerChallengeInvalid.data(),
                     "Authentication failed", 0, false);
      return 1;
     }

  XrdSecParameters *parm = nullptr;
  const int rc = authProt.get()->Authenticate(cred.get(), &parm, &eMsg);
  if (parm) delete parm;

  if (rc != 0 || !CIA->PostProcess(authProt.get()->Entity, eMsg))
     {TRACEI(REQ, " OAuth2 token validation failed: " << oauth2ErrText(eMsg));
      SendSimpleResp(401, nullptr, kBearerChallengeInvalid.data(),
                     "Authentication failed", 0, false);
      return 1;
     }

  const bool firstToken   = oauth2BearerTokKey.empty();
  const bool tokenChanged = !firstToken && tokKey != oauth2BearerTokKey;

  // If the identity is unchanged (same token re-validated OK) keep the existing
  // login and entity untouched; only the re-validation above mattered. Refresh
  // the recorded expiry so the fast path can resume.
  if (!firstToken && !tokenChanged)
     {oauth2BearerTokExp = XrdOAuth2::CachedTokenExpiry(tok, tlen);
      return 0;
     }

  // A new or changed token must (re)establish the login. Tear down any existing
  // bridge so the request re-logs in under the new identity.
  if (Bridge)
     {if (!Bridge->Disc())
         {const char *busyMsg = tokenChanged
                              ? " OAuth2 token changed but bridge is busy."
                              : " OAuth2 login busy.";
          TRACEI(REQ, busyMsg);
          SendSimpleResp(503, nullptr, nullptr, "Authentication busy", 0, false);
          return 1;
         }
      Bridge = nullptr;
      DoingLogin = false;
      DoneSetInfo = false;
     }
  if (tokenChanged)
     TRACEI(REQ, " OAuth2 bearer token changed; re-authenticating.");

  // Clear any identity/attributes from a previous token before applying the new
  // one, so stale scopes/groups/paths cannot leak into the new identity.
  XrdOAuth2::copyEntityField(SecEntity.name, authProt.get()->Entity.name);
  XrdOAuth2::copyEntityField(SecEntity.role, authProt.get()->Entity.role);
  XrdOAuth2::copyEntityField(SecEntity.grps, authProt.get()->Entity.grps);
  XrdOAuth2::copyProtocolId(SecEntity.prot);
  SecEntity.eaAPI->Reset();
  copyEntityAttrs(SecEntity, authProt.get()->Entity);

  oauth2BearerTokKey = tokKey;
  oauth2BearerTokExp = XrdOAuth2::CachedTokenExpiry(tok, tlen);
  TRACEI(REQ, " OAuth2 authenticated as: " << SecEntity.name);
  return 0;
}

/******************************************************************************/
/*                 H a n d l e A u t h e n t i c a t i o n                    */
/******************************************************************************/

int
XrdHttpProtocol::HandleAuthentication(XrdLink* lp)
{
  EPNAME("HandleAuthentication");
  int rc_ssl = SSL_get_verify_result(ssl);

  if (rc_ssl) { 
    TRACEI(DEBUG, " SSL_get_verify_result returned :" << rc_ssl);
    return 1;
  }

  XrdTlsPeerCerts pc(SSL_get_peer_certificate(ssl),SSL_get_peer_cert_chain(ssl));
  XrdCryptoX509Chain chain;

  if ((!pc.hasCert()) ||
      (myCryptoFactory && !myCryptoFactory->X509ParseStack()(&pc, &chain))) {
    TRACEI(DEBUG, "No certificate found in peer chain.");
    chain.Cleanup();
    return 0;
  }

  // Extract the DN for the current connection that will be used later on when
  // handling the gridmap file
  const char * dn = chain.EECname();
  const char * eechash = chain.EEChash();

  if (!dn || !eechash) {
    // X509Chain doesn't assume it owns the underlying certs unless
    // you explicitly invoke the Cleanup method
    TRACEI(DEBUG, "Failed to extract DN information.");
    chain.Cleanup();
    return 1;
  }

  if (SecEntity.moninfo) {
    free(SecEntity.moninfo);
  }

  SecEntity.moninfo = strdup(dn);
  TRACEI(DEBUG, " Subject name is : '" << SecEntity.moninfo << "'; hash is " << eechash);
  // X509Chain doesn't assume it owns the underlying certs unless
  // you explicitly invoke the Cleanup method

  if (GetVOMSData(lp)) {
    TRACEI(DEBUG, " No VOMS information for DN: " << SecEntity.moninfo);

    if (isRequiredXtractor) {
      eDest.Emsg(epname, "Failed extracting required VOMS info for DN: ",
                 SecEntity.moninfo);
      chain.Cleanup();
      return 1;
    }
  }

  auto retval = HandleGridMap(lp, eechash);
  chain.Cleanup();
  return retval;
}


/******************************************************************************/
/*                          H a n d l e G r i d M a p                         */
/******************************************************************************/

int
XrdHttpProtocol::HandleGridMap(XrdLink* lp, const char * eechash)
{
  EPNAME("HandleGridMap");
  char bufname[256];

  if (servGMap) {
    int mape = servGMap->dn2user(SecEntity.moninfo, bufname, sizeof(bufname), 0);
    if ( !mape && SecEntity.moninfo[0] ) {
      TRACEI(DEBUG, " Mapping name: '" << SecEntity.moninfo << "' --> " << bufname);
      if (SecEntity.name) free(SecEntity.name);
      SecEntity.name = strdup(bufname);
      SecEntity.eaAPI->Add("gridmap.name", "1", true);
    }
    else {
      TRACEI(ALL, " Mapping name: " << SecEntity.moninfo << " Failed. err: " << mape);

      if (isRequiredGridmap) {
        eDest.Emsg(epname, "Required gridmap mapping failed for DN:",
                   SecEntity.moninfo);
        return 1;
      }
    }
  }

  if (!SecEntity.name && !compatNameGeneration) {
    TRACEI(DEBUG, " Will fallback name to subject hash: " << eechash);
    SecEntity.name = strdup(eechash);
    return 0;
  }

  if (!SecEntity.name) {
    // Here we have the user DN, and try to extract an useful user name from it
    if (SecEntity.name) free(SecEntity.name);
    SecEntity.name = 0;
    // To set the name we pick the first CN of the certificate subject
    // and hope that it makes some sense, it usually does
    char *lnpos = strstr(SecEntity.moninfo, "/CN=");
    char bufname2[9];


    if (lnpos) {
      lnpos += 4;
      char *lnpos2 = index(lnpos, '/');
      if (lnpos2) {
        int l = ( lnpos2-lnpos < (int)sizeof(bufname) ? lnpos2-lnpos : (int)sizeof(bufname)-1 );
        strncpy(bufname, lnpos, l);
        bufname[l] = '\0';

        // Here we have the string in the buffer. Take the last 8 non-space characters
        size_t j = 8;
        strcpy(bufname2, "unknown-"); // note it's 8 chars + '\0' at the end
        for (int i = (int)strlen(bufname)-1; i >= 0; i--) {
          if (isalnum(bufname[i])) {
            j--;
            bufname2[j] = bufname[i];
            if (j == 0) break;
          }

        }

        SecEntity.name = strdup(bufname);
        TRACEI(DEBUG, " Setting link name: '" << bufname2+j << "'");
        lp->setID(bufname2+j, 0);
      }
    }
  }

  // If we could not find anything good, take the last 8 non-space characters of the main subject
  if (!SecEntity.name) {
    size_t j = 8;
    SecEntity.name = strdup("unknown-\0"); // note it's 9 chars
    for (int i = (int)strlen(SecEntity.moninfo)-1; i >= 0; i--) {
      if (isalnum(SecEntity.moninfo[i])) {
        j--;
        SecEntity.name[j] = SecEntity.moninfo[i];
        if (j == 0) break;
      }
    }
  }

  return 0;
}


/******************************************************************************/
/*                           G e t V O M S D a t a                            */
/******************************************************************************/

int XrdHttpProtocol::GetVOMSData(XrdLink *lp)
{
  TRACEI(DEBUG, " Extracting auth info.");

  // Invoke the Security exctractor plugin which will fill in the XrdSecEntity
  // with VOMS info, if VOMS is installed. If we have no sec extractor then do
  // nothing, just plain https will work.
  if (secxtractor) {
    // Note: this is kept for compatibility with XrdHttpVOMS which modified the
    // SecEntity.name filed
    char *savestr = 0;

    if (servGMap && SecEntity.name) {
      savestr = strdup(SecEntity.name);
    }

    int r = secxtractor->GetSecData(lp, SecEntity, ssl);

    if (servGMap && savestr) {
      if (SecEntity.name) free(SecEntity.name);
      SecEntity.name = savestr;
    }

    if (r) {
      TRACEI(ALL, " Certificate data extraction failed: " << SecEntity.moninfo
             << " Failed. err: " << r);
    }

    return r;
  }

  return 0;
}
