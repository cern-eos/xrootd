/******************************************************************************/
/*                                                                            */
/*              X r d S e c P r o t o c o l o a u t h 2 . c c               */
/*                                                                            */
/* (c) 2026 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/******************************************************************************/

#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "XrdVersion.hh"

#include "XrdNet/XrdNetAddrInfo.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdOAuth2/XrdOAuth2.hh"
#include "XrdSys/XrdSysE2T.hh"
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdSec/XrdSecInterface.hh"

#ifndef EAUTH
#define EAUTH EBADE
#endif

XrdVERSIONINFO(XrdSecProtocoloauth2Object,secoauth2);

namespace
{

XrdSecCredentials *Fatal(XrdOucErrInfo *erp, const char *eMsg, int rc,
                           bool hdr=true)
{
   if (!erp) std::cerr <<(hdr ? "Secoauth2: " : "") <<eMsg <<"\n" <<std::flush;
      else {const char *eVec[2] = {(hdr ? "Secoauth2: " : ""), eMsg};
            erp->setErrInfo(rc, eVec, 2);
           }
   return 0;
}

} // namespace

class XrdSecProtocoloauth2 : public XrdSecProtocol
{
public:
       int                Authenticate  (XrdSecCredentials *cred,
                                         XrdSecParameters **parms,
                                         XrdOucErrInfo     *einfo=0);

       void               Delete() {delete this;}

       XrdSecCredentials *getCredentials(XrdSecParameters *parms=0,
                                         XrdOucErrInfo    *einfo=0);

       bool               needTLS() {return true;}

       XrdSecProtocoloauth2(const char *parms, XrdOucErrInfo *erp, bool &aOK);
       XrdSecProtocoloauth2(const char *hname, XrdNetAddrInfo &endPoint)
                        : XrdSecProtocol("oauth2"), maxTSize(8192)
                        {XrdOAuth2::setEntityField(Entity.host, hname);
                         XrdOAuth2::setEntityField(Entity.name, "anon");
                         Entity.addrInfo = &endPoint;
                        }

      ~XrdSecProtocoloauth2()
                        {XrdOAuth2::resetEntityField(Entity.host);
                         XrdOAuth2::resetEntityField(Entity.name);
                         XrdOAuth2::resetEntityField(Entity.role);
                         XrdOAuth2::resetEntityField(Entity.grps);
                         XrdOAuth2::resetEntityField(Entity.creds);
                        }

static const int oauth2Version = 0;

private:
XrdSecCredentials *findToken(XrdOucErrInfo *erp, bool &isbad);
XrdSecCredentials *readToken(XrdOucErrInfo *erp, const std::string &path,
                             bool &isbad, bool optional=false);
XrdSecCredentials *retToken(std::string_view tok);

int                 maxTSize;
};

XrdSecProtocoloauth2::XrdSecProtocoloauth2(const char *parms, XrdOucErrInfo *erp,
                                       bool &aOK)
                                      : XrdSecProtocol("oauth2"), maxTSize(8192)
{
   aOK = false;
   if (!parms || !(*parms))
      {Fatal(erp, "Client parameters not specified.", EINVAL);
       return;
      }

   const std::string_view pstr(parms);
   const std::size_t end = pstr.rfind(':');
   if (end == std::string_view::npos || end == 0)
      {Fatal(erp, "Malformed client parameters.", EINVAL);
       return;
      }
   const std::size_t begin = pstr.rfind(':', end - 1);
   if (begin == std::string_view::npos || begin + 1 >= end)
      {Fatal(erp, "Malformed client parameters.", EINVAL);
       return;
      }
   const std::string_view maxField = pstr.substr(begin + 1, end - begin - 1);
   long v = 0;
   const auto [ptr, ec] = std::from_chars(maxField.data(),
                                          maxField.data() + maxField.size(), v);
   if (ec != std::errc() || ptr != maxField.data() + maxField.size()
   ||  v <= 0 || v > 524288)
      {Fatal(erp, "Invalid max token size in parameters.", EINVAL);
       return;
      }
   maxTSize = static_cast<int>(v);
   aOK = true;
}

XrdSecCredentials *XrdSecProtocoloauth2::retToken(std::string_view tok)
{
   auto cred = XrdOAuth2::makeCredentials(tok);
   return cred ? cred.release() : nullptr;
}

XrdSecCredentials *XrdSecProtocoloauth2::readToken(XrdOucErrInfo *erp,
                                                 const std::string &path,
                                                 bool &isbad, bool optional)
{
   isbad = true;
   // For probed (well-known) locations a problematic file should not abort the
   // whole token search: treat ownership/permission/type problems as "not here,
   // keep looking" so a local attacker cannot plant a file (e.g. at
   // /tmp/bt_u<uid>) to DoS the client. Explicitly-named files (BEARER_TOKEN_FILE)
   // keep the stricter fatal behaviour.
   auto skipOrFail = [&](const char *eMsg, int rc) -> XrdSecCredentials *
      {if (optional) {isbad = false; return 0;}
       return Fatal(erp, eMsg, rc);
      };

   int flags = O_RDONLY;
#ifdef O_NOFOLLOW
   flags |= O_NOFOLLOW;
#endif
   int fd = open(path.c_str(), flags);
   if (fd < 0)
      {if (errno == ENOENT) {isbad = false; return 0;}
       return skipOrFail(XrdSysE2T(errno), errno);
      }
   XrdOucUtils::DebugEnabled("XrdSecDEBUG", "Secoauth2",
                         "Using OAuth2 token file '" + path + "'");

   struct stat st;
   if (fstat(fd, &st) != 0)
      {int rc = errno; close(fd);
       return skipOrFail(XrdSysE2T(rc), rc);
      }

   if (!S_ISREG(st.st_mode))
      {close(fd);
       return skipOrFail("Token path must be a regular file.", EINVAL);
      }

   if (st.st_uid != geteuid())
      {close(fd);
       return skipOrFail("Token file owner must match effective uid.", EACCES);
      }

   if (st.st_mode & (S_IRWXG | S_IRWXO))
      {close(fd);
       return skipOrFail("Token file permissions too open; require owner-only access.", EACCES);
      }

   if (st.st_size <= 0 || st.st_size > maxTSize)
      {close(fd);
       return skipOrFail("Token file size invalid or exceeds limit.", EINVAL);
      }

   std::vector<char> buff(static_cast<size_t>(st.st_size) + 1);
   ssize_t got = 0;
   while (got < st.st_size)
      {ssize_t rd = read(fd, buff.data() + got, st.st_size - got);
       if (rd < 0)
          {if (errno == EINTR) continue;
           close(fd);
           return skipOrFail("Unable to read token file.", EIO);
          }
       if (rd == 0)
          {close(fd);
           return skipOrFail("Unable to read token file.", EIO);
          }
       got += rd;
      }
   close(fd);

   buff[st.st_size] = 0;
   int tlen = 0;
   const char *tok = XrdOAuth2::StripToken(buff.data(), tlen);
   XrdSecCredentials *ret = (tok ? retToken(std::string_view(tok, tlen)) : nullptr);
   if (!ret) return skipOrFail("Token value malformed.", EINVAL);
   return ret;
}

XrdSecCredentials *XrdSecProtocoloauth2::findToken(XrdOucErrInfo *erp, bool &isbad)
{
   static const char *loc[] = {
     "BEARER_TOKEN", "BEARER_TOKEN_FILE",
     "XDG_RUNTIME_DIR", "/tmp/bt_u"
   };

   const auto uid = static_cast<unsigned>(geteuid());
   constexpr std::string_view kTmpTokenPath = "/tmp/bt_u";

   for (const char *keyC : loc)
       {const std::string_view key(keyC);
        if (key == kTmpTokenPath)
           {const std::string tokPath = std::string(kTmpTokenPath)
                                      + std::to_string(uid);
            XrdSecCredentials *r = readToken(erp, tokPath, isbad, true);
            if (r || isbad) return r;
            continue;
           }

        const char *env = getenv(std::string(key).c_str());
        if (!env || !(*env)) continue;

        if (XrdOAuth2::hasSuffix(key, "_DIR"))
           {const std::string tokPath = std::string(env) + "/bt_u"
                                      + std::to_string(uid);
            XrdSecCredentials *r = readToken(erp, tokPath, isbad, true);
            if (r || isbad) return r;
            continue;
           }

        if (XrdOAuth2::hasSuffix(key, "_FILE"))
           {XrdOucUtils::DebugEnabled("XrdSecDEBUG", "Secoauth2",
                "Trying OAuth2 token file from "
                + std::string(key) + "='" + std::string(env) + "'");
            XrdSecCredentials *r = readToken(erp, env, isbad);
            if (r || isbad) return r;
            continue;
           }

        const std::string_view envView(env);
        if (static_cast<int>(envView.size()) > maxTSize) continue;
        int tlen = 0;
        const char *tok = XrdOAuth2::StripToken(env, tlen,
                                                   static_cast<int>(envView.size()));
        if (tok) return retToken(std::string_view(tok, tlen));
       }

   isbad = false;
   return 0;
}

XrdSecCredentials *XrdSecProtocoloauth2::getCredentials(XrdSecParameters *parms,
                                                      XrdOucErrInfo *error)
{
   (void)parms;
   bool isbad = false;
   XrdSecCredentials *resp = findToken(error, isbad);
   if (resp || isbad) return resp;
   Fatal(error, "No OAuth2 token found in environment/token-file locations.",
         ENOPROTOOPT);
   return 0;
}

int XrdSecProtocoloauth2::Authenticate(XrdSecCredentials *cred,
                                     XrdSecParameters **parms,
                                     XrdOucErrInfo *erp)
{
   (void)parms;
   if (!cred || !cred->buffer
   ||  cred->size <= static_cast<int>(XrdOAuth2::kProtocolPrefixSize))
      {Fatal(erp, "Missing credential data.", EINVAL);
       return -1;
      }

   if (cred->size > maxTSize + static_cast<int>(XrdOAuth2::kProtocolPrefixSize + 9))
      {Fatal(erp, "Credential too large.", EINVAL);
       return -1;
      }

   const std::string_view credView(cred->buffer, static_cast<std::size_t>(cred->size));
   if (credView.find('\0') == std::string_view::npos)
      {Fatal(erp, "Credential not NUL-terminated.", EINVAL);
       return -1;
      }

   if (std::string_view(cred->buffer) != XrdOAuth2::kProtocolId)
      {Fatal(erp, "Authentication protocol id mismatch.", EINVAL);
       return -1;
      }

   const auto tokenOpt = XrdOAuth2::tokenFromCredentialBuffer(cred->buffer,
                                                                  cred->size);
   if (!tokenOpt || tokenOpt->empty())
      {Fatal(erp, "Null token.", EINVAL);
       return -1;
      }

   int tlen = 0;
   const char *tok = XrdOAuth2::StripToken(tokenOpt->data(), tlen,
                                              static_cast<int>(tokenOpt->size()));
   if (!tok || tlen <= 0)
      {Fatal(erp, "Token value malformed.", EINVAL);
       return -1;
      }

   std::string identity, emsg;
   std::map<std::string, std::string> entityAttrs;
   std::string role, grps;
   if (!XrdOAuth2::ValidateToken(tok, identity, emsg, nullptr, &entityAttrs,
                                    &role, &grps))
      {Fatal(erp, emsg.c_str(), EAUTH, false);
       return -1;
      }

   XrdOAuth2::setEntityField(Entity.name, identity);
   XrdOAuth2::setEntityFieldOptional(Entity.role, role);
   XrdOAuth2::setEntityFieldOptional(Entity.grps, grps);
   XrdOAuth2::copyProtocolId(Entity.prot);
   for (const auto &attr : entityAttrs)
      Entity.eaAPI->Add(attr.first, attr.second, true);
   return 0;
}

extern "C"
{
char *XrdSecProtocoloauth2Init(const char mode, const char *parms, XrdOucErrInfo *erp)
{
   static char nilstr = 0;
   if (mode == 'c') return &nilstr;
   return XrdOAuth2::InitSecProtocol(parms, erp);
}
}

extern "C"
{
XrdSecProtocol *XrdSecProtocoloauth2Object(const char mode,
                                         const char *hostname,
                                               XrdNetAddrInfo &endPoint,
                                         const char *parms,
                                               XrdOucErrInfo *erp)
{
   if (!endPoint.isUsingTLS())
      {Fatal(erp, "security protocol 'oauth2' disallowed for non-TLS connections.",
             ENOTSUP, false);
       return 0;
      }

   if (mode == 'c')
      {bool aOK = false;
       auto prot = std::make_unique<XrdSecProtocoloauth2>(parms, erp, aOK);
       if (aOK) return prot.release();
       return nullptr;
      }

   return std::make_unique<XrdSecProtocoloauth2>(hostname, endPoint).release();
}
}
