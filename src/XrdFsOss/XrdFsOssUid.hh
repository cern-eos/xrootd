#ifndef _XRDFSOSSUID_HH
#define _XRDFSOSSUID_HH
/******************************************************************************/
/*                                                                            */
/*                        X r d F s O s s U i d . h h                         */
/*                                                                            */
/* (C) Copyright 2026 CERN.                                                   */
/******************************************************************************/

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"

#include <cerrno>
#include <cstring>
#include <pwd.h>
#include <sys/types.h>

#ifdef __linux__
#include <sys/fsuid.h>
#endif

//------------------------------------------------------------------------------
//! Per-thread Linux fsuid/fsgid impersonation from XrdSecEntity.
//!
//! Only uid and primary gid are applied. Supplementary groups are not set
//! (setgroups is process-wide). Metadata syscalls must run while this object
//! is alive. I/O on an already open fd does not need it.
//------------------------------------------------------------------------------

class XrdFsOssUid
{
public:
   enum Mode { Off = 0, On = 1, Require = 2 };

   XrdFsOssUid(Mode mode, XrdOucEnv *envP, XrdSysError *eDest)
      : prevUid(-1), prevGid(-1), rc(0), ok(true)
   {
      if (mode == Off || !envP) return;

      const XrdSecEntity *se = envP->secEnv();
      uid_t uid = 0;
      gid_t gid = 0;
      if (!Resolve(se, mode, uid, gid, rc, eDest))
         {ok = (rc == 0); return;}

#ifdef __linux__
      prevUid = setfsuid(uid);
      if (setfsuid(uid) != (int)uid)
         {setfsuid(prevUid); prevUid = -1;
          rc = -EPERM; ok = false; return;}

      prevGid = setfsgid(gid);
      if (setfsgid(gid) != (int)gid)
         {setfsgid(prevGid); prevGid = -1;
          setfsuid(prevUid); prevUid = -1;
          rc = -EPERM; ok = false; return;}
#else
      (void)uid; (void)gid; (void)eDest;
#endif
   }

   ~XrdFsOssUid()
   {
#ifdef __linux__
      if (prevUid >= 0) setfsuid(prevUid);
      if (prevGid >= 0) setfsgid(prevGid);
#endif
   }

   bool Ok() const { return ok; }
   int  RC() const { return rc; }

   XrdFsOssUid(const XrdFsOssUid&) = delete;
   XrdFsOssUid &operator=(const XrdFsOssUid&) = delete;

private:
   static bool Resolve(const XrdSecEntity *se, Mode mode,
                       uid_t &uid, gid_t &gid, int &rc,
                       XrdSysError *eDest)
   {
      rc = 0;
      if (!se) return false;

      uid = se->uid;
      gid = se->gid;
      if (uid != 0) return true;

      const char *name = se->name;
      if (!name || !*name)
         {if (mode == Require) {rc = -EACCES; return false;}
          return false;
         }

      char user[256];
      size_t n = 0;
      while (name[n] && name[n] != '@' && n < sizeof(user)-1)
         {user[n] = name[n]; n++;}
      user[n] = '\0';
      if (!user[0])
         {if (mode == Require) {rc = -EACCES; return false;}
          return false;
         }

      struct passwd pw, *pwp = 0;
      char pbuf[4096];
      int prc = getpwnam_r(user, &pw, pbuf, sizeof(pbuf), &pwp);
      if (prc || !pwp)
         {if (mode == Require)
             {rc = -EACCES;
              if (eDest)
                 eDest->Emsg("FsOss", "cannot map identity", user,
                             "to a unix uid");
              return false;
             }
          return false;
         }

      uid = pwp->pw_uid;
      if (gid == 0) gid = pwp->pw_gid;
      return true;
   }

   int  prevUid;
   int  prevGid;
   int  rc;
   bool ok;
};

#endif
