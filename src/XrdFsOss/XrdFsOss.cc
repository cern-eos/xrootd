/******************************************************************************/
/*                                                                            */
/*                          X r d F s O s s . c c                             */
/*                                                                            */
/* (C) Copyright 2026 CERN.                                                   */
/******************************************************************************/

#include "XrdFsOss/XrdFsOss.hh"

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucN2NLoader.hh"
#include "XrdOuc/XrdOucName2Name.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSys/XrdSysFD.hh"
#include "XrdSys/XrdSysHeaders.hh"
#include "XrdSys/XrdSysPlatform.hh"
#include "XrdVersion.hh"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>
#ifdef __linux__
#include <sys/fsuid.h>
#endif

#ifndef MAXPATHLEN
#define MAXPATHLEN 4096
#endif

XrdVERSIONINFO(XrdOssGetStorageSystem2, XrdFsOss);

static XrdFsOss XrdFsOssSS;

/******************************************************************************/
/*                          p l u g i n   e n t r y                           */
/******************************************************************************/

extern "C"
{
XrdOss *XrdOssGetStorageSystem2(XrdOss       *native_oss,
                                XrdSysLogger *Logger,
                                const char   *cFN,
                                const char   *parms,
                                XrdOucEnv    *envP)
{
   (void)native_oss; (void)parms;
   return XrdFsOssSS.Init(Logger, cFN, envP) ? 0 : (XrdOss *)&XrdFsOssSS;
}
}

/******************************************************************************/
/*                               X r d F s O s s                              */
/******************************************************************************/

XrdFsOss::XrdFsOss()
   : fsuidMode(XrdFsOssUid::On),
     eDest(0, "fsoss_"),
     n2n(0),
     myVersion(&XrdVERSIONINFOVAR(XrdOssGetStorageSystem2)),
     ConfigFN(0), LocalRoot(0), N2N_Lib(0), N2N_Parms(0), spacePath(0)
{}

XrdFsOss::~XrdFsOss()
{
   if (ConfigFN)   free(ConfigFN);
   if (LocalRoot)  free(LocalRoot);
   if (N2N_Lib)    free(N2N_Lib);
   if (N2N_Parms)  free(N2N_Parms);
   if (spacePath)  free(spacePath);
}

int XrdFsOss::Init(XrdSysLogger *lp, const char *cfn)
{
   return Init(lp, cfn, 0);
}

int XrdFsOss::Init(XrdSysLogger *lp, const char *cfn, XrdOucEnv *envP)
{
   eDest.logger(lp);
   eDest.Say("------ POSIX filesystem (FsOss) initialization started.");

   int NoGo = Configure(cfn, envP);
   if (!NoGo && fsuidMode != XrdFsOssUid::Off) NoGo = CheckFsUid();
   const char *tmp = NoGo ? "failed." : "completed.";
   eDest.Say("------ POSIX filesystem (FsOss) initialization ", tmp);
   return NoGo;
}

/******************************************************************************/
/*                                C o n f i g                                 */
/******************************************************************************/

int XrdFsOss::Configure(const char *cfn, XrdOucEnv *envP)
{
   char *var;
   int cfgFD, retc, NoGo = 0;
   XrdOucEnv myEnv;
   XrdOucStream Config(&eDest, getenv("XRDINSTANCE"), &myEnv, "=====> ");

   if (ConfigFN) free(ConfigFN);
   ConfigFN = (cfn && *cfn) ? strdup(cfn) : 0;

   if (!ConfigFN)
      {eDest.Say("Config warning: config file not specified; defaults assumed.");
      }
      else
      {if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
          return eDest.Emsg("Config", errno, "open config file", ConfigFN);
       Config.Attach(cfgFD);
       static const char *cvec[] = { "*** FsOss plugin config:", 0 };
       Config.Capture(cvec);

       while ((var = Config.GetMyFirstWord()))
             {if (!strncmp(var, "oss.", 4))
                 {if (ConfigXeq(var+4, Config)) {Config.Echo(); NoGo = 1;}}
             }

       if ((retc = Config.LastError()))
          NoGo = eDest.Emsg("Config", retc, "read config file", ConfigFN);
       Config.Close();
      }

   if (NoGo) return NoGo;

   if (N2N_Lib || LocalRoot)
      {XrdOucN2NLoader n2nLoader(&eDest, ConfigFN, N2N_Parms, LocalRoot, 0);
       if (!(n2n = n2nLoader.Load(N2N_Lib, *myVersion, envP))) return 1;
      }

   if (spacePath) free(spacePath);
   spacePath = strdup(LocalRoot && *LocalRoot ? LocalRoot : "/");

   const char *uidmsg = "off";
   if (fsuidMode == XrdFsOssUid::On)      uidmsg = "on";
   else if (fsuidMode == XrdFsOssUid::Require) uidmsg = "require";
   eDest.Say("Config FsOss fsuid=", uidmsg,
             LocalRoot ? " localroot=" : "",
             LocalRoot ? LocalRoot : "");
   return 0;
}

int XrdFsOss::CheckFsUid()
{
#ifndef __linux__
   eDest.Emsg("Config", "FsOss requires Linux setfsuid/setfsgid; "
              "set oss.fsuid off to load without impersonation");
   return 1;
#else
   uid_t wantUid = (geteuid() == (uid_t)65534) ? (uid_t)1 : (uid_t)65534;
   gid_t wantGid = (getegid() == (gid_t)65534) ? (gid_t)1 : (gid_t)65534;
   uid_t prevUid = (uid_t)setfsuid(wantUid);
   int   nowUid  = setfsuid(wantUid);
   setfsuid(prevUid);
   gid_t prevGid = (gid_t)setfsgid(wantGid);
   int   nowGid  = setfsgid(wantGid);
   setfsgid(prevGid);

   if (nowUid != (int)wantUid || nowGid != (int)wantGid)
      {eDest.Emsg("Config", "setfsuid/setfsgid cannot change identity; "
                  "need CAP_SETUID and CAP_SETGID "
                  "(systemd xrootd@.service, or start with xrootd-fsuid)");
       return 1;
      }
   eDest.Say("Config FsOss setfsuid probe succeeded.");
   return 0;
#endif
}

int XrdFsOss::ConfigXeq(char *var, XrdOucStream &Config)
{
   if (!strcmp(var, "namelib"))   return xnml(Config);
   if (!strcmp(var, "fsuid"))     return xfsuid(Config);

   if (!strcmp(var, "localroot"))
      {char *val = Config.GetWord();
       if (!val || !val[0])
          {eDest.Emsg("Config", "localroot not specified"); return 1;}
       if (LocalRoot) free(LocalRoot);
       LocalRoot = strdup(val);
       return 0;
      }

   if (!strcmp(var, "space") || !strcmp(var, "cache") || !strcmp(var, "stage")
    || !strcmp(var, "xfr")   || !strcmp(var, "alloc") || !strcmp(var, "defaults"))
      {char junk[2048];
       Config.GetRest(junk, sizeof(junk));
       eDest.Say("Config warning: 'oss.", var, "' is ignored by FsOss.");
       return 0;
      }

   return 0;
}

int XrdFsOss::xfsuid(XrdOucStream &Config)
{
   char *val = Config.GetWord();
   if (!val || !val[0])
      {eDest.Emsg("Config", "fsuid value not specified"); return 1;}
   if (!strcmp(val, "on"))       fsuidMode = XrdFsOssUid::On;
   else if (!strcmp(val, "off")) fsuidMode = XrdFsOssUid::Off;
   else if (!strcmp(val, "require")) fsuidMode = XrdFsOssUid::Require;
   else {eDest.Emsg("Config", "invalid fsuid value -", val); return 1;}
   return 0;
}

int XrdFsOss::xnml(XrdOucStream &Config)
{
   char *val, parms[1040];
   if (!(val = Config.GetWord()) || !val[0])
      {eDest.Emsg("Config", "namelib not specified"); return 1;}
   if (N2N_Lib) free(N2N_Lib);
   N2N_Lib = strdup(val);
   if (!Config.GetRest(parms, sizeof(parms)))
      {eDest.Emsg("Config", "namelib parameters too long"); return 1;}
   if (N2N_Parms) free(N2N_Parms);
   N2N_Parms = (*parms ? strdup(parms) : 0);
   return 0;
}

/******************************************************************************/
/*                                  P f n                                     */
/******************************************************************************/

int XrdFsOss::Pfn(const char *lfn, char *buff, int blen, const char *&use,
                  int opts)
{
   if ((opts & XRDOSS_isPFN) || !n2n)
      {use = lfn; return 0;}
   int rc = n2n->lfn2pfn(lfn, buff, blen);
   if (rc) return rc < 0 ? rc : -rc;
   use = buff;
   return 0;
}

int XrdFsOss::Lfn2Pfn(const char *path, char *buff, int blen)
{
   if (!n2n)
      {if ((int)strlen(path) >= blen) return -ENAMETOOLONG;
       strcpy(buff, path); return 0;
      }
   int rc = n2n->lfn2pfn(path, buff, blen);
   return rc ? (rc < 0 ? rc : -rc) : 0;
}

const char *XrdFsOss::Lfn2Pfn(const char *path, char *buff, int blen, int &rc)
{
   if (!n2n) {rc = 0; return path;}
   if ((rc = Lfn2Pfn(path, buff, blen))) return 0;
   return buff;
}

/******************************************************************************/
/*                                 M k p a t h                                */
/******************************************************************************/

int XrdFsOss::Mkpath(const char *path, mode_t mode)
{
   char local_path[MAXPATHLEN+1];
   int i = strlen(path);
   if (i >= MAXPATHLEN) return -ENAMETOOLONG;
   strcpy(local_path, path);
   while (i && local_path[--i] == '/') local_path[i] = '\0';
   if (!i) return -ENOENT;

   char *next_path = local_path;
   while ((next_path = strchr(next_path+1, '/')))
         {*next_path = '\0';
          if (mkdir(local_path, mode) && errno != EEXIST)
             {int rc = errno; *next_path = '/'; return -rc;}
          *next_path = '/';
         }
   if (mkdir(local_path, mode) && errno != EEXIST) return -errno;
   struct stat st;
   if (stat(local_path, &st)) return -errno;
   return S_ISDIR(st.st_mode) ? 0 : -ENOTDIR;
}

/******************************************************************************/
/*                            p a t h   o p s                                 */
/******************************************************************************/

int XrdFsOss::Chmod(const char *path, mode_t mode, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   return chmod(pfn, mode) ? -errno : 0;
}

int XrdFsOss::Chown(const char *path, uid_t u, gid_t g, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   return lchown(pfn, u, g) ? -errno : 0;
}

int XrdFsOss::Create(const char *tid, const char *path, mode_t mode,
                     XrdOucEnv &env, int opts)
{
   (void)tid;
   XrdFsOssUid uid(fsuidMode, &env, &eDest);
   if (!uid.Ok()) return uid.RC();

   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;

   int oflags = opts >> 8;
   if (opts & XRDOSS_mkpath)
      {char parent[MAXPATHLEN];
       strlcpy(parent, pfn, sizeof(parent));
       char *sl = strrchr(parent, '/');
       if (sl && sl != parent)
          {*sl = '\0';
           if ((rc = Mkpath(parent, S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH)))
              return rc;
          }
      }
   if (!(oflags & O_CREAT)) oflags |= O_CREAT;
   if (opts & XRDOSS_new)   oflags |= O_EXCL;
   if ((oflags & O_ACCMODE) == 0) oflags |= O_RDWR;

   int fd;
   do {fd = XrdSysFD_Open(pfn, oflags, mode);} while (fd < 0 && errno == EINTR);
   if (fd < 0) return -errno;
   close(fd);
   return 0;
}

int XrdFsOss::Link(const char *old_path, const char *new_path, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char ob[MAXPATHLEN], nb[MAXPATHLEN];
   const char *opfn, *npfn;
   int rc = Pfn(old_path, ob, sizeof(ob), opfn);
   if (rc) return rc;
   rc = Pfn(new_path, nb, sizeof(nb), npfn);
   if (rc) return rc;
   return link(opfn, npfn) ? -errno : 0;
}

int XrdFsOss::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   if (!mkdir(pfn, mode)) return 0;
   if (mkpath && errno == ENOENT) return Mkpath(pfn, mode);
   if (errno != EEXIST) return -errno;
   struct stat st;
   if (!stat(pfn, &st) && S_ISDIR(st.st_mode)) return 0;
   return -EEXIST;
}

int XrdFsOss::Remdir(const char *path, int opts, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn, opts);
   if (rc) return rc;
   if (rmdir(pfn)) return (errno == ENOENT ? 0 : -errno);
   return 0;
}

int XrdFsOss::Rename(const char *oPath, const char *nPath,
                     XrdOucEnv *oEnvP, XrdOucEnv *nEnvP)
{
   XrdFsOssUid uid(fsuidMode, oEnvP ? oEnvP : nEnvP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char ob[MAXPATHLEN], nb[MAXPATHLEN];
   const char *opfn, *npfn;
   int rc = Pfn(oPath, ob, sizeof(ob), opfn);
   if (rc) return rc;
   rc = Pfn(nPath, nb, sizeof(nb), npfn);
   if (rc) return rc;
   return rename(opfn, npfn) ? -errno : 0;
}

int XrdFsOss::Stat(const char *path, struct stat *buff, int opts, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   if (lstat(pfn, buff)) return -errno;
   if (opts & XRDOSS_updtatm)
      {struct timespec ts[2];
       ts[0].tv_sec = 0; ts[0].tv_nsec = UTIME_NOW;
       ts[1].tv_sec = 0; ts[1].tv_nsec = UTIME_OMIT;
       utimensat(AT_FDCWD, pfn, ts, AT_SYMLINK_NOFOLLOW);
      }
   return 0;
}

int XrdFsOss::StatFS(const char *path, char *buff, int &blen, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path && *path ? path : spacePath, pb, sizeof(pb), pfn);
   if (rc) return rc;
   struct statvfs vfs;
   if (statvfs(pfn, &vfs)) return -errno;
   unsigned long long total = (unsigned long long)vfs.f_blocks * vfs.f_frsize;
   unsigned long long freeb = (unsigned long long)vfs.f_bavail * vfs.f_frsize;
   int util = total ? (int)((total - freeb) * 100ULL / total) : 0;
   long long fmb = (long long)(freeb >> 20);
   blen = snprintf(buff, blen, "%d %lld %d %d %lld %d",
                   1, fmb, util, 0, 0LL, 0);
   return 0;
}

int XrdFsOss::StatLS(XrdOucEnv &env, const char *path, char *buff, int &blen)
{
   XrdFsOssUid uid(fsuidMode, &env, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path && *path ? path : spacePath, pb, sizeof(pb), pfn);
   if (rc) return rc;
   struct statvfs vfs;
   if (statvfs(pfn, &vfs)) return -errno;
   unsigned long long total = (unsigned long long)vfs.f_blocks * vfs.f_frsize;
   unsigned long long freeb = (unsigned long long)vfs.f_bavail * vfs.f_frsize;
   unsigned long long used  = total > freeb ? total - freeb : 0;
   unsigned long long maxf  = (unsigned long long)vfs.f_bavail * vfs.f_frsize;
   blen = snprintf(buff, blen,
      "oss.cgroup=public&oss.space=%llu&oss.free=%llu&oss.maxf=%llu"
      "&oss.used=%llu&oss.quota=-1",
      (unsigned long long)total, (unsigned long long)freeb,
      (unsigned long long)maxf,  (unsigned long long)used);
   return 0;
}

int XrdFsOss::StatPF(const char *path, struct stat *buff, int opts)
{
   if (opts & PF_dNums)
      {memset(buff, 0, sizeof(*buff));
       buff->st_rdev = 1; buff->st_dev = 1; return 0;}
   if (!path) return -EINVAL;
   char pb[MAXPATHLEN]; const char *pfn = path;
   if (opts & PF_isLFN)
      {int rc = Pfn(path, pb, sizeof(pb), pfn); if (rc) return rc;}
   if (stat(pfn, buff)) return -errno;
   if (opts & PF_dStat) buff->st_rdev = 0;
   return 0;
}

int XrdFsOss::StatVS(XrdOssVSInfo *sP, const char *sname, int updt)
{
   (void)sname; (void)updt;
   if (!sP) return -EINVAL;
   struct statvfs vfs;
   if (statvfs(spacePath, &vfs)) return -errno;
   unsigned long long total = (unsigned long long)vfs.f_blocks * vfs.f_frsize;
   unsigned long long freeb = (unsigned long long)vfs.f_bavail * vfs.f_frsize;
   sP->Total = (long long)total;
   sP->Free  = (long long)freeb;
   sP->Large = (long long)total;
   sP->LFree = (long long)freeb;
   sP->Usage = (long long)(total > freeb ? total - freeb : 0);
   sP->Quota = -1;
   sP->Extents = 1;
   return 0;
}

int XrdFsOss::Symlink(const char *target, const char *path, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   return symlink(target, pfn) ? -errno : 0;
}

int XrdFsOss::Truncate(const char *path, unsigned long long fsize,
                       XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   return truncate(pfn, (off_t)fsize) ? -errno : 0;
}

int XrdFsOss::Unlink(const char *path, int opts, XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn, opts);
   if (rc) return rc;
   if (unlink(pfn)) return (errno == ENOENT ? 0 : -errno);
   return 0;
}

int XrdFsOss::Utimes(const char *path, const struct timespec ts[2],
                     XrdOucEnv *envP)
{
   XrdFsOssUid uid(fsuidMode, envP, &eDest);
   if (!uid.Ok()) return uid.RC();
   char pb[MAXPATHLEN]; const char *pfn;
   int rc = Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   return utimensat(AT_FDCWD, pfn, ts, AT_SYMLINK_NOFOLLOW) ? -errno : 0;
}
