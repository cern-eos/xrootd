#ifndef _XRDFSOSS_HH
#define _XRDFSOSS_HH
/******************************************************************************/
/*                                                                            */
/*                          X r d F s O s s . h h                             */
/*                                                                            */
/* (C) Copyright 2026 CERN.                                                   */
/******************************************************************************/

#include "XrdFsOss/XrdFsOssUid.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdSys/XrdSysError.hh"

#include <dirent.h>
#include <sys/types.h>

struct XrdVersionInfo;
class  XrdOucName2Name;
class  XrdOucStream;
class  XrdSfsAio;

//------------------------------------------------------------------------------
//! POSIX disk storage system: local syscalls under setfsuid/setfsgid.
//!
//! Load with: ofs.osslib libXrdFsOss.so
//! Optional:  oss.localroot, oss.namelib, oss.fsuid {on|off|require}
//!            ofs.posix
//------------------------------------------------------------------------------

class XrdFsOss;

class XrdFsOssDir : public XrdOssDF
{
public:
   int     Close(long long *retsz=0);
   int     Opendir(const char *path, XrdOucEnv &env);
   int     Readdir(char *buff, int blen);
   int     StatRet(struct stat *buff);
   int     getFD() {return fd;}

           XrdFsOssDir(XrdFsOss *oss, const char *tid)
              : XrdOssDF(tid, DF_isDir), ossP(oss), dirP(0), statP(0) {}
          ~XrdFsOssDir() {if (dirP) Close();}

private:
   XrdFsOss *ossP;
   DIR      *dirP;
   struct stat *statP;
};

class XrdFsOssFile : public XrdOssDF
{
public:
   int     Close(long long *retsz=0);
   int     Open(const char *path, int oflag, mode_t mode, XrdOucEnv &env);
   int     Fchmod(mode_t mode);
   int     Fctl(int cmd, int alen, const char *args, char **resp=0);
   void    Flush();
   int     Fstat(struct stat *buf);
   int     Fsync();
   int     Fsync(XrdSfsAio *aiop);
   int     Ftruncate(unsigned long long flen);
   int     getFD() {return fd;}
   ssize_t Read(off_t offset, size_t size);
   ssize_t Read(void *buffer, off_t offset, size_t size);
   int     Read(XrdSfsAio *aiop);
   ssize_t ReadRaw(void *buffer, off_t offset, size_t size);
   ssize_t Write(const void *buffer, off_t offset, size_t size);
   int     Write(XrdSfsAio *aiop);

           XrdFsOssFile(XrdFsOss *oss, const char *tid)
              : XrdOssDF(tid, DF_isFile), ossP(oss) {}
          ~XrdFsOssFile() {if (fd >= 0) Close();}

private:
   XrdFsOss *ossP;
};

class XrdFsOss : public XrdOss
{
public:
   XrdOssDF *newDir(const char *tident)
                   {return new XrdFsOssDir(this, tident);}
   XrdOssDF *newFile(const char *tident)
                    {return new XrdFsOssFile(this, tident);}

   int       Chmod(const char *path, mode_t mode, XrdOucEnv *envP=0);
   int       Chown(const char *path, uid_t u, gid_t g, XrdOucEnv *envP=0);
   int       Create(const char *tid, const char *path, mode_t mode,
                    XrdOucEnv &env, int opts=0);
   uint64_t  Features() {return XRDOSS_HASNAIO | XRDOSS_HASPOSIX;}
   int       Init(XrdSysLogger *lp, const char *cfn);
   int       Init(XrdSysLogger *lp, const char *cfn, XrdOucEnv *envP);
   int       Link(const char *old_path, const char *new_path,
                  XrdOucEnv *envP=0);
   int       Lfn2Pfn(const char *path, char *buff, int blen);
   const char *Lfn2Pfn(const char *path, char *buff, int blen, int &rc);
   int       Mkdir(const char *path, mode_t mode, int mkpath=0,
                   XrdOucEnv *envP=0);
   int       Remdir(const char *path, int opts=0, XrdOucEnv *envP=0);
   int       Rename(const char *oPath, const char *nPath,
                    XrdOucEnv *oEnvP=0, XrdOucEnv *nEnvP=0);
   int       Stat(const char *path, struct stat *buff,
                  int opts=0, XrdOucEnv *envP=0);
   int       StatFS(const char *path, char *buff, int &blen, XrdOucEnv *envP=0);
   int       StatLS(XrdOucEnv &env, const char *path, char *buff, int &blen);
   int       StatPF(const char *path, struct stat *buff, int opts);
   int       StatVS(XrdOssVSInfo *sP, const char *sname, int updt);
   int       Symlink(const char *target, const char *path, XrdOucEnv *envP=0);
   int       Truncate(const char *path, unsigned long long fsize,
                      XrdOucEnv *envP=0);
   int       Unlink(const char *path, int opts=0, XrdOucEnv *envP=0);
   int       Utimes(const char *path, const struct timespec ts[2],
                    XrdOucEnv *envP=0);

   int       Pfn(const char *lfn, char *buff, int blen, const char *&use,
                 int opts=0);
   int       Mkpath(const char *path, mode_t mode);
   XrdSysError &Eroute() {return eDest;}

   XrdFsOssUid::Mode fsuidMode;

             XrdFsOss();
            ~XrdFsOss();

private:
   int       Configure(const char *cfn, XrdOucEnv *envP);
   int       ConfigXeq(char *var, XrdOucStream &Config);
   int       CheckFsUid();
   int       xfsuid(XrdOucStream &Config);
   int       xnml(XrdOucStream &Config);

   XrdSysError       eDest;
   XrdOucName2Name  *n2n;
   XrdVersionInfo   *myVersion;
   char             *ConfigFN;
   char             *LocalRoot;
   char             *N2N_Lib;
   char             *N2N_Parms;
   char             *spacePath;  // path used for StatFS/StatVS
};

#endif
