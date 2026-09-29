#ifndef XRDPOSIX_API_STUBS_HH
#define XRDPOSIX_API_STUBS_HH

#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysLogger.hh"

#include <cerrno>
#include <sys/stat.h>

class DummyDF : public XrdOssDF
{
public:
   DummyDF() : XrdOssDF("dummy", XrdOssDF::DF_isFile) {}
   int Close(long long *retsz=0) override { (void)retsz; return 0; }
};

class DummyOss : public XrdOss
{
public:
   XrdOssDF *newDir(const char *) override { return new DummyDF(); }
   XrdOssDF *newFile(const char *) override { return new DummyDF(); }
   int Chmod(const char *, mode_t, XrdOucEnv * = 0) override { return 0; }
   int Create(const char *, const char *, mode_t, XrdOucEnv &, int = 0) override
      { return 0; }
   int Init(XrdSysLogger *, const char *) override { return 0; }
   int Mkdir(const char *, mode_t, int = 0, XrdOucEnv * = 0) override { return 0; }
   int Remdir(const char *, int = 0, XrdOucEnv * = 0) override { return 0; }
   int Rename(const char *, const char *, XrdOucEnv * = 0,
              XrdOucEnv * = 0) override { return 0; }
   int Stat(const char *, struct stat *, int = 0, XrdOucEnv * = 0) override
      { return -ENOENT; }
   int Truncate(const char *, unsigned long long, XrdOucEnv * = 0) override
      { return 0; }
   int Unlink(const char *, int = 0, XrdOucEnv * = 0) override { return 0; }
};

class StubDir : public XrdSfsDirectory
{
public:
   StubDir(char *user=0, int monid=0) : XrdSfsDirectory(user, monid) {}
   int open(const char *, const XrdSecEntity * = 0, const char * = 0) override
      { return SFS_OK; }
   const char *nextEntry() override { return 0; }
   int close() override { return SFS_OK; }
   const char *FName() override { return ""; }
};

class StubFile : public XrdSfsFile
{
public:
   StubFile(char *user=0, int monid=0) : XrdSfsFile(user, monid) {}
   int open(const char *, XrdSfsFileOpenMode, mode_t,
            const XrdSecEntity * = 0, const char * = 0) override
      { return SFS_OK; }
   int close() override { return SFS_OK; }
   int fctl(const int, const char *, XrdOucErrInfo &) override { return SFS_OK; }
   const char *FName() override { return ""; }
   int getMmap(void **Addr, off_t &Size) override
      { if (Addr) *Addr = 0; Size = 0; return SFS_OK; }
   XrdSfsXferSize read(XrdSfsFileOffset, XrdSfsXferSize) override { return 0; }
   XrdSfsXferSize read(XrdSfsFileOffset, char *, XrdSfsXferSize) override
      { return 0; }
   int read(XrdSfsAio *) override { return SFS_ERROR; }
   XrdSfsXferSize write(XrdSfsFileOffset, const char *, XrdSfsXferSize) override
      { return 0; }
   int write(XrdSfsAio *) override { return SFS_ERROR; }
   int stat(struct stat *) override { return SFS_ERROR; }
   int sync() override { return SFS_OK; }
   int sync(XrdSfsAio *) override { return SFS_ERROR; }
   int truncate(XrdSfsFileOffset) override { return SFS_OK; }
   int getCXinfo(char [4], int &cxrsz) override { cxrsz = 0; return SFS_OK; }
};

class StubFS : public XrdSfsFileSystem
{
public:
   XrdSfsDirectory *newDir(char *user=0, int MonID=0) override
      { return new StubDir(user, MonID); }
   XrdSfsFile *newFile(char *user=0, int MonID=0) override
      { return new StubFile(user, MonID); }
   int chmod(const char *, XrdSfsMode, XrdOucErrInfo &e,
             const XrdSecEntity * = 0, const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int exists(const char *, XrdSfsFileExistence &flag, XrdOucErrInfo &,
              const XrdSecEntity * = 0, const char * = 0) override
      { flag = XrdSfsFileExistNo; return SFS_OK; }
   int fsctl(const int, const char *, XrdOucErrInfo &e,
             const XrdSecEntity * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int getStats(char *, int) override { return 0; }
   const char *getVersion() override { return "stub"; }
   int mkdir(const char *, XrdSfsMode, XrdOucErrInfo &e,
             const XrdSecEntity * = 0, const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int prepare(XrdSfsPrep &, XrdOucErrInfo &, const XrdSecEntity * = 0) override
      { return SFS_OK; }
   int rem(const char *, XrdOucErrInfo &e, const XrdSecEntity * = 0,
           const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int remdir(const char *, XrdOucErrInfo &e, const XrdSecEntity * = 0,
              const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int rename(const char *, const char *, XrdOucErrInfo &e,
              const XrdSecEntity * = 0, const char * = 0,
              const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int stat(const char *, struct stat *, XrdOucErrInfo &e,
            const XrdSecEntity * = 0, const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int stat(const char *, mode_t &mode, XrdOucErrInfo &e,
            const XrdSecEntity * = 0, const char * = 0) override
      { mode = 0; e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
   int truncate(const char *, XrdSfsFileOffset, XrdOucErrInfo &e,
                const XrdSecEntity * = 0, const char * = 0) override
      { e.setErrInfo(ENOTSUP, "no"); return SFS_ERROR; }
};

#endif
