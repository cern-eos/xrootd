/******************************************************************************/
/*                                                                            */
/*                        X r d F s O s s D F . c c                           */
/*                                                                            */
/* (C) Copyright 2026 CERN.                                                   */
/******************************************************************************/

#include "XrdFsOss/XrdFsOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysFD.hh"
#include "XrdSys/XrdSysPlatform.hh"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MAXPATHLEN
#define MAXPATHLEN 4096
#endif

/******************************************************************************/
/*                              D i r e c t o r y                             */
/******************************************************************************/

int XrdFsOssDir::Opendir(const char *path, XrdOucEnv &env)
{
   if (dirP) return -EBADF;
   XrdFsOssUid uid(ossP->fsuidMode, &env, &ossP->Eroute());
   if (!uid.Ok()) return uid.RC();

   char pb[MAXPATHLEN]; const char *pfn;
   int rc = ossP->Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;
   dirP = XrdSysFD_OpenDir(pfn);
   if (!dirP) return -errno;
   fd = dirfd(dirP);
   return 0;
}

int XrdFsOssDir::Readdir(char *buff, int blen)
{
   if (!dirP) return -EBADF;
   errno = 0;
   struct dirent *rp;
   while ((rp = readdir(dirP)))
         {strlcpy(buff, rp->d_name, blen);
          if (statP && fd >= 0)
             {if (fstatat(fd, rp->d_name, statP, 0))
                 {if (errno == ENOENT) {errno = 0; continue;}
                  return -errno;
                 }
             }
          return 0;
         }
   *buff = '\0';
   return errno ? -errno : 0;
}

int XrdFsOssDir::StatRet(struct stat *buff)
{
   statP = buff;
   return 0;
}

int XrdFsOssDir::Close(long long *retsz)
{
   (void)retsz;
   if (!dirP) return 0;
   int rc = closedir(dirP) ? -errno : 0;
   dirP = 0; fd = -1; statP = 0;
   return rc;
}

/******************************************************************************/
/*                                 F i l e                                    */
/******************************************************************************/

int XrdFsOssFile::Open(const char *path, int oflag, mode_t mode, XrdOucEnv &env)
{
   if (fd >= 0) return -EBADF;
   XrdFsOssUid uid(ossP->fsuidMode, &env, &ossP->Eroute());
   if (!uid.Ok()) return uid.RC();

   char pb[MAXPATHLEN]; const char *pfn;
   int rc = ossP->Pfn(path, pb, sizeof(pb), pfn);
   if (rc) return rc;

   int newfd;
   do {newfd = XrdSysFD_Open(pfn, oflag, mode);}
      while (newfd < 0 && errno == EINTR);
   if (newfd < 0) return -errno;

   struct stat st;
   if (fstat(newfd, &st))
      {int er = errno; close(newfd); return -er;}
   if (!S_ISREG(st.st_mode))
      {close(newfd); return S_ISDIR(st.st_mode) ? -EISDIR : -ENOTBLK;}
   fd = newfd;
   return 0;
}

int XrdFsOssFile::Close(long long *retsz)
{
   if (fd < 0) return 0;
   if (retsz)
      {struct stat st;
       *retsz = fstat(fd, &st) ? -1 : st.st_size;
      }
   int rc = close(fd) ? -errno : 0;
   fd = -1;
   return rc;
}

int XrdFsOssFile::Fchmod(mode_t mode)
{
   if (fd < 0) return -EBADF;
   return fchmod(fd, mode) ? -errno : 0;
}

int XrdFsOssFile::Fctl(int cmd, int alen, const char *args, char **resp)
{
   (void)resp;
   switch (cmd)
         {case Fctl_utimes:
               if (fd < 0) return -EBADF;
               if (alen != (int)(sizeof(struct timeval)*2) || !args)
                  return -EINVAL;
               {const struct timeval *tv = (const struct timeval *)args;
                struct timespec ts[2];
                ts[0].tv_sec = tv[0].tv_sec; ts[0].tv_nsec = tv[0].tv_usec*1000;
                ts[1].tv_sec = tv[1].tv_sec; ts[1].tv_nsec = tv[1].tv_usec*1000;
                return futimens(fd, ts) ? -errno : 0;
               }
          case Fctl_setFD:
               if (fd >= 0) return -EALREADY;
               if (alen != (int)sizeof(int) || !args) return -EINVAL;
               memcpy(&fd, args, sizeof(int));
               return 0;
          default: break;
         }
   return -ENOTSUP;
}

void XrdFsOssFile::Flush()
{
#if defined(__linux__)
   if (fd >= 0)
      {fdatasync(fd);
       posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
      }
#endif
}

int XrdFsOssFile::Fstat(struct stat *buf)
{
   if (fd < 0) return -EBADF;
   return fstat(fd, buf) ? -errno : 0;
}

int XrdFsOssFile::Fsync()
{
   if (fd < 0) return -EBADF;
   return fsync(fd) ? -errno : 0;
}

int XrdFsOssFile::Fsync(XrdSfsAio *aiop)
{
   aiop->Result = Fsync();
   aiop->doneWrite();
   return 0;
}

int XrdFsOssFile::Ftruncate(unsigned long long flen)
{
   if (fd < 0) return -EBADF;
   return ftruncate(fd, (off_t)flen) ? -errno : 0;
}

ssize_t XrdFsOssFile::Read(off_t offset, size_t size)
{
   if (fd < 0) return (ssize_t)-EBADF;
#if defined(__linux__)
   posix_fadvise(fd, offset, size, POSIX_FADV_WILLNEED);
#endif
   return 0;
}

ssize_t XrdFsOssFile::Read(void *buffer, off_t offset, size_t size)
{
   if (fd < 0) return (ssize_t)-EBADF;
   ssize_t n;
   do {n = pread(fd, buffer, size, offset);} while (n < 0 && errno == EINTR);
   return n >= 0 ? n : (ssize_t)-errno;
}

int XrdFsOssFile::Read(XrdSfsAio *aiop)
{
   aiop->Result = Read((void *)aiop->sfsAio.aio_buf,
                       (off_t)aiop->sfsAio.aio_offset,
                       (size_t)aiop->sfsAio.aio_nbytes);
   aiop->doneRead();
   return 0;
}

ssize_t XrdFsOssFile::ReadRaw(void *buffer, off_t offset, size_t size)
{
   return Read(buffer, offset, size);
}

ssize_t XrdFsOssFile::Write(const void *buffer, off_t offset, size_t size)
{
   if (fd < 0) return (ssize_t)-EBADF;
   ssize_t n;
   do {n = pwrite(fd, buffer, size, offset);} while (n < 0 && errno == EINTR);
   return n >= 0 ? n : (ssize_t)-errno;
}

int XrdFsOssFile::Write(XrdSfsAio *aiop)
{
   aiop->Result = Write((const void *)aiop->sfsAio.aio_buf,
                        (off_t)aiop->sfsAio.aio_offset,
                        (size_t)aiop->sfsAio.aio_nbytes);
   aiop->doneWrite();
   return 0;
}
