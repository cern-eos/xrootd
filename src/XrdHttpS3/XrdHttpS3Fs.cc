#include "XrdHttpS3Fs.hh"

#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsFAttr.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <unistd.h>

namespace XrdHttpS3 {
namespace {

const char *WhoName(const XrdSecEntity *who) {
  return who && who->name ? who->name : "s3";
}

int Wait(XrdOucErrInfo &ei, int rc) {
  for (int attempt = 0; attempt < 5; attempt++) {
    if (rc != SFS_STALL && rc != SFS_STARTED)
      return rc;
    int secs = ei.getErrInfo();
    if (rc == SFS_STARTED)
      secs = secs / 2 + 5;
    if (secs < 1)
      secs = 1;
    if (secs > 15)
      secs = 15;
    sleep(static_cast<unsigned>(secs));
    return SFS_STALL;
  }
  return rc;
}

struct FileCloser {
  void operator()(XrdSfsFile *f) const {
    if (!f)
      return;
    f->close();
    delete f;
  }
};

struct DirCloser {
  void operator()(XrdSfsDirectory *d) const {
    if (!d)
      return;
    d->close();
    delete d;
  }
};

void SplitMkpath(int flags, mode_t mode, int &useFlags, mode_t &useMode) {
  useFlags = flags;
  useMode = mode;
  if (useFlags & SFS_O_MKPTH) {
    useMode |= SFS_O_MKPTH;
    useFlags &= ~SFS_O_MKPTH;
  }
}

int ErrnoOf(XrdOucErrInfo &ei, int rc) {
  if (rc == SFS_OK)
    return 0;
  int err = ei.getErrInfo();
  // OFS stores the errno with the sign of the oss return, so a missing
  // file arrives as -ENOENT.
  if (err < 0)
    err = -err;
  if (err == 0)
    err = EIO;
  return err;
}

}

OwnedFile::~OwnedFile() {
  if (!fh)
    return;
  fh->close();
  delete fh;
  fh = nullptr;
}

int Fs::Open(const std::string &path, const XrdSecEntity *who, int flags, mode_t mode,
             OwnedFile &file, int &err) {
  err = 0;
  snprintf(file.user, sizeof(file.user), "%s", WhoName(who));
  file.fh = m_sfs.newFile(file.user, 0);
  if (!file.fh) {
    err = ENOMEM;
    return -1;
  }
  int useFlags = 0;
  mode_t useMode = 0;
  SplitMkpath(flags, mode, useFlags, useMode);
  int rc = file.fh->open(path.c_str(), useFlags, useMode, who, nullptr);
  rc = Wait(file.fh->error, rc);
  if (rc == SFS_STALL)
    rc = file.fh->open(path.c_str(), useFlags, useMode, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(file.fh->error, rc);
    file.fh->close();
    delete file.fh;
    file.fh = nullptr;
    return -1;
  }
  return 0;
}

int Fs::ReadAll(const std::string &path, const XrdSecEntity *who, std::string &out,
                int &err) {
  out.clear();
  err = 0;
  std::string user = WhoName(who);
  std::unique_ptr<XrdSfsFile, FileCloser> fh(m_sfs.newFile(&user[0], 0));
  if (!fh) {
    err = ENOMEM;
    return -1;
  }
  int rc = fh->open(path.c_str(), SFS_O_RDONLY, 0, who, nullptr);
  rc = Wait(fh->error, rc);
  if (rc == SFS_STALL)
    rc = fh->open(path.c_str(), SFS_O_RDONLY, 0, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(fh->error, rc);
    return -1;
  }
  struct stat st;
  if (fh->stat(&st) != SFS_OK) {
    err = ErrnoOf(fh->error, SFS_ERROR);
    return -1;
  }
  if (st.st_size < 0 || st.st_size > 1024 * 1024) {
    err = EFBIG;
    return -1;
  }
  out.resize(static_cast<size_t>(st.st_size));
  XrdSfsFileOffset off = 0;
  while (off < st.st_size) {
    int want = static_cast<int>(std::min<long long>(st.st_size - off, 65536));
    int n = fh->read(off, &out[static_cast<size_t>(off)], want);
    if (n < 0) {
      err = ErrnoOf(fh->error, n);
      out.clear();
      return -1;
    }
    if (n == 0)
      break;
    off += n;
  }
  out.resize(static_cast<size_t>(off));
  return 0;
}

int Fs::WriteAll(const std::string &path, const XrdSecEntity *who, const std::string &body,
                 mode_t mode, int &err) {
  err = 0;
  std::string user = WhoName(who);
  std::unique_ptr<XrdSfsFile, FileCloser> fh(m_sfs.newFile(&user[0], 0));
  if (!fh) {
    err = ENOMEM;
    return -1;
  }
  int useFlags = 0;
  mode_t useMode = 0;
  SplitMkpath(SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_MKPTH, mode,
              useFlags, useMode);
  int rc = fh->open(path.c_str(), useFlags, useMode, who, nullptr);
  rc = Wait(fh->error, rc);
  if (rc == SFS_STALL)
    rc = fh->open(path.c_str(), useFlags, useMode, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(fh->error, rc);
    return -1;
  }
  XrdSfsFileOffset off = 0;
  while (off < static_cast<XrdSfsFileOffset>(body.size())) {
    int want = static_cast<int>(std::min<size_t>(body.size() - static_cast<size_t>(off), 65536));
    int n = fh->write(off, body.data() + off, want);
    if (n < 0) {
      err = ErrnoOf(fh->error, n);
      return -1;
    }
    if (n == 0) {
      err = EIO;
      return -1;
    }
    off += n;
  }
  fh->sync();
  return 0;
}

int Fs::Unlink(const std::string &path, const XrdSecEntity *who, int &err) {
  XrdOucErrInfo ei(WhoName(who));
  int rc = m_sfs.rem(path.c_str(), ei, who, nullptr);
  rc = Wait(ei, rc);
  if (rc == SFS_STALL)
    rc = m_sfs.rem(path.c_str(), ei, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(ei, rc);
    return -1;
  }
  err = 0;
  return 0;
}

int Fs::Mkdir(const std::string &path, const XrdSecEntity *who, int &err) {
  XrdOucErrInfo ei(WhoName(who));
  int rc = m_sfs.mkdir(path.c_str(), 0755, ei, who, nullptr);
  rc = Wait(ei, rc);
  if (rc == SFS_STALL)
    rc = m_sfs.mkdir(path.c_str(), 0755, ei, who, nullptr);
  if (rc == SFS_OK) {
    err = 0;
    return 0;
  }
  err = ErrnoOf(ei, rc);
  if (err == EEXIST)
    return 0;
  return -1;
}

int Fs::Chown(const std::string &path, uid_t uid, gid_t gid, const XrdSecEntity *who, int &err) {
  XrdOucErrInfo ei(WhoName(who));
  int rc = m_sfs.chown(path.c_str(), uid, gid, ei, who, nullptr);
  rc = Wait(ei, rc);
  if (rc == SFS_STALL)
    rc = m_sfs.chown(path.c_str(), uid, gid, ei, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(ei, rc);
    return -1;
  }
  err = 0;
  return 0;
}

int Fs::Rmdir(const std::string &path, const XrdSecEntity *who, int &err) {
  XrdOucErrInfo ei(WhoName(who));
  int rc = m_sfs.remdir(path.c_str(), ei, who, nullptr);
  rc = Wait(ei, rc);
  if (rc == SFS_STALL)
    rc = m_sfs.remdir(path.c_str(), ei, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(ei, rc);
    return -1;
  }
  err = 0;
  return 0;
}

int Fs::Stat(const std::string &path, const XrdSecEntity *who, struct stat &st, int &err) {
  memset(&st, 0, sizeof(st));
  XrdOucErrInfo ei(WhoName(who));
  int rc = m_sfs.stat(path.c_str(), &st, ei, who, nullptr);
  rc = Wait(ei, rc);
  if (rc == SFS_STALL)
    rc = m_sfs.stat(path.c_str(), &st, ei, who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(ei, rc);
    return -1;
  }
  err = 0;
  return 0;
}

int Fs::List(const std::string &path, const XrdSecEntity *who,
             std::vector<std::string> &names, int &err) {
  names.clear();
  std::string user = WhoName(who);
  std::unique_ptr<XrdSfsDirectory, DirCloser> dir(m_sfs.newDir(&user[0], 0));
  if (!dir) {
    err = ENOMEM;
    return -1;
  }
  int rc = dir->open(path.c_str(), who, nullptr);
  rc = Wait(dir->error, rc);
  if (rc == SFS_STALL)
    rc = dir->open(path.c_str(), who, nullptr);
  if (rc != SFS_OK) {
    err = ErrnoOf(dir->error, rc);
    return -1;
  }
  for (;;) {
    dir->error.setErrCode(0);
    const char *ent = dir->nextEntry();
    if (!ent) {
      if (dir->error.getErrInfo() != 0) {
        err = dir->error.getErrInfo();
        return -1;
      }
      break;
    }
    if (strcmp(ent, ".") == 0 || strcmp(ent, "..") == 0)
      continue;
    names.emplace_back(ent);
  }
  err = 0;
  return 0;
}

int Fs::SetXattr(const std::string &path, const XrdSecEntity *who, const std::string &name,
                 const std::string &value, int &err) {
  XrdOucErrInfo ei(WhoName(who));
  XrdSfsFACtl ctl(path.c_str(), nullptr, 1);
  ctl.rqst = XrdSfsFACtl::faSet;
  ctl.info = new XrdSfsFAInfo[1];
  std::string nbuf = name;
  std::string vbuf = value;
  ctl.info[0].Name = &nbuf[0];
  ctl.info[0].NLen = static_cast<short>(nbuf.size());
  ctl.info[0].Value = &vbuf[0];
  ctl.info[0].VLen = static_cast<int>(vbuf.size());
  int rc = m_sfs.FAttr(&ctl, ei, who);
  if (rc != SFS_OK || (ctl.info && ctl.info[0].faRC != 0)) {
    err = rc == SFS_OK ? ctl.info[0].faRC : ErrnoOf(ei, rc);
    if (err == 0)
      err = ENOTSUP;
    return -1;
  }
  err = 0;
  return 0;
}

int Fs::GetXattr(const std::string &path, const XrdSecEntity *who, const std::string &name,
                 std::string &value, int &err) {
  value.clear();
  XrdOucErrInfo ei(WhoName(who));
  XrdSfsFACtl ctl(path.c_str(), nullptr, 1);
  ctl.rqst = XrdSfsFACtl::faGet;
  ctl.info = new XrdSfsFAInfo[1];
  std::string nbuf = name;
  ctl.info[0].Name = &nbuf[0];
  ctl.info[0].NLen = static_cast<short>(nbuf.size());
  int rc = m_sfs.FAttr(&ctl, ei, who);
  if (rc != SFS_OK || !ctl.info || ctl.info[0].faRC != 0 || !ctl.info[0].Value) {
    err = (rc == SFS_OK && ctl.info) ? ctl.info[0].faRC : ErrnoOf(ei, rc);
    if (err == 0)
      err = ENOENT;
    return -1;
  }
  value.assign(ctl.info[0].Value, static_cast<size_t>(ctl.info[0].VLen));
  err = 0;
  return 0;
}

}
