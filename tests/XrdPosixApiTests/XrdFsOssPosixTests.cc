#undef NDEBUG

#include "PosixApiStubs.hh"
#include "XrdFsOss/XrdFsOss.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOss/XrdOssWrapper.hh"
#include "XrdOssCsi/XrdOssHandler.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysXAttr.hh"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <memory>
#include <string>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

namespace {

std::string Join(const std::string &dir, const char *name)
{
   return dir + "/" + name;
}

bool MissingXattr(int rc)
{
   return rc == -ENOATTR || rc == -ENODATA || rc == -ENOENT;
}

class PassWrap : public XrdOssWrapper
{
public:
   explicit PassWrap(XrdOss &oss) : XrdOssWrapper(oss) {}
};

class PassHandler : public XrdOssHandler
{
public:
   explicit PassHandler(XrdOss *oss) : XrdOssHandler(oss) {}
   XrdOssDF *newDir(const char *t) override { return successor_->newDir(t); }
   XrdOssDF *newFile(const char *t) override { return successor_->newFile(t); }
   int Init(XrdSysLogger *lp, const char *cfn) override
      { return successor_->Init(lp, cfn); }
};

} // namespace

class FsOssPosixTest : public ::testing::Test
{
protected:
   void SetUp() override
   {
      char tmpl[] = "/tmp/xrdposixXXXXXX";
      char *made = mkdtemp(tmpl);
      ASSERT_TRUE(made);
      root = made;
      nulfd = open("/dev/null", O_WRONLY);
      ASSERT_GE(nulfd, 0);
      log = new XrdSysLogger(nulfd, 0);
      oss = new XrdFsOss();
      const std::string cfgp = Join(root, "fsoss.cfg");
      {
         std::ofstream cfg(cfgp.c_str());
         ASSERT_TRUE(cfg.good());
         cfg << "oss.fsuid off\n";
      }
      setenv("XRDINSTANCE", "xrootd", 1);
      ASSERT_EQ(0, oss->Init(log, cfgp.c_str()));
   }

   void TearDown() override
   {
      delete oss;
      oss = 0;
      delete log;
      log = 0;
      if (nulfd >= 0) close(nulfd);
      if (!root.empty())
         {
          std::string cmd = "rm -rf \"" + root + "\"";
          int rc = system(cmd.c_str());
          (void)rc;
         }
   }

   std::string P(const char *name) const { return Join(root, name); }

   std::string root;
   int nulfd{-1};
   XrdSysLogger *log{0};
   XrdFsOss *oss{0};
};

TEST(XrdOssDefaults, PosixMethodsReturnEnotsup)
{
   DummyOss dummy;
   DummyDF df;
   XrdOucEnv env;
   struct timespec ts[2] = {};
   char buff[16];
   XrdSysXAttr::AList *al = 0;
   struct flock fl;
   memset(&fl, 0, sizeof(fl));

   EXPECT_EQ(-ENOTSUP, dummy.Access("/nope", F_OK, &env));
   EXPECT_EQ(-ENOTSUP, dummy.Mknod("/n", S_IFIFO | 0644, 0, &env));
   EXPECT_EQ(-ENOTSUP, dummy.Chown("/n", 0, 0, &env));
   EXPECT_EQ(-ENOTSUP, dummy.Symlink("t", "/n", &env));
   EXPECT_EQ(-ENOTSUP, dummy.Readlink("/n", buff, (int)sizeof(buff), &env));
   EXPECT_EQ(-ENOTSUP, dummy.Utimes("/n", ts, &env));
   EXPECT_EQ(-ENOTSUP, dummy.DelXattr("a", "/n", &env));
   EXPECT_EQ(-ENOTSUP, dummy.GetXattr("a", buff, (int)sizeof(buff), "/n", &env));
   EXPECT_EQ(-ENOTSUP, dummy.SetXattr("a", "v", 1, "/n", &env));
   EXPECT_EQ(-ENOTSUP, dummy.ListXattr(&al, "/n", &env));
   dummy.FreeXattr(0);
   EXPECT_EQ(0u, dummy.Features());
   EXPECT_EQ(-ENOTSUP, df.Fchown(0, 0));
   EXPECT_EQ(-ENOTSUP, df.Flock(LOCK_EX));
   EXPECT_EQ(-ENOTSUP, df.FcntlLock(F_GETLK, &fl));
   EXPECT_EQ(-ENOTSUP, df.FDelXattr("a"));
   EXPECT_EQ(-ENOTSUP, df.FGetXattr("a", buff, 4));
   EXPECT_EQ(-ENOTSUP, df.FSetXattr("a", "v", 1));
   EXPECT_EQ(-ENOTSUP, df.FListXattr(&al));
}

TEST(XrdOssDefaults, FeaturesFlag)
{
   EXPECT_NE(0u, XRDOSS_HASPOSIX);
   EXPECT_NE(XRDOSS_HASNAIO, XRDOSS_HASPOSIX);
}

TEST_F(FsOssPosixTest, FeaturesAndInit)
{
   EXPECT_EQ(XRDOSS_HASNAIO | XRDOSS_HASPOSIX, oss->Features());
   EXPECT_EQ(XrdFsOssUid::Off, oss->fsuidMode);
}

TEST_F(FsOssPosixTest, CreateStatAccessUnlink)
{
   XrdOucEnv env;
   const std::string f = P("file");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
   EXPECT_TRUE(S_ISREG(st.st_mode));
   EXPECT_EQ(0, oss->Access(f.c_str(), F_OK));
   EXPECT_EQ(0, oss->Access(f.c_str(), R_OK));
   EXPECT_EQ(0, oss->Access(f.c_str(), W_OK));
   EXPECT_EQ(-ENOENT, oss->Access(P("missing").c_str(), F_OK));
   ASSERT_EQ(0, chmod(f.c_str(), 0444));
   EXPECT_EQ(-EACCES, oss->Access(f.c_str(), W_OK));
   ASSERT_EQ(0, chmod(f.c_str(), 0644));
   EXPECT_EQ(0, oss->Unlink(f.c_str()));
   EXPECT_EQ(0, oss->Unlink(P("already-gone").c_str()));
}

TEST_F(FsOssPosixTest, AccessExecuteOnDirectory)
{
   XrdOucEnv env;
   const std::string d = P("dirx");
   ASSERT_EQ(0, oss->Mkdir(d.c_str(), 0755, 0, &env));
   EXPECT_EQ(0, oss->Access(d.c_str(), X_OK));
   EXPECT_EQ(0, oss->Remdir(d.c_str()));
}

TEST_F(FsOssPosixTest, ChownSelfAndMissing)
{
   XrdOucEnv env;
   const std::string f = P("chownf");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   EXPECT_EQ(0, oss->Chown(f.c_str(), getuid(), getgid(), &env));
   EXPECT_EQ(0, oss->Chown(f.c_str(), (uid_t)-1, (gid_t)-1, &env));
   EXPECT_EQ(-ENOENT, oss->Chown(P("nope").c_str(), getuid(), getgid(), &env));
}

TEST_F(FsOssPosixTest, SymlinkReadlinkLink)
{
   XrdOucEnv env;
   const std::string tgt = P("target");
   const std::string lnk = P("symlink");
   const std::string hln = P("hardlink");
   ASSERT_EQ(0, oss->Create("t", tgt.c_str(), 0644, env));
   ASSERT_EQ(0, oss->Symlink(tgt.c_str(), lnk.c_str(), &env));
   char buff[MAXPATHLEN];
   int n = oss->Readlink(lnk.c_str(), buff, (int)sizeof(buff), &env);
   ASSERT_GE(n, 0);
   EXPECT_EQ(tgt, std::string(buff));
   EXPECT_EQ(-EINVAL, oss->Readlink(lnk.c_str(), buff, 0, &env));
   EXPECT_EQ(-EINVAL, oss->Readlink(lnk.c_str(), 0, 16, &env));
   char tiny[2];
   n = oss->Readlink(lnk.c_str(), tiny, 1, &env);
   EXPECT_EQ(1, n);
   struct stat st;
   ASSERT_EQ(0, oss->Stat(lnk.c_str(), &st));
   EXPECT_TRUE(S_ISLNK(st.st_mode));
   EXPECT_EQ(0, oss->Link(tgt.c_str(), hln.c_str(), &env));
   ASSERT_EQ(0, oss->Stat(hln.c_str(), &st));
   EXPECT_TRUE(S_ISREG(st.st_mode));
   EXPECT_GE(st.st_nlink, 2u);
}

TEST_F(FsOssPosixTest, Utimes)
{
   XrdOucEnv env;
   const std::string f = P("ut");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   struct timespec ts[2];
   ts[0].tv_sec = 1700000000; ts[0].tv_nsec = 0;
   ts[1].tv_sec = 1700000100; ts[1].tv_nsec = 0;
   ASSERT_EQ(0, oss->Utimes(f.c_str(), ts, &env));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
   EXPECT_EQ(1700000100, (long long)st.st_mtime);
   EXPECT_EQ(-ENOENT, oss->Utimes(P("nope").c_str(), ts, &env));
}

TEST_F(FsOssPosixTest, MkdirRenameRemdir)
{
   XrdOucEnv env;
   const std::string a = P("da");
   const std::string b = P("db");
   ASSERT_EQ(0, oss->Mkdir(a.c_str(), 0755, 0, &env));
   ASSERT_EQ(0, oss->Rename(a.c_str(), b.c_str(), &env, &env));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(b.c_str(), &st));
   EXPECT_TRUE(S_ISDIR(st.st_mode));
   EXPECT_EQ(0, oss->Remdir(b.c_str()));
}

TEST_F(FsOssPosixTest, Truncate)
{
   XrdOucEnv env;
   const std::string f = P("tr");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   ASSERT_EQ(0, oss->Truncate(f.c_str(), 1234, &env));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
   EXPECT_EQ(1234, (long long)st.st_size);
}

TEST_F(FsOssPosixTest, MknodFifo)
{
   XrdOucEnv env;
   const std::string f = P("fifo");
   int rc = oss->Mknod(f.c_str(), S_IFIFO | 0644, 0, &env);
   ASSERT_EQ(0, rc);
   struct stat st;
   ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
   EXPECT_TRUE(S_ISFIFO(st.st_mode));
}

TEST_F(FsOssPosixTest, MknodDeviceMayFailWithoutCap)
{
   XrdOucEnv env;
   const std::string f = P("devnode");
   int rc = oss->Mknod(f.c_str(), S_IFCHR | 0644, 0, &env);
   if (rc == 0)
      {
       struct stat st;
       ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
       EXPECT_TRUE(S_ISCHR(st.st_mode));
      }
   else
      EXPECT_TRUE(rc == -EPERM || rc == -EACCES || rc == -ENOTSUP ||
                  rc == -EINVAL || rc == -ENOENT);
}

TEST_F(FsOssPosixTest, PathXattrs)
{
   XrdOucEnv env;
   const std::string f = P("xa");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   const char *name = "posixut";
   const char *val = "hello";
   ASSERT_EQ(0, oss->SetXattr(name, val, 5, f.c_str(), &env));
   int sz = oss->GetXattr(name, 0, 0, f.c_str(), &env);
   ASSERT_EQ(5, sz);
   char buf[16];
   memset(buf, 0, sizeof(buf));
   ASSERT_EQ(5, oss->GetXattr(name, buf, (int)sizeof(buf), f.c_str(), &env));
   EXPECT_STREQ("hello", buf);
   EXPECT_EQ(-EEXIST, oss->SetXattr(name, "x", 1, f.c_str(), &env, -1, 1));
   XrdSysXAttr::AList *al = 0;
   ASSERT_GE(oss->ListXattr(&al, f.c_str(), &env), 0);
   bool found = false;
   for (XrdSysXAttr::AList *p = al; p; p = p->Next)
      if (!strcmp(p->Name, name)) found = true;
   EXPECT_TRUE(found);
   oss->FreeXattr(al);
   ASSERT_EQ(0, oss->DelXattr(name, f.c_str(), &env));
   EXPECT_TRUE(MissingXattr(oss->GetXattr(name, buf, (int)sizeof(buf),
                                          f.c_str(), &env)));
}

TEST_F(FsOssPosixTest, FileOpenReadWriteFstatFchownFchmod)
{
   XrdOucEnv env;
   const std::string f = P("rw");
   std::unique_ptr<XrdOssDF> fp(oss->newFile("t"));
   ASSERT_TRUE(fp.get());
   ASSERT_EQ(0, fp->Open(f.c_str(), O_RDWR | O_CREAT, 0644, env));
   EXPECT_EQ(4, fp->Write("abcd", 0, 4));
   char buf[8];
   memset(buf, 0, sizeof(buf));
   EXPECT_EQ(4, fp->Read(buf, 0, 4));
   EXPECT_STREQ("abcd", buf);
   struct stat st;
   ASSERT_EQ(0, fp->Fstat(&st));
   EXPECT_EQ(4, (long long)st.st_size);
   EXPECT_EQ(0, fp->Fchmod(0600));
   EXPECT_EQ(0, fp->Fchown(getuid(), getgid()));
   EXPECT_EQ(0, fp->Ftruncate(2));
   EXPECT_EQ(0, fp->Fsync());
   EXPECT_EQ(0, fp->Close());
}

TEST_F(FsOssPosixTest, FileXattrs)
{
   XrdOucEnv env;
   const std::string f = P("fxa");
   std::unique_ptr<XrdOssDF> fp(oss->newFile("t"));
   ASSERT_EQ(0, fp->Open(f.c_str(), O_RDWR | O_CREAT, 0644, env));
   ASSERT_EQ(0, fp->FSetXattr("fattr", "vv", 2));
   char buf[8];
   memset(buf, 0, sizeof(buf));
   ASSERT_EQ(2, fp->FGetXattr("fattr", buf, (int)sizeof(buf)));
   EXPECT_STREQ("vv", buf);
   XrdSysXAttr::AList *al = 0;
   ASSERT_GE(fp->FListXattr(&al), 0);
   oss->FreeXattr(al);
   EXPECT_EQ(0, fp->FDelXattr("fattr"));
   EXPECT_TRUE(MissingXattr(fp->FGetXattr("fattr", buf, 8)));
   EXPECT_EQ(-ENOTSUP, DummyDF().FGetXattr("a", buf, 1));
}

TEST_F(FsOssPosixTest, FlockExclusive)
{
   XrdOucEnv env;
   const std::string f = P("lk");
   std::unique_ptr<XrdOssDF> a(oss->newFile("a"));
   std::unique_ptr<XrdOssDF> b(oss->newFile("b"));
   ASSERT_EQ(0, a->Open(f.c_str(), O_RDWR | O_CREAT, 0644, env));
   ASSERT_EQ(0, b->Open(f.c_str(), O_RDWR, 0, env));
   ASSERT_EQ(0, a->Flock(LOCK_EX));
   int rc = b->Flock(LOCK_EX | LOCK_NB);
   EXPECT_TRUE(rc == 0 || rc == -EAGAIN || rc == -EACCES || rc == -EWOULDBLOCK);
   ASSERT_EQ(0, a->Flock(LOCK_UN));
   if (rc != 0)
      {
       EXPECT_EQ(0, b->Flock(LOCK_EX | LOCK_NB));
      }
   EXPECT_EQ(0, b->Flock(LOCK_UN));
   EXPECT_EQ(-ENOTSUP, DummyDF().Flock(LOCK_EX));
}

TEST_F(FsOssPosixTest, FcntlRecordLock)
{
   XrdOucEnv env;
   const std::string f = P("flk");
   std::unique_ptr<XrdOssDF> a(oss->newFile("a"));
   std::unique_ptr<XrdOssDF> b(oss->newFile("b"));
   ASSERT_EQ(0, a->Open(f.c_str(), O_RDWR | O_CREAT, 0644, env));
   ASSERT_EQ(0, b->Open(f.c_str(), O_RDWR, 0, env));
   struct flock fl;
   memset(&fl, 0, sizeof(fl));
   fl.l_type = F_WRLCK;
   fl.l_whence = SEEK_SET;
   fl.l_start = 0;
   fl.l_len = 0;
   ASSERT_EQ(0, a->FcntlLock(F_SETLK, &fl));
   struct flock probe = fl;
   probe.l_type = F_WRLCK;
   EXPECT_EQ(0, b->FcntlLock(F_GETLK, &probe));
   memset(&fl, 0, sizeof(fl));
   fl.l_type = F_WRLCK;
   fl.l_whence = SEEK_SET;
   int rc = b->FcntlLock(F_SETLK, &fl);
   EXPECT_TRUE(rc == -EAGAIN || rc == -EACCES || rc == -EWOULDBLOCK || rc == 0);
   memset(&fl, 0, sizeof(fl));
   fl.l_type = F_UNLCK;
   fl.l_whence = SEEK_SET;
   EXPECT_EQ(0, a->FcntlLock(F_SETLK, &fl));
   EXPECT_EQ(-EINVAL, a->FcntlLock(F_SETLK, 0));
}

TEST_F(FsOssPosixTest, DirectoryRead)
{
   XrdOucEnv env;
   const std::string d = P("dird");
   ASSERT_EQ(0, oss->Mkdir(d.c_str(), 0755, 0, &env));
   const std::string inner = Join(d, "inner");
   ASSERT_EQ(0, oss->Create("t", inner.c_str(), 0644, env));
   std::unique_ptr<XrdOssDF> dp(oss->newDir("t"));
   ASSERT_EQ(0, dp->Opendir(d.c_str(), env));
   char name[256];
   bool saw = false;
   for (;;)
      {
       ASSERT_EQ(0, dp->Readdir(name, (int)sizeof(name)));
       if (!name[0]) break;
       if (!strcmp(name, "inner")) saw = true;
      }
   EXPECT_TRUE(saw);
   EXPECT_EQ(0, dp->Close());
}

TEST_F(FsOssPosixTest, WrapperAndHandlerForward)
{
   XrdOucEnv env;
   const std::string f = P("wrapf");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   PassWrap wrap(*oss);
   EXPECT_EQ(XRDOSS_HASNAIO | XRDOSS_HASPOSIX, wrap.Features());
   EXPECT_EQ(0, wrap.Access(f.c_str(), F_OK, &env));
   EXPECT_EQ(0, wrap.Chown(f.c_str(), getuid(), getgid(), &env));
   PassHandler h(oss);
   EXPECT_EQ(0, h.Access(f.c_str(), F_OK, &env));
   DummyOss dummy;
   PassWrap dw(dummy);
   EXPECT_EQ(-ENOTSUP, dw.Access("/x", F_OK));
   EXPECT_EQ(-ENOTSUP, dw.Mknod("/x", S_IFIFO | 0644, 0));
   std::unique_ptr<XrdOssDF> inner(new DummyDF());
   XrdOssDFHandler dh(inner.release());
   EXPECT_EQ(-ENOTSUP, dh.Flock(LOCK_EX));
   EXPECT_EQ(-ENOTSUP, dh.Fchown(0, 0));
}

TEST_F(FsOssPosixTest, Lfn2PfnWithoutNamlib)
{
   char buff[64];
   EXPECT_EQ(0, oss->Lfn2Pfn("/abs/path", buff, (int)sizeof(buff)));
   EXPECT_STREQ("/abs/path", buff);
   int rc = 0;
   const char *p = oss->Lfn2Pfn("/z", buff, (int)sizeof(buff), rc);
   EXPECT_EQ(0, rc);
   EXPECT_STREQ("/z", p);
}

TEST_F(FsOssPosixTest, InitMissingConfigFails)
{
   XrdFsOss other;
   EXPECT_NE(0, other.Init(log, P("missing.cfg").c_str()));
}

TEST_F(FsOssPosixTest, Chmod)
{
   XrdOucEnv env;
   const std::string f = P("mod");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   ASSERT_EQ(0, oss->Chmod(f.c_str(), 0600, &env));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
   EXPECT_EQ(0600, (int)(st.st_mode & 0777));
}

TEST_F(FsOssPosixTest, MissingPathErrors)
{
   XrdOucEnv env;
   char buff[32];
   struct stat st;
   EXPECT_EQ(-ENOENT, oss->Stat(P("no").c_str(), &st));
   EXPECT_EQ(-ENOENT, oss->Chmod(P("no").c_str(), 0600, &env));
   EXPECT_EQ(-ENOENT, oss->Truncate(P("no").c_str(), 1, &env));
   EXPECT_EQ(-ENOENT, oss->Readlink(P("no").c_str(), buff, (int)sizeof(buff), &env));
   EXPECT_EQ(-ENOENT, oss->Access(P("no").c_str(), R_OK | W_OK));
   EXPECT_EQ(0, oss->Remdir(P("no").c_str()));
   EXPECT_EQ(0, oss->Unlink(P("no").c_str()));
}

TEST_F(FsOssPosixTest, ReadlinkRegularFile)
{
   XrdOucEnv env;
   const std::string f = P("notlink");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   char buff[32];
   EXPECT_EQ(-EINVAL, oss->Readlink(f.c_str(), buff, (int)sizeof(buff), &env));
}

TEST_F(FsOssPosixTest, CreateExclusiveAndMkpath)
{
   XrdOucEnv env;
   const std::string nested = Join(P("a"), "b/cfile");
   ASSERT_EQ(0, oss->Create("t", nested.c_str(), 0644, env, XRDOSS_mkpath));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(nested.c_str(), &st));
   EXPECT_TRUE(S_ISREG(st.st_mode));
   EXPECT_EQ(-EEXIST, oss->Create("t", nested.c_str(), 0644, env, XRDOSS_new));
}

TEST_F(FsOssPosixTest, MknodExistingFifo)
{
   XrdOucEnv env;
   const std::string f = P("fifo2");
   ASSERT_EQ(0, oss->Mknod(f.c_str(), S_IFIFO | 0644, 0, &env));
   EXPECT_EQ(-EEXIST, oss->Mknod(f.c_str(), S_IFIFO | 0644, 0, &env));
}

TEST_F(FsOssPosixTest, SymlinkExistingAndMissingTargetOk)
{
   XrdOucEnv env;
   const std::string lnk = P("dangling");
   ASSERT_EQ(0, oss->Symlink("/does/not/exist", lnk.c_str(), &env));
   EXPECT_EQ(-EEXIST, oss->Symlink("x", lnk.c_str(), &env));
}

TEST_F(FsOssPosixTest, StatFSStatVSStatLS)
{
   char buff[256];
   int blen = (int)sizeof(buff);
   ASSERT_EQ(0, oss->StatFS(root.c_str(), buff, blen));
   EXPECT_GT(blen, 0);
   EXPECT_NE('\0', buff[0]);
   XrdOssVSInfo vs;
   ASSERT_EQ(0, oss->StatVS(&vs, 0, 0));
   EXPECT_GT(vs.Total, 0);
   EXPECT_EQ(-EINVAL, oss->StatVS(0, 0, 0));
   blen = (int)sizeof(buff);
   XrdOucEnv env;
   ASSERT_EQ(0, oss->StatLS(env, root.c_str(), buff, blen));
   EXPECT_NE(std::string::npos, std::string(buff).find("oss.space="));
}

TEST_F(FsOssPosixTest, InitInvalidFsuidFails)
{
   XrdFsOss other;
   const std::string cfgp = P("bad.cfg");
   {
      std::ofstream cfg(cfgp.c_str());
      cfg << "oss.fsuid nope\n";
   }
   EXPECT_NE(0, other.Init(log, cfgp.c_str()));
}

TEST_F(FsOssPosixTest, InitIgnoresCacheStageSpace)
{
   XrdFsOss other;
   const std::string cfgp = P("ignored.cfg");
   {
      std::ofstream cfg(cfgp.c_str());
      cfg << "oss.fsuid off\n"
          << "oss.cache /tmp\n"
          << "oss.stage /tmp\n"
          << "oss.space public /tmp\n";
   }
   EXPECT_EQ(0, other.Init(log, cfgp.c_str()));
}

TEST_F(FsOssPosixTest, AccessCombinedBits)
{
   XrdOucEnv env;
   const std::string f = P("combo");
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   EXPECT_EQ(0, oss->Access(f.c_str(), R_OK | W_OK));
   EXPECT_EQ(-EACCES, oss->Access(f.c_str(), X_OK));
}

TEST_F(FsOssPosixTest, FileFcntlInvalid)
{
   XrdOucEnv env;
   const std::string f = P("badlk");
   std::unique_ptr<XrdOssDF> a(oss->newFile("a"));
   ASSERT_EQ(0, a->Open(f.c_str(), O_RDWR | O_CREAT, 0644, env));
   EXPECT_EQ(-EINVAL, a->FcntlLock(F_SETLK, 0));
   EXPECT_EQ(-ENOTSUP, DummyDF().FcntlLock(F_GETLK, 0));
}
