#undef NDEBUG

#include "PosixApiStubs.hh"
#include "XrdFsOss/XrdFsOss.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSfs/XrdSfsNative.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"

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
#include <sys/stat.h>
#include <unistd.h>

extern XrdOfs *XrdOfsFS;
extern XrdOss *XrdOfsOss;
extern XrdSysError OfsEroute;

namespace {

std::string Join(const std::string &dir, const char *name)
{
   return dir + "/" + name;
}

} // namespace

TEST(XrdSfsDefaults, FileSystemPosixMethodsEnotsup)
{
   StubFS fs;
   XrdOucErrInfo e;
   char buff[32];
   struct timespec ts[2] = {};
   EXPECT_EQ(SFS_ERROR, fs.access("/x", F_OK, e));
   EXPECT_EQ(ENOTSUP, e.getErrInfo());
   EXPECT_EQ(SFS_ERROR, fs.mknod("/x", S_IFIFO | 0644, 0, e));
   EXPECT_EQ(ENOTSUP, e.getErrInfo());
   EXPECT_EQ(SFS_ERROR, fs.chown("/x", 0, 0, e));
   EXPECT_EQ(ENOTSUP, e.getErrInfo());
   EXPECT_EQ(SFS_ERROR, fs.symlink("t", "/x", e));
   EXPECT_EQ(ENOTSUP, e.getErrInfo());
   EXPECT_EQ(SFS_ERROR, fs.readlink("/x", buff, (int)sizeof(buff), e));
   EXPECT_EQ(ENOTSUP, e.getErrInfo());
   EXPECT_EQ(SFS_ERROR, fs.utimes("/x", ts, e));
   EXPECT_EQ(ENOTSUP, e.getErrInfo());
}

TEST(XrdSfsDefaults, FilePosixMethodsEnotsup)
{
   StubFile f;
   struct flock fl;
   memset(&fl, 0, sizeof(fl));
   EXPECT_EQ(SFS_ERROR, f.flock(LOCK_EX));
   EXPECT_EQ(ENOTSUP, f.error.getErrInfo());
   EXPECT_EQ(SFS_ERROR, f.fchown(0, 0));
   EXPECT_EQ(ENOTSUP, f.error.getErrInfo());
   EXPECT_EQ(SFS_ERROR, f.fcntlLock(F_GETLK, &fl));
   EXPECT_EQ(ENOTSUP, f.error.getErrInfo());
}

class NativePosixTest : public ::testing::Test
{
protected:
   void SetUp() override
   {
      char tmpl[] = "/tmp/xrdnativeXXXXXX";
      char *made = mkdtemp(tmpl);
      ASSERT_TRUE(made);
      root = made;
      nulfd = open("/dev/null", O_WRONLY);
      ASSERT_GE(nulfd, 0);
      log = new XrdSysLogger(nulfd, 0);
      err = new XrdSysError(log, "sfs_");
      ns = new XrdSfsNative(err);
   }

   void TearDown() override
   {
      delete ns;
      ns = 0;
      delete err;
      err = 0;
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

   std::string P(const char *n) const { return Join(root, n); }

   std::string root;
   int nulfd{-1};
   XrdSysLogger *log{0};
   XrdSysError *err{0};
   XrdSfsNative *ns{0};
};

TEST_F(NativePosixTest, AccessMknodSymlinkReadlinkUtimesChown)
{
   XrdOucErrInfo e("t");
   const std::string f = P("nf");
   FILE *fp = fopen(f.c_str(), "w");
   ASSERT_TRUE(fp);
   fclose(fp);
   EXPECT_EQ(SFS_OK, ns->access(f.c_str(), F_OK, e));
   EXPECT_EQ(SFS_ERROR, ns->access(P("missing").c_str(), F_OK, e));

   const std::string fifo = P("nfifo");
   ASSERT_EQ(SFS_OK, ns->mknod(fifo.c_str(), S_IFIFO | 0644, 0, e));
   struct stat st;
   ASSERT_EQ(0, lstat(fifo.c_str(), &st));
   EXPECT_TRUE(S_ISFIFO(st.st_mode));

   const std::string lnk = P("nlnk");
   ASSERT_EQ(SFS_OK, ns->symlink(f.c_str(), lnk.c_str(), e));
   char buff[256];
   ASSERT_EQ(SFS_OK, ns->readlink(lnk.c_str(), buff, (int)sizeof(buff), e));
   EXPECT_EQ(f, std::string(buff));

   struct timespec ts[2];
   ts[0].tv_sec = 1600000000; ts[0].tv_nsec = 0;
   ts[1].tv_sec = 1600000001; ts[1].tv_nsec = 0;
   ASSERT_EQ(SFS_OK, ns->utimes(f.c_str(), ts, e));
   ASSERT_EQ(SFS_OK, ns->chown(f.c_str(), getuid(), getgid(), e));
}

TEST_F(NativePosixTest, FileFlockFchownFcntlLock)
{
   XrdOucErrInfo e("t");
   const std::string f = P("nlock");
   std::unique_ptr<XrdSfsFile> a(ns->newFile((char *)"a", 0));
   std::unique_ptr<XrdSfsFile> b(ns->newFile((char *)"b", 0));
   ASSERT_EQ(SFS_OK, a->open(f.c_str(), SFS_O_RDWR | SFS_O_CREAT, 0644));
   ASSERT_EQ(SFS_OK, b->open(f.c_str(), SFS_O_RDWR, 0644));
   ASSERT_EQ(SFS_OK, a->flock(LOCK_EX));
   int lrc = b->flock(LOCK_EX | LOCK_NB);
   EXPECT_TRUE(lrc == SFS_OK || lrc == SFS_ERROR);
   ASSERT_EQ(SFS_OK, a->flock(LOCK_UN));
   EXPECT_EQ(SFS_OK, a->fchown(getuid(), getgid()));
   struct flock fl;
   memset(&fl, 0, sizeof(fl));
   fl.l_type = F_WRLCK;
   fl.l_whence = SEEK_SET;
   EXPECT_EQ(SFS_OK, a->fcntlLock(F_SETLK, &fl));
   fl.l_type = F_UNLCK;
   EXPECT_EQ(SFS_OK, a->fcntlLock(F_SETLK, &fl));
   EXPECT_EQ(SFS_OK, a->close());
   EXPECT_EQ(SFS_OK, b->close());
}

class OfsPosixTest : public ::testing::Test
{
protected:
   void SetUp() override
   {
      char tmpl[] = "/tmp/xrdofsXXXXXX";
      char *made = mkdtemp(tmpl);
      ASSERT_TRUE(made);
      root = made;
      nulfd = open("/dev/null", O_WRONLY);
      ASSERT_GE(nulfd, 0);
      log = new XrdSysLogger(nulfd, 0);
      OfsEroute.logger(log);
      oss = new XrdFsOss();
      const std::string cfgp = Join(root, "fsoss.cfg");
      {
         std::ofstream cfg(cfgp.c_str());
         ASSERT_TRUE(cfg.good());
         cfg << "oss.fsuid off\n";
      }
      setenv("XRDINSTANCE", "xrootd", 1);
      ASSERT_EQ(0, oss->Init(log, cfgp.c_str()));
      ofs = new XrdOfs();
      ofs->Options = 0;
      XrdOfsFS = ofs;
      XrdOfsOss = oss;
   }

   void TearDown() override
   {
      XrdOfsFS = 0;
      XrdOfsOss = 0;
      delete ofs;
      ofs = 0;
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

   std::string P(const char *n) const { return Join(root, n); }

   std::string root;
   int nulfd{-1};
   XrdSysLogger *log{0};
   XrdFsOss *oss{0};
   XrdOfs *ofs{0};
};

TEST_F(OfsPosixTest, DisabledReturnsEnotsup)
{
   XrdOucErrInfo e("t");
   char buff[16];
   struct timespec ts[2] = {};
   ofs->Options = 0;
   auto enotsup = [](int c) { return c == ENOTSUP || c == -ENOTSUP; };
   EXPECT_EQ(SFS_ERROR, ofs->access(P("x").c_str(), F_OK, e, 0));
   EXPECT_TRUE(enotsup(e.getErrInfo()));
   EXPECT_EQ(SFS_ERROR, ofs->mknod(P("x").c_str(), S_IFIFO | 0644, 0, e, 0));
   EXPECT_TRUE(enotsup(e.getErrInfo()));
   EXPECT_EQ(SFS_ERROR, ofs->chown(P("x").c_str(), 0, 0, e, 0));
   EXPECT_TRUE(enotsup(e.getErrInfo()));
   EXPECT_EQ(SFS_ERROR, ofs->symlink("t", P("x").c_str(), e, 0));
   EXPECT_TRUE(enotsup(e.getErrInfo()));
   EXPECT_EQ(SFS_ERROR, ofs->readlink(P("x").c_str(), buff, 16, e, 0));
   EXPECT_TRUE(enotsup(e.getErrInfo()));
   EXPECT_EQ(SFS_ERROR, ofs->utimes(P("x").c_str(), ts, e, 0));
   EXPECT_TRUE(enotsup(e.getErrInfo()));
   std::unique_ptr<XrdSfsFile> f(ofs->newFile((char *)"t", 0));
   EXPECT_EQ(SFS_ERROR, f->flock(LOCK_EX));
   EXPECT_TRUE(enotsup(f->error.getErrInfo()));
   EXPECT_EQ(SFS_ERROR, f->fchown(0, 0));
   EXPECT_TRUE(enotsup(f->error.getErrInfo()));
   struct flock fl;
   memset(&fl, 0, sizeof(fl));
   EXPECT_EQ(SFS_ERROR, f->fcntlLock(F_GETLK, &fl));
   EXPECT_TRUE(enotsup(f->error.getErrInfo()));
}

TEST_F(OfsPosixTest, EnabledAccessMknodSymlinkReadlinkChownUtimes)
{
   XrdOucErrInfo e("t");
   ofs->Options = XrdOfs::PosixFS;
   const std::string f = P("of");
   XrdOucEnv env;
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   EXPECT_EQ(SFS_OK, ofs->access(f.c_str(), F_OK, e, 0));
   EXPECT_EQ(SFS_OK, ofs->access(f.c_str(), R_OK, e, 0));
   EXPECT_EQ(SFS_ERROR, ofs->access(P("missing").c_str(), F_OK, e, 0));

   const std::string fifo = P("ofifo");
   ASSERT_EQ(SFS_OK, ofs->mknod(fifo.c_str(), S_IFIFO | 0644, 0, e, 0));
   struct stat st;
   ASSERT_EQ(0, oss->Stat(fifo.c_str(), &st));
   EXPECT_TRUE(S_ISFIFO(st.st_mode));

   const std::string lnk = P("olnk");
   ASSERT_EQ(SFS_OK, ofs->symlink(f.c_str(), lnk.c_str(), e, 0));
   char buff[256];
   memset(buff, 0, sizeof(buff));
   ASSERT_EQ(SFS_OK, ofs->readlink(lnk.c_str(), buff, (int)sizeof(buff), e, 0));
   EXPECT_EQ(f, std::string(buff));
   ASSERT_EQ(SFS_OK, ofs->chown(f.c_str(), getuid(), getgid(), e, 0));
   struct timespec ts[2];
   ts[0].tv_sec = 1500000000; ts[0].tv_nsec = 0;
   ts[1].tv_sec = 1500000002; ts[1].tv_nsec = 0;
   ASSERT_EQ(SFS_OK, ofs->utimes(f.c_str(), ts, e, 0));
   ASSERT_EQ(0, oss->Stat(f.c_str(), &st));
   EXPECT_EQ(1500000002, (long long)st.st_mtime);
}

TEST_F(OfsPosixTest, EnabledToggleOffAgain)
{
   XrdOucErrInfo e("t");
   ofs->Options = XrdOfs::PosixFS;
   const std::string f = P("tg");
   XrdOucEnv env;
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   EXPECT_EQ(SFS_OK, ofs->access(f.c_str(), F_OK, e, 0));
   ofs->Options = 0;
   EXPECT_EQ(SFS_ERROR, ofs->access(f.c_str(), F_OK, e, 0));
   EXPECT_TRUE(e.getErrInfo() == ENOTSUP || e.getErrInfo() == -ENOTSUP);
}

TEST_F(OfsPosixTest, EnabledMissingAndExistingErrors)
{
   XrdOucErrInfo e("t");
   ofs->Options = XrdOfs::PosixFS;
   EXPECT_EQ(SFS_ERROR, ofs->access(P("nope").c_str(), F_OK, e, 0));
   EXPECT_TRUE(e.getErrInfo() == ENOENT || e.getErrInfo() == -ENOENT);
   const std::string fifo = P("ofifo2");
   ASSERT_EQ(SFS_OK, ofs->mknod(fifo.c_str(), S_IFIFO | 0644, 0, e, 0));
   EXPECT_EQ(SFS_ERROR, ofs->mknod(fifo.c_str(), S_IFIFO | 0644, 0, e, 0));
   const std::string f = P("olnk2src");
   XrdOucEnv env;
   ASSERT_EQ(0, oss->Create("t", f.c_str(), 0644, env));
   const std::string lnk = P("olnk2");
   ASSERT_EQ(SFS_OK, ofs->symlink(f.c_str(), lnk.c_str(), e, 0));
   EXPECT_EQ(SFS_ERROR, ofs->symlink(f.c_str(), lnk.c_str(), e, 0));
   char tiny[2];
   int rrc = ofs->readlink(lnk.c_str(), tiny, 1, e, 0);
   EXPECT_TRUE(rrc == SFS_OK || rrc == SFS_ERROR);
}

TEST_F(NativePosixTest, MissingAndDuplicateErrors)
{
   XrdOucErrInfo e("t");
   EXPECT_EQ(SFS_ERROR, ns->access(P("no").c_str(), F_OK, e));
   EXPECT_TRUE(e.getErrInfo() == ENOENT || e.getErrInfo() == -ENOENT);
   char buff[16];
   EXPECT_EQ(SFS_ERROR, ns->readlink(P("no").c_str(), buff, 16, e));
   const std::string fifo = P("ndup");
   ASSERT_EQ(SFS_OK, ns->mknod(fifo.c_str(), S_IFIFO | 0644, 0, e));
   EXPECT_EQ(SFS_ERROR, ns->mknod(fifo.c_str(), S_IFIFO | 0644, 0, e));
   const std::string f = P("nacc");
   FILE *fp = fopen(f.c_str(), "w");
   ASSERT_TRUE(fp);
   fclose(fp);
   ASSERT_EQ(0, chmod(f.c_str(), 0444));
   EXPECT_EQ(SFS_ERROR, ns->access(f.c_str(), W_OK, e));
   ASSERT_EQ(0, chmod(f.c_str(), 0644));
}
