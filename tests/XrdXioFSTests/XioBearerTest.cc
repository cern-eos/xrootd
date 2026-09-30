#undef NDEBUG

#include "XioBearer.hh"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

using XioFS::loadBearerToken;

namespace {

bool writeMode(const std::string &path, const std::string &data, mode_t mode)
{
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd < 0)
    return false;
  ssize_t n = ::write(fd, data.data(), data.size());
  if (n < 0 || static_cast<size_t>(n) != data.size()) {
    close(fd);
    return false;
  }
  if (fchmod(fd, mode) < 0) {
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

bool backupFile(const std::string &src, std::string &dst)
{
  dst.clear();
  struct stat st;
  if (lstat(src.c_str(), &st) < 0)
    return false;
  dst = src + ".xiofs-test-bak";
  unlink(dst.c_str());
  if (rename(src.c_str(), dst.c_str()) < 0) {
    dst.clear();
    return false;
  }
  return true;
}

} // namespace

class XioBearerTest : public ::testing::Test {
protected:
  uid_t uid{};
  std::string runPath;
  std::string tmpPath;
  std::string runBak;
  std::string tmpBak;
  bool hadRun{false};
  bool hadTmp{false};

  void SetUp() override
  {
    uid = geteuid();
    runPath = "/run/user/" + std::to_string(uid) + "/bt_u" +
              std::to_string(uid);
    tmpPath = "/tmp/bt_u" + std::to_string(uid);
    hadRun = backupFile(runPath, runBak);
    hadTmp = backupFile(tmpPath, tmpBak);
    unlink(runPath.c_str());
    unlink(tmpPath.c_str());
  }

  void TearDown() override
  {
    unlink(runPath.c_str());
    unlink(tmpPath.c_str());
    if (hadRun)
      rename(runBak.c_str(), runPath.c_str());
    if (hadTmp)
      rename(tmpBak.c_str(), tmpPath.c_str());
  }

  bool writeTmp(const std::string &data, mode_t mode)
  {
    return writeMode(tmpPath, data, mode);
  }

  bool writeRun(const std::string &data, mode_t mode)
  {
    return writeMode(runPath, data, mode);
  }
};

TEST_F(XioBearerTest, MissingBothPaths)
{
  std::string tok, err;
  bool missing = false;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_TRUE(missing);
  EXPECT_EQ(ENOENT, errno);
  EXPECT_TRUE(tok.empty());
  EXPECT_FALSE(err.empty());
}

TEST_F(XioBearerTest, AcceptsOwnerOnly0600)
{
  ASSERT_TRUE(writeTmp("eyJhbGciOiJub25lIn0.payload.sig", 0600));
  std::string tok, err;
  bool missing = true;
  ASSERT_TRUE(loadBearerToken(uid, tok, err, &missing)) << err;
  EXPECT_FALSE(missing);
  EXPECT_EQ("eyJhbGciOiJub25lIn0.payload.sig", tok);
}

TEST_F(XioBearerTest, AcceptsOwnerOnly0400)
{
  ASSERT_TRUE(writeTmp("opaque-token", 0400));
  std::string tok, err;
  ASSERT_TRUE(loadBearerToken(uid, tok, err)) << err;
  EXPECT_EQ("opaque-token", tok);
}

TEST_F(XioBearerTest, RejectsWorldReadable)
{
  ASSERT_TRUE(writeTmp("secret", 0644));
  std::string tok, err;
  bool missing = true;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_FALSE(missing);
  EXPECT_EQ(EPERM, errno);
  EXPECT_NE(std::string::npos, err.find("group or world"));
}

TEST_F(XioBearerTest, RejectsGroupReadable)
{
  ASSERT_TRUE(writeTmp("secret", 0640));
  std::string tok, err;
  bool missing = true;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_FALSE(missing);
  EXPECT_EQ(EPERM, errno);
}

TEST_F(XioBearerTest, RejectsGroupWritable)
{
  ASSERT_TRUE(writeTmp("secret", 0620));
  std::string tok, err;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err));
  EXPECT_EQ(EPERM, errno);
}

TEST_F(XioBearerTest, RejectsWorldExecutable)
{
  ASSERT_TRUE(writeTmp("secret", 0701));
  std::string tok, err;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err));
  EXPECT_EQ(EPERM, errno);
}

TEST_F(XioBearerTest, StripsSurroundingWhitespace)
{
  ASSERT_TRUE(writeTmp("  tok-value \r\n", 0600));
  std::string tok, err;
  ASSERT_TRUE(loadBearerToken(uid, tok, err)) << err;
  EXPECT_EQ("tok-value", tok);
}

TEST_F(XioBearerTest, EmptyFileCountsAsMissingThenTmp)
{
  ASSERT_TRUE(writeTmp("\n\t  \r\n", 0600));
  std::string tok, err;
  bool missing = false;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_TRUE(missing);
  EXPECT_EQ(ENOENT, errno);
}

TEST_F(XioBearerTest, RejectsTooLarge)
{
  std::string big(XIOFS_BEARER_MAX, 'A');
  ASSERT_TRUE(writeTmp(big, 0600));
  std::string tok, err;
  bool missing = true;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_FALSE(missing);
  EXPECT_EQ(EMSGSIZE, errno);
}

TEST_F(XioBearerTest, AcceptsMaxMinusOne)
{
  std::string almost(XIOFS_BEARER_MAX - 1, 'B');
  ASSERT_TRUE(writeTmp(almost, 0600));
  std::string tok, err;
  ASSERT_TRUE(loadBearerToken(uid, tok, err)) << err;
  EXPECT_EQ(almost, tok);
}

TEST_F(XioBearerTest, RejectsSymlink)
{
  const std::string target = tmpPath + ".real";
  ASSERT_TRUE(writeMode(target, "via-symlink", 0600));
  ASSERT_EQ(0, symlink(target.c_str(), tmpPath.c_str()));
  std::string tok, err;
  bool missing = true;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_FALSE(missing);
  unlink(target.c_str());
}

TEST_F(XioBearerTest, RejectsDirectory)
{
  ASSERT_EQ(0, mkdir(tmpPath.c_str(), 0700));
  std::string tok, err;
  bool missing = true;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_FALSE(missing);
  EXPECT_EQ(EPERM, errno);
  rmdir(tmpPath.c_str());
}

TEST_F(XioBearerTest, InsecureRunUserDoesNotFallThroughToTmp)
{
  if (!writeRun("from-run", 0644))
    return;
  ASSERT_TRUE(writeTmp("from-tmp", 0600));
  std::string tok, err;
  bool missing = true;
  errno = 0;
  ASSERT_FALSE(loadBearerToken(uid, tok, err, &missing));
  EXPECT_FALSE(missing);
  EXPECT_EQ(EPERM, errno);
  EXPECT_NE("from-tmp", tok);
}

TEST_F(XioBearerTest, PrefersRunUserOverTmp)
{
  if (!writeRun("from-run", 0600))
    return;
  ASSERT_TRUE(writeTmp("from-tmp", 0600));
  std::string tok, err;
  ASSERT_TRUE(loadBearerToken(uid, tok, err)) << err;
  EXPECT_EQ("from-run", tok);
}

TEST_F(XioBearerTest, DoesNotParseJwt)
{
  const char *raw = "not.a.valid.jwt!!!";
  ASSERT_TRUE(writeTmp(raw, 0600));
  std::string tok, err;
  ASSERT_TRUE(loadBearerToken(uid, tok, err)) << err;
  EXPECT_EQ(raw, tok);
}
