#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "XrdNet/XrdNetAddr.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecInterface.hh"

extern "C" {
char *XrdSecProtocolztnInit(const char mode, const char *parms, XrdOucErrInfo *erp);
XrdSecProtocol *XrdSecProtocolztnObject(const char mode,
                                        const char *hostname,
                                        XrdNetAddrInfo &endPoint,
                                        const char *parms,
                                        XrdOucErrInfo *erp);
}

namespace {

std::string MakeTempDir()
{
  char tpl[] = "/tmp/xrdsecztn-test-XXXXXX";
  char *dir = mkdtemp(tpl);
  EXPECT_NE(dir, nullptr);
  return dir ? std::string(dir) : std::string();
}

void WriteFile(const std::string &path, const std::string &content, mode_t mode)
{
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(out.good());
  out << content;
  out.close();
  ASSERT_EQ(chmod(path.c_str(), mode), 0);
}

void ClearTokenEnv()
{
  unsetenv("BEARER_TOKEN");
  unsetenv("BEARER_TOKEN_FILE");
  unsetenv("XDG_RUNTIME_DIR");
}

} // namespace

TEST(XrdSecztnInitTest, ParsesExpiryAndMaxSizeOptions)
{
  XrdOucErrInfo err("ztn-test");
  char *cfg = XrdSecProtocolztnInit('s', "-tokenlib none -expiry ignore -maxsz 32k", &err);
  ASSERT_NE(cfg, nullptr);
  std::string cfgStr(cfg);
  free(cfg);

  EXPECT_NE(cfgStr.find("TLS:"), std::string::npos);
  EXPECT_NE(cfgStr.find(":32768:"), std::string::npos);
}

TEST(XrdSecztnInitTest, RejectsInvalidExpiryValue)
{
  XrdOucErrInfo err("ztn-test");
  char *cfg = XrdSecProtocolztnInit('s', "-tokenlib none -expiry bogus", &err);
  EXPECT_EQ(cfg, nullptr);
  EXPECT_EQ(err.getErrInfo(), EINVAL);
}

TEST(XrdSecztnAuthTest, RejectsInsecureTokenFilePermissions)
{
  ClearTokenEnv();
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());
  const std::string tokenPath = dir + "/token.jwt";
  WriteFile(tokenPath, "header.payload.sig\n", 0644); // group/other readable: should fail
  ASSERT_EQ(setenv("BEARER_TOKEN_FILE", tokenPath.c_str(), 1), 0);

  XrdNetAddr ep(1094);
  ep.SetTLS(true);
  XrdOucErrInfo err("ztn-test");

  XrdSecProtocol *prot = XrdSecProtocolztnObject('c', "localhost", ep, "0:4096:", &err);
  ASSERT_NE(prot, nullptr);

  XrdSecCredentials *cred = prot->getCredentials(nullptr, &err);
  EXPECT_EQ(cred, nullptr);
  EXPECT_EQ(err.getErrInfo(), EPERM);

  prot->Delete();
  ClearTokenEnv();
}

TEST(XrdSecztnAuthTest, AcceptsSecureTokenFilePermissions)
{
  ClearTokenEnv();
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());
  const std::string tokenPath = dir + "/token.jwt";
  WriteFile(tokenPath, "header.payload.sig\n", 0600);
  ASSERT_EQ(setenv("BEARER_TOKEN_FILE", tokenPath.c_str(), 1), 0);

  XrdNetAddr ep(1094);
  ep.SetTLS(true);
  XrdOucErrInfo err("ztn-test");

  XrdSecProtocol *prot = XrdSecProtocolztnObject('c', "localhost", ep, "0:4096:", &err);
  ASSERT_NE(prot, nullptr);

  XrdSecCredentials *cred = prot->getCredentials(nullptr, &err);
  ASSERT_NE(cred, nullptr);
  ASSERT_NE(cred->buffer, nullptr);
  EXPECT_GT(cred->size, 0);

  delete cred;
  prot->Delete();
  ClearTokenEnv();
}
