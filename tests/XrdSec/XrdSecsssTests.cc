#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "XrdOuc/XrdOucPup.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSecsss/XrdSecsssEnt.hh"
#include "XrdSecsss/XrdSecsssKT.hh"
#include "XrdSecsss/XrdSecsssRR.hh"

namespace {

constexpr char kHostTag = static_cast<char>(0x20);

void FreeEntityFields(XrdSecEntity &entity)
{
  if (entity.name) free(entity.name);
  entity.name = nullptr;
}

std::string MakeTempDir()
{
  char tpl[] = "/tmp/xrdsecsss-test-XXXXXX";
  char *dir = mkdtemp(tpl);
  EXPECT_NE(dir, nullptr);
  return dir ? std::string(dir) : std::string();
}

void AddKey(XrdSecsssKT &kt, const char *name, const char *user, const char *group, time_t exp = 0)
{
  auto *key = new XrdSecsssKT::ktEnt();
  std::strncpy(key->Data.Name, name, sizeof(key->Data.Name) - 1);
  std::strncpy(key->Data.User, user, sizeof(key->Data.User) - 1);
  std::strncpy(key->Data.Grup, group, sizeof(key->Data.Grup) - 1);
  key->Data.Len = 16;
  key->Data.Exp = exp;
  kt.addKey(*key);
}

std::string ReadFileToString(const std::string &path)
{
  std::ifstream in(path);
  if (!in.is_open()) return {};
  std::ostringstream content;
  content << in.rdbuf();
  return content.str();
}

} // namespace

TEST(XrdSecsssEntTest, RRDataEncodesHostIpAndHostname)
{
  XrdSecsssEnt::setHostName("unit-host.example");

  XrdSecEntity entity("sss");
  entity.name = strdup("test-user");
  ASSERT_NE(entity.name, nullptr);

  XrdSecsssEnt *serialized = new XrdSecsssEnt(&entity);
  char *buffer = nullptr;
  const int len = serialized->RR_Data(buffer, "192.0.2.55", XrdSecsssEnt::v2Client);
  ASSERT_GT(len, XrdSecsssRR_Data_HdrLen);
  ASSERT_NE(buffer, nullptr);

  char *cursor = buffer + XrdSecsssRR_Data_HdrLen;
  const char *end = buffer + len;
  ASSERT_LT(cursor, end);

  ASSERT_EQ(*cursor, kHostTag);
  ++cursor;

  char *hostIp = nullptr;
  int hostIpLen = 0;
  ASSERT_TRUE(XrdOucPup::Unpack(&cursor, end, &hostIp, hostIpLen));
  EXPECT_STREQ(hostIp, "192.0.2.55");

  ASSERT_LT(cursor, end);
  EXPECT_EQ(*cursor, kHostTag);

  free(buffer);
  serialized->Delete();
  FreeEntityFields(entity);
}

TEST(XrdSecsssEntTest, RRDataReturnsZeroWithoutEntity)
{
  XrdSecsssEnt *serialized = new XrdSecsssEnt(nullptr);
  char *buffer = nullptr;
  const int len = serialized->RR_Data(buffer, nullptr, XrdSecsssEnt::v2Client);
  EXPECT_EQ(len, 0);
  EXPECT_EQ(buffer, nullptr);
  serialized->Delete();
}

TEST(XrdSecsssKTTest, RewriteReturnsEnoDataWhenNoUsableKeys)
{
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());

  const std::string ktPath = dir + "/sss.keytab";
  XrdOucErrInfo errInfo("xrdsec-unit");
  XrdSecsssKT kt(&errInfo, ktPath.c_str(), XrdSecsssKT::isAdmin, 60);

  int numKeys = -1, numTot = -1, numExp = -1;
  const int rc = kt.Rewrite(0, numKeys, numTot, numExp);
  EXPECT_EQ(rc, ENODATA);
  EXPECT_EQ(numKeys, 0);
  EXPECT_EQ(numTot, 0);
}

TEST(XrdSecsssKTTest, RewriteKeepLimitRestrictsPerIdentityEntries)
{
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());

  const std::string ktPath = dir + "/sss.keytab";
  XrdOucErrInfo errInfo("xrdsec-unit");
  XrdSecsssKT kt(&errInfo, ktPath.c_str(), XrdSecsssKT::isAdmin, 60);

  AddKey(kt, "dup", "alice", "group1");
  AddKey(kt, "dup", "alice", "group1");
  AddKey(kt, "dup", "alice", "group1");

  int numKeys = 0, numTot = 0, numExp = 0;
  const int rc = kt.Rewrite(1, numKeys, numTot, numExp);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(numTot, 3);
  EXPECT_EQ(numKeys, 1);
  EXPECT_EQ(numExp, 0);

  const std::string fileBody = ReadFileToString(ktPath);
  ASSERT_FALSE(fileBody.empty());

  int occurrences = 0;
  std::string::size_type pos = 0;
  while ((pos = fileBody.find(" n:dup ", pos)) != std::string::npos)
  {
    ++occurrences;
    ++pos;
  }
  EXPECT_EQ(occurrences, 1);
}
