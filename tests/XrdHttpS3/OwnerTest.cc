#include "XrdHttpS3Client.hh"

#include <gtest/gtest.h>
#include <string>

using namespace XrdHttpS3;

TEST(FsOssName, StopsAtHostSuffixAndFitsThePasswdKey) {
  EXPECT_EQ(FsOssName("foo"), "foo");
  EXPECT_EQ(FsOssName("foo@cern.ch"), "foo");
  EXPECT_EQ(FsOssName("@host"), "");
  EXPECT_EQ(FsOssName("a@b@c"), "a");
  EXPECT_EQ(FsOssName(std::string(300, 'u')), std::string(255, 'u'));
}

TEST(OwnerClient, LeavesUidUnsetSoFsOssLooksUpTheName) {
  Client client("foo@cern.ch");
  ASSERT_NE(client.get(), nullptr);
  EXPECT_STREQ(client.get()->name, "foo");
  EXPECT_STREQ(client.get()->prot, "s3");
  EXPECT_EQ(client.get()->uid, 0u);
  EXPECT_EQ(client.get()->gid, 0u);
  EXPECT_EQ(Client("@only").get(), nullptr);
}

TEST(PosixId, ResolvesARealAccountAndRejectsAnUnknownOne) {
  uid_t uid = 99;
  gid_t gid = 99;
  ASSERT_EQ(PosixId("root", uid, gid), 0);
  EXPECT_EQ(uid, 0u);
  uid = 99;
  EXPECT_NE(PosixId("not-a-real-s3-user", uid, gid), 0);
  EXPECT_EQ(uid, 99u);
  ASSERT_EQ(PosixId("root@host", uid, gid), 0);
  EXPECT_EQ(uid, 0u);
}
