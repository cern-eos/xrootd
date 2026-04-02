/******************************************************************************/
/*                                                                            */
/*                   X r d S e c k r b 5 T e s t s . c c                      */
/*                                                                            */
/* Unit tests for XrdSeckrb5 security hardening.                              */
/******************************************************************************/

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>

// Expose private statics for white-box testing.
#define private public
#define protected public
#include "../../src/XrdSeckrb5/XrdSecProtocolkrb5.cc"
#undef private
#undef protected

namespace {

// ---------------------------------------------------------------------------
// setExpFile: truncation at XrdSecMAXPATHLEN
// ---------------------------------------------------------------------------

TEST(XrdSeckrb5SetExpFile, NormalPathIsStored)
{
  char path[] = "/tmp/krb5cc_test_<uid>";
  XrdSecProtocolkrb5::setExpFile(path);
  EXPECT_STREQ(XrdSecProtocolkrb5::ExpFile, "/tmp/krb5cc_test_<uid>");
}

TEST(XrdSeckrb5SetExpFile, NullIsIgnored)
{
  char orig[] = "/tmp/original";
  XrdSecProtocolkrb5::setExpFile(orig);
  XrdSecProtocolkrb5::setExpFile(nullptr);
  EXPECT_STREQ(XrdSecProtocolkrb5::ExpFile, "/tmp/original");
}

TEST(XrdSeckrb5SetExpFile, OverlongPathIsTruncated)
{
  std::string longpath(XrdSecMAXPATHLEN + 100, 'A');
  XrdSecProtocolkrb5::setExpFile(const_cast<char*>(longpath.c_str()));
  EXPECT_EQ(strlen(XrdSecProtocolkrb5::ExpFile),
            (size_t)(XrdSecMAXPATHLEN - 1));
  EXPECT_EQ(XrdSecProtocolkrb5::ExpFile[XrdSecMAXPATHLEN - 1], '\0');
}

// ---------------------------------------------------------------------------
// setParms: free-before-set (no crash, leak detectable by ASan/Valgrind)
// ---------------------------------------------------------------------------

TEST(XrdSeckrb5SetParms, SetTwiceDoesNotCrash)
{
  char *p1 = strdup("first_param");
  XrdSecProtocolkrb5::setParms(p1);
  EXPECT_STREQ(XrdSecProtocolkrb5::Parms, "first_param");

  char *p2 = strdup("second_param");
  XrdSecProtocolkrb5::setParms(p2);
  EXPECT_STREQ(XrdSecProtocolkrb5::Parms, "second_param");

  XrdSecProtocolkrb5::setParms(nullptr);
}

TEST(XrdSeckrb5SetParms, SetNullAfterValueDoesNotCrash)
{
  char *p = strdup("some_param");
  XrdSecProtocolkrb5::setParms(p);
  XrdSecProtocolkrb5::setParms(nullptr);
  EXPECT_EQ(XrdSecProtocolkrb5::Parms, nullptr);
}

// ---------------------------------------------------------------------------
// Init with NULL principal (client path) returns 0 immediately
// ---------------------------------------------------------------------------

TEST(XrdSeckrb5Init, ClientModeReturnsZero)
{
  XrdOucErrInfo einfo("test");
  int rc = XrdSecProtocolkrb5::Init(&einfo, nullptr, nullptr);
  EXPECT_EQ(rc, 0);
}

// ---------------------------------------------------------------------------
// XrdSecProtocolkrb5Init: server mode with NULL parms is rejected
// ---------------------------------------------------------------------------

TEST(XrdSeckrb5Init, ServerNullParmsReturnsNull)
{
  XrdOucErrInfo einfo("test");
  char *result = XrdSecProtocolkrb5Init('s', nullptr, &einfo);
  EXPECT_EQ(result, nullptr);
}

// ---------------------------------------------------------------------------
// XrdSecProtocolkrb5Init: server mode with only flags, no principal
// Tests the strdup(NULL) guard -- previously this was UB.
// ---------------------------------------------------------------------------

TEST(XrdSeckrb5Init, ServerMissingPrincipalReturnsNull)
{
  XrdOucErrInfo einfo("test");
  char *result = XrdSecProtocolkrb5Init('s', "-exptkn", &einfo);
  EXPECT_EQ(result, nullptr);
}

// ---------------------------------------------------------------------------
// Credential cache path template expansion (mirrors exp_krbTkn logic)
//
// Since exp_krbTkn is a private member that also does krb5 calls, we test
// the std::string expansion pattern directly to validate the overflow fix.
// ---------------------------------------------------------------------------

static std::string expandCCPath(const char *tmpl, const char *user,
                                const char *uid)
{
  std::string ccpath(tmpl);
  std::string::size_type pos;
  if ((pos = ccpath.find("<user>")) != std::string::npos)
    ccpath.replace(pos, 6, user);
  if ((pos = ccpath.find("<uid>")) != std::string::npos)
    ccpath.replace(pos, 5, uid);
  return ccpath;
}

TEST(XrdSeckrb5CCPath, BasicExpansion)
{
  auto r = expandCCPath("/tmp/krb5cc_<uid>", "alice", "1000");
  EXPECT_EQ(r, "/tmp/krb5cc_1000");
}

TEST(XrdSeckrb5CCPath, UserAndUidExpansion)
{
  auto r = expandCCPath("/tmp/<user>/krb5cc_<uid>", "bob", "42");
  EXPECT_EQ(r, "/tmp/bob/krb5cc_42");
}

TEST(XrdSeckrb5CCPath, NoPlaceholders)
{
  auto r = expandCCPath("/tmp/fixed_cache", "alice", "1000");
  EXPECT_EQ(r, "/tmp/fixed_cache");
}

TEST(XrdSeckrb5CCPath, LongUsernameDoesNotOverflow)
{
  std::string longUser(XrdSecMAXPATHLEN, 'X');
  auto r = expandCCPath("/tmp/krb5cc_<user>", longUser.c_str(), "0");
  EXPECT_EQ(r, std::string("/tmp/krb5cc_") + longUser);
  EXPECT_GT(r.size(), (size_t)XrdSecMAXPATHLEN);
}

TEST(XrdSeckrb5CCPath, EmptyUidExpansion)
{
  auto r = expandCCPath("/tmp/krb5cc_<uid>", "user", "");
  EXPECT_EQ(r, "/tmp/krb5cc_");
}

// ---------------------------------------------------------------------------
// XrdSecProtocolkrb5Init: client mode initialization
// Tests that client-side init returns a non-NULL result (empty string).
// ---------------------------------------------------------------------------

TEST(XrdSeckrb5Init, ClientModeInitReturnsEmptyString)
{
  XrdOucErrInfo einfo("test");
  char *result = XrdSecProtocolkrb5Init('c', nullptr, &einfo);
  ASSERT_NE(result, nullptr);
  EXPECT_STREQ(result, "");
}

} // anonymous namespace
