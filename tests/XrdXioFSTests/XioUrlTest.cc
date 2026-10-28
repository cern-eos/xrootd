#undef NDEBUG

#include "XioUrl.hh"

#include <gtest/gtest.h>

using XioFS::parseUrl;
using XioFS::Url;

TEST(XioUrl, SingleHost)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://storage.example:1094/export", u, err)) << err;
  EXPECT_EQ(u.host, "storage.example");
  EXPECT_EQ(u.port, 1094);
  EXPECT_EQ(u.path, "/export");
  ASSERT_EQ(u.hops.size(), 1u);
  EXPECT_FALSE(u.hops[0].alias);
}

TEST(XioUrl, ServerList)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://s1.example:1094,s2.example:1095/data", u, err))
      << err;
  ASSERT_EQ(u.hops.size(), 2u);
  EXPECT_EQ(u.hops[0].host, "s1.example");
  EXPECT_EQ(u.hops[0].port, 1094);
  EXPECT_FALSE(u.hops[0].alias);
  EXPECT_EQ(u.hops[1].host, "s2.example");
  EXPECT_EQ(u.hops[1].port, 1095);
  EXPECT_EQ(u.path, "/data");
}

TEST(XioUrl, ManagerAlias)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://eosatlas.cern.ch,1094/eos", u, err)) << err;
  ASSERT_EQ(u.hops.size(), 1u);
  EXPECT_EQ(u.hops[0].host, "eosatlas.cern.ch");
  EXPECT_EQ(u.hops[0].port, 1094);
  EXPECT_TRUE(u.hops[0].alias);
  EXPECT_EQ(u.host, "eosatlas.cern.ch");
  EXPECT_EQ(u.port, 1094);
}

TEST(XioUrl, TwoAliases)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://mgr1.example,1094,mgr2.example,1095/", u, err))
      << err;
  ASSERT_EQ(u.hops.size(), 2u);
  EXPECT_TRUE(u.hops[0].alias);
  EXPECT_TRUE(u.hops[1].alias);
  EXPECT_EQ(u.hops[1].host, "mgr2.example");
  EXPECT_EQ(u.hops[1].port, 1095);
}

TEST(XioUrl, ColonIsServerCommaIsAlias)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://mgr.example:1094/eos", u, err)) << err;
  ASSERT_EQ(u.hops.size(), 1u);
  EXPECT_FALSE(u.hops[0].alias);
  ASSERT_TRUE(parseUrl("https://mgr.example,1094/eos", u, err)) << err;
  ASSERT_EQ(u.hops.size(), 1u);
  EXPECT_TRUE(u.hops[0].alias);
}

TEST(XioUrl, Ipv6ServerList)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl(
      "https://[2001:db8::1]:1094,[2001:db8::2]:1095/export", u, err))
      << err;
  ASSERT_EQ(u.hops.size(), 2u);
  EXPECT_EQ(u.hops[0].host, "2001:db8::1");
  EXPECT_EQ(u.hops[0].port, 1094);
  EXPECT_FALSE(u.hops[0].alias);
  EXPECT_EQ(u.hops[1].host, "2001:db8::2");
  EXPECT_EQ(u.hops[1].port, 1095);
}

TEST(XioUrl, FormatUrlRoundTrip)
{
  const char *in[] = {
      "https://s1.example:1094,s2.example:1095/data",
      "https://eosatlas.cern.ch,1094/eos",
      "https://[2001:db8::1]:1094,[2001:db8::2]:1095/export",
      "http://mgr1.example,1094,mgr2.example,1095/",
      "spt://storage.example:8443/data",
  };
  for (const char *s : in) {
    Url u;
    std::string err;
    ASSERT_TRUE(parseUrl(s, u, err)) << err << " in=" << s;
    std::string out = XioFS::formatUrl(u);
    Url u2;
    ASSERT_TRUE(parseUrl(out, u2, err)) << err << " out=" << out;
    ASSERT_EQ(u2.hops.size(), u.hops.size()) << out;
    for (size_t i = 0; i < u.hops.size(); ++i) {
      EXPECT_EQ(u2.hops[i].host, u.hops[i].host);
      EXPECT_EQ(u2.hops[i].port, u.hops[i].port);
      EXPECT_EQ(u2.hops[i].alias, u.hops[i].alias);
    }
  }
}

TEST(XioUrl, ParsockScheme)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("spt://storage.example/data", u, err)) << err;
  EXPECT_EQ(u.scheme, "spt");
  EXPECT_TRUE(u.parsock);
  EXPECT_TRUE(u.tls);
  EXPECT_EQ(u.host, "storage.example");
  EXPECT_EQ(u.port, 8443);
  EXPECT_EQ(u.path, "/data");

  ASSERT_TRUE(parseUrl("parsock://[2001:db8::1]:9443/export", u, err)) << err;
  EXPECT_EQ(u.scheme, "parsock");
  EXPECT_TRUE(u.parsock);
  EXPECT_TRUE(u.tls);
  EXPECT_EQ(u.host, "2001:db8::1");
  EXPECT_EQ(u.port, 9443);
  EXPECT_EQ(u.path, "/export");
}
