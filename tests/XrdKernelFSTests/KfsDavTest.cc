#undef NDEBUG

#include "KfsDav.hh"
#include "KfsUrl.hh"

#include <gtest/gtest.h>

using namespace Kfs;

TEST(KfsUrl, HttpsWithPortAndPath)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://localhost:7097/h2-alphabet.txt", u, err)) << err;
  EXPECT_EQ("https", u.scheme);
  EXPECT_EQ("localhost", u.host);
  EXPECT_EQ(7097, u.port);
  EXPECT_EQ("/h2-alphabet.txt", u.path);
  EXPECT_TRUE(u.tls);
  EXPECT_EQ("localhost:7097", u.authority);
}

TEST(KfsUrl, DefaultHttpsPortOmittedFromAuthority)
{
  Url u;
  std::string err;
  ASSERT_TRUE(parseUrl("https://storage.example/export", u, err)) << err;
  EXPECT_EQ(443, u.port);
  EXPECT_EQ("storage.example", u.authority);
  EXPECT_EQ("/export", u.path);
}

TEST(KfsUrl, JoinPath)
{
  EXPECT_EQ("/export/file", joinPath("/export", "/file"));
  EXPECT_EQ("/file", joinPath("/", "/file"));
  EXPECT_EQ("/export", joinPath("/export", "/"));
}

TEST(KfsDav, ParseXrdHttpMultistatus)
{
  const char *xml =
      "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
      "<D:multistatus xmlns:D=\"DAV:\" xmlns:ns1=\"http://apache.org/dav/props/\">\n"
      "<D:response xmlns:lp1=\"DAV:\">\n"
      "<D:href>/h2-list/</D:href>\n"
      "<D:propstat>\n<D:prop>\n"
      "<lp1:getcontentlength>0</lp1:getcontentlength>\n"
      "<lp1:getlastmodified>Tue, 01 May 2012 02:42:13 GMT</lp1:getlastmodified>\n"
      "<lp1:resourcetype><D:collection/></lp1:resourcetype>\n"
      "<lp1:iscollection>1</lp1:iscollection>\n"
      "</D:prop>\n<D:status>HTTP/1.1 200 OK</D:status>\n</D:propstat>\n"
      "</D:response>\n"
      "<D:response xmlns:lp1=\"DAV:\">\n"
      "<D:href>/h2-list/testlistings</D:href>\n"
      "<D:propstat>\n<D:prop>\n"
      "<lp1:getcontentlength>26</lp1:getcontentlength>\n"
      "<lp1:getlastmodified>Tue, 01 May 2012 02:42:13 GMT</lp1:getlastmodified>\n"
      "<lp1:resourcetype/>\n"
      "<lp1:iscollection>0</lp1:iscollection>\n"
      "</D:prop>\n<D:status>HTTP/1.1 200 OK</D:status>\n</D:propstat>\n"
      "</D:response>\n"
      "</D:multistatus>\n";

  std::vector<DavEntry> ents;
  std::string err;
  ASSERT_TRUE(parseMultistatus(xml, ents, err)) << err;
  ASSERT_EQ(2u, ents.size());
  EXPECT_TRUE(ents[0].is_dir);
  EXPECT_EQ("h2-list", ents[0].name);
  EXPECT_FALSE(ents[1].is_dir);
  EXPECT_EQ("testlistings", ents[1].name);
  EXPECT_EQ(26, ents[1].size);
  EXPECT_GT(ents[1].mtime, 0);
}

TEST(KfsDav, HrefBasename)
{
  EXPECT_EQ("file.txt", hrefBasename("/export/file.txt"));
  EXPECT_EQ("dir", hrefBasename("/export/dir/"));
}

TEST(KfsDav, HttpDate)
{
  time_t t = 0;
  ASSERT_TRUE(parseHttpDate("Tue, 01 May 2012 02:42:13 GMT", t));
  EXPECT_EQ(1335840133, static_cast<long>(t));
}
