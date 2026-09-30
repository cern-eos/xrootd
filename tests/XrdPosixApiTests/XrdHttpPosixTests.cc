#undef NDEBUG

#include "XrdHttp/XrdHttpReadRangeHandler.hh"
#include "XrdHttp/XrdHttpReq.hh"
#ifdef HAVE_NGHTTP2
#include "XrdHttp/wire/XrdHttp2Session.hh"
#endif

#include <gtest/gtest.h>

#include <clocale>
#include <cstring>
#include <string>
#include <vector>

namespace {

XrdHttpReadRangeHandler::Configuration Rcfg()
{
   return XrdHttpReadRangeHandler::Configuration();
}

XrdHttpReq MakeReq()
{
   return XrdHttpReq(0, Rcfg());
}

int ParseLine(XrdHttpReq &req, const std::string &line)
{
   std::vector<char> buf(line.begin(), line.end());
   buf.push_back(0);
   return req.parseFirstLine(buf.data(), (int)line.size());
}

int ParseHdr(XrdHttpReq &req, const std::string &line)
{
   std::vector<char> buf(line.begin(), line.end());
   buf.push_back(0);
   return req.parseLine(buf.data(), (int)line.size());
}

int ParsePatch(XrdHttpReq &req, const std::string &xml)
{
   std::vector<char> buf(xml.begin(), xml.end());
   buf.push_back(0);
   return req.parsePropPatch(buf.data(), (long long)xml.size());
}

} // namespace

TEST(XrdHttpPosix, ParseSymlinkReadlinkLinkBind)
{
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "SYMLINK /data/link HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtSYMLINK, req.request);
      EXPECT_EQ("SYMLINK", req.requestverb);
      EXPECT_STREQ("/data/link", req.resource.c_str());
      EXPECT_TRUE(req.keepalive);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "READLINK /data/link HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtREADLINK, req.request);
      EXPECT_EQ("READLINK", req.requestverb);
      EXPECT_STREQ("/data/link", req.resource.c_str());
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "LINK /data/a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtLINK, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "BIND /data/a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtLINK, req.request);
      EXPECT_EQ("BIND", req.requestverb);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "PROPPATCH /data/a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtPROPPATCH, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "SYMLINK /data/link HTTP/1.0\r\n"));
      EXPECT_EQ(XrdHttpReq::rtSYMLINK, req.request);
      EXPECT_FALSE(req.keepalive);
   }
}

TEST(XrdHttpPosix, ParseUnknownAndMalformed)
{
   XrdHttpReq req = MakeReq();
   ASSERT_EQ(0, ParseLine(req, "FOO /x HTTP/1.1\r\n"));
   EXPECT_EQ(XrdHttpReq::rtUnknown, req.request);
   EXPECT_EQ(-4, ParseLine(req, " /x HTTP/1.1\r\n"));
   EXPECT_EQ(XrdHttpReq::rtMalformed, req.request);
}

TEST(XrdHttpPosix, ParseDoubleSlashResource)
{
   XrdHttpReq req = MakeReq();
   ASSERT_EQ(0, ParseLine(req, "READLINK //data//link HTTP/1.1\r\n"));
   EXPECT_STREQ("/data/link", req.resource.c_str());
}

TEST(XrdHttpPosix, ParseDestinationHeader)
{
   XrdHttpReq req = MakeReq();
   ASSERT_EQ(0, ParseHdr(req, "Destination: /tgt/path\r\n"));
   EXPECT_EQ("/tgt/path", req.destination);
   EXPECT_EQ("/tgt/path", req.allheaders["destination"]);
}

TEST(XrdHttpPosix, PropPatchUidGidTimes)
{
   XrdHttpReq req = MakeReq();
   const char *xml =
      "<propertyupdate><set><prop>"
      "<uid>1000</uid><gid>100</gid>"
      "<atime>1700000000</atime><mtime>1700000001</mtime>"
      "</prop></set></propertyupdate>";
   ASSERT_EQ(0, ParsePatch(req, xml));
   EXPECT_TRUE(req.proppatchHaveUid);
   EXPECT_TRUE(req.proppatchHaveGid);
   EXPECT_EQ((uid_t)1000, req.proppatchUid);
   EXPECT_EQ((gid_t)100, req.proppatchGid);
   EXPECT_TRUE(req.proppatchHaveAtime);
   EXPECT_TRUE(req.proppatchHaveMtime);
   EXPECT_EQ(1700000000, req.proppatchAtime);
   EXPECT_EQ(1700000001, req.proppatchMtime);
   ASSERT_EQ(4u, req.proppatchItems.size());
   EXPECT_EQ(200, req.proppatchItems[0].status);
   EXPECT_EQ(200, req.proppatchItems[1].status);
   EXPECT_EQ(200, req.proppatchItems[2].status);
   EXPECT_EQ(200, req.proppatchItems[3].status);
}

TEST(XrdHttpPosix, PropPatchAliasesAndRfc1123)
{
   setlocale(LC_TIME, "C");
   XrdHttpReq req = MakeReq();
   const char *xml =
      "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"urn:x\">"
      "<D:set><D:prop>"
      "<X:owner-uid>7</X:owner-uid>"
      "<X:owner-gid>8</X:owner-gid>"
      "<D:getlastaccessed>1700000099</D:getlastaccessed>"
      "<D:getlastmodified>Wed, 01 Jan 2020 00:00:00 GMT</D:getlastmodified>"
      "</D:prop></D:set></D:propertyupdate>";
   ASSERT_EQ(0, ParsePatch(req, xml));
   EXPECT_TRUE(req.proppatchHaveUid);
   EXPECT_EQ((uid_t)7, req.proppatchUid);
   EXPECT_TRUE(req.proppatchHaveGid);
   EXPECT_EQ((gid_t)8, req.proppatchGid);
   EXPECT_TRUE(req.proppatchHaveAtime);
   EXPECT_EQ(1700000099, req.proppatchAtime);
   EXPECT_TRUE(req.proppatchHaveMtime);
   EXPECT_EQ(1577836800, req.proppatchMtime);
}

TEST(XrdHttpPosix, PropPatchInvalidAndRemove)
{
   {
      XrdHttpReq req = MakeReq();
      const char *xml = "<propertyupdate><set><prop><uid>abc</uid></prop></set></propertyupdate>";
      ASSERT_EQ(0, ParsePatch(req, xml));
      ASSERT_EQ(1u, req.proppatchItems.size());
      EXPECT_EQ(400, req.proppatchItems[0].status);
      EXPECT_FALSE(req.proppatchHaveUid);
   }
   {
      XrdHttpReq req = MakeReq();
      const char *xml = "<propertyupdate><remove><prop><uid/></prop></remove></propertyupdate>";
      ASSERT_EQ(0, ParsePatch(req, xml));
      ASSERT_EQ(1u, req.proppatchItems.size());
      EXPECT_EQ(403, req.proppatchItems[0].status);
   }
   {
      XrdHttpReq req = MakeReq();
      EXPECT_EQ(0, req.parsePropPatch(0, 0));
      EXPECT_FALSE(req.proppatchHaveUid);
   }
}

TEST(XrdHttpPosix, PropPatchUnixModeStillWorks)
{
   XrdHttpReq req = MakeReq();
   const char *xml = "<propertyupdate><set><prop><unix-mode>0755</unix-mode></prop></set></propertyupdate>";
   ASSERT_EQ(0, ParsePatch(req, xml));
   EXPECT_EQ(0755, req.proppatchUnixMode);
}

TEST(XrdHttpPosix, VerbEnumOrder)
{
   EXPECT_LT(XrdHttpReq::rtSYMLINK, XrdHttpReq::rtCount);
   EXPECT_LT(XrdHttpReq::rtREADLINK, XrdHttpReq::rtCount);
   EXPECT_EQ(XrdHttpReq::rtLINK + 1, XrdHttpReq::rtSYMLINK);
   EXPECT_EQ(XrdHttpReq::rtSYMLINK + 1, XrdHttpReq::rtREADLINK);
   EXPECT_EQ(XrdHttpReq::rtREADLINK + 1, XrdHttpReq::rtMKNOD);
   EXPECT_EQ(XrdHttpReq::rtMKNOD + 1, XrdHttpReq::rtLOCK);
   EXPECT_EQ(XrdHttpReq::rtLOCK + 1, XrdHttpReq::rtUNLOCK);
   EXPECT_EQ(XrdHttpReq::rtUNLOCK + 1, XrdHttpReq::rtFATTR);
}

#ifdef HAVE_NGHTTP2
TEST(XrdHttpPosix, Http2BodyMethods)
{
   EXPECT_TRUE(XrdHttp2Session::isBodyMethod("PUT"));
   EXPECT_TRUE(XrdHttp2Session::isBodyMethod("POST"));
   EXPECT_TRUE(XrdHttp2Session::isBodyMethod("PATCH"));
   EXPECT_TRUE(XrdHttp2Session::isBodyMethod("PROPPATCH"));
   EXPECT_TRUE(XrdHttp2Session::isBodyMethod("PROPFIND"));
   EXPECT_TRUE(XrdHttp2Session::isBodyMethod("SYMLINK"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("GET"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("HEAD"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("DELETE"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("OPTIONS"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("LINK"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("READLINK"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("MKCOL"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("MOVE"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("COPY"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("BIND"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("MKNOD"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("LOCK"));
   EXPECT_FALSE(XrdHttp2Session::isBodyMethod("UNLOCK"));
}
#endif

TEST(XrdHttpPosix, ParseMoreVerbs)
{
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "OPTIONS * HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtOPTIONS, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "HEAD /x HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtHEAD, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "MOVE /a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtMOVE, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "COPY /a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtCOPY, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "PROPFIND /a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtPROPFIND, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "GET /a HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtGET, req.request);
   }
}

TEST(XrdHttpPosix, PropPatchGidInvalidAndUidZero)
{
   {
      XrdHttpReq req = MakeReq();
      const char *xml = "<propertyupdate><set><prop><gid>nope</gid></prop></set></propertyupdate>";
      ASSERT_EQ(0, ParsePatch(req, xml));
      ASSERT_EQ(1u, req.proppatchItems.size());
      EXPECT_EQ(400, req.proppatchItems[0].status);
      EXPECT_FALSE(req.proppatchHaveGid);
   }
   {
      XrdHttpReq req = MakeReq();
      const char *xml = "<propertyupdate><set><prop><uid>0</uid><mtime>1</mtime></prop></set></propertyupdate>";
      ASSERT_EQ(0, ParsePatch(req, xml));
      EXPECT_TRUE(req.proppatchHaveUid);
      EXPECT_EQ((uid_t)0, req.proppatchUid);
      EXPECT_TRUE(req.proppatchHaveMtime);
      EXPECT_EQ(1, req.proppatchMtime);
      EXPECT_FALSE(req.proppatchHaveAtime);
   }
}

TEST(XrdHttpPosix, ParseMknodLockUnlock)
{
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "MKNOD /data/pipe HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtMKNOD, req.request);
      EXPECT_STREQ("/data/pipe", req.resource.c_str());
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "LOCK /data/f HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtLOCK, req.request);
   }
   {
      XrdHttpReq req = MakeReq();
      ASSERT_EQ(0, ParseLine(req, "UNLOCK /data/f HTTP/1.1\r\n"));
      EXPECT_EQ(XrdHttpReq::rtUNLOCK, req.request);
   }
}

TEST(XrdHttpPosix, PropPatchXattr)
{
   XrdHttpReq req = MakeReq();
   const char *xml =
      "<propertyupdate><set><prop>"
      "<xattr-name>user.foo</xattr-name>"
      "<xattr-value>bar</xattr-value>"
      "</prop></set></propertyupdate>";
   ASSERT_EQ(0, ParsePatch(req, xml));
   EXPECT_EQ("user.foo", req.proppatchXattrName);
   EXPECT_EQ("bar", req.proppatchXattrValue);
   {
      XrdHttpReq del = MakeReq();
      const char *dxml =
         "<propertyupdate><set><prop><xattr-del>user.foo</xattr-del></prop></set></propertyupdate>";
      ASSERT_EQ(0, ParsePatch(del, dxml));
      EXPECT_EQ("user.foo", del.proppatchXattrDel);
   }
}

TEST(XrdHttpPosix, ParseReadlinkKeepaliveAndLinkHttp10)
{
   XrdHttpReq req = MakeReq();
   ASSERT_EQ(0, ParseLine(req, "READLINK /l HTTP/1.0\r\n"));
   EXPECT_EQ(XrdHttpReq::rtREADLINK, req.request);
   EXPECT_FALSE(req.keepalive);
   XrdHttpReq req2 = MakeReq();
   ASSERT_EQ(0, ParseLine(req2, "LINK /l HTTP/1.0\r\n"));
   EXPECT_EQ(XrdHttpReq::rtLINK, req2.request);
   EXPECT_FALSE(req2.keepalive);
}
