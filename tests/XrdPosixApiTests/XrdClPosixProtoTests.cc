#undef NDEBUG

#include "XProtocol/XProtocol.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClMessage.hh"
#include "XrdCl/XrdClMessageUtils.hh"
#include "XrdCl/XrdClStatus.hh"
#include "XrdCl/XrdClXRootDTransport.hh"
#include "XrdSys/XrdSysPlatform.hh"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <string>

using namespace XrdCl;

namespace {

Message MakePathReq(kXR_unt16 reqid, const std::string &path)
{
   Message msg((uint32_t)(sizeof(ClientRequest) + path.size() + 64));
   memset(msg.GetBuffer(), 0, msg.GetSize());
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   req->header.requestid = reqid;
   req->header.dlen = (kXR_int32)path.size();
   memcpy(msg.GetBuffer() + sizeof(ClientRequest), path.c_str(), path.size());
   return msg;
}

} // namespace

TEST(XrdClPosixProto, RequestCodesAndNames)
{
   EXPECT_EQ(3033, (int)kXR_link);
   EXPECT_EQ(3034, (int)kXR_chown);
   EXPECT_EQ(3035, (int)kXR_symlink);
   EXPECT_EQ(3036, (int)kXR_readlink);
   EXPECT_EQ(3037, (int)kXR_utimes);
   EXPECT_EQ(3038, (int)kXR_mknod);
   EXPECT_EQ(3039, (int)kXR_fcntlLock);
   EXPECT_EQ(kXR_fcntlLock + 1, kXR_REQFENCE);
   EXPECT_STREQ("link", XProtocol::reqName(kXR_link));
   EXPECT_STREQ("chown", XProtocol::reqName(kXR_chown));
   EXPECT_STREQ("symlink", XProtocol::reqName(kXR_symlink));
   EXPECT_STREQ("readlink", XProtocol::reqName(kXR_readlink));
   EXPECT_STREQ("utimes", XProtocol::reqName(kXR_utimes));
   EXPECT_STREQ("mknod", XProtocol::reqName(kXR_mknod));
   EXPECT_STREQ("fcntlLock", XProtocol::reqName(kXR_fcntlLock));
   EXPECT_STREQ("fattr", XProtocol::reqName(kXR_fattr));
   EXPECT_STREQ("!unknown", XProtocol::reqName(kXR_REQFENCE));
}

TEST(XrdClPosixProto, StructSizes)
{
   EXPECT_EQ(24u, sizeof(ClientRequest));
   EXPECT_EQ(24u, sizeof(ClientChownRequest));
   EXPECT_EQ(24u, sizeof(ClientSymlinkRequest));
   EXPECT_EQ(24u, sizeof(ClientReadlinkRequest));
   EXPECT_EQ(24u, sizeof(ClientUtimesRequest));
   EXPECT_EQ(24u, sizeof(ClientMknodRequest));
   EXPECT_EQ(24u, sizeof(ClientFcntlLockRequest));
   EXPECT_EQ(24u, sizeof(ClientLinkRequest));
   EXPECT_EQ(24u, sizeof(ClientRequestHdr));
}

TEST(XrdClPosixProto, MarshallUnmarshallChown)
{
   Message msg = MakePathReq(kXR_chown, "/data/file");
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   req->chown.uid = 0x01020304u;
   req->chown.gid = 0x05060708u;
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&msg).IsOK());
   EXPECT_TRUE(msg.IsMarshalled());
   EXPECT_EQ(htons(kXR_chown), req->header.requestid);
   EXPECT_EQ(htonl(0x01020304u), req->chown.uid);
   EXPECT_EQ(htonl(0x05060708u), req->chown.gid);
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&msg).IsOK());
   EXPECT_FALSE(msg.IsMarshalled());
   EXPECT_EQ(kXR_chown, req->header.requestid);
   EXPECT_EQ(0x01020304u, req->chown.uid);
   EXPECT_EQ(0x05060708u, req->chown.gid);
   EXPECT_EQ(10, req->header.dlen);
}

TEST(XrdClPosixProto, MarshallUnmarshallUtimes)
{
   Message msg = MakePathReq(kXR_utimes, "/t");
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   kXR_int64 at = 1700000000, mt = -1;
   memcpy(req->utimes.times, &at, 8);
   memcpy(req->utimes.times + 8, &mt, 8);
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&msg).IsOK());
   kXR_int64 at2, mt2;
   memcpy(&at2, req->utimes.times, 8);
   memcpy(&mt2, req->utimes.times + 8, 8);
   EXPECT_EQ(htonll(1700000000), at2);
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&msg).IsOK());
   memcpy(&at2, req->utimes.times, 8);
   memcpy(&mt2, req->utimes.times + 8, 8);
   EXPECT_EQ(1700000000, at2);
   EXPECT_EQ(-1, mt2);
}

TEST(XrdClPosixProto, MarshallUnmarshallSymlinkAndLink)
{
   const std::string tgt = "target";
   const std::string path = "/data/l";
   const std::string body = tgt + " " + path;
   Message msg((uint32_t)(sizeof(ClientRequest) + body.size() + 8));
   memset(msg.GetBuffer(), 0, msg.GetSize());
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   req->symlink.requestid = kXR_symlink;
   req->symlink.arg1len = (kXR_int16)tgt.size();
   req->symlink.dlen = (kXR_int32)body.size();
   memcpy(msg.GetBuffer() + sizeof(ClientRequest), body.c_str(), body.size());
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&msg).IsOK());
   EXPECT_EQ(htons((kXR_int16)tgt.size()), req->symlink.arg1len);
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&msg).IsOK());
   EXPECT_EQ((kXR_int16)tgt.size(), req->symlink.arg1len);
   EXPECT_EQ(kXR_symlink, req->symlink.requestid);

   Message lmsg = MakePathReq(kXR_link, body);
   ClientRequest *lr = (ClientRequest *)lmsg.GetBuffer();
   lr->link.arg1len = (kXR_int16)tgt.size();
   lr->link.dlen = (kXR_int32)body.size();
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&lmsg).IsOK());
   EXPECT_EQ(htons((kXR_int16)tgt.size()), lr->link.arg1len);
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&lmsg).IsOK());
   EXPECT_EQ((kXR_int16)tgt.size(), lr->link.arg1len);
}

TEST(XrdClPosixProto, MarshallReadlink)
{
   Message msg = MakePathReq(kXR_readlink, "/data/link");
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&msg).IsOK());
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   EXPECT_EQ(htons(kXR_readlink), req->header.requestid);
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&msg).IsOK());
   EXPECT_EQ(kXR_readlink, req->header.requestid);
}

TEST(XrdClPosixProto, UnmarshallNotMarshalled)
{
   Message msg = MakePathReq(kXR_chown, "/x");
   XRootDStatus st = XRootDTransport::UnMarshallRequest(&msg);
   EXPECT_TRUE(st.IsOK());
   EXPECT_EQ(suAlreadyDone, st.code);
}

TEST(XrdClPosixProto, Descriptions)
{
   Log *log = DefaultEnv::GetLog();
   const Log::LogLevel prev = log->GetLevel();
   log->SetLevel(Log::DumpMsg);
   {
      Message msg = MakePathReq(kXR_chown, "/data/file");
      ClientRequest *req = (ClientRequest *)msg.GetBuffer();
      req->chown.uid = 9;
      req->chown.gid = 10;
      XRootDTransport::SetDescription(&msg);
      EXPECT_NE(std::string::npos, msg.GetDescription().find("kXR_chown"));
      EXPECT_NE(std::string::npos, msg.GetDescription().find("/data/file"));
      EXPECT_NE(std::string::npos, msg.GetDescription().find("uid: 9"));
      EXPECT_NE(std::string::npos, msg.GetDescription().find("gid: 10"));
   }
   {
      const std::string body = std::string("tgt") + " " + "/p";
      Message msg((uint32_t)(sizeof(ClientRequest) + body.size() + 8));
      memset(msg.GetBuffer(), 0, msg.GetSize());
      ClientRequest *req = (ClientRequest *)msg.GetBuffer();
      req->symlink.requestid = kXR_symlink;
      req->symlink.arg1len = 3;
      req->symlink.dlen = (kXR_int32)body.size();
      memcpy(msg.GetBuffer() + sizeof(ClientRequest), body.c_str(), body.size());
      XRootDTransport::SetDescription(&msg);
      EXPECT_NE(std::string::npos, msg.GetDescription().find("kXR_symlink"));
      EXPECT_NE(std::string::npos, msg.GetDescription().find("tgt"));
   }
   {
      Message msg = MakePathReq(kXR_readlink, "/rl");
      XRootDTransport::SetDescription(&msg);
      EXPECT_NE(std::string::npos, msg.GetDescription().find("kXR_readlink"));
      EXPECT_NE(std::string::npos, msg.GetDescription().find("/rl"));
   }
   {
      Message msg = MakePathReq(kXR_utimes, "/ut");
      XRootDTransport::SetDescription(&msg);
      EXPECT_NE(std::string::npos, msg.GetDescription().find("kXR_utimes"));
   }
   {
      const std::string body = std::string("src") + " " + "/dst";
      Message msg((uint32_t)(sizeof(ClientRequest) + body.size() + 8));
      memset(msg.GetBuffer(), 0, msg.GetSize());
      ClientRequest *req = (ClientRequest *)msg.GetBuffer();
      req->link.requestid = kXR_link;
      req->link.arg1len = 3;
      req->link.dlen = (kXR_int32)body.size();
      memcpy(msg.GetBuffer() + sizeof(ClientRequest), body.c_str(), body.size());
      XRootDTransport::SetDescription(&msg);
      EXPECT_NE(std::string::npos, msg.GetDescription().find("kXR_link"));
   }
   log->SetLevel(prev);
}

TEST(XrdClPosixProto, RewriteCgiAndPath)
{
   Message msg = MakePathReq(kXR_chown, "/old");
   URL::ParamsMap cgi;
   cgi["foo"] = "bar";
   MessageUtils::RewriteCGIAndPath(&msg, cgi, true, "/new");
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   std::string path(msg.GetBuffer(24), (size_t)req->header.dlen);
   EXPECT_NE(std::string::npos, path.find("/new"));
   EXPECT_NE(std::string::npos, path.find("foo=bar"));

   Message rmsg = MakePathReq(kXR_readlink, "/rl");
   MessageUtils::RewriteCGIAndPath(&rmsg, cgi, true, "/rl2");
   ClientRequest *rr = (ClientRequest *)rmsg.GetBuffer();
   std::string rp(rmsg.GetBuffer(24), (size_t)rr->header.dlen);
   EXPECT_NE(std::string::npos, rp.find("/rl2"));

   Message umsg = MakePathReq(kXR_utimes, "/ut");
   MessageUtils::RewriteCGIAndPath(&umsg, cgi, true, "/ut2");
   ClientRequest *ur = (ClientRequest *)umsg.GetBuffer();
   std::string up(umsg.GetBuffer(24), (size_t)ur->header.dlen);
   EXPECT_NE(std::string::npos, up.find("/ut2"));

   const std::string body = std::string("target") + " " + "/oldp";
   Message smsg((uint32_t)(sizeof(ClientRequest) + body.size() + 64));
   memset(smsg.GetBuffer(), 0, smsg.GetSize());
   ClientRequest *sr = (ClientRequest *)smsg.GetBuffer();
   sr->symlink.requestid = kXR_symlink;
   sr->symlink.arg1len = 6;
   sr->symlink.dlen = (kXR_int32)body.size();
   memcpy(smsg.GetBuffer() + sizeof(ClientRequest), body.c_str(), body.size());
   MessageUtils::RewriteCGIAndPath(&smsg, cgi, true, "/newp");
   sr = (ClientRequest *)smsg.GetBuffer();
   std::string sb(smsg.GetBuffer(24), (size_t)sr->header.dlen);
   EXPECT_EQ(0, (int)sb.find("target "));
   EXPECT_NE(std::string::npos, sb.find("/newp"));

   const std::string lbody = std::string("src") + " " + "/oldl";
   Message lmsg((uint32_t)(sizeof(ClientRequest) + lbody.size() + 64));
   memset(lmsg.GetBuffer(), 0, lmsg.GetSize());
   ClientRequest *lr = (ClientRequest *)lmsg.GetBuffer();
   lr->link.requestid = kXR_link;
   lr->link.arg1len = 3;
   lr->link.dlen = (kXR_int32)lbody.size();
   memcpy(lmsg.GetBuffer() + sizeof(ClientRequest), lbody.c_str(), lbody.size());
   MessageUtils::RewriteCGIAndPath(&lmsg, cgi, true, "/newl");
   lr = (ClientRequest *)lmsg.GetBuffer();
   std::string lb(lmsg.GetBuffer(24), (size_t)lr->header.dlen);
   EXPECT_EQ(0, (int)lb.find("src "));
   EXPECT_NE(std::string::npos, lb.find("/newl"));
}

TEST(XrdClPosixProto, ChownMinusOne)
{
   Message msg = MakePathReq(kXR_chown, "/x");
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   req->chown.uid = (kXR_unt32)-1;
   req->chown.gid = (kXR_unt32)-1;
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&msg).IsOK());
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&msg).IsOK());
   EXPECT_EQ((kXR_unt32)-1, req->chown.uid);
   EXPECT_EQ((kXR_unt32)-1, req->chown.gid);
}

TEST(XrdClPosixProto, FattrCodeAndFence)
{
   EXPECT_EQ(3020, (int)kXR_fattr);
   EXPECT_STREQ("fattr", XProtocol::reqName(kXR_fattr));
   EXPECT_GT((int)kXR_chown, (int)kXR_link);
   EXPECT_GT((int)kXR_symlink, (int)kXR_chown);
   EXPECT_GT((int)kXR_readlink, (int)kXR_symlink);
   EXPECT_GT((int)kXR_utimes, (int)kXR_readlink);
   EXPECT_GT((int)kXR_mknod, (int)kXR_utimes);
   EXPECT_GT((int)kXR_fcntlLock, (int)kXR_mknod);
   EXPECT_EQ((int)kXR_fcntlLock + 1, (int)kXR_REQFENCE);
}

TEST(XrdClPosixProto, MarshallUtimesBothTimes)
{
   Message msg = MakePathReq(kXR_utimes, "/t");
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   kXR_int64 at = -1, mt = 42;
   memcpy(req->utimes.times, &at, 8);
   memcpy(req->utimes.times + 8, &mt, 8);
   ASSERT_TRUE(XRootDTransport::MarshallRequest(&msg).IsOK());
   ASSERT_TRUE(XRootDTransport::UnMarshallRequest(&msg).IsOK());
   kXR_int64 at2, mt2;
   memcpy(&at2, req->utimes.times, 8);
   memcpy(&mt2, req->utimes.times + 8, 8);
   EXPECT_EQ(-1, at2);
   EXPECT_EQ(42, mt2);
}

TEST(XrdClPosixProto, RewriteChownKeepsCgi)
{
   Message msg = MakePathReq(kXR_chown, "/old");
   URL::ParamsMap cgi;
   cgi["foo"] = "bar";
   cgi["z"] = "1";
   MessageUtils::RewriteCGIAndPath(&msg, cgi, true, "/new");
   ClientRequest *req = (ClientRequest *)msg.GetBuffer();
   std::string path(msg.GetBuffer(24), (size_t)req->header.dlen);
   EXPECT_NE(std::string::npos, path.find("/new"));
   EXPECT_NE(std::string::npos, path.find("foo=bar"));
   EXPECT_NE(std::string::npos, path.find("z=1"));
   EXPECT_EQ(kXR_chown, req->header.requestid);
}
