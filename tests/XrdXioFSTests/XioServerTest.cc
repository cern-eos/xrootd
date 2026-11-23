#include "XioClient.hh"
#include "XioPosixFs.hh"
#include "XioServer.hh"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>
#include <sys/stat.h>

namespace {

std::string makeRoot()
{
  char tmpl[] = "/tmp/xio-server-XXXXXX";
  char *dir = mkdtemp(tmpl);
  EXPECT_NE(dir, nullptr);
  return dir ? dir : "";
}

class ServerFix : public ::testing::Test
{
protected:
  void SetUp() override
  {
    root_ = makeRoot();
    ASSERT_FALSE(root_.empty());
    XioFS::Server::Options opt;
    opt.bind_host = "127.0.0.1";
    opt.port = 0;
    opt.workers = 4;
    server_.reset(new XioFS::Server(std::make_shared<XioFS::PosixFs>(root_),
                                    opt));
    std::string err;
    ASSERT_EQ(server_->start(err), 0) << err;
    ASSERT_GT(server_->port(), 0);
    XioFS::Http2Session::Options copt;
    copt.verify_peer = false;
    copt.timeout_ms = 5000;
    std::string url = "http://127.0.0.1:" + std::to_string(server_->port()) + "/";
    ASSERT_EQ(client_.open(url, copt, err), 0) << err;
  }

  void TearDown() override
  {
    client_.close();
    server_->stop();
    if (!root_.empty())
      ::rmdir(root_.c_str());
  }

  std::string root_;
  std::unique_ptr<XioFS::Server> server_;
  XioFS::Client client_;
};

} // namespace

TEST_F(ServerFix, PutGetMkdirReaddirUnlink)
{
  std::string err;
  ASSERT_EQ(client_.mkdir("d", err), 0) << err;
  ASSERT_EQ(client_.put("d/hello.txt", "hello", err), 0) << err;

  XioFS::Attr attr;
  ASSERT_EQ(client_.getattr("d/hello.txt", attr, err), 0) << err;
  EXPECT_EQ(attr.size, 5);
  EXPECT_FALSE(attr.is_dir);

  std::string body;
  ASSERT_EQ(client_.read("d/hello.txt", 0, 5, body, err), 0) << err;
  EXPECT_EQ(body, "hello");

  ASSERT_EQ(client_.write("d/hello.txt", 5, "!", err), 0) << err;
  ASSERT_EQ(client_.read("d/hello.txt", 0, 6, body, err), 0) << err;
  EXPECT_EQ(body, "hello!");

  std::vector<XioFS::DavEntry> ents;
  ASSERT_EQ(client_.readdir("d", ents, err), 0) << err;
  ASSERT_EQ(ents.size(), 1u);
  EXPECT_EQ(ents[0].name, "hello.txt");

  ASSERT_EQ(client_.unlink("d/hello.txt", err), 0) << err;
  EXPECT_NE(client_.getattr("d/hello.txt", attr, err), 0);

  std::string disk = root_ + "/d";
  ::rmdir(disk.c_str());
}
