#include "XrdHttpS3Config.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdHttpS3SigV4.hh"

#include <gtest/gtest.h>

using namespace XrdHttpS3;

TEST(S3Paths, UserHashAndObjectLayout) {
  Config cfg;
  cfg.userprefix = "/prefix";
  cfg.sysmeta = "/s3meta";
  cfg.userhash = true;
  EXPECT_EQ(UserHome(cfg, "foo"), "/prefix/f/foo");
  EXPECT_EQ(UserMeta(cfg, "foo"), "/prefix/f/foo/.s3");
  EXPECT_EQ(UserBucketFile(cfg, "foo", "mybucket"), "/prefix/f/foo/.s3/buckets/mybucket");
  EXPECT_EQ(KeyIndex(cfg, "AKIAIOSFODNN7EXAMPLE"), "/s3meta/keys/AKIAIOSFODNN7EXAMPLE");
  EXPECT_EQ(ObjectFile(cfg, "foo", "mybucket", "a/b"), "/prefix/f/foo/mybucket/a/b");
  EXPECT_EQ(ObjectFile(cfg, "foo", "mybucket", "a/"), "/prefix/f/foo/mybucket/a/.s3folder");
  cfg.userhash = false;
  EXPECT_EQ(UserMeta(cfg, "foo"), "/prefix/foo/.s3");
  EXPECT_TRUE(ValidBucket("mybucket"));
  EXPECT_FALSE(ValidBucket(".s3"));
  EXPECT_FALSE(ValidObjectKey("../x"));
  EXPECT_FALSE(ValidObjectKey("a/.s3/b"));
  EXPECT_TRUE(ValidObjectKey("a/b"));
  EXPECT_TRUE(ValidAccessKey("AKIAIOSFODNN7EXAMPLE"));
}

TEST(S3SigV4, AwsGetObjectExample) {
  std::vector<std::pair<std::string, std::string>> headers = {
      {"host", "examplebucket.s3.amazonaws.com"},
      {"range", "bytes=0-9"},
      {"x-amz-content-sha256",
       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
      {"x-amz-date", "20130524T000000Z"},
  };
  std::string canonical = CanonicalRequest(
      "GET", "/test.txt", "", headers,
      "host;range;x-amz-content-sha256;x-amz-date",
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(HexSha256(canonical),
            "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972");
  EXPECT_EQ(SignatureHex("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524T000000Z",
                         "us-east-1", "s3", canonical),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");

  SignInput in;
  in.method = "GET";
  in.uri = "/test.txt";
  in.host = "examplebucket.s3.amazonaws.com";
  in.amzDate = "20130524T000000Z";
  in.payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  in.secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  in.accessKey = "AKIAIOSFODNN7EXAMPLE";
  in.region = "us-east-1";
  in.extra.push_back({"range", "bytes=0-9"});
  SignOutput signedReq = SignHeader(in);
  EXPECT_EQ(signedReq.signature,
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");

  VerifyInput vin;
  vin.method = "GET";
  vin.resource = "/test.txt";
  vin.region = "us-east-1";
  vin.skew = 2000000000;
  vin.now = 1369353600;
  vin.authorization = signedReq.authorization;
  vin.headers["host"] = in.host;
  vin.headers["range"] = "bytes=0-9";
  vin.headers["x-amz-content-sha256"] = in.payloadHash;
  vin.headers["x-amz-date"] = in.amzDate;
  std::string detail;
  EXPECT_EQ(VerifySigV4(vin, in.secret, detail), AuthFail::Ok);
}

TEST(S3SigV4, ChunkSignatureVector) {
  EXPECT_EQ(ChunkSignature("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524T000000Z",
                           "us-east-1",
                           "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
                           "hello"),
            "b4816a35e1ca6cde896ef2fb98ea432d592c147832bfc1e9f7d3456f800ea44b");
}

TEST(S3SigV4, PresignRoundTrip) {
  SignInput in;
  in.method = "GET";
  in.uri = "/mybucket/hello.txt";
  in.host = "127.0.0.1:8080";
  in.amzDate = "20130524T000000Z";
  in.secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  in.accessKey = "AKIAIOSFODNN7EXAMPLE";
  in.region = "us-east-1";
  SignOutput signedReq = SignQuery(in, 60);
  EXPECT_NE(signedReq.query.find("X-Amz-Signature="), std::string::npos);
  EXPECT_NE(signedReq.query.find("X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F"),
            std::string::npos);
  VerifyInput vin;
  vin.method = "GET";
  vin.resource = "/mybucket/hello.txt";
  vin.query = signedReq.query;
  vin.region = "us-east-1";
  vin.skew = 900;
  vin.now = 1369353600;
  vin.headers["host"] = in.host;
  ParsedAuth auth;
  std::string detail;
  EXPECT_EQ(VerifyPresign(vin, in.secret, auth, detail), AuthFail::Ok) << detail;
  EXPECT_EQ(auth.accessKey, in.accessKey);
}

TEST(S3SigV4, CanonicalQueryEncodesSlash) {
  EXPECT_EQ(CanonicalQuery("prefix=a/b&list-type=2"), "list-type=2&prefix=a%2Fb");
  EXPECT_EQ(CanonicalQuery("&list-type=2"), "list-type=2");
  EXPECT_EQ(CanonicalQuery("acl"), "acl=");
}
