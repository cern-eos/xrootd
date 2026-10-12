#include "XrdHttpS3SigV4.hh"

#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

namespace {

std::string NowAmz() {
  std::time_t now = std::time(nullptr);
  std::tm tm{};
  gmtime_r(&now, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
  return buf;
}

std::string FileBytes(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

}

int main(int argc, char **argv) {
  if (argc >= 7 && std::string(argv[1]) == "chunk") {
    if (argc != 7) {
      std::cerr << "usage: s3sign chunk secret region amz-date previous-signature payload-file\n";
      return 2;
    }
    std::cout << XrdHttpS3::ChunkSignature(argv[2], argv[4], argv[3], argv[5],
                                           FileBytes(argv[6]))
              << '\n';
    return 0;
  }
  if (argc == 6 && std::string(argv[1]) == "policy") {
    std::cout << XrdHttpS3::PolicySignature(argv[2], argv[4], argv[3], argv[5]) << '\n';
    return 0;
  }
  if (argc == 9 && std::string(argv[1]) == "presign") {
    XrdHttpS3::SignInput in;
    in.secret = argv[2];
    in.accessKey = argv[3];
    in.region = argv[4];
    in.method = argv[5];
    in.host = argv[6];
    in.uri = argv[7];
    in.amzDate = NowAmz();
    long long expires = std::strtoll(argv[8], nullptr, 10);
    XrdHttpS3::SignOutput out = XrdHttpS3::SignQuery(in, expires);
    std::cout << out.query << '\n';
    return 0;
  }
  if (argc < 9 || ((argc - 9) % 2) != 0) {
    std::cerr << "usage: s3sign secret access-key region method host uri query hash-or-@file [header value]...\n"
                 "       s3sign presign secret access-key region method host uri expires\n"
                 "       s3sign chunk secret region amz-date previous-signature payload-file\n"
                 "       s3sign policy secret region amz-date policy-b64\n";
    return 2;
  }
  XrdHttpS3::SignInput in;
  in.secret = argv[1];
  in.accessKey = argv[2];
  in.region = argv[3];
  in.method = argv[4];
  in.host = argv[5];
  in.uri = argv[6];
  in.query = argv[7];
  std::string hash = argv[8];
  if (!hash.empty() && hash[0] == '@')
    in.payloadHash = XrdHttpS3::HexSha256(FileBytes(hash.substr(1)));
  else
    in.payloadHash = hash;
  in.amzDate = NowAmz();
  for (int i = 9; i + 1 < argc; i += 2)
    in.extra.push_back({argv[i], argv[i + 1]});
  XrdHttpS3::SignOutput out = XrdHttpS3::SignHeader(in);
  std::cout << in.amzDate << '\n' << in.payloadHash << '\n' << out.authorization << '\n';
  return 0;
}
