#include "XrdHttpS3SigV4.hh"

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
  if (argc != 9) {
    std::cerr << "usage: s3sign secret access-key region method host uri query hash-or-@file\n";
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
  XrdHttpS3::SignOutput out = XrdHttpS3::SignHeader(in);
  std::cout << in.amzDate << '\n' << in.payloadHash << '\n' << out.authorization << '\n';
  return 0;
}
