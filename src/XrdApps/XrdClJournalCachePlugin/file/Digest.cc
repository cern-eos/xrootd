#include "file/Digest.hh"

#include <filesystem>
#include <iomanip>
#include <iostream>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <sstream>

namespace fs = std::filesystem;

namespace JournalCache {

std::string computeSHA256(const std::string &data) {
  unsigned int length = 0;
#if OPENSSL_VERSION_NUMBER < 0x30000000L
  length = SHA256_DIGEST_LENGTH;
  unsigned char hash[length];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, data.c_str(), data.size());
  SHA256_Final(hash, &sha256);
#else
  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  const EVP_MD *md = EVP_sha256();
  EVP_DigestInit_ex(ctx, md, NULL);
  EVP_DigestUpdate(ctx, data.data(), data.size());
  length = EVP_MD_size(md);
  unsigned char hash[length];
  EVP_DigestFinal_ex(ctx, hash, &length);
  EVP_MD_CTX_free(ctx);
#endif
  std::stringstream ss;
  for (unsigned int i = 0; i < length; ++i) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
  }
  return ss.str();
}

bool ensureCacheDirectory(const std::string &dirName) {
  fs::path dirPath(dirName);

  if (fs::exists(dirPath) && fs::is_directory(dirPath)) {
    return true;
  }

  fs::path parentPath = dirPath.parent_path();
  if (!fs::exists(parentPath)) {
    std::cerr << "error: parent directory does not exist. Cannot create "
                 "subdirectory.\n";
    return false;
  }

  if (fs::create_directory(dirPath)) {
    return true;
  }

  std::cerr << "error: failed to create subdirectory.\n";
  return false;
}

} // namespace JournalCache
