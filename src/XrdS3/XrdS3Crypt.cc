//
// Created by segransm on 11/3/23.
//

#include "XrdS3Crypt.hh"

#include <openssl/evp.h>

#include <exception>
#include <stdexcept>

namespace S3 {

S3Crypt::S3SHA256::S3SHA256() {
  md = EVP_MD_fetch(nullptr, "SHA256", nullptr);
  if (md == nullptr) {
    throw std::bad_alloc();
  }

  ctx = EVP_MD_CTX_new();
  if (ctx == nullptr) {
    EVP_MD_free(md);
    throw std::bad_alloc();
  }

  if (!EVP_DigestInit(ctx, md)) {
    EVP_MD_CTX_free(ctx);
    EVP_MD_free(md);
    throw std::runtime_error("Unable to init digest");
  }
}

S3Crypt::S3SHA256::~S3SHA256() {
  EVP_MD_CTX_free(ctx);
  EVP_MD_free(md);
}

void S3Crypt::S3SHA256::Init() { EVP_DigestInit_ex2(ctx, nullptr, nullptr); }

void S3Crypt::S3SHA256::Update(const char *src, size_t size) {
  EVP_DigestUpdate(ctx, src, size);
}
sha256_digest S3Crypt::S3SHA256::Finish() {
  unsigned int outl;
  if (!EVP_DigestFinal_ex(ctx, digest.data(), &outl)) {
    return {};
  }
  return digest;
}

}  // namespace S3
