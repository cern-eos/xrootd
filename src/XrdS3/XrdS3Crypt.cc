//
// Created by segransm on 11/3/23.
//

#include "XrdS3Crypt.hh"

#include <openssl/core_names.h>
#include <openssl/ecdsa.h>
#include <openssl/evp.h>

#include <exception>
#include <stdexcept>

namespace S3 {

S3Crypt::HMAC_SHA256::HMAC_SHA256() {
  fprintf(stderr, "Constructing HMAC\n");
  static const std::string digestName = "SHA256";
  OSSL_PARAM params[2];

  mac = EVP_MAC_fetch(nullptr, "HMAC", nullptr);
  if (mac == nullptr) {
    throw std::bad_alloc();
  }
  ctx = EVP_MAC_CTX_new(mac);
  if (ctx == nullptr) {
    EVP_MAC_free(mac);
    throw std::bad_alloc();
  }

  params[0] = OSSL_PARAM_construct_utf8_string(
      OSSL_MAC_PARAM_DIGEST, (char *)digestName.c_str(), digestName.size());
  params[1] = OSSL_PARAM_construct_end();

  if (!EVP_MAC_CTX_set_params(ctx, params)) {
    EVP_MAC_CTX_free(ctx);
    EVP_MAC_free(mac);
    throw std::runtime_error("Unable to set ctx params");
  }
}
S3Crypt::HMAC_SHA256::~HMAC_SHA256() {
  fprintf(stderr, "Destroying HMAC\n");
  EVP_MAC_CTX_free(ctx);
  EVP_MAC_free(mac);
}

S3Crypt::SHA256::SHA256() {
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

S3Crypt::SHA256::~SHA256() {
  EVP_MD_CTX_free(ctx);
  EVP_MD_free(md);
}

S3Crypt::Base64::Base64() {
  ctx = EVP_ENCODE_CTX_new();
  if (ctx == nullptr) {
    throw std::bad_alloc();
  }
}

S3Crypt::Base64::~Base64() { EVP_ENCODE_CTX_free(ctx); }
}  // namespace S3
