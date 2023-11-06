//
// Created by segransm on 11/3/23.
//

#ifndef XROOTD_XRDS3CRYPT_HH
#define XROOTD_XRDS3CRYPT_HH

#include <openssl/evp.h>
#include <openssl/sha.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace S3 {

using sha256_digest = std::array<unsigned char, SHA256_DIGEST_LENGTH>;

class S3Crypt {
 public:
  S3Crypt() = default;
  ~S3Crypt() = default;

  class HMAC_SHA256 {
   public:
    HMAC_SHA256();
    ~HMAC_SHA256();

    template <typename T, typename U>
    sha256_digest calculate(const T &src, const U &key) {
      if (!EVP_MAC_init(ctx, (const unsigned char *)key.data(), key.size(),
                        nullptr)) {
        return {};
      }

      if (!EVP_MAC_update(ctx, (const unsigned char *)src.data(), src.size())) {
        return {};
      }

      size_t outl;
      if (!EVP_MAC_final(ctx, digest.data(), &outl, digest.size())) {
        return {};
      }

      return digest;
    }

   private:
    EVP_MAC *mac;
    EVP_MAC_CTX *ctx;
    sha256_digest digest{};
  };

  class SHA256 {
   public:
    SHA256();
    ~SHA256();

    template <typename T>
    sha256_digest calculate(const T &src) {
      if (!EVP_DigestInit_ex2(ctx, nullptr, nullptr)) {
        return {};
      }

      if (!EVP_DigestUpdate(ctx, src.data(), src.size())) {
        return {};
      }

      unsigned int outl;
      if (!EVP_DigestFinal_ex(ctx, digest.data(), &outl)) {
        return {};
      }

      return digest;
    }

    void Init() { EVP_DigestInit_ex2(ctx, nullptr, nullptr); }

    template <typename T>
    void Update(const T &src) {
      EVP_DigestUpdate(ctx, src.data(), src.size());
    }

    void Update(const char *src, size_t size) {
      EVP_DigestUpdate(ctx, src, size);
    }

    sha256_digest Finish() {
      unsigned int outl;
      if (!EVP_DigestFinal_ex(ctx, digest.data(), &outl)) {
        return {};
      }

      return digest;
    }

   private:
    EVP_MD *md;
    EVP_MD_CTX *ctx;
    sha256_digest digest{};
  };

  class Base64 {
   public:
    Base64();
    ~Base64();

    template <typename T>
    std::string encode(const T &src) {
      EVP_EncodeInit(ctx);
      std::vector<unsigned char> res;
      res.resize(4 * (src.size() + 2) / 3 + 1);

      int outl;
      if (!EVP_EncodeUpdate(ctx, res.data(), &outl,
                            reinterpret_cast<const unsigned char *>(src.data()),
                            src.size())) {
        return {};
      }

      int outlf;
      EVP_EncodeFinal(ctx, res.data() + outl, &outlf);

      assert(static_cast<size_t>(outl + outlf) <= res.size());
      return {reinterpret_cast<char *>(res.data()),
              static_cast<size_t>(outl + outlf - 1)};
    }

    template <typename T>
    std::vector<unsigned char> decode(const T &src) {
      EVP_DecodeInit(ctx);

      std::vector<unsigned char> res;
      res.resize(3 * src.size() / 4 + 1);

      int outl;
      if (EVP_DecodeUpdate(ctx, res.data(), &outl,
                           reinterpret_cast<const unsigned char *>(src.data()),
                           src.size()) < 0) {
        return {};
      }

      int outlf;
      if (EVP_DecodeFinal(ctx, res.data() + outl, &outlf) == -1) {
        return {};
      }

      assert(static_cast<size_t>(outl + outlf) <= res.size());

      res.resize(outl + outlf);
      return res;
    }

   private:
    EVP_ENCODE_CTX *ctx;
  };

  HMAC_SHA256 mHmac;
  SHA256 mSha256;
  Base64 mBase64;
};

}  // namespace S3

#endif  // XROOTD_XRDS3CRYPT_HH
