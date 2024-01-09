//
// Created by segransm on 11/3/23.
//

#ifndef XROOTD_XRDS3CRYPT_HH
#define XROOTD_XRDS3CRYPT_HH

#include <openssl/evp.h>
#include <openssl/hmac.h>
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

  template <typename T>
  static sha256_digest SHA256_OS(const T &src) {
    sha256_digest digest;

    SHA256(reinterpret_cast<const unsigned char *>(src.data()), src.size(),
           digest.data());

    return digest;
  }
  template <typename T, typename U>
  static sha256_digest HMAC_SHA256(const T &src, const U &key) {
    sha256_digest digest;
    unsigned int outl;

    HMAC(EVP_sha256(), key.data(), key.size(),
         reinterpret_cast<const unsigned char *>(src.data()), src.size(),
         digest.data(), &outl);

    // todo: use outl;
    return digest;
  }

  class S3SHA256 {
   public:
    S3SHA256();
    ~S3SHA256();

    void Init();

    template <typename T>
    void Update(const T &src) {
      EVP_DigestUpdate(ctx, src.data(), src.size());
    }

    void Update(const char *src, size_t size);

    sha256_digest Finish();

   private:
    EVP_MD *md;
    EVP_MD_CTX *ctx;
    sha256_digest digest{};
  };

  class Base64 {
   public:
    Base64() = default;
    ~Base64() = default;

    template <typename T>
    static std::string encode(const T &src) {
      std::vector<unsigned char> res;
      res.resize(4 * (src.size() + 2) / 3 + 1);

      size_t outl = EVP_EncodeBlock(
          res.data(), reinterpret_cast<const unsigned char *>(src.data()),
          src.size());

      assert(outl <= res.size());
      return {reinterpret_cast<char *>(res.data()), outl - 1};
    }

    template <typename T>
    static std::vector<unsigned char> decode(const T &src) {
      if (src.size() < 4) {
        return {};
      }

      std::vector<unsigned char> res;
      res.resize(3 * src.size() / 4 + 1);

      int outl = EVP_DecodeBlock(
          res.data(), reinterpret_cast<const unsigned char *>(src.data()),
          src.size());
      if (outl < 0) {
        return {};
      }

      auto padding = src.find('=', src.size() - 2);
      if (padding != std::string::npos) {
        res.resize(outl - (src.size() - padding));
      } else {
        res.resize(outl);
      }

      return res;
    }
  };
};

}  // namespace S3

#endif  // XROOTD_XRDS3CRYPT_HH
