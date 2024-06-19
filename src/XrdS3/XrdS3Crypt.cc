//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
#include "XrdS3Crypt.hh"
//------------------------------------------------------------------------------
#include <openssl/evp.h>

#include <exception>
#include <stdexcept>
//------------------------------------------------------------------------------
namespace S3 {

#if OPENSSL_VERSION_NUMBER < 0x30000000L

//------------------------------------------------------------------------------
//! \brief SHA256 implementation using OpenSSL
//------------------------------------------------------------------------------
S3Crypt::S3SHA256::S3SHA256() {
  md = (EVP_MD *)EVP_sha256();
  if (md == nullptr) {
    throw std::bad_alloc();
  }

  ctx = EVP_MD_CTX_new();
  if (ctx == nullptr) {
    throw std::bad_alloc();
  }

  if (EVP_DigestInit_ex(ctx, md, NULL) != 1) {
    EVP_MD_CTX_free(ctx);
    throw std::runtime_error("Unable to init digest");
  }
}

S3Crypt::S3SHA256::~S3SHA256() {
  EVP_MD_CTX_free(ctx);
  EVP_cleanup();
}

//------------------------------------------------------------------------------
//! \brief Initialize the digest
//! @param ctx The digest context
//! @param md The digest object
//! @param digest The digest buffer
//------------------------------------------------------------------------------
void S3Crypt::S3SHA256::Init() { EVP_DigestInit_ex(ctx, nullptr, nullptr); }
#else

//------------------------------------------------------------------------------
//! \brief SHA256 implementation using OpenSSL
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! \brief destructor
//------------------------------------------------------------------------------
S3Crypt::S3SHA256::~S3SHA256() {
  EVP_MD_CTX_free(ctx);
  EVP_MD_free(md);
}

//------------------------------------------------------------------------------
//! \brief Initialize the digest
//! @param ctx The digest context
//! @param md The digest object
//! @param digest The digest buffer
//------------------------------------------------------------------------------
void S3Crypt::S3SHA256::Init() { EVP_DigestInit_ex2(ctx, nullptr, nullptr); }

#endif

//------------------------------------------------------------------------------
//! \brief Update the digest
//! @param src The source buffer
//! @param size The size of the buffer
//------------------------------------------------------------------------------
void S3Crypt::S3SHA256::Update(const char *src, size_t size) {
  EVP_DigestUpdate(ctx, src, size);
}

//------------------------------------------------------------------------------
//! \brief Finish the digest and return the digest
//! @return The digest
//------------------------------------------------------------------------------
sha256_digest S3Crypt::S3SHA256::Finish() {
  unsigned int outl;
  if (!EVP_DigestFinal_ex(ctx, digest.data(), &outl)) {
    return {};
  }
  return digest;
}

}  // namespace S3
