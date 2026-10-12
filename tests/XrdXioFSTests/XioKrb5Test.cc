//------------------------------------------------------------------------------
// Tests for XIOFS Kerberos SPNEGO helper.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioKrb5.hh"

#include <gtest/gtest.h>

TEST(XioKrb5Test, TargetImportAndErrorHandling)
{
  std::string token;
  std::string err;
  // Without an active TGT or for non-existent service, it should fail cleanly
  // with an informative error message rather than crashing.
  bool ok = XioFS::getSpnegoToken("nonexistent.test.example.org", "", token, err);
  if (!ok) {
    EXPECT_FALSE(err.empty());
  } else {
    EXPECT_FALSE(token.empty());
  }
}
