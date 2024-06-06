#undef NDEBUG

#include "XrdApps/XrdClJCachePlugin/XrdClVectorCache.hh"
#include "XrdSys/XrdSysPlatform.hh"

#include <gtest/gtest.h>

#include <climits>
#include <cstdlib>

using namespace testing;

// Test the JCAche VectorCache class

class VectorCacheTest : public ::testing::Test {};

TEST(VectorCacheTest, Store)
{
  XrdCl::ChunkList chunks;
  std::string name = "root://localhost//dummy";
  std::string prefix = "/tmp/";

  char data[100]; // 100 bytes of fake data
  char cdata[100]; // 100 bytes of zeroed data
  
  for (auto i=0; i< 100; i++) {
    XrdCl::ChunkInfo s(i,1);
    chunks.push_back(s);
    data[i] = i;
    cdata[i] = 0;
  }
  
  EXPECT_TRUE(bcmp(data, cdata, 100) != 0) << "Data not zeroed" << std::endl;
  VectorCache cacheout ( chunks, name, data, prefix);
  EXPECT_TRUE(cacheout.store()) << "Failed to store vector read into cache" << std::endl;
  VectorCache cachein ( chunks, name, cdata, prefix);
  EXPECT_TRUE(cachein.retrieve()) << "Failed to retrieve vector read from cache" << std::endl;
  EXPECT_TRUE(bcmp(data, cdata, 100) == 0) << "Cached data is wrong" << std::endl;
  EXPECT_TRUE(truncate("/tmp/d1a4e9081bd37839e4b4f486ed8b13397ce9ffa0198edb586208d6b73e15b19a/3ec7dea73b7880fdce09e1c8f804054ae685e3dbda4d467c71ab2327ea5ad93e",99)==0) << "Failed to truncate cached file" << std::endl;
  EXPECT_TRUE(!cachein.retrieve()) << "Truncate cache entry was not seen" << std::endl;
  EXPECT_TRUE(unlink("/tmp/d1a4e9081bd37839e4b4f486ed8b13397ce9ffa0198edb586208d6b73e15b19a/3ec7dea73b7880fdce09e1c8f804054ae685e3dbda4d467c71ab2327ea5ad93e") == 0) << "Failed to unlink cached file" << std::endl;
  EXPECT_TRUE(!cachein.retrieve()) << "Unlinked cache entry was not seen" << std::endl;
  // re-cache the block an check if it is ok
  EXPECT_TRUE(cacheout.store()) << "Failed to store vector read into cache" << std::endl;
  EXPECT_TRUE(cachein.retrieve()) << "Failed to retrieve vector read from cache" << std::endl;
}

