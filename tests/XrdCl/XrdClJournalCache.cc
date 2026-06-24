#undef NDEBUG

#include "cache/IntervalTree.hh"
#include "cache/Journal.hh"
#include "file/CachePath.hh"
#include "file/CacheHeaders.hh"
#include "http/ForwardingUrl.hh"
#include "http/HttpHeaderMap.hh"
#include "file/OriginAllowlist.hh"
#include "file/ExternalRedirect.hh"
#include "file/CleanerConfig.hh"
#include "file/PolicyConfig.hh"
#include "file/PolicyRuntime.hh"
#include "daemon/XjcdRender.hh"
#include "daemon/XjcdState.hh"
#include "file/Digest.hh"
#include "system/ListCache.hh"

#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {

void fillPattern(char *buf, size_t len, char seed) {
  for (size_t i = 0; i < len; ++i) {
    buf[i] = seed + static_cast<char>(i);
  }
}

class JournalTest : public ::testing::Test {
protected:
  void SetUp() override {
    char tmpl[] = "/tmp/xrdcl_journal_XXXXXX";
    dir = mkdtemp(tmpl);
    ASSERT_NE(dir, nullptr);
    path = std::string(dir) + "/journal";
  }

  void TearDown() override {
    Journal::sDefaultEnableCrc = false;
    if (dir) {
      fs::remove_all(dir);
      dir = nullptr;
    }
  }

  char *dir = nullptr;
  std::string path;
};

} // namespace

class IntervalTreeTest : public ::testing::Test {};

TEST(IntervalTreeTest, InsertAndQueryOverlap) {
  interval_tree<uint64_t, uint64_t> tree;
  tree.insert(10, 20, 100);
  tree.insert(30, 40, 200);

  auto hit = tree.query(15, 16);
  ASSERT_EQ(hit.size(), 1u);
  EXPECT_EQ((*hit.begin())->value, 100u);

  auto miss = tree.query(21, 29);
  EXPECT_TRUE(miss.empty());

  auto span = tree.query(15, 35);
  EXPECT_EQ(span.size(), 2u);
}

TEST_F(JournalTest, WriteAndReadRoundTrip) {
  Journal journal;
  const uint64_t filesize = 4096;
  ASSERT_EQ(journal.attach(path, 1000, 0, filesize, false), 0);

  char writeBuf[128];
  fillPattern(writeBuf, sizeof(writeBuf), 'a');
  EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 64), (ssize_t)sizeof(writeBuf));

  char readBuf[128] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 64, eof), (ssize_t)sizeof(readBuf));
  EXPECT_FALSE(eof);
  EXPECT_EQ(std::memcmp(writeBuf, readBuf, sizeof(writeBuf)), 0);
}

TEST_F(JournalTest, ReadMissReturnsZero) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 512, eof), 0);
}

TEST_F(JournalTest, PartialCoverageReturnsZero) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char writeBuf[32];
  fillPattern(writeBuf, sizeof(writeBuf), 'b');
  EXPECT_EQ(journal.pwrite(writeBuf, 16, 0), 16);

  char readBuf[64] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 0, eof), 0);
}

TEST_F(JournalTest, PersistAndReattach) {
  const uint64_t mtime = 2000;
  const uint64_t filesize = 8192;

  {
    Journal journal;
    ASSERT_EQ(journal.attach(path, mtime, 0, filesize, false), 0);
    char writeBuf[64];
    fillPattern(writeBuf, sizeof(writeBuf), 'c');
    EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 256), (ssize_t)sizeof(writeBuf));
    EXPECT_EQ(journal.sync(), 0);
  }

  Journal reopened;
  ASSERT_EQ(reopened.attach(path, mtime, 0, filesize, true), 0);

  char readBuf[64] = {};
  bool eof = false;
  EXPECT_EQ(reopened.pread(readBuf, sizeof(readBuf), 256, eof),
            (ssize_t)sizeof(readBuf));
  char expected[64];
  fillPattern(expected, sizeof(expected), 'c');
  EXPECT_EQ(std::memcmp(expected, readBuf, sizeof(readBuf)), 0);
  EXPECT_EQ(reopened.get_max_offset(), 320);
}

TEST_F(JournalTest, OverwriteExistingRange) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char first[32];
  char second[32];
  fillPattern(first, sizeof(first), 'd');
  fillPattern(second, sizeof(second), 'e');

  EXPECT_EQ(journal.pwrite(first, sizeof(first), 128), (ssize_t)sizeof(first));
  EXPECT_EQ(journal.pwrite(second, sizeof(second), 128), (ssize_t)sizeof(second));

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 128, eof), (ssize_t)sizeof(readBuf));
  EXPECT_EQ(std::memcmp(second, readBuf, sizeof(readBuf)), 0);
}

TEST_F(JournalTest, SparseRegionsAreIndependent) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 8192, false), 0);

  char low[16];
  char high[16];
  fillPattern(low, sizeof(low), 'f');
  fillPattern(high, sizeof(high), 'g');

  EXPECT_EQ(journal.pwrite(low, sizeof(low), 0), (ssize_t)sizeof(low));
  EXPECT_EQ(journal.pwrite(high, sizeof(high), 1024), (ssize_t)sizeof(high));

  char readLow[16] = {};
  char readHigh[16] = {};
  bool eof = false;

  EXPECT_EQ(journal.pread(readLow, sizeof(readLow), 0, eof), (ssize_t)sizeof(readLow));
  EXPECT_EQ(journal.pread(readHigh, sizeof(readHigh), 1024, eof),
            (ssize_t)sizeof(readHigh));
  EXPECT_EQ(std::memcmp(low, readLow, sizeof(readLow)), 0);
  EXPECT_EQ(std::memcmp(high, readHigh, sizeof(readHigh)), 0);
}

TEST_F(JournalTest, EofShortReadFromCache) {
  Journal journal;
  const uint64_t filesize = 96;
  ASSERT_EQ(journal.attach(path, 1000, 0, filesize, false), 0);

  char writeBuf[96];
  fillPattern(writeBuf, sizeof(writeBuf), 'h');
  EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 0), (ssize_t)sizeof(writeBuf));

  char readBuf[128] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 0, eof), 96);
  EXPECT_TRUE(eof);
  EXPECT_EQ(std::memcmp(writeBuf, readBuf, sizeof(writeBuf)), 0);
}

TEST_F(JournalTest, GetChunksReturnsStoredData) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char writeBuf[48];
  fillPattern(writeBuf, sizeof(writeBuf), 'i');
  EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 32), (ssize_t)sizeof(writeBuf));

  auto chunks = journal.get_chunks(32, sizeof(writeBuf));
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 32);
  EXPECT_EQ(chunks[0].size, sizeof(writeBuf));
  EXPECT_EQ(std::memcmp(writeBuf, chunks[0].buff, sizeof(writeBuf)), 0);
}

TEST_F(JournalTest, JournalManagerReusesInstance) {
  JournalManager manager;
  std::shared_ptr<Journal> first = manager.attach("file-a");
  std::shared_ptr<Journal> second = manager.attach("file-a");
  EXPECT_EQ(first.get(), second.get());

  first.reset();
  second.reset();
  manager.release("file-a");

  std::shared_ptr<Journal> third = manager.attach("file-a");
  ASSERT_NE(third, nullptr);
}

TEST_F(JournalTest, DetachSyncsAndClosesFd) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char writeBuf[32];
  fillPattern(writeBuf, sizeof(writeBuf), 'd');
  EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 128), (ssize_t)sizeof(writeBuf));
  EXPECT_EQ(journal.detach(), 0);

  Journal reopened;
  ASSERT_EQ(reopened.attach(path, 1000, 0, 4096, true), 0);

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(reopened.pread(readBuf, sizeof(readBuf), 128, eof),
            (ssize_t)sizeof(readBuf));
  EXPECT_EQ(std::memcmp(writeBuf, readBuf, sizeof(writeBuf)), 0);
}

TEST_F(JournalTest, ReattachInvalidatesOnMtimeChange) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char writeBuf[32];
  fillPattern(writeBuf, sizeof(writeBuf), 'e');
  EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 64), (ssize_t)sizeof(writeBuf));

  ASSERT_EQ(journal.attach(path, 5000, 0, 4096, false), 0);

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 64, eof), 0);
}

class JournalCrcTest : public JournalTest {
protected:
  void SetUp() override {
    Journal::sDefaultEnableCrc = true;
    JournalTest::SetUp();
  }
};

TEST_F(JournalCrcTest, WriteReadAndPersist) {
  const uint64_t mtime = 3000;
  const uint64_t filesize = 4096;

  {
    Journal journal;
    ASSERT_EQ(journal.attach(path, mtime, 0, filesize, false), 0);
    char writeBuf[64];
    fillPattern(writeBuf, sizeof(writeBuf), 'j');
    EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 512),
              (ssize_t)sizeof(writeBuf));
    EXPECT_EQ(journal.sync(), 0);
  }

  Journal reopened;
  ASSERT_EQ(reopened.attach(path, mtime, 0, filesize, true), 0);

  char readBuf[64] = {};
  bool eof = false;
  EXPECT_EQ(reopened.pread(readBuf, sizeof(readBuf), 512, eof),
            (ssize_t)sizeof(readBuf));
  char expected[64];
  fillPattern(expected, sizeof(expected), 'j');
  EXPECT_EQ(std::memcmp(expected, readBuf, sizeof(readBuf)), 0);
}

TEST_F(JournalCrcTest, OverwriteUpdatesChecksum) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char first[32];
  char second[32];
  fillPattern(first, sizeof(first), 'k');
  fillPattern(second, sizeof(second), 'l');

  EXPECT_EQ(journal.pwrite(first, sizeof(first), 64), (ssize_t)sizeof(first));
  EXPECT_EQ(journal.pwrite(second, sizeof(second), 64), (ssize_t)sizeof(second));

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(journal.pread(readBuf, sizeof(readBuf), 64, eof), (ssize_t)sizeof(readBuf));
  EXPECT_EQ(std::memcmp(second, readBuf, sizeof(readBuf)), 0);
}

TEST_F(JournalCrcTest, CorruptChecksumTreatedAsMiss) {
  Journal journal;
  ASSERT_EQ(journal.attach(path, 1000, 0, 4096, false), 0);

  char writeBuf[32];
  fillPattern(writeBuf, sizeof(writeBuf), 'm');
  EXPECT_EQ(journal.pwrite(writeBuf, sizeof(writeBuf), 128), (ssize_t)sizeof(writeBuf));

  struct stat st {};
  ASSERT_EQ(stat(path.c_str(), &st), 0);
  ASSERT_GT(st.st_size, 0);
  std::vector<char> file(st.st_size);
  int fd = open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(read(fd, file.data(), file.size()), (ssize_t)file.size());
  file.back() ^= 0xff;
  ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
  ASSERT_EQ(write(fd, file.data(), file.size()), (ssize_t)file.size());
  close(fd);

  Journal reloaded;
  ASSERT_EQ(reloaded.attach(path, 1000, 0, 4096, true), 0);

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(reloaded.pread(readBuf, sizeof(readBuf), 128, eof), 0);
}

TEST_F(JournalCrcTest, CorruptEntrySkippedOnLoad) {
  const uint64_t mtime = 4000;
  const uint64_t filesize = 4096;

  {
    Journal journal;
    ASSERT_EQ(journal.attach(path, mtime, 0, filesize, false), 0);

    char good[32];
    fillPattern(good, sizeof(good), 'g');
    EXPECT_EQ(journal.pwrite(good, sizeof(good), 0), (ssize_t)sizeof(good));
    EXPECT_EQ(journal.pwrite(good, sizeof(good), 256), (ssize_t)sizeof(good));
    EXPECT_EQ(journal.sync(), 0);
  }

  struct stat st {};
  ASSERT_EQ(stat(path.c_str(), &st), 0);
  std::vector<char> file(st.st_size);
  int fd = open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(read(fd, file.data(), file.size()), (ssize_t)file.size());
  file.back() ^= 0xff;
  ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
  ASSERT_EQ(write(fd, file.data(), file.size()), (ssize_t)file.size());
  close(fd);

  Journal reloaded;
  ASSERT_EQ(reloaded.attach(path, mtime, 0, filesize, true), 0);

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(reloaded.pread(readBuf, sizeof(readBuf), 0, eof), (ssize_t)sizeof(readBuf));
  char expected[32];
  fillPattern(expected, sizeof(expected), 'g');
  EXPECT_EQ(std::memcmp(expected, readBuf, sizeof(readBuf)), 0);
  EXPECT_EQ(reloaded.pread(readBuf, sizeof(readBuf), 256, eof), 0);
}

TEST_F(JournalTest, LegacyJournalRemainsReadableWithCrcEnabled) {
  Journal::sDefaultEnableCrc = true;

  {
    Journal legacy;
    Journal::sDefaultEnableCrc = false;
    ASSERT_EQ(legacy.attach(path, 1000, 0, 4096, false), 0);
    char writeBuf[32];
    fillPattern(writeBuf, sizeof(writeBuf), 'n');
    EXPECT_EQ(legacy.pwrite(writeBuf, sizeof(writeBuf), 256), (ssize_t)sizeof(writeBuf));
  }

  Journal::sDefaultEnableCrc = true;
  Journal reader;
  ASSERT_EQ(reader.attach(path, 1000, 0, 4096, true), 0);

  char readBuf[32] = {};
  bool eof = false;
  EXPECT_EQ(reader.pread(readBuf, sizeof(readBuf), 256, eof), (ssize_t)sizeof(readBuf));
  char expected[32];
  fillPattern(expected, sizeof(expected), 'n');
  EXPECT_EQ(std::memcmp(expected, readBuf, sizeof(readBuf)), 0);
}

class ListCacheTest : public ::testing::Test {
protected:
  void SetUp() override {
    Journal::sDefaultEnableCrc = false;
    char tmpl[] = "/tmp/xrdcl_listcache_XXXXXX";
    char *created = mkdtemp(tmpl);
    ASSERT_NE(created, nullptr);
    tempDir = created;
    cacheRoot = tempDir + "/";
  }

  void TearDown() override {
    if (!tempDir.empty()) {
      fs::remove_all(tempDir);
      tempDir.clear();
    }
    Journal::sDefaultEnableCrc = false;
  }

  std::string tempDir;
  std::string cacheRoot;
};

TEST_F(ListCacheTest, SerializeRoundTripBasicAndExtended) {
  {
    XrdCl::StatInfo stat("file-1", 42, XrdCl::StatInfo::IsReadable, 1000);
    const auto line = JournalCache::serializeListEntry("host:1094", "data.root", &stat);
    auto [host, name, decoded] = JournalCache::deserializeListEntry(line);
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(host, "host:1094");
    EXPECT_EQ(name, "data.root");
    EXPECT_EQ(decoded->GetId(), "file-1");
    EXPECT_EQ(decoded->GetSize(), 42u);
    EXPECT_FALSE(decoded->ExtendedFormat());
    delete decoded;
  }

  {
    XrdCl::StatInfo stat("dir-1", 0, XrdCl::StatInfo::IsDir, 2000, 2001, 2002,
                         "755", "user", "group", "");
    const auto line = JournalCache::serializeListEntry("host:1094", "subdir", &stat);
    auto [host, name, decoded] = JournalCache::deserializeListEntry(line);
    ASSERT_NE(decoded, nullptr);
    EXPECT_TRUE(decoded->ExtendedFormat());
    EXPECT_FALSE(decoded->HasChecksum());
    EXPECT_EQ(decoded->GetOwner(), "user");
    delete decoded;
  }

  {
    XrdCl::StatInfo stat("file-2", 99, XrdCl::StatInfo::IsReadable, 3000, 3001,
                         3002, "644", "user", "group", "abc123");
    const auto line = JournalCache::serializeListEntry("host:1094", "chk.root", &stat);
    auto [host, name, decoded] = JournalCache::deserializeListEntry(line);
    ASSERT_NE(decoded, nullptr);
    EXPECT_TRUE(decoded->HasChecksum());
    EXPECT_EQ(decoded->GetChecksum(), "abc123");
    delete decoded;
  }
}

TEST_F(ListCacheTest, SaveAndLoadDirectoryListWithFlags) {
  auto *list = new XrdCl::DirectoryList();
  list->Add(new XrdCl::DirectoryList::ListEntry(
      "host:1094", "a.root",
      new XrdCl::StatInfo("a", 10, XrdCl::StatInfo::IsReadable, 100)));
  list->Add(new XrdCl::DirectoryList::ListEntry(
      "host:1094", "b.root",
      new XrdCl::StatInfo("b", 20, XrdCl::StatInfo::IsReadable, 200)));

  const std::string path = cacheRoot + ".journalcache_list";
  const auto flags = XrdCl::DirListFlags::Stat;
  ASSERT_TRUE(JournalCache::saveDirectoryList(path, list, flags));

  XrdCl::DirectoryList *loaded =
      JournalCache::loadDirectoryList(path, flags, 0);
  ASSERT_NE(loaded, nullptr);
  EXPECT_EQ(loaded->GetSize(), 2u);
  delete loaded;
  delete list;
}

TEST_F(ListCacheTest, DirectoryListExpiresWithTtl) {
  auto *list = new XrdCl::DirectoryList();
  list->Add(new XrdCl::DirectoryList::ListEntry(
      "host:1094", "a.root",
      new XrdCl::StatInfo("a", 10, XrdCl::StatInfo::IsReadable, 100)));

  const std::string path = cacheRoot + "journalcache_list_ttl";
  const auto flags = XrdCl::DirListFlags::Stat;
  ASSERT_TRUE(JournalCache::saveDirectoryList(path, list, flags));
  delete list;

  std::ifstream in(path);
  std::string header;
  ASSERT_TRUE(static_cast<bool>(std::getline(in, header)));
  in.close();
  const auto createdPos = header.find("created=");
  ASSERT_NE(createdPos, std::string::npos);
  const uint64_t created = std::stoull(header.substr(createdPos + 8)) - 7200;

  std::ofstream out(path, std::ios::trunc);
  out << "# journalcache-list-v2 flags="
      << static_cast<uint32_t>(flags) << " created=" << created << '\n';
  XrdCl::StatInfo entry("a", 10, XrdCl::StatInfo::IsReadable, 100);
  out << JournalCache::serializeListEntry("host:1094", "a.root", &entry);
  out.close();

  EXPECT_EQ(JournalCache::loadDirectoryList(path, flags, 3600), nullptr);
  EXPECT_NE(JournalCache::loadDirectoryList(path, flags, 0), nullptr);
}

TEST_F(ListCacheTest, LegacyDirectoryListReadableForStatOnly) {
  const std::string path = cacheRoot + "legacy_list";
  XrdCl::StatInfo stat("legacy", 5, XrdCl::StatInfo::IsReadable, 1234);
  std::ofstream out(path);
  out << JournalCache::serializeListEntry("host:1094", "legacy.root", &stat);
  out.close();

  EXPECT_NE(JournalCache::loadDirectoryList(path, XrdCl::DirListFlags::Stat, 0),
            nullptr);
  EXPECT_EQ(
      JournalCache::loadDirectoryList(
          path, static_cast<XrdCl::DirListFlags::Flags>(
                    XrdCl::DirListFlags::Stat | XrdCl::DirListFlags::Locate),
          0),
      nullptr);
}

TEST_F(ListCacheTest, StatCacheRoundTripAndTtl) {
  const std::string path = cacheRoot + ".journalcache_stat";
  XrdCl::StatInfo stat("path-id", 4096, XrdCl::StatInfo::IsReadable, 5000, 5001,
                       5002, "644", "user", "group", "deadbeef");

  ASSERT_TRUE(JournalCache::saveStatInfo(path, &stat));

  XrdCl::StatInfo *loaded = nullptr;
  ASSERT_TRUE(JournalCache::loadStatInfo(path, 0, loaded));
  ASSERT_NE(loaded, nullptr);
  EXPECT_EQ(loaded->GetChecksum(), "deadbeef");
  delete loaded;

  std::ifstream in(path);
  std::string header;
  ASSERT_TRUE(static_cast<bool>(std::getline(in, header)));
  in.close();
  const auto createdPos = header.find("created=");
  ASSERT_NE(createdPos, std::string::npos);
  const uint64_t created = std::stoull(header.substr(createdPos + 8)) - 7200;

  std::ofstream out(path, std::ios::trunc);
  out << "# journalcache-stat-v1 created=" << created << '\n';
  out << "1\tpath-id\t4096\t" << XrdCl::StatInfo::IsReadable
      << "\t5000\t5001\t5002\t644\tuser\tgroup\t1\tdeadbeef\n";
  out.close();

  loaded = nullptr;
  EXPECT_FALSE(JournalCache::loadStatInfo(path, 3600, loaded));
  EXPECT_TRUE(JournalCache::loadStatInfo(path, 0, loaded));
  delete loaded;
}

TEST_F(ListCacheTest, CachePathUsesBasePathAndFlatHierarchy) {
  const std::string fsUrl = "root://host.example:1094//store/data";

  EXPECT_EQ(JournalCache::resolveCacheDirWithSettings(
                cacheRoot, fsUrl, "/store/data", false, "/store/"),
            cacheRoot + "/store/data");

  const std::string listingDir = JournalCache::resolveCacheDirWithSettings(
      cacheRoot, fsUrl, "/store/data", false, "/store/");
  EXPECT_TRUE(fs::path(listingDir).filename().string().empty() ||
              listingDir.find("/store/data") != std::string::npos);

  const std::string flatDir = JournalCache::resolveCacheDirWithSettings(
      cacheRoot, fsUrl, "/store/data", true, "");
  EXPECT_EQ(flatDir, cacheRoot + JournalCache::computeSHA256(fsUrl + "/store/data"));
}

TEST_F(ListCacheTest, DirListCacheabilityAndInvalidation) {
  EXPECT_TRUE(JournalCache::isDirListCacheable(XrdCl::DirListFlags::Stat));
  EXPECT_FALSE(JournalCache::isDirListCacheable(
      XrdCl::DirListFlags::Stat | XrdCl::DirListFlags::Recursive));

  const auto flags = XrdCl::DirListFlags::Stat;
  const std::string listingDir =
      cacheRoot + "host.example:1094/store/data";
  const std::string listingPath = listingDir + "/.journalcache_list";

  auto *list = new XrdCl::DirectoryList();
  list->Add(new XrdCl::DirectoryList::ListEntry(
      "host:1094", "a.root",
      new XrdCl::StatInfo("a", 10, XrdCl::StatInfo::IsReadable, 100)));
  fs::create_directories(listingDir);
  ASSERT_TRUE(JournalCache::saveDirectoryList(listingPath, list, flags));
  delete list;

  JournalCache::invalidateListingCacheInDir(listingDir);
  EXPECT_FALSE(fs::exists(listingPath));
}

class CacheHeadersTest : public ::testing::Test {
protected:
  void SetUp() override {
    char tmpl[] = "/tmp/xrdcl_cachehdr_XXXXXX";
    char *created = mkdtemp(tmpl);
    ASSERT_NE(created, nullptr);
    tempDir = created;
    journalPath = tempDir + "/journal";
    std::ofstream out(journalPath);
    out << "journal-placeholder";
    out.close();
  }

  void TearDown() override {
    if (!tempDir.empty()) {
      fs::remove_all(tempDir);
      tempDir.clear();
    }
  }

  std::string tempDir;
  std::string journalPath;
};

TEST_F(CacheHeadersTest, ParseCacheControlDirectives) {
  const auto policy =
      JournalCache::parseCacheControl("public, s-maxage=120, max-age=60");
  EXPECT_FALSE(policy.noStore);
  EXPECT_FALSE(policy.noCache);
  EXPECT_EQ(policy.sMaxAge, 120);
  EXPECT_EQ(policy.maxAge, 60);

  const auto noStore =
      JournalCache::parseCacheControl("no-store, max-age=3600");
  EXPECT_TRUE(noStore.noStore);
}

TEST_F(CacheHeadersTest, ExtractFromUrlParams) {
  XrdCl::URL::ParamsMap params;
  params["xrd.journalcache.cache-control"] = "s-maxage=300";
  params["xrd.journalcache.expires"] = "Wed, 21 Oct 2015 07:28:00 GMT";

  JournalCache::CacheHeaders headers;
  ASSERT_TRUE(JournalCache::extractCacheHeadersFromParams(params, headers));
  EXPECT_EQ(headers.cacheControl, "s-maxage=300");
  EXPECT_EQ(headers.expires, "Wed, 21 Oct 2015 07:28:00 GMT");
}

TEST_F(CacheHeadersTest, StoreLoadAndStaleByMaxAge) {
  JournalCache::CacheHeaders headers;
  headers.cacheControl = "s-maxage=60";
  headers.cachedAt = 1000;
  ASSERT_TRUE(JournalCache::storeCacheHeaders(journalPath, headers, nullptr));

  JournalCache::CacheHeaders loaded;
  ASSERT_TRUE(JournalCache::loadCacheHeaders(journalPath, loaded));
  EXPECT_EQ(loaded.cacheControl, headers.cacheControl);
  EXPECT_EQ(loaded.cachedAt, 1000u);

  EXPECT_FALSE(JournalCache::isCacheEntryStale(loaded, 1050));
  EXPECT_TRUE(JournalCache::isCacheEntryStale(loaded, 1061));
}

TEST_F(CacheHeadersTest, NoStoreDisablesCacheUse) {
  JournalCache::CacheHeaders headers;
  headers.cacheControl = "no-store";
  headers.cachedAt = 1000;
  EXPECT_FALSE(JournalCache::shouldUseJournalCache(headers, 1000));
  EXPECT_TRUE(JournalCache::isCacheEntryStale(headers, 1000));
}

TEST_F(CacheHeadersTest, GetterHeadersRoundTrip) {
  JournalCache::CacheHeaders headers;
  headers.cacheControl = "public, max-age=600";
  headers.etag = "\"abc123\"";
  headers.lastModified = "Wed, 21 Oct 2015 07:28:00 GMT";
  headers.cachedAt = 2000;
  ASSERT_TRUE(JournalCache::storeCacheHeaders(journalPath, headers, nullptr));

  JournalCache::CacheHeaders loaded;
  ASSERT_TRUE(JournalCache::loadCacheHeaders(journalPath, loaded));
  EXPECT_EQ(loaded.etag, headers.etag);
  EXPECT_EQ(loaded.lastModified, headers.lastModified);
}

TEST_F(CacheHeadersTest, EnrichFromStatUsesChecksumAndModTime) {
  XrdCl::StatInfo stat("id", 4096, XrdCl::StatInfo::IsReadable, 1445419680, 0,
                       0, "644", "user", "group", "deadbeef");

  JournalCache::CacheHeaders headers;
  JournalCache::enrichCacheHeadersFromStat(&stat, headers);
  EXPECT_EQ(headers.etag, "\"deadbeef\"");
  EXPECT_FALSE(headers.lastModified.empty());
}

TEST_F(CacheHeadersTest, ValidationRequiresRefreshOnEtagMismatch) {
  JournalCache::CacheHeaders stored;
  stored.etag = "\"v1\"";
  JournalCache::CacheValidators request;
  request.ifNoneMatch = "\"v2\"";
  EXPECT_TRUE(JournalCache::validationRequiresRefresh(stored, request));

  request.ifNoneMatch = "\"v1\"";
  EXPECT_FALSE(JournalCache::validationRequiresRefresh(stored, request));
}

TEST_F(CacheHeadersTest, RequiresRevalidationForNoCache) {
  JournalCache::CacheHeaders headers;
  headers.cacheControl = "no-cache";
  EXPECT_TRUE(JournalCache::requiresRevalidation(headers));
}

TEST_F(CacheHeadersTest, CanRespondNotModifiedWhenValidatorsMatch) {
  JournalCache::CacheHeaders stored;
  stored.etag = "\"v1\"";
  stored.cacheControl = "public, max-age=3600";
  stored.cachedAt = 1000;

  JournalCache::CacheValidators request;
  request.ifNoneMatch = "\"v1\"";

  EXPECT_TRUE(JournalCache::canRespondNotModified(stored, request, 1200));
  EXPECT_FALSE(JournalCache::canRespondNotModified(stored, request, 5000));
}

TEST_F(CacheHeadersTest, AppendGetterResponseHeaders) {
  JournalCache::CacheHeaders headers;
  headers.cacheControl = "public, max-age=60";
  headers.etag = "\"abc\"";
  headers.lastModified = "Wed, 21 Oct 2015 07:28:00 GMT";

  std::string out;
  JournalCache::appendGetterResponseHeaders(headers, out);
  EXPECT_NE(out.find("Cache-Control: public, max-age=60"), std::string::npos);
  EXPECT_NE(out.find("ETag: \"abc\""), std::string::npos);
  EXPECT_NE(out.find("Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT"),
            std::string::npos);
}

TEST(CacheHeadersParamTest, ParamEnabledAcceptsTruthyValues) {
  XrdCl::URL::ParamsMap params;
  params[JournalCache::BYPASS_CGI] = "1";
  EXPECT_TRUE(JournalCache::paramEnabled(params, JournalCache::BYPASS_CGI));

  params[JournalCache::FORCE_CLEAN_CGI] = "yes";
  EXPECT_TRUE(JournalCache::paramEnabled(params, JournalCache::FORCE_CLEAN_CGI));

  params[JournalCache::BYPASS_CGI] = "0";
  EXPECT_FALSE(JournalCache::paramEnabled(params, JournalCache::BYPASS_CGI));
}

TEST(HttpHeaderMapTest, BuildFileUrlAndMapSetterHeaders) {
  EXPECT_EQ(JournalCache::HttpHeaderMap::buildFileUrl("root://host:1094",
                                                      "/store/file.root"),
            "root://host:1094//store/file.root");

  std::map<std::string, std::string> headers;
  headers["Cache-Control"] = "public, max-age=60";
  headers["ETag"] = "\"abc\"";

  const auto mapped = JournalCache::HttpHeaderMap::mapHeaders(
      headers, JournalCache::HttpHeaderMap::setterMappings());
  ASSERT_EQ(mapped.size(), 2u);
  EXPECT_EQ(mapped[0].first, JournalCache::CACHE_CONTROL_CGI);
  EXPECT_EQ(mapped[1].first, JournalCache::ETAG_CGI);
}

TEST(ForwardingUrlTest, ParseEmbeddedHttpUrl) {
  const auto embedded = JournalCache::parseEmbeddedFileUrl(
      "/https://cdn.example.org/store/file.dat");
  ASSERT_TRUE(embedded.valid);
  EXPECT_NE(embedded.fileUrl.find("cdn.example.org"), std::string::npos);
  EXPECT_NE(embedded.fileUrl.find("/store/file.dat"), std::string::npos);

  EXPECT_FALSE(
      JournalCache::parseEmbeddedFileUrl("/store/file.dat").valid);
}

TEST(ForwardingUrlTest, ParseEmbeddedRootUrl) {
  const auto embedded = JournalCache::parseEmbeddedFileUrl(
      "/root://origin.cern.ch:1094//store/file.dat");
  ASSERT_TRUE(embedded.valid);
  EXPECT_NE(embedded.fileUrl.find("origin.cern.ch"), std::string::npos);
  EXPECT_NE(embedded.fileUrl.find("/store/file.dat"), std::string::npos);
}

TEST(ForwardingUrlTest, ParseChainedRootUrl) {
  const auto chained = JournalCache::parseChainedFileUrl(
      "root://proxy.cern.ch:1095//root://origin.cern.ch:1094//store/file.dat");
  ASSERT_TRUE(chained.valid);
  EXPECT_NE(chained.fileUrl.find("origin.cern.ch"), std::string::npos);
  EXPECT_EQ(chained.fileUrl.find("proxy.cern.ch"), std::string::npos);
}

TEST(ForwardingUrlTest, ParseChainedTripleRootUrl) {
  const auto chained = JournalCache::parseChainedFileUrl(
      "root://proxy1:1095//root://proxy2:1096//root://origin.cern.ch:1094//"
      "store/file.dat");
  ASSERT_TRUE(chained.valid);
  EXPECT_NE(chained.fileUrl.find("origin.cern.ch"), std::string::npos);
  EXPECT_NE(chained.fileUrl.find("/store/file.dat"), std::string::npos);
  EXPECT_EQ(chained.fileUrl.find("proxy1"), std::string::npos);
  EXPECT_EQ(chained.fileUrl.find("proxy2"), std::string::npos);
}

TEST(ForwardingUrlTest, ParseChainedTripleEmbeddedPath) {
  const auto chained = JournalCache::parseChainedFileUrl(
      "/root://proxy2:1096//root://origin.cern.ch:1094//store/file.dat");
  ASSERT_TRUE(chained.valid);
  EXPECT_NE(chained.fileUrl.find("origin.cern.ch"), std::string::npos);
  EXPECT_EQ(chained.fileUrl.find("proxy2"), std::string::npos);
}

TEST(ForwardingUrlTest, ParseChainedMixedProtocols) {
  const auto chained = JournalCache::parseChainedFileUrl(
      "root://proxy:1095//root://relay:1096//https://cdn.example.org/store/"
      "file.dat");
  ASSERT_TRUE(chained.valid);
  EXPECT_NE(chained.fileUrl.find("cdn.example.org"), std::string::npos);
  EXPECT_EQ(chained.fileUrl.find("proxy"), std::string::npos);
  EXPECT_EQ(chained.fileUrl.find("relay"), std::string::npos);
}

TEST(OriginAllowlistTest, AllowsMatchingHostOrUrl) {
  JournalCache::OriginAllowlist allowlist;
  allowlist.addPattern(R"(^root://([a-z0-9.-]+\.)?cern\.ch(:1094)?/)");
  allowlist.addPattern(R"(^https://([a-z0-9.-]+\.)?example\.org/)");

  EXPECT_TRUE(allowlist.isAllowed(
      "root://origin.cern.ch:1094//store/file.dat"));
  EXPECT_TRUE(
      allowlist.isAllowed("https://cdn.example.org/store/file.dat"));
  EXPECT_FALSE(allowlist.isAllowed("root://evil.example.net//store/file.dat"));
}

TEST(ExternalRedirectTest, ResolvesLongestPrefixMatch) {
  JournalCache::ExternalRedirect redirects;
  redirects.addRule("/store/", "root://origin.cern.ch:1094//store/");
  redirects.addRule("/store/live/", "https://stream.example.org/live/");

  EXPECT_EQ(redirects.resolve("/store/data/file.dat"),
            "root://origin.cern.ch:1094//store/data/file.dat");
  EXPECT_EQ(redirects.resolve("/store/live/event/1"),
            "https://stream.example.org/live/event/1");
  EXPECT_EQ(redirects.resolve("/other/file.dat"), "");
}

TEST(ExternalRedirectTest, PreservesQuerySuffix) {
  JournalCache::ExternalRedirect redirects;
  redirects.addRule("/live/", "https://stream.example.org/live/");

  EXPECT_EQ(redirects.resolve("/live/event/1", "?token=abc"),
            "https://stream.example.org/live/event/1?token=abc");
}

TEST(ExternalRedirectTest, ParsesPipeSeparatedConfig) {
  JournalCache::ExternalRedirect redirects;
  redirects.addRulesFromCsv(
      "/live/|https://stream.example.org/live/,/raw/|root://data.cern.ch//raw/");

  EXPECT_NE(redirects.resolve("/live/foo").find("stream.example.org"),
            std::string::npos);
  EXPECT_NE(redirects.resolve("/raw/bar").find("data.cern.ch"),
            std::string::npos);
}

TEST(PolicyConfigTest, RoundTripPolicyFile) {
  JournalCache::PolicySettings settings;
  settings.bypass = true;
  settings.multiOriginUnwrap = true;
  settings.originAllowlist.addPattern(R"(^https://example\.org/)");
  settings.externalRedirect.addRule("/live/", "https://stream.example.org/live/");

  const std::string text = JournalCache::formatPolicyFile(settings);
  JournalCache::PolicySettings loaded;
  ASSERT_TRUE(JournalCache::parsePolicyText(text, loaded));
  EXPECT_TRUE(loaded.bypass);
  EXPECT_TRUE(loaded.multiOriginUnwrap);
  EXPECT_EQ(loaded.originAllowlist.patterns().size(), 1u);
  EXPECT_EQ(loaded.externalRedirect.rules().size(), 1u);
}

TEST(PolicyRuntimeTest, ReloadsWhenMtimeChanges) {
  const std::string path =
      (std::filesystem::temp_directory_path() / "xjc-policy-test.conf").string();
  std::filesystem::remove(path);

  JournalCache::PolicySettings bootstrap;
  bootstrap.bypass = false;
  auto &runtime = JournalCache::PolicyRuntime::instance();
  runtime.stopWatcher();
  runtime.configure(path, bootstrap);

  JournalCache::PolicySettings updated = bootstrap;
  updated.bypass = true;
  ASSERT_TRUE(JournalCache::savePolicyFile(path, updated));

  runtime.reloadIfChanged();
  EXPECT_TRUE(runtime.snapshot().bypass);

  std::filesystem::remove(path);
}

TEST(ForwardingUrlTest, ResolveJournalPathMatchesFilePluginLayout) {
  const std::string cacheRoot = "/cache/";
  const std::string fileUrl = "https://cdn.example.org:443/store/file.dat";

  const std::string journalPath = JournalCache::resolveJournalPathFromCacheKey(
      cacheRoot, fileUrl, false, "");
  EXPECT_EQ(journalPath,
            "/cache/cdn.example.org:443/store/file.dat/journal");

  const std::string flatPath = JournalCache::resolveJournalPathFromCacheKey(
      cacheRoot, fileUrl, true, "");
  EXPECT_EQ(flatPath.substr(0, 7), "/cache/");
  EXPECT_EQ(flatPath.substr(flatPath.size() - 8), "/journal");
}

TEST(XjcdStateTest, RoundTripStateFile) {
  char tmpl[] = "/tmp/xjcd_state_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_NE(dir, nullptr);

  JournalCache::XjcdState state;
  state.journal = dir;
  state.xrootPort = 1094;
  state.httpsPort = 8443;
  state.tlsCert = "/etc/ssl/cert.pem";
  state.tlsKey = "/etc/ssl/key.pem";
  state.libDir = "/usr/lib64";
  state.pluginSuffix = "5";

  ASSERT_TRUE(state.save());

  JournalCache::XjcdState loaded;
  ASSERT_TRUE(loaded.load(dir));
  EXPECT_EQ(loaded.journal, state.journal);
  EXPECT_EQ(loaded.xrootPort, 1094u);
  EXPECT_EQ(loaded.httpsPort, 8443u);
  EXPECT_EQ(loaded.tlsCert, state.tlsCert);
  EXPECT_EQ(loaded.tlsKey, state.tlsKey);

  fs::remove_all(dir);
}

TEST(XjcdRenderTest, RendersConfigsAndOpenPolicy) {
  char tmpl[] = "/tmp/xjcd_render_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_NE(dir, nullptr);

  JournalCache::XjcdState state;
  state.journal = dir;
  state.xrootPort = 1094;
  state.httpsPort = 8443;
  state.tlsCert = "/etc/ssl/cert.pem";
  state.tlsKey = "/etc/ssl/key.pem";
  state.libDir = "/usr/lib64";
  state.pluginSuffix = "5";

  ASSERT_TRUE(JournalCache::renderXjcdConfigs(state, "testhost"));

  const std::string xrootdPath = state.xrootdConfigPath();
  const std::string policyPath = state.policyPath();
  ASSERT_TRUE(fs::exists(xrootdPath));
  ASSERT_TRUE(fs::exists(policyPath));

  std::ifstream xrootdIn(xrootdPath);
  std::string xrootdText((std::istreambuf_iterator<char>(xrootdIn)),
                         std::istreambuf_iterator<char>());
  EXPECT_NE(xrootdText.find("port 1094"), std::string::npos);
  EXPECT_NE(xrootdText.find("port tls 8443"), std::string::npos);
  EXPECT_NE(xrootdText.find("http.cert /etc/ssl/cert.pem"), std::string::npos);
  EXPECT_NE(xrootdText.find("http.key /etc/ssl/key.pem"), std::string::npos);
  EXPECT_NE(xrootdText.find("libXrdClJournalCacheHttpExt-5.so"),
            std::string::npos);

  JournalCache::PolicySettings policy;
  ASSERT_TRUE(JournalCache::loadPolicyFile(policyPath, policy));
  EXPECT_FALSE(policy.bypass);
  EXPECT_TRUE(policy.multiOriginUnwrap);
  EXPECT_TRUE(policy.originAllowlist.patterns().empty());

  const std::string unitPath = state.systemdUnitPath();
  ASSERT_TRUE(fs::exists(unitPath));
  std::ifstream unitIn(unitPath);
  std::string unitText((std::istreambuf_iterator<char>(unitIn)),
                       std::istreambuf_iterator<char>());
  EXPECT_NE(unitText.find("ExecStart=/usr/bin/xrootd -c " + xrootdPath),
            std::string::npos);
  EXPECT_NE(unitText.find("EnvironmentFile=-" + state.systemdEnvPath()),
            std::string::npos);
  EXPECT_NE(unitText.find("RequiresMountsFor=" + state.journal),
            std::string::npos);

  const std::string cleanerPath = state.cleanerPath();
  ASSERT_TRUE(fs::exists(cleanerPath));
  ASSERT_TRUE(fs::exists(state.cleanerSystemdUnitPath()));
  std::ifstream cleanerUnitIn(state.cleanerSystemdUnitPath());
  std::string cleanerUnitText((std::istreambuf_iterator<char>(cleanerUnitIn)),
                              std::istreambuf_iterator<char>());
  EXPECT_NE(cleanerUnitText.find("ExecStart=/usr/bin/xjccleand --journal " +
                                 state.journal),
            std::string::npos);

  fs::remove_all(dir);
}

TEST(CleanerConfigTest, RoundTripCleanerFile) {
  JournalCache::CleanerSettings settings;
  settings.enabled = true;
  settings.journal = "/var/tmp/journalcache";
  settings.highWatermark = 10000000000ull;
  settings.lowWatermark = 0;
  settings.interval = 60;
  settings.configPoll = 2;

  const std::string text = JournalCache::formatCleanerFile(settings);
  JournalCache::CleanerSettings loaded;
  ASSERT_TRUE(JournalCache::parseCleanerText(text, loaded));
  EXPECT_TRUE(loaded.enabled);
  EXPECT_EQ(loaded.journal, settings.journal);
  EXPECT_EQ(loaded.highWatermark, settings.highWatermark);
  EXPECT_EQ(JournalCache::effectiveLowWatermark(loaded), 9000000000ull);
}

TEST(XjcdSystemdTest, ValidatesUnitName) {
  EXPECT_TRUE(JournalCache::isValidSystemdUnitName("xjcd.service"));
  EXPECT_TRUE(JournalCache::isValidSystemdUnitName("journalcache-foo.service"));
  EXPECT_TRUE(JournalCache::isValidSystemdUnitName("xjcd@.service"));
  EXPECT_FALSE(JournalCache::isValidSystemdUnitName("xjcd"));
  EXPECT_FALSE(JournalCache::isValidSystemdUnitName("/etc/xjcd.service"));
  EXPECT_FALSE(JournalCache::isValidSystemdUnitName("bad name.service"));
}

TEST(XjcdSystemdTest, InstallPath) {
  EXPECT_EQ(JournalCache::systemdUnitInstallPath("xjcd.service"),
            "/etc/systemd/system/xjcd.service");
}
