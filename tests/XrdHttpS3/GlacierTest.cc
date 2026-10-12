#include "XrdHttpS3Glacier.hh"
#include "XrdSfs/XrdSfsFlags.hh"

#include <cstring>
#include <gtest/gtest.h>

using namespace XrdHttpS3;

TEST(S3Glacier, OfflineBit) {
  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_dev = 1;
  st.st_ino = 1;
  EXPECT_FALSE(FileOffline(st));
  st.st_rdev = XRDSFS_OFFLINE;
  EXPECT_TRUE(FileOffline(st));
  st.st_rdev = 0;
  st.st_dev = 0;
  st.st_ino = 0;
  EXPECT_TRUE(FileOffline(st));
}

TEST(S3Glacier, ClassifyRestore) {
  time_t now = 0;
  ASSERT_TRUE(ParseRestoreDate("Fri, 21 Dec 2012 00:00:00 GMT", now));
  EXPECT_EQ(RestoreDate(now), "Fri, 21 Dec 2012 00:00:00 GMT");
  std::string future = RestoreDate(now + 86400);
  std::string past = RestoreDate(now - 86400);

  EXPECT_EQ(ClassifyRestore(false, "", "", now).kind, ArchiveKind::Standard);
  EXPECT_EQ(ClassifyRestore(true, "", "", now).kind, ArchiveKind::Archived);
  EXPECT_EQ(ClassifyRestore(true, "true", future, now).kind, ArchiveKind::Restoring);
  Archive restored = ClassifyRestore(false, "false", future, now);
  EXPECT_EQ(restored.kind, ArchiveKind::Restored);
  EXPECT_EQ(restored.expiry, now + 86400);
  EXPECT_EQ(ClassifyRestore(false, "true", past, now).kind, ArchiveKind::Expired);
  EXPECT_EQ(ClassifyRestore(true, "true", past, now).kind, ArchiveKind::Expired);
  EXPECT_TRUE(ArchiveReadable(ArchiveKind::Restored));
  EXPECT_FALSE(ArchiveReadable(ArchiveKind::Archived));
}

TEST(S3Glacier, RestoreRequestAndHeaders) {
  RestoreRequest req;
  std::string xml =
      "<RestoreRequest><Days>2</Days><GlacierJobParameters><Tier>Expedited</Tier>"
      "</GlacierJobParameters></RestoreRequest>";
  EXPECT_EQ(ParseRestoreRequest(xml, req), RestoreXml::Ok);
  EXPECT_EQ(req.days, 2);
  EXPECT_EQ(req.priority, 3);
  xml = "<RestoreRequest><Days>5</Days><Tier>Bulk</Tier></RestoreRequest>";
  EXPECT_EQ(ParseRestoreRequest(xml, req), RestoreXml::Ok);
  EXPECT_EQ(req.priority, 0);
  EXPECT_EQ(ParseRestoreRequest("<RestoreRequest></RestoreRequest>", req), RestoreXml::Malformed);
  EXPECT_EQ(ParseRestoreRequest("<RestoreRequest><Days>1</Days><Tier>Fast</Tier></RestoreRequest>",
                                req),
            RestoreXml::BadTier);
  EXPECT_EQ(ParseRestoreRequest(
                "<RestoreRequest><Days>1</Days><OutputLocation></OutputLocation></RestoreRequest>",
                req),
            RestoreXml::Unsupported);

  Archive arc;
  arc.kind = ArchiveKind::Restoring;
  EXPECT_NE(GlacierHeaders(arc).find("ongoing-request=\"true\""), std::string::npos);
  EXPECT_NE(GlacierHeaders(arc).find("x-amz-storage-class: GLACIER"), std::string::npos);
  arc.kind = ArchiveKind::Restored;
  arc.expiry = 0;
  ASSERT_TRUE(ParseRestoreDate("Fri, 21 Dec 2012 00:00:00 GMT", arc.expiry));
  std::string hdr = GlacierHeaders(arc);
  EXPECT_NE(hdr.find("ongoing-request=\"false\""), std::string::npos);
  EXPECT_NE(hdr.find("Fri, 21 Dec 2012 00:00:00 GMT"), std::string::npos);
  arc.kind = ArchiveKind::Standard;
  EXPECT_TRUE(GlacierHeaders(arc).empty());
  arc.kind = ArchiveKind::Expired;
  EXPECT_EQ(GlacierHeaders(arc).find("x-amz-restore"), std::string::npos);
}
