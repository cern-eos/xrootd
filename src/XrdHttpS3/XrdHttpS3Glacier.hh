#ifndef XRDHTTPS3_GLACIER_HH
#define XRDHTTPS3_GLACIER_HH

#include <string>
#include <sys/stat.h>
#include <ctime>

namespace XrdHttpS3 {

// Live-object attributes. ongoing is "true" or "false". expiry is an HTTP date.
extern const char kRestoreOngoing[];
extern const char kRestoreExpiry[];

enum class ArchiveKind { Standard, Archived, Restoring, Restored, Expired };

struct Archive {
  ArchiveKind kind{ArchiveKind::Standard};
  time_t expiry{0};
  bool ongoing{false};
};

inline bool ArchiveReadable(ArchiveKind kind) {
  return kind == ArchiveKind::Standard || kind == ArchiveKind::Restored;
}

// True when stat reports XRDSFS_OFFLINE, or the older st_dev and st_ino of zero.
bool FileOffline(const struct stat &st);

// offline is the stat bit. ongoing and expiry are the attribute values, empty when absent.
Archive ClassifyRestore(bool offline, const std::string &ongoing, const std::string &expiryText,
                        time_t now);

enum class RestoreXml { Ok, Malformed, Unsupported, BadTier };

struct RestoreRequest {
  int days{0};
  // 0 Bulk, 1 Standard, 3 Expedited. These are Prep_PRTY0, Prep_PRTY1, and Prep_PRTY3.
  int priority{1};
};

RestoreXml ParseRestoreRequest(const std::string &xml, RestoreRequest &out);

std::string RestoreDate(time_t t);
bool ParseRestoreDate(const std::string &in, time_t &out);

// Empty for a standard object. Otherwise the storage-class and restore headers,
// each beginning with CRLF.
std::string GlacierHeaders(const Archive &arc);

}

#endif
