#include "XrdHttpS3Glacier.hh"

#include "XrdSfs/XrdSfsFlags.hh"

#include <cctype>
#include <ctime>

namespace XrdHttpS3 {

const char kRestoreOngoing[] = "user.s3.restore-ongoing";
const char kRestoreExpiry[] = "user.s3.restore-expiry";

namespace {

std::string Trim(const std::string &in) {
  size_t a = 0;
  while (a < in.size() && (in[a] == ' ' || in[a] == '\t' || in[a] == '\r' || in[a] == '\n'))
    a++;
  size_t b = in.size();
  while (b > a && (in[b - 1] == ' ' || in[b - 1] == '\t' || in[b - 1] == '\r' || in[b - 1] == '\n'))
    b--;
  return in.substr(a, b - a);
}

bool TagValue(const std::string &xml, const std::string &tag, std::string &value) {
  std::string needle = "<" + tag;
  size_t pos = 0;
  while (pos < xml.size()) {
    size_t a = xml.find(needle, pos);
    if (a == std::string::npos)
      return false;
    size_t after = a + needle.size();
    if (after < xml.size()) {
      char c = xml[after];
      if (c != '>' && c != ' ' && c != '\t' && c != '\r' && c != '\n' && c != '/') {
        pos = after;
        continue;
      }
    }
    size_t gt = xml.find('>', a);
    if (gt == std::string::npos)
      return false;
    if (gt > a && xml[gt - 1] == '/') {
      value.clear();
      return true;
    }
    std::string close = "</" + tag + ">";
    size_t b = xml.find(close, gt + 1);
    if (b == std::string::npos)
      return false;
    value = Trim(xml.substr(gt + 1, b - gt - 1));
    return true;
  }
  return false;
}

int TierPriority(const std::string &tier, bool &ok) {
  ok = true;
  if (tier.empty() || tier == "Standard")
    return 1;
  if (tier == "Bulk")
    return 0;
  if (tier == "Expedited")
    return 3;
  ok = false;
  return 1;
}

}

bool FileOffline(const struct stat &st) {
  if (st.st_dev == 0 && st.st_ino == 0)
    return true;
  if ((st.st_rdev & XRDSFS_RDVMASK) != 0)
    return false;
  return (st.st_rdev & XRDSFS_OFFLINE) != 0;
}

std::string RestoreDate(time_t t) {
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[64];
  strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
  return buf;
}

bool ParseRestoreDate(const std::string &in, time_t &out) {
  std::string s = Trim(in);
  if (s.size() >= 4 && s.compare(s.size() - 4, 4, " GMT") == 0)
    s.resize(s.size() - 4);
  std::tm tm{};
  const char *rest = strptime(s.c_str(), "%a, %d %b %Y %H:%M:%S", &tm);
  if (!rest || *rest != '\0')
    return false;
  tm.tm_isdst = 0;
  out = timegm(&tm);
  return out != static_cast<time_t>(-1);
}

Archive ClassifyRestore(bool offline, const std::string &ongoing, const std::string &expiryText,
                        time_t now) {
  Archive arc;
  arc.ongoing = ongoing == "true";
  time_t exp = 0;
  bool hasExp = ParseRestoreDate(expiryText, exp);
  if (hasExp)
    arc.expiry = exp;
  if (hasExp && now >= exp) {
    arc.kind = ArchiveKind::Expired;
    return arc;
  }
  if (offline) {
    arc.kind = arc.ongoing ? ArchiveKind::Restoring : ArchiveKind::Archived;
    return arc;
  }
  if (hasExp) {
    arc.kind = ArchiveKind::Restored;
    return arc;
  }
  arc.kind = ArchiveKind::Standard;
  return arc;
}

RestoreXml ParseRestoreRequest(const std::string &xml, RestoreRequest &out) {
  out = RestoreRequest();
  if (xml.find("RestoreRequest") == std::string::npos)
    return RestoreXml::Malformed;
  if (xml.find("OutputLocation") != std::string::npos ||
      xml.find("SelectParameters") != std::string::npos)
    return RestoreXml::Unsupported;
  std::string days;
  if (!TagValue(xml, "Days", days) || days.empty())
    return RestoreXml::Malformed;
  for (unsigned char c : days) {
    if (!std::isdigit(c))
      return RestoreXml::Malformed;
  }
  try {
    out.days = std::stoi(days);
  } catch (...) {
    return RestoreXml::Malformed;
  }
  if (out.days < 1 || out.days > 36500)
    return RestoreXml::Malformed;
  std::string tier;
  TagValue(xml, "Tier", tier);
  bool ok = true;
  out.priority = TierPriority(tier, ok);
  if (!ok)
    return RestoreXml::BadTier;
  return RestoreXml::Ok;
}

std::string GlacierHeaders(const Archive &arc) {
  if (arc.kind == ArchiveKind::Standard)
    return std::string();
  std::string h = "\r\nx-amz-storage-class: GLACIER";
  if (arc.kind == ArchiveKind::Restoring)
    h += "\r\nx-amz-restore: ongoing-request=\"true\"";
  else if (arc.kind == ArchiveKind::Restored)
    h += "\r\nx-amz-restore: ongoing-request=\"false\", expiry-date=\"" + RestoreDate(arc.expiry) +
         "\"";
  return h;
}

}
