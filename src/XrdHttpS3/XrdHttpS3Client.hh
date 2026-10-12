#ifndef XRDHTTPS3_CLIENT_HH
#define XRDHTTPS3_CLIENT_HH

#include "XrdSec/XrdSecEntity.hh"

#include <pwd.h>
#include <string>
#include <sys/types.h>

namespace XrdHttpS3 {

// Name XrdFsOss resolves with getpwnam_r: the text before '@', at most 255 bytes.
inline std::string FsOssName(const std::string &user) {
  std::string out;
  for (char c : user) {
    if (c == '@' || out.size() == 255)
      break;
    out.push_back(c);
  }
  return out;
}

// Data-plane identity. uid and gid stay 0 so XrdFsOss looks the name up itself.
struct Client {
  std::string name;
  XrdSecEntity ent;

  explicit Client(const std::string &user) : name(FsOssName(user)), ent("s3") {
    if (!name.empty())
      ent.name = &name[0];
  }

  Client(const Client &) = delete;
  Client &operator=(const Client &) = delete;

  const XrdSecEntity *get() const { return name.empty() ? nullptr : &ent; }
};

// 0 when the FsOss name is a passwd entry. uid 0 is a real result for root.
inline int PosixId(const std::string &user, uid_t &uid, gid_t &gid) {
  std::string name = FsOssName(user);
  if (name.empty())
    return -1;
  struct passwd pw;
  struct passwd *pwp = nullptr;
  char buf[4096];
  int rc = getpwnam_r(name.c_str(), &pw, buf, sizeof(buf), &pwp);
  if (rc != 0 || !pwp)
    return -1;
  uid = pwp->pw_uid;
  gid = pwp->pw_gid;
  return 0;
}

}

#endif
