#ifndef XRDHTTPS3_FS_HH
#define XRDHTTPS3_FS_HH

#include <string>
#include <sys/stat.h>
#include <vector>

class XrdSecEntity;
class XrdSfsFile;
class XrdSfsFileSystem;

namespace XrdHttpS3 {

struct OwnedFile {
  XrdSfsFile *fh{nullptr};
  char user[256];
  OwnedFile() : fh(nullptr) { user[0] = 0; }
  OwnedFile(const OwnedFile &) = delete;
  OwnedFile &operator=(const OwnedFile &) = delete;
  ~OwnedFile();
};

class Fs {
public:
  explicit Fs(XrdSfsFileSystem &sfs) : m_sfs(sfs) {}

  int Open(const std::string &path, const XrdSecEntity *who, int flags, mode_t mode,
           OwnedFile &file, int &err);

  int ReadAll(const std::string &path, const XrdSecEntity *who, std::string &out,
              int &err);
  int WriteAll(const std::string &path, const XrdSecEntity *who, const std::string &body,
               mode_t mode, int &err);
  int Unlink(const std::string &path, const XrdSecEntity *who, int &err);
  int Mkdir(const std::string &path, const XrdSecEntity *who, int &err);
  int Rmdir(const std::string &path, const XrdSecEntity *who, int &err);
  int Stat(const std::string &path, const XrdSecEntity *who, struct stat &st, int &err);
  int List(const std::string &path, const XrdSecEntity *who,
           std::vector<std::string> &names, int &err);
  int SetXattr(const std::string &path, const XrdSecEntity *who, const std::string &name,
               const std::string &value, int &err);
  int GetXattr(const std::string &path, const XrdSecEntity *who, const std::string &name,
               std::string &value, int &err);

private:
  XrdSfsFileSystem &m_sfs;
};

}

#endif
