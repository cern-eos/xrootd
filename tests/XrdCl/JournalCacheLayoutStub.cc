#include "file/XrdClJournalCacheFile.hh"

namespace XrdCl {

std::string JournalCacheFile::sCachePath;
std::string JournalCacheFile::sBasePath;
bool JournalCacheFile::sFlatHierarchy = false;

} // namespace XrdCl
