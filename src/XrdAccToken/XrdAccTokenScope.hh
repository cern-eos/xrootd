#ifndef XRDACCTOKENSCOPE_HH
#define XRDACCTOKENSCOPE_HH
//------------------------------------------------------------------------------
// Copyright (c) 2025 by European Organization for Nuclear Research (CERN)
//------------------------------------------------------------------------------
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
//------------------------------------------------------------------------------

#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdAcc/XrdAccPrivs.hh"

#include <string>
#include <vector>

namespace XrdAccTokenScope {

enum class StorageCapability {
    Read,
    Create,
    Modify,
    Stage,
    Poll
};

enum class ScopeProfile {
    WLCG,
    SciTokens
};

struct ScopeRule {
    StorageCapability capability;
    std::string path;
};

bool MakeCanonical(const std::string &path, std::string &result);

bool IsWlcgProfile(const std::string &wlcgVer);

ScopeProfile DetectProfile(const std::string &wlcgVer);

bool ParseScopeClaim(const std::string &scopeClaim,
                     const std::string &wlcgVer,
                     std::vector<ScopeRule> &rules);

bool OperationMatchesCapability(Access_Operation oper, StorageCapability cap);

bool PathMatchesScope(const std::string &scopePath, const std::string &reqPath,
                      Access_Operation oper);

std::string ApplyBasePath(const std::string &basePath, const std::string &scopePath);

std::string ResolveBasePath(const std::string &configBasePath,
                            const std::string &entityBasePath);

bool ParseRestrictedPathsJson(const std::string &json,
                              std::vector<std::string> &paths,
                              std::string &emsg);

bool RequestMatchesRestrictedPaths(const std::string &reqPath,
                                   const std::vector<std::string> &restrictedPaths);

bool AuthorizeOperation(const std::vector<ScopeRule> &rules,
                        const std::string &basePath,
                        const std::string &reqPath,
                        Access_Operation oper);

XrdAccPrivs PrivilegesForPath(const std::vector<ScopeRule> &rules,
                              const std::string &basePath,
                              const std::string &reqPath);

XrdAccPrivs AddPriv(Access_Operation op, XrdAccPrivs privs);

} // namespace XrdAccTokenScope

#endif
