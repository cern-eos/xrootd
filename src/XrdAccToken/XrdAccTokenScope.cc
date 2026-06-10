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

#include "XrdAccToken/XrdAccTokenScope.hh"

#include "XrdOuc/XrdOucPrivateUtils.hh"

#include <cctype>
#include <sstream>
#include <string_view>

namespace XrdAccTokenScope {

namespace {

std::string TrimCopy(const std::string &s)
{
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

bool ParseWlcgCapability(std::string_view cap, StorageCapability &out)
{
    if (cap == "read") out = StorageCapability::Read;
    else if (cap == "create") out = StorageCapability::Create;
    else if (cap == "modify") out = StorageCapability::Modify;
    else if (cap == "stage") out = StorageCapability::Stage;
    else if (cap == "poll") out = StorageCapability::Poll;
    else return false;
    return true;
}

bool ParseWlcgScopeToken(const std::string &token, ScopeRule &rule)
{
    const auto colon = token.find(':');
    if (colon == std::string::npos) return false;

    const std::string prefix = TrimCopy(std::string(token.substr(0, colon)));
    const std::string path = TrimCopy(std::string(token.substr(colon + 1)));
    if (prefix.empty() || path.empty()) return false;

    const std::string_view storage_prefix = "storage.";
    if (prefix.compare(0, storage_prefix.size(), storage_prefix) != 0) return false;

    if (!ParseWlcgCapability(std::string_view(prefix).substr(storage_prefix.size()),
                             rule.capability))
        return false;

    if (!MakeCanonical(path, rule.path)) return false;
    return true;
}

bool ParseSciTokensScopeToken(const std::string &token, ScopeRule &rule)
{
    const auto colon = token.find(':');
    if (colon == std::string::npos) return false;

    const std::string prefix = TrimCopy(std::string(token.substr(0, colon)));
    const std::string path = TrimCopy(std::string(token.substr(colon + 1)));
    if (prefix.empty() || path.empty()) return false;

    if (prefix == "read") rule.capability = StorageCapability::Read;
    else if (prefix == "write") rule.capability = StorageCapability::Modify;
    else return false;

    if (!MakeCanonical(path, rule.path)) return false;
    return true;
}

bool ParseScopeClaimForProfile(const std::string &scopeClaim,
                               ScopeProfile profile,
                               std::vector<ScopeRule> &rules)
{
    rules.clear();
    std::istringstream iss(scopeClaim);
    std::string token;
    while (iss >> token) {
        ScopeRule rule;
        const bool parsed = (profile == ScopeProfile::WLCG)
            ? ParseWlcgScopeToken(token, rule)
            : ParseSciTokensScopeToken(token, rule);
        if (parsed) rules.push_back(std::move(rule));
    }
    return !rules.empty();
}

} // namespace

bool MakeCanonical(const std::string &path, std::string &result)
{
    if (path.empty() || path[0] != '/') return false;

    size_t pos = 0;
    std::vector<std::string> components;
    do {
        while (path.size() > pos && path[pos] == '/') pos++;
        auto next_pos = path.find_first_of('/', pos);
        auto next_component = path.substr(pos, next_pos - pos);
        pos = next_pos;
        if (next_component.empty() || next_component == ".") continue;
        if (next_component == "..") {
            if (!components.empty()) components.pop_back();
        } else {
            components.emplace_back(std::move(next_component));
        }
    } while (pos != std::string::npos);

    if (components.empty()) {
        result = "/";
        return true;
    }

    std::stringstream ss;
    for (const auto &comp : components) ss << "/" << comp;
    result = ss.str();
    return true;
}

bool IsWlcgProfile(const std::string &wlcgVer)
{
    return !TrimCopy(wlcgVer).empty();
}

ScopeProfile DetectProfile(const std::string &wlcgVer)
{
    return IsWlcgProfile(wlcgVer) ? ScopeProfile::WLCG : ScopeProfile::SciTokens;
}

bool ParseScopeClaim(const std::string &scopeClaim,
                     const std::string &wlcgVer,
                     std::vector<ScopeRule> &rules)
{
    return ParseScopeClaimForProfile(scopeClaim, DetectProfile(wlcgVer), rules);
}

bool OperationMatchesCapability(Access_Operation oper, StorageCapability cap)
{
    switch (cap) {
        case StorageCapability::Read:
            return oper == AOP_Read || oper == AOP_Readdir || oper == AOP_Stat;
        case StorageCapability::Create:
            return oper == AOP_Excl_Create || oper == AOP_Mkdir || oper == AOP_Rename
                || oper == AOP_Excl_Insert || oper == AOP_Stat;
        case StorageCapability::Modify:
            return oper == AOP_Create || oper == AOP_Mkdir || oper == AOP_Rename
                || oper == AOP_Insert || oper == AOP_Update || oper == AOP_Chmod
                || oper == AOP_Stat || oper == AOP_Delete;
        case StorageCapability::Stage:
            return oper == AOP_Stage;
        case StorageCapability::Poll:
            return oper == AOP_Poll;
    };
    return false;
}

bool PathMatchesScope(const std::string &scopePath, const std::string &reqPath,
                      Access_Operation oper)
{
    if (is_subdirectory(scopePath, reqPath)) return true;

    if (oper == AOP_Stat || oper == AOP_Mkdir) {
        return is_subdirectory(reqPath, scopePath);
    }
    return false;
}

std::string ApplyBasePath(const std::string &basePath, const std::string &scopePath)
{
    if (basePath.empty()) return scopePath;
    std::string combined;
    if (!MakeCanonical(basePath + scopePath, combined)) return scopePath;
    return combined;
}

std::string ResolveBasePath(const std::string &configBasePath,
                            const std::string &entityBasePath)
{
    std::string entity = TrimCopy(entityBasePath);
    if (!entity.empty()) {
        std::string canonical;
        if (!MakeCanonical(entity, canonical)) return {};
        entity = canonical;
    }
    if (configBasePath.empty()) return entity;
    if (entity.empty()) return configBasePath;
    return ApplyBasePath(configBasePath, entity);
}

bool ParseRestrictedPathsJson(const std::string &json,
                              std::vector<std::string> &paths,
                              std::string &emsg)
{
    paths.clear();
    emsg.clear();
    const std::string s = TrimCopy(json);
    if (s.empty() || s == "[]") return true;
    if (s.size() < 2 || s.front() != '[' || s.back() != ']') {
        emsg = "restricted_path must be a JSON array";
        return false;
    }

    size_t i = 1;
    while (i < s.size() - 1) {
        while (i < s.size() - 1
            && (s[i] == ',' || std::isspace(static_cast<unsigned char>(s[i])))) {
            ++i;
        }
        if (i >= s.size() - 1) break;
        if (s[i] != '"') {
            emsg = "restricted_path JSON array must contain strings";
            return false;
        }
        ++i;
        std::string path;
        while (i < s.size() - 1 && s[i] != '"') {
            path.push_back(s[i++]);
        }
        if (i >= s.size() - 1 || s[i] != '"') {
            emsg = "restricted_path JSON array contains an unterminated string";
            return false;
        }
        ++i;
        std::string canonical;
        if (!MakeCanonical(path, canonical)) {
            emsg = "restricted_path contains an invalid absolute path";
            return false;
        }
        paths.push_back(canonical);
    }
    return true;
}

bool RequestMatchesRestrictedPaths(const std::string &reqPath,
                                   const std::vector<std::string> &restrictedPaths)
{
    if (restrictedPaths.empty()) return false;
    for (const auto &restrictedPath : restrictedPaths) {
        if (is_subdirectory(restrictedPath, reqPath)) return true;
    }
    return false;
}

bool AuthorizeOperation(const std::vector<ScopeRule> &rules,
                        const std::string &basePath,
                        const std::string &reqPath,
                        Access_Operation oper)
{
    for (const auto &rule : rules) {
        const auto effectivePath = ApplyBasePath(basePath, rule.path);
        if (!OperationMatchesCapability(oper, rule.capability)) continue;
        if (!PathMatchesScope(effectivePath, reqPath, oper)) continue;
        return true;
    }
    return false;
}

XrdAccPrivs AddPriv(Access_Operation op, XrdAccPrivs privs)
{
    int new_privs = privs;
    switch (op) {
        case AOP_Any:
            break;
        case AOP_Chmod:
            new_privs |= static_cast<int>(XrdAccPriv_Chmod);
            break;
        case AOP_Chown:
            new_privs |= static_cast<int>(XrdAccPriv_Chown);
            break;
        case AOP_Excl_Create: // fallthrough
        case AOP_Create:
            new_privs |= static_cast<int>(XrdAccPriv_Create);
            break;
        case AOP_Delete:
            new_privs |= static_cast<int>(XrdAccPriv_Delete);
            break;
        case AOP_Excl_Insert: // fallthrough
        case AOP_Insert:
            new_privs |= static_cast<int>(XrdAccPriv_Insert);
            break;
        case AOP_Lock:
            new_privs |= static_cast<int>(XrdAccPriv_Lock);
            break;
        case AOP_Mkdir:
            new_privs |= static_cast<int>(XrdAccPriv_Mkdir);
            break;
        case AOP_Read:
            new_privs |= static_cast<int>(XrdAccPriv_Read);
            break;
        case AOP_Readdir:
            new_privs |= static_cast<int>(XrdAccPriv_Readdir);
            break;
        case AOP_Rename:
            new_privs |= static_cast<int>(XrdAccPriv_Rename);
            break;
        case AOP_Stat:
            new_privs |= static_cast<int>(XrdAccPriv_Lookup);
            break;
        case AOP_Update:
            new_privs |= static_cast<int>(XrdAccPriv_Update);
            break;
        case AOP_Stage:
            new_privs |= static_cast<int>(XrdAccPriv_Stage);
            break;
        case AOP_Poll:
            new_privs |= static_cast<int>(XrdAccPriv_Poll);
            break;
    };
    return static_cast<XrdAccPrivs>(new_privs);
}

XrdAccPrivs PrivilegesForPath(const std::vector<ScopeRule> &rules,
                              const std::string &basePath,
                              const std::string &reqPath)
{
    static const Access_Operation allOps[] = {
        AOP_Read, AOP_Readdir, AOP_Stat, AOP_Excl_Create, AOP_Mkdir,
        AOP_Rename, AOP_Excl_Insert, AOP_Create, AOP_Insert, AOP_Update,
        AOP_Chmod, AOP_Delete, AOP_Stage, AOP_Poll
    };

    XrdAccPrivs privs = XrdAccPriv_None;
    for (const auto &rule : rules) {
        const auto effectivePath = ApplyBasePath(basePath, rule.path);
        for (const auto checkOp : allOps) {
            if (!OperationMatchesCapability(checkOp, rule.capability)) continue;
            if (!PathMatchesScope(effectivePath, reqPath, checkOp)) continue;
            privs = AddPriv(checkOp, privs);
        }
    }
    return privs;
}

} // namespace XrdAccTokenScope
