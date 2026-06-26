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

#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucGatherConf.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdVersion.hh"

#include <cctype>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

XrdVERSIONINFO(XrdAccAuthorizeObject, XrdAccToken);
XrdVERSIONINFO(XrdAccAuthorizeObjAdd, XrdAccToken);

using namespace XrdAccTokenScope;

namespace {

enum LogMask {
    Debug   = 0x01,
    Info    = 0x02,
    Warning = 0x04,
    Error   = 0x08,
    All     = 0xff
};

enum class AuthzBehavior {
    PASSTHROUGH,
    ALLOW,
    DENY
};

const char *OpToName(Access_Operation op)
{
    switch (op) {
        case AOP_Any: return "any";
        case AOP_Chmod: return "chmod";
        case AOP_Chown: return "chown";
        case AOP_Create: return "create";
        case AOP_Excl_Create: return "excl_create";
        case AOP_Delete: return "del";
        case AOP_Excl_Insert: return "excl_insert";
        case AOP_Insert: return "insert";
        case AOP_Lock: return "lock";
        case AOP_Mkdir: return "mkdir";
        case AOP_Read: return "read";
        case AOP_Readdir: return "dir";
        case AOP_Rename: return "mv";
        case AOP_Stat: return "stat";
        case AOP_Update: return "update";
        case AOP_Stage: return "stage";
        case AOP_Poll: return "poll";
    };
    return "unknown";
}

bool ParseCanonicalPaths(const std::string &path, std::vector<std::string> &results)
{
    size_t pos = 0;
    do {
        while (path.size() > pos && (path[pos] == ',' || path[pos] == ' ')) pos++;
        auto next_pos = path.find_first_of(", ", pos);
        auto next_path = path.substr(pos, next_pos - pos);
        pos = next_pos;
        if (!next_path.empty()) {
            std::string canonical_path;
            if (MakeCanonical(next_path, canonical_path)) {
                results.emplace_back(std::move(canonical_path));
            }
        }
    } while (pos != std::string::npos);
    return !results.empty();
}

std::string TrimCopy(const std::string &s)
{
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

bool ParseAuthzBehavior(const std::string &value, AuthzBehavior &behavior)
{
    if (value.empty()) return false;
    if (!strcasecmp(value.c_str(), "passthrough")) {
        behavior = AuthzBehavior::PASSTHROUGH;
        return true;
    }
    if (!strcasecmp(value.c_str(), "allow")) {
        behavior = AuthzBehavior::ALLOW;
        return true;
    }
    if (!strcasecmp(value.c_str(), "deny")) {
        behavior = AuthzBehavior::DENY;
        return true;
    }
    return false;
}

bool ParsePluginParm(const std::string &parm, const std::string &key, std::string &value)
{
    size_t pos = 0;
    do {
        while (parm.size() > pos && (parm[pos] == ' ' || parm[pos] == ',')) pos++;
        auto next_pos = parm.find_first_of(", ", pos);
        auto arg = parm.substr(pos, next_pos - pos);
        pos = next_pos;
        if (arg.empty()) continue;

        auto eq = arg.find('=');
        if (eq == std::string::npos) continue;
        auto argKey = TrimCopy(arg.substr(0, eq));
        if (argKey != key) continue;
        value = TrimCopy(arg.substr(eq + 1));
        return true;
    } while (pos != std::string::npos);
    return false;
}

std::string LogMaskToString(int mask)
{
    if (mask == LogMask::All) return "all";

    bool has_entry = false;
    std::stringstream ss;
    if (mask & LogMask::Debug) {
        ss << "debug";
        has_entry = true;
    }
    if (mask & LogMask::Info) {
        ss << (has_entry ? ", " : "") << "info";
        has_entry = true;
    }
    if (mask & LogMask::Warning) {
        ss << (has_entry ? ", " : "") << "warning";
        has_entry = true;
    }
    if (mask & LogMask::Error) {
        ss << (has_entry ? ", " : "") << "error";
    }
    return ss.str();
}

} // namespace

class XrdAccToken : public XrdAccAuthorize
{
public:
    XrdAccToken(XrdSysLogger *lp, const char *parms, XrdAccAuthorize *chain)
        : m_chain(chain),
          m_parms(parms ? parms : ""),
          m_log(lp, "acctoken_")
    {
        m_log.Say("++++++ XrdAccToken: Initialized token scope authorization.");
        if (!Config()) {
            throw std::runtime_error("Failed to configure XrdAccToken authorization.");
        }
    }

    ~XrdAccToken() override = default;

    XrdAccPrivs Access(const XrdSecEntity *Entity,
                       const char *path,
                       const Access_Operation oper,
                       XrdOucEnv *env) override
    {
        std::string unused;
        return Access(Entity, path, oper, unused, env);
    }

    XrdAccPrivs Access(const XrdSecEntity *Entity,
                       const char *path,
                       const Access_Operation oper,
                       std::string &eInfo,
                       XrdOucEnv *env) override
    {
        (void)env;
        if (!Entity || !Entity->eaAPI) return OnMissing(Entity, path, oper, env);

        std::string scopeClaim;
        if (!Entity->eaAPI->Get(m_scopeAttr, scopeClaim) || scopeClaim.empty()) {
            return OnMissing(Entity, path, oper, env);
        }

        std::string wlcgVer;
        Entity->eaAPI->Get(m_wlcgVerAttr, wlcgVer);

        std::string entityBasePath;
        Entity->eaAPI->Get("base_path", entityBasePath);
        const std::string effectiveBase = ResolveBasePath(m_basePath, entityBasePath);
        if (!entityBasePath.empty() && effectiveBase.empty()) {
            eInfo = "Invalid base_path entity attribute";
            return XrdAccPriv_None;
        }

        std::vector<std::string> restrictedPaths;
        std::string restrictedJson;
        if (Entity->eaAPI->Get("restricted_path", restrictedJson)
        &&  !restrictedJson.empty()) {
            if (!ParseRestrictedPathsJson(restrictedJson, restrictedPaths, eInfo)) {
                return XrdAccPriv_None;
            }
        }

        std::vector<ScopeRule> rules;
        if (!ParseScopeClaim(scopeClaim, wlcgVer, rules)) {
            const auto profile = DetectProfile(wlcgVer);
            m_log.Log(LogMask::Warning, "Access", profile == ScopeProfile::WLCG
                      ? "No valid WLCG storage scopes in"
                      : "No valid SciTokens storage scopes in",
                      m_scopeAttr.c_str());
            eInfo = (profile == ScopeProfile::WLCG)
                ? "No valid WLCG storage scopes in token"
                : "No valid SciTokens storage scopes in token";
            return XrdAccPriv_None;
        }

        std::string reqPath;
        if (!MakeCanonical(path, reqPath)) {
            eInfo = "Invalid request path";
            return XrdAccPriv_None;
        }

        if (!restrictedJson.empty()
        &&  !RequestMatchesRestrictedPaths(reqPath, restrictedPaths)) {
            eInfo = "Request path is outside restricted paths";
            return XrdAccPriv_None;
        }

        if (oper == AOP_Any) {
            return PrivilegesForPath(rules, effectiveBase, reqPath);
        }

        if (AuthorizeOperation(rules, effectiveBase, reqPath, oper)) {
            if (m_log.getMsgMask() & LogMask::Debug) {
                std::stringstream ss;
                ss << "Grant " << (IsWlcgProfile(wlcgVer) ? "WLCG" : "SciTokens")
                   << " authorization for operation=" << OpToName(oper)
                   << ", path=" << reqPath;
                m_log.Log(LogMask::Debug, "Access", ss.str().c_str());
            }
            return AddPriv(oper, XrdAccPriv_None);
        }

        eInfo = "Token scope does not authorize this operation";
        return XrdAccPriv_None;
    }

    int Audit(const int accok,
              const XrdSecEntity *Entity,
              const char *path,
              const Access_Operation oper,
              XrdOucEnv *Env) override
    {
        (void)accok;
        (void)Entity;
        (void)path;
        (void)oper;
        (void)Env;
        return 0;
    }

    int Test(const XrdAccPrivs priv, const Access_Operation oper) override
    {
        return m_chain ? m_chain->Test(priv, oper) : 0;
    }

private:
    XrdAccPrivs OnMissing(const XrdSecEntity *Entity, const char *path,
                          const Access_Operation oper, XrdOucEnv *env)
    {
        switch (m_authzBehavior) {
            case AuthzBehavior::PASSTHROUGH:
                return m_chain ? m_chain->Access(Entity, path, oper, env)
                               : XrdAccPriv_None;
            case AuthzBehavior::ALLOW:
                return AddPriv(oper, XrdAccPriv_None);
            case AuthzBehavior::DENY:
                return XrdAccPriv_None;
        }
        return XrdAccPriv_None;
    }

    bool Config()
    {
        m_log.setMsgMask(LogMask::Error | LogMask::Warning);

        ParsePluginParm(m_parms, "base_path", m_basePath);
        ParsePluginParm(m_parms, "scope_attr", m_scopeAttr);
        ParsePluginParm(m_parms, "wlcg_ver_attr", m_wlcgVerAttr);
        ParsePluginParm(m_parms, "onmissing", m_onmissingParm);

        if (m_scopeAttr.empty()) m_scopeAttr = "token.scope";
        if (m_wlcgVerAttr.empty()) m_wlcgVerAttr = "token.wlcg.ver";

        if (!m_onmissingParm.empty()) {
            if (!ParseAuthzBehavior(m_onmissingParm, m_authzBehavior)) {
                m_log.Emsg("Config", "Invalid onmissing value in authlib parms:",
                           m_onmissingParm.c_str());
                return false;
            }
        }

        char *config_filename = nullptr;
        if (!XrdOucEnv::Import("XRDCONFIGFN", config_filename)) return false;

        XrdOucGatherConf acctoken_conf("acctoken.", &m_log);
        int result = acctoken_conf.Gather(config_filename, XrdOucGatherConf::trim_lines);
        if (result < 0) {
            m_log.Emsg("Config", -result, "parsing config file", config_filename);
            return false;
        }

        char *val;
        while (acctoken_conf.GetLine()) {
            acctoken_conf.GetToken();
            if (!(val = acctoken_conf.GetToken())) continue;

            if (!strcmp(val, "trace")) {
                if (!ParseTrace(acctoken_conf)) return false;
            } else if (!strcmp(val, "basepath")) {
                if (!(val = acctoken_conf.GetToken()) || !*val) {
                    m_log.Emsg("Config", "acctoken.basepath requires a value.");
                    return false;
                }
                std::vector<std::string> paths;
                if (!ParseCanonicalPaths(val, paths)) {
                    m_log.Emsg("Config", "acctoken.basepath contains no valid paths:", val);
                    return false;
                }
                m_basePath = paths.front();
            } else if (!strcmp(val, "scopeattr")) {
                if (!(val = acctoken_conf.GetToken()) || !*val) {
                    m_log.Emsg("Config", "acctoken.scopeattr requires a value.");
                    return false;
                }
                m_scopeAttr = val;
            } else if (!strcmp(val, "wlcgverattr")) {
                if (!(val = acctoken_conf.GetToken()) || !*val) {
                    m_log.Emsg("Config", "acctoken.wlcgverattr requires a value.");
                    return false;
                }
                m_wlcgVerAttr = val;
            } else if (!strcmp(val, "onmissing")) {
                if (!(val = acctoken_conf.GetToken()) || !*val) {
                    m_log.Emsg("Config", "acctoken.onmissing requires a value.");
                    return false;
                }
                if (!ParseAuthzBehavior(val, m_authzBehavior)) {
                    m_log.Emsg("Config", "acctoken.onmissing is invalid:", val);
                    return false;
                }
            } else {
                m_log.Emsg("Config", "acctoken encountered unknown directive:", val);
                return false;
            }
        }

        if (!m_basePath.empty()) {
            std::string canonical;
            if (!MakeCanonical(m_basePath, canonical)) {
                m_log.Emsg("Config", "acctoken.basepath is not a valid path:",
                           m_basePath.c_str());
                return false;
            }
            m_basePath = canonical;
        }

        m_log.Emsg("Config", "Logging levels enabled -", LogMaskToString(m_log.getMsgMask()).c_str());
        if (!m_basePath.empty()) {
            m_log.Say("Config using base path ", m_basePath.c_str());
        }
        m_log.Say("Config reading scopes from entity attribute ", m_scopeAttr.c_str());
        m_log.Say("Config reading WLCG profile marker from ", m_wlcgVerAttr.c_str());
        return true;
    }

    bool ParseTrace(XrdOucGatherConf &conf)
    {
        char *val = conf.GetToken();
        if (!val || !*val) {
            m_log.Emsg("Config", "acctoken.trace requires an argument.");
            return false;
        }
        do {
            if (!strcmp(val, "all")) m_log.setMsgMask(m_log.getMsgMask() | LogMask::All);
            else if (!strcmp(val, "error")) m_log.setMsgMask(m_log.getMsgMask() | LogMask::Error);
            else if (!strcmp(val, "warning")) m_log.setMsgMask(m_log.getMsgMask() | LogMask::Warning);
            else if (!strcmp(val, "info")) m_log.setMsgMask(m_log.getMsgMask() | LogMask::Info);
            else if (!strcmp(val, "debug")) m_log.setMsgMask(m_log.getMsgMask() | LogMask::Debug);
            else if (!strcmp(val, "none")) m_log.setMsgMask(0);
            else {
                m_log.Emsg("Config", "acctoken.trace encountered unknown directive:", val);
                return false;
            }
        } while ((val = conf.GetToken()));
        return true;
    }

    XrdAccAuthorize *m_chain;
    std::string m_parms;
    std::string m_basePath;
    std::string m_scopeAttr;
    std::string m_wlcgVerAttr;
    std::string m_onmissingParm;
    AuthzBehavior m_authzBehavior{AuthzBehavior::PASSTHROUGH};
    XrdSysError m_log;
};

XrdAccToken *accToken = nullptr;

void InitAccToken(XrdSysLogger *lp, const char *cfn, const char *parm,
                  XrdAccAuthorize *accP)
{
    (void)cfn;
    try {
        accToken = new XrdAccToken(lp, parm, accP);
    } catch (std::exception &) {
        accToken = nullptr;
    }
}

extern "C" {

XrdAccAuthorize *XrdAccAuthorizeObjAdd(XrdSysLogger *lp,
                                       const char *cfn,
                                       const char *parm,
                                       XrdOucEnv *envP,
                                       XrdAccAuthorize *accP)
{
    (void)envP;
    if (!accToken) InitAccToken(lp, cfn, parm, accP);
    return accToken;
}

XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp,
                                       const char *cfn,
                                       const char *parm)
{
    if (!accToken) InitAccToken(lp, cfn, parm, nullptr);
    return accToken;
}

XrdAccAuthorize *XrdAccAuthorizeObject2(XrdSysLogger *lp,
                                        const char *cfn,
                                        const char *parm,
                                        XrdOucEnv *envP)
{
    (void)envP;
    if (!accToken) InitAccToken(lp, cfn, parm, nullptr);
    return accToken;
}

}
