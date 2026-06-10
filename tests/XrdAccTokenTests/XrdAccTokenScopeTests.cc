#undef NDEBUG

#include "XrdAccToken/XrdAccTokenScope.hh"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace XrdAccTokenScope;

namespace {

constexpr const char *kWlcgVer = "1.0";

std::vector<ScopeRule> ParseScopes(const std::string &claim,
                                   const std::string &wlcgVer = kWlcgVer)
{
    std::vector<ScopeRule> rules;
    EXPECT_TRUE(ParseScopeClaim(claim, wlcgVer, rules));
    return rules;
}

bool Authorized(const std::string &claim,
                const std::string &basePath,
                const std::string &path,
                Access_Operation oper,
                const std::string &wlcgVer = kWlcgVer)
{
    const auto rules = ParseScopes(claim, wlcgVer);
    return AuthorizeOperation(rules, basePath, path, oper);
}

} // namespace

TEST(XrdAccTokenScopeTests, DetectProfileFromWlcgVer)
{
    EXPECT_TRUE(IsWlcgProfile("1.0"));
    EXPECT_EQ(DetectProfile("1.0"), ScopeProfile::WLCG);
    EXPECT_FALSE(IsWlcgProfile(""));
    EXPECT_FALSE(IsWlcgProfile("   "));
    EXPECT_EQ(DetectProfile(""), ScopeProfile::SciTokens);
}

TEST(XrdAccTokenScopeTests, ParseMultipleWlcgStorageScopes)
{
    std::vector<ScopeRule> rules;
    ASSERT_TRUE(ParseScopeClaim("storage.read:/raw storage.modify:/users/alice", kWlcgVer, rules));
    ASSERT_EQ(rules.size(), 2u);
    EXPECT_EQ(rules[0].capability, StorageCapability::Read);
    EXPECT_EQ(rules[0].path, "/raw");
    EXPECT_EQ(rules[1].capability, StorageCapability::Modify);
    EXPECT_EQ(rules[1].path, "/users/alice");
}

TEST(XrdAccTokenScopeTests, WlcgScopesRequireWlcgVer)
{
    std::vector<ScopeRule> rules;
    EXPECT_FALSE(ParseScopeClaim("storage.read:/data", "", rules));
}

TEST(XrdAccTokenScopeTests, IgnoreWlcgComputeScopes)
{
    std::vector<ScopeRule> rules;
    EXPECT_FALSE(ParseScopeClaim("compute.read compute.create", kWlcgVer, rules));

    ASSERT_TRUE(ParseScopeClaim("storage.read:/data compute.read", kWlcgVer, rules));
    ASSERT_EQ(rules.size(), 1u);
    EXPECT_EQ(rules[0].path, "/data");
}

TEST(XrdAccTokenScopeTests, ReadScopeSubtreeAuthorization)
{
    const std::string claim = "storage.read:/datasets";

    EXPECT_TRUE(Authorized(claim, "", "/datasets/file1", AOP_Read));
    EXPECT_TRUE(Authorized(claim, "", "/datasets/subdir/file2", AOP_Readdir));
    EXPECT_TRUE(Authorized(claim, "", "/datasets", AOP_Stat));
    EXPECT_FALSE(Authorized(claim, "", "/other/file", AOP_Read));
    EXPECT_FALSE(Authorized(claim, "", "/datasets/file", AOP_Delete));
}

TEST(XrdAccTokenScopeTests, CreateScopeDoesNotAuthorizeRead)
{
    const std::string claim = "storage.create:/scratch";

    EXPECT_TRUE(Authorized(claim, "", "/scratch/newfile", AOP_Excl_Create));
    EXPECT_TRUE(Authorized(claim, "", "/scratch", AOP_Mkdir));
    EXPECT_FALSE(Authorized(claim, "", "/scratch/existing", AOP_Read));
    EXPECT_FALSE(Authorized(claim, "", "/scratch/existing", AOP_Delete));
}

TEST(XrdAccTokenScopeTests, ModifyScopeAuthorizesWriteOperations)
{
    const std::string claim = "storage.modify:/scratch";

    EXPECT_TRUE(Authorized(claim, "", "/scratch/file", AOP_Update));
    EXPECT_TRUE(Authorized(claim, "", "/scratch/file", AOP_Delete));
    EXPECT_TRUE(Authorized(claim, "", "/scratch/other", AOP_Create));
    EXPECT_FALSE(Authorized(claim, "", "/scratch/file", AOP_Read));
}

TEST(XrdAccTokenScopeTests, CombinedReadAndModifyOnSamePath)
{
    const std::string claim = "storage.read:/data storage.modify:/data";

    EXPECT_TRUE(Authorized(claim, "", "/data/file", AOP_Read));
    EXPECT_TRUE(Authorized(claim, "", "/data/file", AOP_Delete));
}

TEST(XrdAccTokenScopeTests, StageScopeAuthorizesTapeStagingOnly)
{
    const std::string claim = "storage.stage:/archive";

    EXPECT_TRUE(Authorized(claim, "", "/archive/tape/file", AOP_Stage));
    EXPECT_TRUE(Authorized(claim, "", "/archive", AOP_Stage));
    EXPECT_FALSE(Authorized(claim, "", "/archive/tape/file", AOP_Read));
    EXPECT_FALSE(Authorized(claim, "", "/archive/tape/file", AOP_Poll));
    EXPECT_FALSE(Authorized(claim, "", "/other/archive/file", AOP_Stage));
}

TEST(XrdAccTokenScopeTests, PollScopeAuthorizesStagingStatusOnly)
{
    const std::string claim = "storage.poll:/archive";

    EXPECT_TRUE(Authorized(claim, "", "/archive/tape/file", AOP_Poll));
    EXPECT_FALSE(Authorized(claim, "", "/archive/tape/file", AOP_Stage));
    EXPECT_FALSE(Authorized(claim, "", "/archive/tape/file", AOP_Read));
}

TEST(XrdAccTokenScopeTests, StageAndPollAreIndependent)
{
    const std::string stageOnly = "storage.stage:/archive";
    const std::string pollOnly = "storage.poll:/archive";

    EXPECT_TRUE(Authorized(stageOnly, "", "/archive/file", AOP_Stage));
    EXPECT_FALSE(Authorized(stageOnly, "", "/archive/file", AOP_Poll));

    EXPECT_TRUE(Authorized(pollOnly, "", "/archive/file", AOP_Poll));
    EXPECT_FALSE(Authorized(pollOnly, "", "/archive/file", AOP_Stage));
}

TEST(XrdAccTokenScopeTests, ParseSciTokensReadAndWriteScopes)
{
    std::vector<ScopeRule> rules;
    ASSERT_TRUE(ParseScopeClaim("read:/datasets write:/scratch", "", rules));
    ASSERT_EQ(rules.size(), 2u);
    EXPECT_EQ(rules[0].capability, StorageCapability::Read);
    EXPECT_EQ(rules[0].path, "/datasets");
    EXPECT_EQ(rules[1].capability, StorageCapability::Modify);
    EXPECT_EQ(rules[1].path, "/scratch");
}

TEST(XrdAccTokenScopeTests, SciTokensReadScopeAuthorization)
{
    const std::string claim = "read:/datasets";

    EXPECT_TRUE(Authorized(claim, "", "/datasets/file1", AOP_Read, ""));
    EXPECT_TRUE(Authorized(claim, "", "/datasets/subdir/file2", AOP_Readdir, ""));
    EXPECT_FALSE(Authorized(claim, "", "/datasets/file", AOP_Delete, ""));
}

TEST(XrdAccTokenScopeTests, SciTokensWriteScopeAuthorizesFullWrite)
{
    const std::string claim = "write:/scratch";

    EXPECT_TRUE(Authorized(claim, "", "/scratch/file", AOP_Update, ""));
    EXPECT_TRUE(Authorized(claim, "", "/scratch/file", AOP_Delete, ""));
    EXPECT_TRUE(Authorized(claim, "", "/scratch/other", AOP_Create, ""));
    EXPECT_FALSE(Authorized(claim, "", "/scratch/file", AOP_Read, ""));
}

TEST(XrdAccTokenScopeTests, SciTokensCombinedReadAndWriteOnSamePath)
{
    const std::string claim = "read:/users/alice write:/users/alice";

    EXPECT_TRUE(Authorized(claim, "", "/users/alice/file", AOP_Read, ""));
    EXPECT_TRUE(Authorized(claim, "", "/users/alice/file", AOP_Delete, ""));
}

TEST(XrdAccTokenScopeTests, IgnoreSciTokensComputeScopes)
{
    std::vector<ScopeRule> rules;
    EXPECT_FALSE(ParseScopeClaim("queue execute", "", rules));

    ASSERT_TRUE(ParseScopeClaim("read:/data queue execute", "", rules));
    ASSERT_EQ(rules.size(), 1u);
    EXPECT_EQ(rules[0].path, "/data");
}

TEST(XrdAccTokenScopeTests, SciTokensScopesIgnoredWhenWlcgVerPresent)
{
    std::vector<ScopeRule> rules;
    EXPECT_FALSE(ParseScopeClaim("read:/data write:/scratch", kWlcgVer, rules));
}

TEST(XrdAccTokenScopeTests, ParentDirectoryStatAllowed)
{
    const std::string claim = "storage.read:/data/subdir/file";

    EXPECT_TRUE(Authorized(claim, "", "/data", AOP_Stat));
    EXPECT_TRUE(Authorized(claim, "", "/data/subdir", AOP_Stat));
    EXPECT_FALSE(Authorized(claim, "", "/data", AOP_Read));
}

TEST(XrdAccTokenScopeTests, BasePathPrefixApplied)
{
    const std::string claim = "storage.read:/data";

    EXPECT_TRUE(Authorized(claim, "/atlas", "/atlas/data/file", AOP_Read));
    EXPECT_FALSE(Authorized(claim, "/atlas", "/data/file", AOP_Read));
}

TEST(XrdAccTokenScopeTests, ResolveBasePathAppendsEntityPathToConfig)
{
    EXPECT_EQ(ResolveBasePath("/atlas", "/eos"), "/atlas/eos");
    EXPECT_EQ(ResolveBasePath("/atlas", ""), "/atlas");
    EXPECT_EQ(ResolveBasePath("", "/eos"), "/eos");
    EXPECT_EQ(ResolveBasePath("", ""), "");
    EXPECT_EQ(ResolveBasePath("/atlas", "/eos/"), "/atlas/eos");
}

TEST(XrdAccTokenScopeTests, ResolveBasePathRejectsInvalidEntityPath)
{
    EXPECT_TRUE(ResolveBasePath("/atlas", "relative").empty());
}

TEST(XrdAccTokenScopeTests, CombinedConfigAndEntityBasePathAuthorizesUnderBoth)
{
    const auto rules = ParseScopes("storage.read:/data");
    const std::string base = ResolveBasePath("/atlas", "/eos");

    EXPECT_TRUE(AuthorizeOperation(rules, base, "/atlas/eos/data/file", AOP_Read));
    EXPECT_FALSE(AuthorizeOperation(rules, base, "/atlas/data/file", AOP_Read));
}

TEST(XrdAccTokenScopeTests, ParseRestrictedPathsJsonAcceptsArray)
{
    std::vector<std::string> paths;
    std::string emsg;
    ASSERT_TRUE(ParseRestrictedPathsJson(R"(["/public","/shared"])", paths, emsg)) << emsg;
    ASSERT_EQ(paths.size(), 2u);
    EXPECT_EQ(paths[0], "/public");
    EXPECT_EQ(paths[1], "/shared");
}

TEST(XrdAccTokenScopeTests, RequestMatchesRestrictedPaths)
{
    const std::vector<std::string> restricted = {"/public", "/shared"};

    EXPECT_TRUE(RequestMatchesRestrictedPaths("/public", restricted));
    EXPECT_TRUE(RequestMatchesRestrictedPaths("/public/file", restricted));
    EXPECT_TRUE(RequestMatchesRestrictedPaths("/shared/dir/file", restricted));
    EXPECT_FALSE(RequestMatchesRestrictedPaths("/private/file", restricted));
    EXPECT_FALSE(RequestMatchesRestrictedPaths("/publicx/file", restricted));
}

TEST(XrdAccTokenScopeTests, PrivilegesForPathIncludesStageAndPoll)
{
    const auto rules = ParseScopes("storage.stage:/archive storage.poll:/archive");
    const auto privs = PrivilegesForPath(rules, "", "/archive/file");

    EXPECT_NE(privs & XrdAccPriv_Stage, 0);
    EXPECT_NE(privs & XrdAccPriv_Poll, 0);
    EXPECT_EQ(privs & XrdAccPriv_Read, 0);
}

TEST(XrdAccTokenScopeTests, MakeCanonicalCollapsesDotSegments)
{
    std::string out;
    ASSERT_TRUE(MakeCanonical("/atlas/./data/../reco", out));
    EXPECT_EQ(out, "/atlas/reco");
}
