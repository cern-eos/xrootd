//------------------------------------------------------------------------------
// Copyright (c) 2011-2012 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
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

#include "TestEnv.hh"
#include "XrdCl/XrdClOptimizers.hh"

#include <cstdlib>
#include <fstream>
#include <string>

namespace {

bool ImportPortsEnvFile( XrdCl::Env *env, const char *path )
{
  std::ifstream in( path );
  if( !in.is_open() )
    return false;

  static const struct
  {
    const char *fileKey;
    const char *envKey;
  } mapping[] =
  {
    { "XRDTEST_MAINSERVERURL",    "MainServerURL" },
    { "XRDTEST_DISKSERVERURL",    "DiskServerURL" },
    { "XRDTEST_MANAGER1URL",      "Manager1URL" },
    { "XRDTEST_MANAGER2URL",      "Manager2URL" },
    { "XRDTEST_SERVER1URL",       "Server1URL" },
    { "XRDTEST_SERVER2URL",       "Server2URL" },
    { "XRDTEST_SERVER3URL",       "Server3URL" },
    { "XRDTEST_SERVER4URL",       "Server4URL" },
    { "XRDTEST_DATAPATH",         "DataPath" },
    { "XRDTEST_LOCALFILE",        "LocalFile" },
    { "XRDTEST_REMOTEFILE",       "RemoteFile" },
    { "XRDTEST_MULTIIPSERVERURL", "MultiIPServerURL" },
    { "XRDTEST_LOCALDATAPATH",    "LocalDataPath" },
  };

  std::string line;
  while( std::getline( in, line ) )
  {
    if( line.empty() || line[0] == '#' )
      continue;

    const auto eq = line.find( '=' );
    if( eq == std::string::npos )
      continue;

    const std::string key = line.substr( 0, eq );
    const std::string val = line.substr( eq + 1 );

    for( const auto &entry : mapping )
    {
      if( key == entry.fileKey )
      {
        env->PutString( entry.envKey, val );
        break;
      }
    }
  }

  return true;
}

void LoadPortsEnvFile( XrdCl::Env *env )
{
  if( const char *path = getenv( "XRDTEST_PORTS_ENV" ) )
    if( *path && ImportPortsEnvFile( env, path ) )
      return;

#ifdef XRDTEST_DEFAULT_PORTS_ENV
  if( ImportPortsEnvFile( env, XRDTEST_DEFAULT_PORTS_ENV ) )
    return;
#endif

  ImportPortsEnvFile( env, "../cluster/ports.env" );
}

} // anonymous namespace

namespace XrdClTests {

XrdSysMutex     TestEnv::sEnvMutex;
XrdCl::Env *TestEnv::sEnv       = 0;
XrdCl::Log *TestEnv::sLog       = 0;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TestEnv::TestEnv()
{
  PutString( "MainServerURL",    "localhost:10940" );
  PutString( "Manager1URL",      "localhost:10941" );
  PutString( "Manager2URL",      "localhost:10942" );
  PutString( "Server1URL",       "localhost:10943" );
  PutString( "Server2URL",       "localhost:10944" );
  PutString( "Server3URL",       "localhost:10945" );
  PutString( "Server4URL",       "localhost:10946" );
  PutString( "DiskServerURL",    "localhost:10940" );
  PutString( "DataPath",         "/data"         );
  PutString( "RemoteFile",       "/data/cb4aacf1-6f28-42f2-b68a-90a73460f424.dat" );
  PutString( "LocalFile",        "/data/testFile.dat" );
  PutString( "MultiIPServerURL", "multiip:1099" );
#ifdef XRDTEST_DEFAULT_LOCAL_DATA_PATH
  PutString( "LocalDataPath",    XRDTEST_DEFAULT_LOCAL_DATA_PATH );
#else
  PutString( "LocalDataPath",    "../cluster/data" );
#endif
  LoadPortsEnvFile( this );
  ImportString( "MainServerURL",    "XRDTEST_MAINSERVERURL" );
  ImportString( "DiskServerURL",    "XRDTEST_DISKSERVERURL" );
  ImportString( "Manager1URL",      "XRDTEST_MANAGER1URL" );
  ImportString( "Manager2URL",      "XRDTEST_MANAGER2URL" );
  ImportString( "Server1URL",       "XRDTEST_SERVER1URL" );
  ImportString( "Server2URL",       "XRDTEST_SERVER2URL" );
  ImportString( "Server3URL",       "XRDTEST_SERVER3URL" );
  ImportString( "Server4URL",       "XRDTEST_SERVER4URL" );
  ImportString( "DataPath",         "XRDTEST_DATAPATH" );
  ImportString( "LocalFile",        "XRDTEST_LOCALFILE" );
  ImportString( "RemoteFile",       "XRDTEST_REMOTEFILE" );
  ImportString( "MultiIPServerURL", "XRDTEST_MULTIIPSERVERURL" );
}

//------------------------------------------------------------------------------
// Get default client environment
//------------------------------------------------------------------------------
XrdCl::Env *TestEnv::GetEnv()
{
  if( !sEnv )
  {
    XrdSysMutexHelper scopedLock( sEnvMutex );
    if( sEnv )
      return sEnv;
    sEnv = new TestEnv();
  }
  return sEnv;
}

XrdCl::Log *TestEnv::GetLog()
{
  //----------------------------------------------------------------------------
  // This is actually thread safe because it is first called from
  // a static initializer in a thread safe context
  //----------------------------------------------------------------------------
  if( unlikely( !sLog ) )
    sLog = new XrdCl::Log();
  return sLog;
}

//------------------------------------------------------------------------------
// Release the environment
//------------------------------------------------------------------------------
void TestEnv::Release()
{
  delete sEnv;
  sEnv = 0;
//  delete sLog;
//  sLog = 0;
}
}

//------------------------------------------------------------------------------
// Finalizer
//------------------------------------------------------------------------------
namespace
{
  static struct EnvInitializer
  {
    //--------------------------------------------------------------------------
    // Initializer
    //--------------------------------------------------------------------------
    EnvInitializer()
    {
      using namespace XrdCl;
      Log *log = XrdClTests::TestEnv::GetLog();
      char *level = getenv( "XRDTEST_LOGLEVEL" );
      if( level )
        log->SetLevel( level );
    }

    //--------------------------------------------------------------------------
    // Finalizer
    //--------------------------------------------------------------------------
    ~EnvInitializer()
    {
      XrdClTests::TestEnv::Release();
    }
  } initializer;
}
