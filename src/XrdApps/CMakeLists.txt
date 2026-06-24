add_library(XrdAppUtils SHARED
  XrdCpConfig.cc  XrdCpConfig.hh
  XrdCpFile.cc    XrdCpFile.hh
  XrdMpxXml.cc    XrdMpxXml.hh
)

target_link_libraries(XrdAppUtils PRIVATE XrdUtils)

set_target_properties(XrdAppUtils
  PROPERTIES
    SOVERSION ${XRootD_VERSION_MAJOR}
    VERSION ${XRootD_LIBVERSION}
)

set(XrdClProxyPlugin XrdClProxyPlugin-${PLUGIN_VERSION})

add_library(${XrdClProxyPlugin} MODULE
  XrdClProxyPlugin/ProxyPrefixPlugin.cc
  XrdClProxyPlugin/ProxyPrefixFile.cc
)

target_link_libraries(${XrdClProxyPlugin} PRIVATE XrdCl)

set(XrdClRecorder XrdClRecorder-${PLUGIN_VERSION})

add_library(${XrdClRecorder} MODULE
  XrdClRecordPlugin/XrdClRecorderPlugin.cc
)

target_link_libraries(${XrdClRecorder} PRIVATE XrdCl)

set(XrdClJournalCachePlugin XrdClJournalCachePlugin-${PLUGIN_VERSION})

add_library(${XrdClJournalCachePlugin} MODULE
  XrdClJournalCachePlugin/plugin/XrdClJournalCachePlugin.cc
  XrdClJournalCachePlugin/file/XrdClJournalCacheFile.cc
  XrdClJournalCachePlugin/file/Digest.cc
  XrdClJournalCachePlugin/file/CachePath.cc
  XrdClJournalCachePlugin/file/CacheHeaders.cc
  XrdClJournalCachePlugin/file/OriginAllowlist.cc
  XrdClJournalCachePlugin/http/ForwardingUrl.cc
  XrdClJournalCachePlugin/system/XrdClJournalCacheSystem.cc
  XrdClJournalCachePlugin/system/ListCache.cc
  XrdClJournalCachePlugin/handler/XrdClJournalCacheDirListHandler.cc
  XrdClJournalCachePlugin/handler/XrdClJournalCacheStatHandler.cc
  XrdClJournalCachePlugin/handler/XrdClJournalCacheOpenHandler.cc
  XrdClJournalCachePlugin/handler/XrdClJournalCacheOpenHandler.hh
  XrdClJournalCachePlugin/handler/XrdClJournalCacheReadHandler.hh
  XrdClJournalCachePlugin/handler/XrdClJournalCacheReadVHandler.hh
  XrdClJournalCachePlugin/cache/Journal.cc
  XrdClJournalCachePlugin/cache/Journal.hh
  XrdClJournalCachePlugin/cache/IntervalTree.hh
  XrdClJournalCachePlugin/cache/RbTree.hh
  XrdClJournalCachePlugin/cleaner/Cleaner.cc
  XrdClJournalCachePlugin/cleaner/Cleaner.hh
)

target_include_directories(${XrdClJournalCachePlugin}
  PRIVATE "${CMAKE_SOURCE_DIR}/src/XrdApps/XrdClJournalCachePlugin/")

target_link_libraries(${XrdClJournalCachePlugin}
  PRIVATE XrdCl XrdUtils OpenSSL::SSL)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
  AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.1
  AND NOT APPLE)
  target_link_libraries(${XrdClJournalCachePlugin} PRIVATE stdc++fs)
endif()

add_executable(xrdclcacheclean
  XrdClJournalCachePlugin/app/XrdClCacheCleaner.cc)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
  AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.1
  AND NOT APPLE)
  target_link_libraries(xrdclcacheclean PRIVATE stdc++fs)
endif()

if(BUILD_HTTP AND NOT XRDCL_ONLY)
  set(XrdClJournalCacheHttpExt XrdClJournalCacheHttpExt-${PLUGIN_VERSION})

  add_library(${XrdClJournalCacheHttpExt} MODULE
    XrdClJournalCachePlugin/http/JournalCacheHttpExt.cc
    XrdClJournalCachePlugin/http/ForwardingUrl.cc
    XrdClJournalCachePlugin/file/CacheHeaders.cc
    XrdClJournalCachePlugin/file/Digest.cc
    XrdClJournalCachePlugin/file/OriginAllowlist.cc
  )

  target_include_directories(${XrdClJournalCacheHttpExt}
    PRIVATE "${CMAKE_SOURCE_DIR}/src/XrdApps/XrdClJournalCachePlugin/")

  target_link_libraries(${XrdClJournalCacheHttpExt}
    PRIVATE XrdCl XrdHttpUtils XrdUtils)

  if(CURL_FOUND)
    target_compile_definitions(${XrdClJournalCacheHttpExt}
      PRIVATE JOURNALCACHE_HTTP_EXT_HAVE_CURL)
    target_link_libraries(${XrdClJournalCacheHttpExt}
      PRIVATE ${CURL_LIBRARIES})
    target_include_directories(${XrdClJournalCacheHttpExt}
      PRIVATE ${CURL_INCLUDE_DIRS})
  endif()

  install(TARGETS ${XrdClJournalCacheHttpExt}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

add_executable(xrdreplay XrdClRecordPlugin/XrdClReplay.cc)

target_link_libraries(xrdreplay
  XrdCl
  XrdUtils 
  ${CMAKE_THREAD_LIBS_INIT}
)

install(
  TARGETS
    xrdreplay
    xrdclcacheclean
    XrdAppUtils
    ${XrdClProxyPlugin}
    ${XrdClRecorder}
    ${XrdClJournalCachePlugin}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

if(NOT XRDCL_ONLY)
  add_executable(cconfig XrdAppsCconfig.cc)
  target_link_libraries(cconfig XrdUtils)

  add_executable(mpxstats XrdMpxStats.cc)
  target_link_libraries(
    mpxstats
    XrdAppUtils
    XrdUtils
    ${EXTRA_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${SOCKET_LIBRARY}
  )

  add_executable(wait41 XrdWait41.cc)
  target_link_libraries(wait41
    XrdUtils
    ${CMAKE_THREAD_LIBS_INIT}
    ${EXTRA_LIBS}
  )

  add_executable(xrdacctest XrdAccTest.cc)
  target_link_libraries(xrdacctest XrdServer XrdUtils)

  add_executable(xrdadler32 Xrdadler32.cc)
  target_link_libraries(xrdadler32
    XrdPosix
    XrdUtils
    ZLIB::ZLIB
    ${CMAKE_THREAD_LIBS_INIT}
  )

  add_executable(xrdcks XrdCks.cc)
  target_link_libraries(xrdcks XrdUtils)

  add_executable(xrdcrc32c XrdCrc32c.cc)
  target_link_libraries(xrdcrc32c XrdUtils)

  add_executable(xrdmapc XrdMapCluster.cc)
  target_link_libraries(xrdmapc XrdCl XrdUtils)

  add_executable(xrdpinls XrdPinls.cc)

  add_executable(xrdprep XrdPrep.cc)
  target_link_libraries(xrdprep XrdCl XrdUtils)

  add_executable(xrdqstats XrdQStats.cc)
  target_link_libraries(xrdqstats
    XrdCl
    XrdAppUtils
    XrdUtils
    ${EXTRA_LIBS}
  )

  install(
    TARGETS
      cconfig
      mpxstats
      wait41
      xrdacctest
      xrdadler32
      xrdcks
      xrdcrc32c
      xrdmapc
      xrdpinls
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  )
endif()
