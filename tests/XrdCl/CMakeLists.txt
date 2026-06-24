add_executable(xrdcl-unit-tests
  XrdClEnv.cc
  XrdClURL.cc
  XrdClPoller.cc
  XrdClSocket.cc
  XrdClUtilsTest.cc
  XrdClJournalCache.cc
  JournalCacheLayoutStub.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/cache/Journal.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/CachePath.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/CacheHeaders.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/Digest.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/OriginAllowlist.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/ExternalRedirect.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/PolicyConfig.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/file/PolicyRuntime.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/daemon/XjcdState.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/daemon/XjcdRender.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/http/ForwardingUrl.cc
  ../../src/XrdApps/XrdClJournalCachePlugin/system/ListCache.cc
  )

target_link_libraries(xrdcl-unit-tests
  XrdTestUtils GTest::gtest GTest::gtest_main OpenSSL::Crypto)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
  AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.1
  AND NOT APPLE)
  target_link_libraries(xrdcl-unit-tests stdc++fs)
endif()

target_include_directories(xrdcl-unit-tests
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/XrdApps/XrdClJournalCachePlugin
)

gtest_discover_tests(xrdcl-unit-tests TEST_PREFIX XrdCl:: 
  PROPERTIES DISCOVERY_TIMEOUT 10)

if(NOT ENABLE_SERVER_TESTS)
  return()
endif()

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp")

add_executable(xrdcl-cluster-tests
  IdentityPlugIn.cc
  # XrdClFileTest.cc
  # XrdClFileCopyTest.cc
  # XrdClFileSystemTest.cc
  # XrdClOperationsWorkflowTest.cc
  XrdClLocalFileHandlerTest.cc
  XrdClPostMasterTest.cc
  XrdClThreadingTest.cc
  XrdClZip.cc
  )

target_link_libraries(xrdcl-cluster-tests
  XrdTestUtils GTest::gtest GTest::gtest_main)

gtest_discover_tests(xrdcl-cluster-tests TEST_PREFIX XrdCl::
  PROPERTIES DEPENDS XRootD_Cluster FIXTURES_REQUIRED XRootD_Cluster DISCOVERY_TIMEOUT 10)

# tests to be run in serial, otherwise they are problematic
set(SERIAL_TESTS_FILES
  XrdClFileTest.cc
  XrdClFileClone.cc
  XrdClFileCopyTest.cc
  XrdClFileSystemTest.cc
  XrdClOperationsWorkflowTest.cc
  XrdClTTLManyStreams.cc
)

# create a separate executable target for each "problematic" test suite
foreach(i ${SERIAL_TESTS_FILES})
  add_executable(${i}-tests ${i} IdentityPlugIn.cc)

  target_link_libraries(${i}-tests
    XrdTestUtils GTest::gtest GTest::gtest_main)

  gtest_discover_tests(${i}-tests TEST_PREFIX XrdCl::
    PROPERTIES DEPENDS XRootD_Cluster FIXTURES_REQUIRED XRootD_Cluster RUN_SERIAL 1 DISCOVERY_TIMEOUT 10)
endforeach()
