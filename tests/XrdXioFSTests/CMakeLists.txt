if(NOT BUILD_HTTP2)
  return()
endif()

add_executable(xrdxiofs-unit-tests XioDavTest.cc XioBearerTest.cc XioUrlTest.cc XioKrb5Test.cc XioNixlTest.cc XioServerTest.cc)
target_link_libraries(xrdxiofs-unit-tests PRIVATE XrdXioFS GTest::gtest GTest::gtest_main)
target_include_directories(xrdxiofs-unit-tests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/XrdXioFS/server
  ${PROJECT_SOURCE_DIR}/src/XrdXioFS/include)

gtest_discover_tests(xrdxiofs-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)
