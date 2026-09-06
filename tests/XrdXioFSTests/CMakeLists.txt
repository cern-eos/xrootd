if(NOT BUILD_HTTP2)
  return()
endif()

add_executable(xrdxiofs-unit-tests XioDavTest.cc)
target_link_libraries(xrdxiofs-unit-tests PRIVATE XrdXioFS GTest::gtest GTest::gtest_main)

gtest_discover_tests(xrdxiofs-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)
