if(NOT BUILD_HTTP2)
  return()
endif()

add_executable(xrdkernelfs-unit-tests KfsDavTest.cc)
target_link_libraries(xrdkernelfs-unit-tests PRIVATE XrdKernelFS GTest::gtest GTest::gtest_main)

gtest_discover_tests(xrdkernelfs-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)
