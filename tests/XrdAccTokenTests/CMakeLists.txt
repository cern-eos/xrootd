add_executable(xrdacctoken-unit-tests
  XrdAccTokenScopeTests.cc
  ${CMAKE_SOURCE_DIR}/src/XrdAccToken/XrdAccTokenScope.cc
)

target_include_directories(xrdacctoken-unit-tests
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(xrdacctoken-unit-tests
  XrdUtils
  GTest::gtest
  GTest::gtest_main
)

gtest_discover_tests(xrdacctoken-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)
