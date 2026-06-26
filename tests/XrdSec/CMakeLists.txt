find_package(CURL REQUIRED)

add_executable(xrdsecoauth2-unit-tests
  XrdSecOAuth2.cc
)

target_link_libraries(xrdsecoauth2-unit-tests
  PRIVATE GTest::gtest GTest::gtest_main OpenSSL::Crypto CURL::libcurl XrdUtils)

gtest_discover_tests(xrdsecoauth2-unit-tests
  DISCOVERY_MODE PRE_TEST
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  DISCOVERY_TIMEOUT 10)
