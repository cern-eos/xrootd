add_executable(xrdsecsss-unit-tests
  XrdSecsssTests.cc
  ../../src/XrdSecsss/XrdSecProtocolsss.cc
)

target_link_libraries(xrdsecsss-unit-tests
  XrdCryptoLite XrdUtils GTest::GTest GTest::Main)

gtest_discover_tests(xrdsecsss-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)

