add_executable(xrdsecztn-unit-tests
  XrdSecztnTests.cc
  ../../src/XrdSecztn/XrdSecProtocolztn.cc
  ../../src/XrdSecztn/XrdSecztn.cc
)

target_link_libraries(xrdsecztn-unit-tests
  XrdUtils GTest::GTest GTest::Main)

gtest_discover_tests(xrdsecztn-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)
