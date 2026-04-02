if(BUILD_KRB5)
  add_executable(xrdseckrb5-unit-tests
    XrdSeckrb5Tests.cc
  )

  target_link_libraries(xrdseckrb5-unit-tests
    XrdUtils GTest::GTest GTest::Main ${KERBEROS5_LIBRARIES})

  target_include_directories(xrdseckrb5-unit-tests
    PRIVATE ${KERBEROS5_INCLUDE_DIR})

  gtest_discover_tests(xrdseckrb5-unit-tests
    PROPERTIES DISCOVERY_TIMEOUT 10)
endif()
