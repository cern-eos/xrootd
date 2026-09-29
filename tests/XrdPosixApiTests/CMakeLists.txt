add_executable(xrdposix-cl-unit-tests XrdClPosixProtoTests.cc)
target_link_libraries(xrdposix-cl-unit-tests XrdTestUtils XrdCl XrdUtils GTest::gtest GTest::gtest_main)
gtest_discover_tests(xrdposix-cl-unit-tests TEST_PREFIX XrdPosixCl::
  PROPERTIES DISCOVERY_TIMEOUT 10)

if(XRDCL_ONLY)
  return()
endif()

add_executable(xrdposix-oss-unit-tests
  XrdFsOssPosixTests.cc
  ${PROJECT_SOURCE_DIR}/src/XrdFsOss/XrdFsOss.cc
  ${PROJECT_SOURCE_DIR}/src/XrdFsOss/XrdFsOssDF.cc)
target_link_libraries(xrdposix-oss-unit-tests XrdServer XrdUtils GTest::gtest GTest::gtest_main)
gtest_discover_tests(xrdposix-oss-unit-tests TEST_PREFIX XrdPosixOss::
  PROPERTIES DISCOVERY_TIMEOUT 10)

add_executable(xrdposix-ofs-unit-tests
  XrdSfsOfsPosixTests.cc
  ${PROJECT_SOURCE_DIR}/src/XrdFsOss/XrdFsOss.cc
  ${PROJECT_SOURCE_DIR}/src/XrdFsOss/XrdFsOssDF.cc)
target_link_libraries(xrdposix-ofs-unit-tests XrdServer XrdUtils GTest::gtest GTest::gtest_main)
gtest_discover_tests(xrdposix-ofs-unit-tests TEST_PREFIX XrdPosixOfs::
  PROPERTIES RUN_SERIAL 1 DISCOVERY_TIMEOUT 10)

if(BUILD_HTTP)
  add_executable(xrdposix-http-unit-tests XrdHttpPosixTests.cc)
  target_link_libraries(xrdposix-http-unit-tests XrdHttpUtils XrdUtils GTest::gtest GTest::gtest_main)
  if(BUILD_HTTP2)
    target_compile_definitions(xrdposix-http-unit-tests PRIVATE HAVE_NGHTTP2)
  endif()
  gtest_discover_tests(xrdposix-http-unit-tests TEST_PREFIX XrdPosixHttp::
    PROPERTIES DISCOVERY_TIMEOUT 10)
endif()
