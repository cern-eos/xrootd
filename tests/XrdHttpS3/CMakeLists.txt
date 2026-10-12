if(NOT BUILD_HTTP)
  return()
endif()

add_executable(xrdhttp-s3-unit
  SigV4Test.cc
  OwnerTest.cc
  ${PROJECT_SOURCE_DIR}/src/XrdHttpS3/XrdHttpS3SigV4.cc
  ${PROJECT_SOURCE_DIR}/src/XrdHttpS3/XrdHttpS3Paths.cc
)
target_include_directories(xrdhttp-s3-unit PRIVATE
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/src/XrdHttpS3
)
target_link_libraries(xrdhttp-s3-unit PRIVATE GTest::gtest_main OpenSSL::Crypto XrdUtils)
gtest_discover_tests(xrdhttp-s3-unit PROPERTIES DISCOVERY_TIMEOUT 10)

add_executable(xrdhttp-s3sign
  s3sign.cc
  ${PROJECT_SOURCE_DIR}/src/XrdHttpS3/XrdHttpS3SigV4.cc
)
target_include_directories(xrdhttp-s3sign PRIVATE ${PROJECT_SOURCE_DIR}/src/XrdHttpS3)
target_link_libraries(xrdhttp-s3sign PRIVATE OpenSSL::Crypto)

if(ENABLE_SERVER_TESTS)
  add_test(NAME XrdHttpS3::integration
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test.sh)
  set_tests_properties(XrdHttpS3::integration PROPERTIES
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    ENVIRONMENT "XROOTD=$<TARGET_FILE:xrootd>;XRD_HTTP_LIB=$<TARGET_FILE:XrdHttp-${PLUGIN_VERSION}>;XRD_S3_LIB=$<TARGET_FILE:XrdHttpS3-${PLUGIN_VERSION}>;S3SIGN=$<TARGET_FILE:xrdhttp-s3sign>"
    TIMEOUT 120
  )
endif()
