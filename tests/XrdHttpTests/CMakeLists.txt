if(NOT BUILD_HTTP)
    return()
endif()

add_executable(xrdhttp-unit-tests XrdHttpTests.cc
        ${PROJECT_SOURCE_DIR}/src/XrdHttpCors/XrdHttpCorsHandler.cc)

target_link_libraries(xrdhttp-unit-tests XrdHttpUtils GTest::gtest GTest::gtest_main XrdUtils)

if(BUILD_HTTP2)
  target_link_libraries(xrdhttp-unit-tests XrdXioFS)
  target_include_directories(xrdhttp-unit-tests PRIVATE
    ${PROJECT_SOURCE_DIR}/src/XrdXioFS/http
    ${PROJECT_SOURCE_DIR}/src/XrdHttpParsockTpc)
endif()

if(BUILD_ZSTD)
  target_link_libraries(xrdhttp-unit-tests ${ZSTD_LIBRARIES})
  target_include_directories(xrdhttp-unit-tests PRIVATE ${ZSTD_INCLUDE_DIRS})
endif()

gtest_discover_tests(xrdhttp-unit-tests
  PROPERTIES DISCOVERY_TIMEOUT 10)
