add_library(XrdTestUtils SHARED
  Server.cc  Server.hh
  TestEnv.cc TestEnv.hh
  Utils.cc   Utils.hh)

target_link_libraries(XrdTestUtils ZLIB::ZLIB XrdCl XrdUtils)

target_compile_definitions(XrdTestUtils PRIVATE
  XRDTEST_DEFAULT_PORTS_ENV="${CMAKE_BINARY_DIR}/tests/cluster/ports.env"
  XRDTEST_DEFAULT_LOCAL_DATA_PATH="${CMAKE_BINARY_DIR}/tests/cluster/data")

target_include_directories(XrdTestUtils
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
