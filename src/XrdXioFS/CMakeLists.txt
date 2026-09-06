if(NOT BUILD_HTTP2)
  return()
endif()

add_library(XrdXioFS STATIC
  http/XioUrl.cc
  http/XioDav.cc
  http/XioHttp2.cc
  http/XioClient.cc
)

target_include_directories(XrdXioFS
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/http
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${NGHTTP2_INCLUDE_DIR}
)

target_link_libraries(XrdXioFS
  PUBLIC
    OpenSSL::SSL
    OpenSSL::Crypto
    ${NGHTTP2_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
)

add_executable(xiofscli fuse/xiofscli.cc)
target_link_libraries(xiofscli PRIVATE XrdXioFS)
install(TARGETS xiofscli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# TLS handshake agent: Linux only (kTLS + /dev/xiofsctl). RDMA/GPU stay stubs.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(xiofsagent agent/xiofsagent.cc)
  target_link_libraries(xiofsagent PRIVATE XrdXioFS)
  target_include_directories(xiofsagent PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/kernel)
  install(TARGETS xiofsagent RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

# FUSE daemon: Linux libfuse, or macFUSE (/usr/local, /opt/homebrew, /opt/brew).
if(ENABLE_FUSE)
  find_package(fuse)
  if(FUSE_FOUND)
    add_executable(xiofsd fuse/xiofsd.cc)
    target_link_libraries(xiofsd PRIVATE XrdXioFS ${FUSE_LIBRARIES})
    target_include_directories(xiofsd PRIVATE ${FUSE_INCLUDE_DIR})
    if(APPLE)
      target_link_libraries(xiofsd PRIVATE
        "-framework CoreFoundation"
        "-framework DiskArbitration")
    endif()
    install(TARGETS xiofsd RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif()
endif()
