if(NOT BUILD_HTTP2)
  return()
endif()

add_library(XrdKernelFS STATIC
  http/KfsUrl.cc
  http/KfsDav.cc
  http/KfsHttp2.cc
  http/KfsClient.cc
)

target_include_directories(XrdKernelFS
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/http
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE
    ${NGHTTP2_INCLUDE_DIR}
)

target_link_libraries(XrdKernelFS
  PUBLIC
    OpenSSL::SSL
    OpenSSL::Crypto
    ${NGHTTP2_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
)

add_executable(kfscli fuse/kfscli.cc)
target_link_libraries(kfscli PRIVATE XrdKernelFS)
install(TARGETS kfscli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# FUSE daemon: Linux libfuse, or macFUSE (/usr/local, /opt/homebrew, /opt/brew).
if(ENABLE_FUSE)
  find_package(fuse)
  if(FUSE_FOUND)
    add_executable(kfsd fuse/kfsd.cc)
    target_link_libraries(kfsd PRIVATE XrdKernelFS ${FUSE_LIBRARIES})
    target_include_directories(kfsd PRIVATE ${FUSE_INCLUDE_DIR})
    if(APPLE)
      target_link_libraries(kfsd PRIVATE
        "-framework CoreFoundation"
        "-framework DiskArbitration")
    endif()
    install(TARGETS kfsd RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif()
endif()
