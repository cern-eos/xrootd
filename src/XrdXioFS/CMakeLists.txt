if(NOT BUILD_HTTP2)
  return()
endif()

add_library(XrdXioFS STATIC
  http/XioUrl.cc
  http/XioDav.cc
  http/XioHttp2.cc
  http/XioClient.cc
  http/XioBearer.cc
  http/XioKrb5.cc
)

# Linked into libXrdHttpParsockTpc.so. A static archive needs -fPIC or the
# shared link fails with R_X86_64_32 relocations.
set_target_properties(XrdXioFS PROPERTIES POSITION_INDEPENDENT_CODE ON)

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

if(BUILD_PARSOCKET)
  target_compile_definitions(XrdXioFS PUBLIC HAVE_PARSOCKET)
  target_include_directories(XrdXioFS PUBLIC ${PARSOCK_INCLUDE_DIRS})
  target_link_libraries(XrdXioFS PUBLIC ${PARSOCK_LIBRARIES})
endif()

if(BUILD_ZSTD)
  target_compile_definitions(XrdXioFS PUBLIC HAVE_ZSTD)
  target_include_directories(XrdXioFS PUBLIC ${ZSTD_INCLUDE_DIRS})
  target_link_libraries(XrdXioFS PUBLIC ${ZSTD_LIBRARIES})
endif()

if(BUILD_KRB5 AND KERBEROS5_FOUND)
  target_compile_definitions(XrdXioFS PUBLIC HAVE_KRB5)
  target_include_directories(XrdXioFS PUBLIC ${KERBEROS5_INCLUDE_DIR})
  target_link_libraries(XrdXioFS PUBLIC ${KERBEROS5_LIBRARIES})
elseif(APPLE)
  target_link_libraries(XrdXioFS PUBLIC "-framework GSS")
elseif(WIN32)
  target_link_libraries(XrdXioFS PUBLIC Secur32)
endif()

add_executable(xiofscli fuse/xiofscli.cc)
target_link_libraries(xiofscli PRIVATE XrdXioFS)
install(TARGETS xiofscli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# TLS handshake agent: Linux only (kTLS + /dev/xiofsctl). RDMA/GPU stay stubs.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(xiofsagent agent/xiofsagent.cc agent/XioShard.cc)
  target_link_libraries(xiofsagent PRIVATE XrdXioFS ${CMAKE_THREAD_LIBS_INIT})
  target_include_directories(xiofsagent PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/kernel)
  if(ENABLE_XRDCL)
    target_link_libraries(xiofsagent PRIVATE XrdCl)
    target_compile_definitions(xiofsagent PRIVATE HAVE_XRDCL)
  endif()
  if(BUILD_KRB5)
    target_sources(xiofsagent PRIVATE agent/xiofsagent_krb5.cc)
    target_compile_definitions(xiofsagent PRIVATE HAVE_KRB5)
    target_include_directories(xiofsagent PRIVATE ${KERBEROS5_INCLUDE_DIR})
    target_link_libraries(xiofsagent PRIVATE ${KERBEROS5_LIBRARIES})
  endif()
  install(TARGETS xiofsagent RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  add_executable(xiofs-server server/xiofs-server.cc)
  install(TARGETS xiofs-server RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  # Prefix /usr with a relative SYSCONFDIR installs under /usr/etc. The
  # unit and the RPM look for this file in /etc/xrootd.
  if(CMAKE_INSTALL_PREFIX STREQUAL "/usr" OR CMAKE_INSTALL_PREFIX STREQUAL "/usr/")
    set(XIOFS_SERVER_CONF_DIR "/etc/xrootd")
  else()
    set(XIOFS_SERVER_CONF_DIR "${CMAKE_INSTALL_SYSCONFDIR}/xrootd")
  endif()
  install(FILES ${CMAKE_SOURCE_DIR}/config/xiofs-server.conf
          DESTINATION ${XIOFS_SERVER_CONF_DIR})
  # util-linux looks up mount.<type> in sbindir.
  install(CODE "
    file(MAKE_DIRECTORY \"\$ENV{DESTDIR}${CMAKE_INSTALL_FULL_SBINDIR}\")
    execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
      \"../${CMAKE_INSTALL_BINDIR}/xiofsagent\"
      \"\$ENV{DESTDIR}${CMAKE_INSTALL_FULL_SBINDIR}/mount.xiofs\")
  ")
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

# Apple FileProvider: native macOS filesystem integration (Apple Silicon / Intel).
if(APPLE)
  add_library(XrdXioFSApple STATIC
    apple/XioFSFileProviderConfig.mm
    apple/XioFSFileProviderItem.mm
    apple/XioFSFileProviderEnumerator.mm
    apple/XioFSFileProviderExtension.mm
  )
  target_link_libraries(XrdXioFSApple
    PUBLIC
      XrdXioFS
      "-framework Foundation"
      "-framework FileProvider"
      "-framework UniformTypeIdentifiers"
  )
  target_include_directories(XrdXioFSApple
    PUBLIC
      ${CMAKE_CURRENT_SOURCE_DIR}/apple
  )

  add_executable(xiofs-fileprovider apple/xiofs-fileprovider-cli.mm)
  target_link_libraries(xiofs-fileprovider
    PRIVATE
      XrdXioFSApple
  )
  install(TARGETS xiofs-fileprovider RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

  add_library(XioFSFileProviderExtension MODULE
    apple/XioFSFileProviderConfig.mm
    apple/XioFSFileProviderItem.mm
    apple/XioFSFileProviderEnumerator.mm
    apple/XioFSFileProviderExtension.mm
  )
  set_target_properties(XioFSFileProviderExtension PROPERTIES
    BUNDLE TRUE
    BUNDLE_EXTENSION "appex"
    MACOSX_BUNDLE_INFO_PLIST "${CMAKE_CURRENT_SOURCE_DIR}/apple/Info.plist.in"
  )
  target_link_libraries(XioFSFileProviderExtension
    PRIVATE
      XrdXioFS
      "-framework Foundation"
      "-framework FileProvider"
      "-framework UniformTypeIdentifiers"
  )
  target_include_directories(XioFSFileProviderExtension
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/apple
  )
  install(TARGETS XioFSFileProviderExtension LIBRARY DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
