if(NOT BUILD_HTTP)
  return()
endif()

# Note that XrdHttpUtils is marked as a shared library as XrdHttp plugins
# are expected to link against it for the XrdHttpExt class implementations.

set(XRDHTTPUTILS_SRCS
  XrdHttpChecksum.cc         XrdHttpChecksum.hh
  XrdHttpChecksumHandler.cc  XrdHttpChecksumHandler.hh
  XrdHttpExtHandler.cc       XrdHttpExtHandler.hh
  XrdHttpProtocol.cc         XrdHttpProtocol.hh
  XrdHttpReadRangeHandler.cc XrdHttpReadRangeHandler.hh
  XrdHttpReq.cc              XrdHttpReq.hh
  XrdHttpMon.cc              XrdHttpMon.hh
                             XrdHttpMonState.hh
                             XrdHttpSecXtractor.hh
  XrdHttpSecurity.cc
                             XrdHttpStatic.hh
                             XrdHttpTrace.hh
  XrdHttpUtils.cc            XrdHttpUtils.hh
  XrdHttpHeaderUtils.cc      XrdHttpHeaderUtils.hh
)

if(BUILD_KRB5)
  list(APPEND XRDHTTPUTILS_SRCS XrdHttpKrb5.cc XrdHttpKrb5.hh)
endif()

add_library(XrdHttpUtils SHARED ${XRDHTTPUTILS_SRCS})

set_target_properties(XrdHttpUtils
  PROPERTIES
    SOVERSION ${XRootD_VERSION_MAJOR}
    VERSION ${XRootD_LIBVERSION}
)

target_link_libraries(XrdHttpUtils
  PRIVATE
    XrdServer
    XrdUtils
    XrdCrypto
  PUBLIC
    OpenSSL::SSL
    OpenSSL::Crypto
)

if(BUILD_KRB5)
  # PUBLIC so all consumers of XrdHttpProtocol (e.g. the XrdHttp plugin)
  # see the same class layout as XrdHttpUtils.
  target_compile_definitions(XrdHttpUtils PUBLIC HAVE_HTTP_KRB5)
  target_include_directories(XrdHttpUtils PRIVATE ${KERBEROS5_INCLUDE_DIR})
  target_link_libraries(XrdHttpUtils PRIVATE ${KERBEROS5_LIBRARIES})
endif()

set(XrdHttp XrdHttp-${PLUGIN_VERSION})
add_library(${XrdHttp} MODULE XrdHttpModule.cc)
target_link_libraries(${XrdHttp} PRIVATE XrdUtils XrdHttpUtils)
set_target_properties(${XrdHttp} PROPERTIES SUFFIX ".so")

install(
  TARGETS
    ${XrdHttp}
    XrdHttpUtils
  LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
