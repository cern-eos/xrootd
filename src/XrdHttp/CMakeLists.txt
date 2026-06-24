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
  wire/XrdHttp1Session.cc    wire/XrdHttp1Session.hh
  wire/XrdHttp1ResponseWriter.cc wire/XrdHttp1ResponseWriter.hh
  wire/XrdHttpConnection.hh
  vendor/llhttp/api.c
  vendor/llhttp/http.c
  vendor/llhttp/llhttp.c
)

if(BUILD_KRB5)
  list(APPEND XRDHTTPUTILS_SRCS XrdHttpKrb5.cc XrdHttpKrb5.hh)
endif()

if(BUILD_HTTP2)
  list(APPEND XRDHTTPUTILS_SRCS
    wire/XrdHttp2Session.cc wire/XrdHttp2Session.hh
    wire/XrdHttp2ResponseWriter.cc wire/XrdHttp2ResponseWriter.hh)
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

target_include_directories(XrdHttpUtils PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(BUILD_KRB5)
  target_compile_definitions(XrdHttpUtils PRIVATE HAVE_HTTP_KRB5)
  target_include_directories(XrdHttpUtils PRIVATE ${KERBEROS5_INCLUDE_DIR})
  target_link_libraries(XrdHttpUtils PRIVATE ${KERBEROS5_LIBRARIES})
endif()

if(BUILD_HTTP2)
  target_compile_definitions(XrdHttpUtils PRIVATE HAVE_NGHTTP2)
  target_include_directories(XrdHttpUtils PRIVATE ${NGHTTP2_INCLUDE_DIR})
  target_link_libraries(XrdHttpUtils PRIVATE ${NGHTTP2_LIBRARY})
endif()

set(XrdHttp XrdHttp-${PLUGIN_VERSION})
add_library(${XrdHttp} MODULE XrdHttpModule.cc)
target_link_libraries(${XrdHttp} PRIVATE XrdUtils XrdHttpUtils)
target_include_directories(${XrdHttp} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(${XrdHttp} PROPERTIES SUFFIX ".so")

install(
  TARGETS
    ${XrdHttp}
    XrdHttpUtils
  LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
