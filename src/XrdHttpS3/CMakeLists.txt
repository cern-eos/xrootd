if(NOT BUILD_HTTP)
  return()
endif()

set(XrdHttpS3 XrdHttpS3-${PLUGIN_VERSION})

add_library(${XrdHttpS3} MODULE
  XrdHttpS3Config.cc   XrdHttpS3Config.hh
  XrdHttpS3Fs.cc       XrdHttpS3Fs.hh
  XrdHttpS3Handler.cc  XrdHttpS3Handler.hh
  XrdHttpS3Meta.cc     XrdHttpS3Meta.hh
  XrdHttpS3Ops.cc      XrdHttpS3Ops.hh
  XrdHttpS3OpsPhase2.cc
  XrdHttpS3OpsPhase3.cc
  XrdHttpS3OpsVersion.cc
  XrdHttpS3OpsGlacier.cc
  XrdHttpS3Glacier.cc   XrdHttpS3Glacier.hh
  XrdHttpS3Paths.cc    XrdHttpS3Paths.hh
  XrdHttpS3SigV4.cc    XrdHttpS3SigV4.hh
  XrdHttpS3Xml.cc      XrdHttpS3Xml.hh
)

target_link_libraries(${XrdHttpS3}
  PRIVATE
    XrdUtils
    XrdHttpUtils
    OpenSSL::Crypto
    ${CMAKE_THREAD_LIBS_INIT}
)

target_include_directories(${XrdHttpS3} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(NOT APPLE)
  target_link_options(${XrdHttpS3} PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/export-lib-symbols")
endif()

set_target_properties(${XrdHttpS3} PROPERTIES SUFFIX ".so")

install(TARGETS ${XrdHttpS3} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
