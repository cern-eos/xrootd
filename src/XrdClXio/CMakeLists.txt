if(NOT BUILD_HTTP2)
  return()
endif()

add_library(XrdClXioObj OBJECT
  XrdClXioFactory.cc
  XrdClXioFile.cc       XrdClXioFile.hh
  XrdClXioFilesystem.cc XrdClXioFilesystem.hh
  XrdClXioUtil.cc       XrdClXioUtil.hh
)

target_link_libraries(XrdClXioObj
  PRIVATE
    XrdCl
    XrdXioFS
)

set_target_properties(XrdClXioObj PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(XrdClXio "XrdClXio-${PLUGIN_VERSION}")

add_library(${XrdClXio} MODULE)
target_link_libraries(${XrdClXio}
  PRIVATE
    XrdClXioObj
    XrdXioFS
    XrdCl
)

if(NOT APPLE)
  target_link_options(${XrdClXio} PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/configs/export-lib-symbols")
endif()

install(TARGETS ${XrdClXio} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
