unset(BUILD_ACCTOKEN CACHE)

if(XRDCL_ONLY)
  return()
endif()

set(BUILD_ACCTOKEN TRUE CACHE INTERNAL "")

set(XrdAccToken XrdAccToken-${PLUGIN_VERSION})

add_library(${XrdAccToken} MODULE
  XrdAccTokenAccess.cc
  XrdAccTokenScope.cc
)

target_include_directories(${XrdAccToken}
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(${XrdAccToken}
  PRIVATE
    XrdUtils
    XrdServer
    ${CMAKE_DL_LIBS}
)

if(NOT APPLE)
  target_link_options(${XrdAccToken} PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/export-lib-symbols")
endif()

install(TARGETS ${XrdAccToken} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
