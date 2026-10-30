if(NOT BUILD_HTTP2)
  return()
endif()

set(XrdHttpParsockTpc XrdHttpParsockTpc-${PLUGIN_VERSION})

add_library(${XrdHttpParsockTpc} MODULE XrdHttpParsockTpc.cc)

target_link_libraries(${XrdHttpParsockTpc}
  PRIVATE
    XrdXioFS
    XrdHttpUtils
    XrdServer
    XrdUtils
    ${CMAKE_THREAD_LIBS_INIT}
)

if(NOT APPLE)
  target_link_options(${XrdHttpParsockTpc} PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/export-lib-symbols")
endif()

install(TARGETS ${XrdHttpParsockTpc} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
