set(XrdFsOss XrdFsOss-${PLUGIN_VERSION})

add_library(${XrdFsOss} MODULE
  XrdFsOss.cc     XrdFsOss.hh
  XrdFsOssDF.cc
                  XrdFsOssUid.hh
)

target_link_libraries(${XrdFsOss} PRIVATE XrdUtils XrdServer)

install(TARGETS ${XrdFsOss} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Root trampoline: drop uid and keep CAP_SETUID/CAP_SETGID for FsOss.
add_executable(xrootd-fsuid xrootd-fsuid.c)
install(TARGETS xrootd-fsuid RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
