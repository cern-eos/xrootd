set(XrdFsOss XrdFsOss-${PLUGIN_VERSION})

add_library(${XrdFsOss} MODULE
  XrdFsOss.cc     XrdFsOss.hh
  XrdFsOssDF.cc
                  XrdFsOssUid.hh
)

target_link_libraries(${XrdFsOss} PRIVATE XrdUtils XrdServer)

install(TARGETS ${XrdFsOss} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
