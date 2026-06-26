set(XrdSecoauth2 XrdSecoauth2-${PLUGIN_VERSION})

add_library(${XrdSecoauth2} MODULE XrdSecProtocoloauth2.cc)
target_link_libraries(${XrdSecoauth2} PRIVATE XrdUtils)

install(TARGETS ${XrdSecoauth2} LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
