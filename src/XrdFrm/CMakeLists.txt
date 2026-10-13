add_library(XrdFrm STATIC
  XrdFrmConfig.cc     XrdFrmConfig.hh
  XrdFrmFiles.cc      XrdFrmFiles.hh
  XrdFrmMonitor.cc    XrdFrmMonitor.hh
  XrdFrmTSort.cc      XrdFrmTSort.hh
  XrdFrmCns.cc        XrdFrmCns.hh

  XrdFrmMigrate.cc    XrdFrmMigrate.hh
  XrdFrmReqBoss.cc    XrdFrmReqBoss.hh
  XrdFrmTransfer.cc   XrdFrmTransfer.hh
  XrdFrmXfrAgent.cc   XrdFrmXfrAgent.hh
  XrdFrmXfrDaemon.cc  XrdFrmXfrDaemon.hh
                      XrdFrmXfrJob.hh
  XrdFrmXfrQueue.cc   XrdFrmXfrQueue.hh
)

add_executable(frm_admin
  XrdFrmAdminAudit.cc
  XrdFrmAdmin.cc       XrdFrmAdmin.hh
  XrdFrmAdminFiles.cc
  XrdFrmAdminFind.cc
  XrdFrmAdminMain.cc
  XrdFrmAdminQuery.cc
  XrdFrmAdminUnlink.cc
)

target_link_libraries(frm_admin
  XrdFrm
  XrdServer
  XrdUtils
  ${CMAKE_THREAD_LIBS_INIT}
  ${READLINE_LIBRARY}
  ${NCURSES_LIBRARY}
  ${EXTRA_LIBS}
  ${SOCKET_LIBRARY}
)

if(READLINE_FOUND)
  target_include_directories(frm_admin PRIVATE ${READLINE_INCLUDE_DIR})
endif()

add_executable(frm_purged
  XrdFrmPurge.cc     XrdFrmPurge.hh
  XrdFrmPurgMain.cc
)

target_link_libraries(frm_purged
  XrdFrm
  XrdServer
  XrdUtils
  ${CMAKE_THREAD_LIBS_INIT}
  ${EXTRA_LIBS}
  ${SOCKET_LIBRARY}
)

# frm_xfragent and frm_xfrd are the same program; persona comes from argv[0].
# One binary plus a symlink keeps their GNU build-ids distinct for rpmbuild.
add_executable(frm_xfrd XrdFrmXfrMain.cc)

target_link_libraries(frm_xfrd
  XrdFrm
  XrdServer
  XrdUtils
  ${CMAKE_THREAD_LIBS_INIT}
  ${EXTRA_LIBS}
  ${SOCKET_LIBRARY}
)

add_custom_command(TARGET frm_xfrd POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E create_symlink frm_xfrd frm_xfragent
  WORKING_DIRECTORY $<TARGET_FILE_DIR:frm_xfrd>
  COMMENT "Symlink frm_xfragent -> frm_xfrd"
)

install(
  TARGETS
    frm_admin
    frm_purged
    frm_xfrd
  RUNTIME
    DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(CODE "
  execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
    frm_xfrd
    \"\$ENV{DESTDIR}${CMAKE_INSTALL_FULL_BINDIR}/frm_xfragent\")
")
