#-----------------------------------------------------------------------------
# xrootd-s3@s3 systemd unit
#-----------------------------------------------------------------------------
set(XS3_SYSTEMD_SERVICE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/systemd/xrootd-s3@.service")
install(FILES ${XS3_SYSTEMD_SERVICE_FILE}
        DESTINATION /lib/systemd/system)


#-----------------------------------------------------------------------------
# /etc/xrootd/xrootd-s3.cfg
#-----------------------------------------------------------------------------
set(XS3_CONFIG_FILE "${CMAKE_CURRENT_SOURCE_DIR}/config/xrootd-s3.cfg")
install(FILES ${XS3_CONFIG_FILE}
        DESTINATION /etc/xrootd/)

#-----------------------------------------------------------------------------
# xrds3
#-----------------------------------------------------------------------------
set(XS3_APP_FILE "${CMAKE_CURRENT_SOURCE_DIR}/app/xrds3")
install(PROGRAMS ${XS3_APP_FILE} DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
