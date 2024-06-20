set(XS3_SYSTEMD_SERVICE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/systemd/xrootd-s3@.service")

install(FILES ${XS3_SYSTEMD_SERVICE_FILE}
        DESTINATION /lib/systemd/system)

set(XS3_CONFIG_FILE "${CMAKE_CURRENT_SOURCE_DIR}/config/xrootd-s3.cfg")

install(FILES ${XS3_CONFIG_FILE}
        DESTINATION /etc/xrootd/)


#-----------------------------------------------------------------------------
# xs3
#-----------------------------------------------------------------------------
#add_executable(
#  xrds3
#  app/xs3.cc )

#target_link_libraries(
#  xrds3
#  ${JSONCPP_LIBRARIES} uuid stdc++fs XrdUtils )
