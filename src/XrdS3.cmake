
#-------------------------------------------------------------------------------
# Modules
#-------------------------------------------------------------------------------
set(LIB_XRD_S3 XrdHttpS3-${PLUGIN_VERSION})

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------

if (BUILD_S3)
    #-----------------------------------------------------------------------------
    # The XrdHttp library
    #-----------------------------------------------------------------------------

    if (TINYXML_FOUND)
        set(TINYXML_FILES "")
        set(TINYXML_LIBRARIES ${TINYXML_LIBRARIES})
    else ()
        set(TINYXML_FILES
                XrdXml/tinyxml/tinystr.cpp XrdXml/tinyxml/tinystr.h
                XrdXml/tinyxml/tinyxml.cpp XrdXml/tinyxml/tinyxml.h
                XrdXml/tinyxml/tinyxmlerror.cpp
                XrdXml/tinyxml/tinyxmlparser.cpp)
        set(TINYXML_LIBRARIES "")
    endif ()

    add_library(
            ${LIB_XRD_S3}
            MODULE
            XrdS3/XrdS3.cc XrdS3/XrdS3.hh
            XrdS3/XrdS3Utils.cc
            XrdS3/XrdS3Utils.hh
            XrdS3/XrdS3Crypt.cc
            XrdS3/XrdS3Crypt.hh
            XrdS3/XrdS3Auth.cc
            XrdS3/XrdS3Auth.hh
            XrdS3/XrdS3Router.cc
            XrdS3/XrdS3Router.hh
            XrdS3/XrdS3Req.cc
            XrdS3/XrdS3Req.hh
            ${TINYXML_FILES}
            XrdS3/XrdS3Xml.cc
            XrdS3/XrdS3Xml.hh
            XrdS3/XrdS3ErrorResponse.hh
            XrdS3/XrdS3Api.cc
            XrdS3/XrdS3Api.hh
            XrdS3/XrdS3Response.cc
            XrdS3/XrdS3Response.hh
            XrdS3/XrdS3ObjectStore.cc
            XrdS3/XrdS3ObjectStore.hh
            XrdS3/XrdS3Action.hh
    )

    target_link_libraries(
            ${LIB_XRD_S3}
            PRIVATE
            XrdServer
            XrdUtils
            XrdHttpUtils
            XrdPosixPreload
            tinyxml2
            ${CMAKE_DL_LIBS}
            ${CMAKE_THREAD_LIBS_INIT})

    if (TINYXML_FOUND)
        target_include_directories(${LIB_XRD_S3} PRIVATE ${TINYXML_INCLUDE_DIR})
    else ()
        target_include_directories(${LIB_XRD_S3} PRIVATE XrdXml/tinyxml)
    endif ()

    if (MacOSX)
        set(S3_LINK_FLAGS, "-Wl")
    else ()
        set(S3_LINK_FLAGS, "-Wl,--version-script=${CMAKE_SOURCE_DIR}/src/XrdS3/export-lib-symbols")
    endif ()

    set_target_properties(
            ${LIB_XRD_S3}
            PROPERTIES
            LINK_FLAGS "${S3_LINK_FLAGS}"
            COMPILE_DEFINITIONS "${XRD_COMPILE_DEFS}")

    #-----------------------------------------------------------------------------
    # Install
    #-----------------------------------------------------------------------------
    install(
            TARGETS ${LIB_XRD_S3}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

endif ()
