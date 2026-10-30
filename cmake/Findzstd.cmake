#-------------------------------------------------------------------------------
# Find zstd (libzstd) library and headers
#-------------------------------------------------------------------------------
include(FindPackageHandleStandardArgs)

if(APPLE)
  if(EXISTS "/opt/homebrew/opt/zstd")
    list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew/opt/zstd")
  elseif(EXISTS "/usr/local/opt/zstd")
    list(APPEND CMAKE_PREFIX_PATH "/usr/local/opt/zstd")
  endif()
endif()

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_ZSTD QUIET libzstd)
endif()

find_path(
  ZSTD_INCLUDE_DIR
  NAMES zstd.h
  HINTS
    ${ZSTD_ROOT_DIR}
    $ENV{ZSTD_ROOT_DIR}
    ${PC_ZSTD_INCLUDEDIR}
    ${PC_ZSTD_INCLUDE_DIRS}
    /opt/homebrew/opt/zstd
    /usr/local/opt/zstd
  PATH_SUFFIXES
    include)

find_library(
  ZSTD_LIBRARY
  NAMES zstd
  HINTS
    ${ZSTD_ROOT_DIR}
    $ENV{ZSTD_ROOT_DIR}
    ${PC_ZSTD_LIBDIR}
    ${PC_ZSTD_LIBRARY_DIRS}
    /opt/homebrew/opt/zstd
    /usr/local/opt/zstd
  PATH_SUFFIXES
    lib
    ${LIBRARY_PATH_PREFIX}
    ${LIB_SEARCH_OPTIONS})

if(PC_ZSTD_VERSION)
  set(ZSTD_VERSION "${PC_ZSTD_VERSION}")
endif()

find_package_handle_standard_args(
  zstd
  REQUIRED_VARS ZSTD_LIBRARY ZSTD_INCLUDE_DIR
  VERSION_VAR ZSTD_VERSION)

if(ZSTD_FOUND)
  set(ZSTD_LIBRARIES ${ZSTD_LIBRARY})
  set(ZSTD_INCLUDE_DIRS ${ZSTD_INCLUDE_DIR})
endif()

mark_as_advanced(ZSTD_INCLUDE_DIR ZSTD_LIBRARY)
