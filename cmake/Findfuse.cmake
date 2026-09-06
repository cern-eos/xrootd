# Try to find fuse (devel)
# Once done, this will define
#
# FUSE_FOUND - system has fuse
# FUSE_INCLUDE_DIRS - the fuse include directories
# FUSE_LIBRARIES - fuse libraries directories

if(FUSE_INCLUDE_DIRS AND FUSE_LIBRARIES)
  set(FUSE_FIND_QUIETLY TRUE)
endif()

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_FUSE QUIET fuse)
endif()

# macFUSE installs into /usr/local even on Apple Silicon Homebrew hosts.
# /opt/brew is an alternate Homebrew prefix some machines use.
set(_FUSE_HINTS
  ${FUSE_ROOT_DIR}
  ${PC_FUSE_INCLUDEDIR}
  ${PC_FUSE_LIBDIR}
  /usr/local
  /opt/homebrew
  /opt/brew
)

find_path(FUSE_INCLUDE_DIR
  NAMES fuse.h
  HINTS ${_FUSE_HINTS} ${PC_FUSE_INCLUDE_DIRS}
  PATHS /usr/local /opt/homebrew /opt/brew
  PATH_SUFFIXES include/fuse include fuse
)

if(NOT FUSE_INCLUDE_DIR)
  find_path(_FUSE_PARENT
    NAMES fuse/fuse.h
    HINTS ${_FUSE_HINTS}
    PATHS /usr/local /opt/homebrew /opt/brew
    PATH_SUFFIXES include)
  if(_FUSE_PARENT)
    set(FUSE_INCLUDE_DIR "${_FUSE_PARENT}/fuse")
  endif()
endif()

find_library(FUSE_LIBRARY
  NAMES fuse
  HINTS ${_FUSE_HINTS} ${PC_FUSE_LIBRARY_DIRS}
  PATHS /usr/local /opt/homebrew /opt/brew
  PATH_SUFFIXES lib
)

set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
set(FUSE_LIBRARIES ${FUSE_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fuse DEFAULT_MSG FUSE_INCLUDE_DIR FUSE_LIBRARY)

mark_as_advanced(FUSE_INCLUDE_DIR FUSE_LIBRARY)
