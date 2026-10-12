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

# macFUSE / FUSE-T install into /usr/local or /opt/homebrew on macOS.
# WinFsp installs into %ProgramFiles(x86)%\WinFsp on Windows.
# /opt/brew is an alternate Homebrew prefix some machines use.
set(_FUSE_HINTS
  ${FUSE_ROOT_DIR}
  ${PC_FUSE_INCLUDEDIR}
  ${PC_FUSE_LIBDIR}
  /usr/local
  /opt/homebrew
  /opt/brew
  "/Library/Application Support/fuse-t"
  "$ENV{WINFSP_DIR}"
  "$ENV{ProgramFiles\(x86\)}/WinFsp"
  "$ENV{ProgramFiles}/WinFsp"
  "C:/Program Files (x86)/WinFsp"
  "C:/Program Files/WinFsp"
)

find_path(FUSE_INCLUDE_DIR
  NAMES fuse.h fuse-t/fuse.h
  HINTS ${_FUSE_HINTS} ${PC_FUSE_INCLUDE_DIRS}
  PATHS /usr/local /opt/homebrew /opt/brew "/Library/Application Support/fuse-t"
  PATH_SUFFIXES include/fuse include/fuse-t include fuse fuse-t inc/fuse inc
)

if(NOT FUSE_INCLUDE_DIR)
  find_path(_FUSE_PARENT
    NAMES fuse/fuse.h fuse-t/fuse.h
    HINTS ${_FUSE_HINTS}
    PATHS /usr/local /opt/homebrew /opt/brew "/Library/Application Support/fuse-t"
    PATH_SUFFIXES include inc)
  if(_FUSE_PARENT)
    set(FUSE_INCLUDE_DIR "${_FUSE_PARENT}/fuse")
  endif()
endif()

find_library(FUSE_LIBRARY
  NAMES fuse fuse-t osxfuse winfsp-x64 winfsp-a64 winfsp-x86 winfsp fuse-2.8
  HINTS ${_FUSE_HINTS} ${PC_FUSE_LIBRARY_DIRS}
  PATHS /usr/local /opt/homebrew /opt/brew "/Library/Application Support/fuse-t"
  PATH_SUFFIXES lib lib/x64 lib/arm64 lib/x86
)

set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
set(FUSE_LIBRARIES ${FUSE_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fuse DEFAULT_MSG FUSE_INCLUDE_DIR FUSE_LIBRARY)

mark_as_advanced(FUSE_INCLUDE_DIR FUSE_LIBRARY)
