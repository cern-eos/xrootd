#-------------------------------------------------------------------------------
# Find Parsock (libparsocket) library and headers
#-------------------------------------------------------------------------------
include(FindPackageHandleStandardArgs)

find_path(
  PARSOCK_INCLUDE_DIR
  NAMES parsock.h
  HINTS
    ${PARSOCK_ROOT_DIR}
    $ENV{PARSOCK_ROOT_DIR}
    /Users/apeters/Software/libparsocket
    /usr/local
    /opt/homebrew
  PATH_SUFFIXES
    include)

find_library(
  PARSOCK_LIBRARY
  NAMES parsock
  HINTS
    ${PARSOCK_ROOT_DIR}
    $ENV{PARSOCK_ROOT_DIR}
    /Users/apeters/Software/libparsocket/build
    /usr/local
    /opt/homebrew
  PATH_SUFFIXES
    lib
    build)

set(PARSOCK_EXTRA_LIBRARIES "")

# If libparsocket was built with QUIC (ngtcp2) or RDMA, resolve extra dependencies
find_library(PARSOCK_NGTCP2_LIB NAMES ngtcp2 HINTS /opt/homebrew/lib /usr/local/lib)
find_library(PARSOCK_NGTCP2_CRYPTO_LIB NAMES ngtcp2_crypto_ossl HINTS /opt/homebrew/lib /usr/local/lib)
if(PARSOCK_NGTCP2_LIB AND PARSOCK_NGTCP2_CRYPTO_LIB)
  list(APPEND PARSOCK_EXTRA_LIBRARIES ${PARSOCK_NGTCP2_LIB} ${PARSOCK_NGTCP2_CRYPTO_LIB})
endif()

find_library(PARSOCK_RDMACM_LIB NAMES rdmacm)
find_library(PARSOCK_IBVERBS_LIB NAMES ibverbs)
if(PARSOCK_RDMACM_LIB AND PARSOCK_IBVERBS_LIB)
  list(APPEND PARSOCK_EXTRA_LIBRARIES ${PARSOCK_RDMACM_LIB} ${PARSOCK_IBVERBS_LIB})
endif()

find_package_handle_standard_args(
  Parsock
  DEFAULT_MSG
  PARSOCK_LIBRARY PARSOCK_INCLUDE_DIR)

if(PARSOCK_FOUND)
  set(PARSOCK_LIBRARIES ${PARSOCK_LIBRARY} ${PARSOCK_EXTRA_LIBRARIES})
  set(PARSOCK_INCLUDE_DIRS ${PARSOCK_INCLUDE_DIR})
endif()

mark_as_advanced(PARSOCK_INCLUDE_DIR PARSOCK_LIBRARY)
