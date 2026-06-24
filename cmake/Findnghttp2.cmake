include(FindPackageHandleStandardArgs)

if(APPLE AND EXISTS "/opt/homebrew/opt/libnghttp2")
  list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew/opt/libnghttp2")
endif()

if(NGHTTP2_INCLUDE_DIR AND NGHTTP2_LIBRARY)
  set(NGHTTP2_FOUND TRUE)
else()
  find_path(
    NGHTTP2_INCLUDE_DIR
    NAMES nghttp2/nghttp2.h
    HINTS
      ${NGHTTP2_ROOT_DIR}
    PATH_SUFFIXES
      include)

  find_library(
    NGHTTP2_LIBRARY
    NAMES nghttp2
    HINTS
      ${NGHTTP2_ROOT_DIR}
    PATH_SUFFIXES
      ${LIBRARY_PATH_PREFIX}
      ${LIB_SEARCH_OPTIONS})

  find_package_handle_standard_args(
    nghttp2
    DEFAULT_MSG
    NGHTTP2_LIBRARY NGHTTP2_INCLUDE_DIR)

  mark_as_advanced(NGHTTP2_INCLUDE_DIR NGHTTP2_LIBRARY)
endif()
