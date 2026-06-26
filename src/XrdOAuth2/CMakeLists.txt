find_package(CURL REQUIRED)

target_sources(XrdUtils
  PRIVATE
    XrdOAuth2.cc
    XrdOAuth2.hh
)

target_link_libraries(XrdUtils
  PRIVATE
    OpenSSL::Crypto
    CURL::libcurl
)
