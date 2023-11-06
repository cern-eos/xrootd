//
// Created by segransm on 11/13/23.
//

#ifndef XROOTD_XRDS3XML_HH
#define XROOTD_XRDS3XML_HH

#include <tinyxml2.h>

#include <string>

namespace S3 {

class S3Xml : public tinyxml2::XMLPrinter {
 public:
  S3Xml();

  void OpenElement(const char *elem);

  void CloseElement();

  void AddElement(const char *key, const std::string &value);

  template <class T>
  void AddElement(const char *key, const T &value) {
    OpenElement(key);
    PushText(value);
    CloseElement();
  }
};

}  // namespace S3

#endif  // XROOTD_XRDS3XML_HH
