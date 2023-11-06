//
// Created by segransm on 11/13/23.
//

#include "XrdS3Xml.hh"

namespace S3 {

S3Xml::S3Xml() : tinyxml2::XMLPrinter(nullptr, true) {
  PushDeclaration(R"(xml version="1.0" encoding="UTF-8")");
}

void S3Xml::OpenElement(const char *elem) {
  tinyxml2::XMLPrinter::OpenElement(elem, true);
}

void S3Xml::CloseElement() { tinyxml2::XMLPrinter::CloseElement(true); }

void S3Xml::AddElement(const char *key, const std::string &value) {
  OpenElement(key);
  if (!value.empty()) {
    PushText(value.c_str());
  }
  CloseElement();
}

}  // namespace S3
