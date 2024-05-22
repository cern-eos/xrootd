//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
#include "XrdS3Xml.hh"
//------------------------------------------------------------------------------

namespace S3 {

//------------------------------------------------------------------------------
// S3Xml
//------------------------------------------------------------------------------
S3Xml::S3Xml() : tinyxml2::XMLPrinter(nullptr, true) {
  PushDeclaration(R"(xml version="1.0" encoding="UTF-8")");
}

//------------------------------------------------------------------------------
//! \brief Open an element with the given name
//! \param[in] elem The name of the element to open
//------------------------------------------------------------------------------
void S3Xml::OpenElement(const char *elem) {
  tinyxml2::XMLPrinter::OpenElement(elem, true);
}

//------------------------------------------------------------------------------
//! \brief Close the current element
//------------------------------------------------------------------------------
void S3Xml::CloseElement() { tinyxml2::XMLPrinter::CloseElement(true); }

//------------------------------------------------------------------------------
//! \brief Add a text element to the current element
//! \param[in] value The text to add
//------------------------------------------------------------------------------
void S3Xml::AddElement(const char *key, const std::string &value) {
  OpenElement(key);
  if (!value.empty()) {
    PushText(value.c_str());
  }
  CloseElement();
}

}  // namespace S3
