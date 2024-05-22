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

#pragma once

//------------------------------------------------------------------------------
#include <tinyxml2.h>
#include <string>
//------------------------------------------------------------------------------

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

