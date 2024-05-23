//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters / CERN EOS Project <andreas.joachim.peters@cern.ch>
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

#ifdef __APPLE__
// Macros to translate Linux xattr function names to macOS equivalents
#define getxattr(path, name, value, size) \
    xattr_getxattr(path, name, value, size, 0, 0)

#define lgetxattr(path, name, value, size) \
    xattr_getxattr(path, name, value, size, 0, XATTR_NOFOLLOW)

#define fgetxattr(fd, name, value, size) \
    fgetxattr(fd, name, value, size)

#define setxattr(path, name, value, size, flags) \
    xattr_setxattr(path, name, value, size, 0, flags)

#define lsetxattr(path, name, value, size, flags) \
    xattr_setxattr(path, name, value, size, 0, XATTR_NOFOLLOW)

#define fsetxattr(fd, name, value, size, flags) \
    fsetxattr(fd, name, value, size, flags)

#define removexattr(path, name) \
    xattr_removexattr(path, name, 0)

#define lremovexattr(path, name) \
    xattr_removexattr(path, name, XATTR_NOFOLLOW)

#define fremovexattr(fd, name) \
    fremovexattr(fd, name)

#define listxattr(path, list, size) \
    xattr_listxattr(path, list, size, 0)

#define llistxattr(path, list, size) \
    xattr_listxattr(path, list, size, XATTR_NOFOLLOW)

#define flistxattr(fd, list, size) \
    flistxattr(fd, list, size)
#endif

