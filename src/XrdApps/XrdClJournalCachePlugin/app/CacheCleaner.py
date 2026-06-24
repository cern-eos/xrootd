#/usr/bin/pythyon3 
##------------------------------------------------------------------------------
## Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
## Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
##------------------------------------------------------------------------------
## This file is part of the XRootD software suite.
##
## XRootD is free software: you can redistribute it and/or modify
## it under the terms of the GNU Lesser General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## XRootD is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public License
## along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
##
## In applying this licence, CERN does not waive the privileges and immunities
## granted to it by virtue of its status as an Intergovernmental Organization
## or submit itself to any jurisdiction.

import os
import time
import argparse

def get_directory_size(directory):
    """Calculate the total size of all files in the directory subtree."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    return total_size

def get_files_by_access_time(directory):
    """Get a list of files sorted by their access time (oldest first)."""
    file_list = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                access_time = os.path.getatime(fp)
                file_list.append((access_time, fp))
    file_list.sort()  # Sort by access time (oldest first)
    return file_list

def clean_directory(directory, high_watermark, low_watermark):
    """Clean the directory by deleting files until the size is below the low watermark."""
    current_size = get_directory_size(directory)
    if current_size <= high_watermark:
        print("Directory size is within the limit. No action needed.")
        return

    files = get_files_by_access_time(directory)
    
    for access_time, file_path in files:
        if current_size <= low_watermark:
            break
        file_size = os.path.getsize(file_path)
        try:
            os.remove(file_path)
            current_size -= file_size
            print(f"Deleted: {file_path} (Size: {file_size} bytes)")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Directory size monitor and cleaner.")
    parser.add_argument("directory", type=str, help="Directory to monitor and clean.")
    parser.add_argument("highwatermark", type=int, help="High watermark in bytes.")
    parser.add_argument("lowwatermark", type=int, help="Low watermark in bytes.")
    parser.add_argument("interval", type=int, help="Interval time in seconds between checks.")
    
    args = parser.parse_args()
    
    directory = args.directory
    high_watermark = args.highwatermark
    low_watermark = args.lowwatermark
    interval = args.interval

    while True:
        clean_directory(directory, high_watermark, low_watermark)
        time.sleep(interval)

if __name__ == "__main__":
    main()
