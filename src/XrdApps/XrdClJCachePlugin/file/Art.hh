//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
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

#pragma once

/*----------------------------------------------------------------------------*/
#include <iostream>
#include <iomanip>
#include <vector>
#include <cmath>
#include <numeric>
/*----------------------------------------------------------------------------*/


class Art {
public:
  Art() {}
  virtual ~Art() {}

  void drawCurve(const std::vector<double>& dataPoints) {
    if (dataPoints.size() != 10) {
      std::cerr << "Error: Exactly 10 data points are required." << std::endl;
      return;
    }
    
    double maxValue = *std::max_element(dataPoints.begin(), dataPoints.end());
    double minValue = *std::min_element(dataPoints.begin(), dataPoints.end());
    
    const int plotHeight = 10; // Number of lines in the plot
    const int plotWidth = 40;  // Width of the plot in characters
    const int yLegendWidth = 8; // Width of the Y legend in characters
    
    std::vector<std::string> plot(plotHeight, std::string(plotWidth, ' '));
    
    // Normalize data points to the plot height
    std::vector<int> normalizedDataPoints;
    for (double point : dataPoints) {
      int normalizedValue = static_cast<int>((point - minValue) / (maxValue - minValue) * (plotHeight - 1));
      normalizedDataPoints.push_back(normalizedValue);
    }
    
    // Draw the curve
    for (size_t i = 0; i < normalizedDataPoints.size(); ++i) {
      int y = plotHeight - 1 - normalizedDataPoints[i];
      plot[y][i * (plotWidth / (dataPoints.size() - 1))] = '*';
    }
    
    // Print the plot with Y legend
    for (int i = 0; i < plotHeight; ++i) {
      double yValue = minValue + (maxValue - minValue) * (plotHeight - 1 - i) / (plotHeight - 1);
      std::cout << std::setw(yLegendWidth) << std::fixed << std::setprecision(2) << yValue << " MB/s | ";
      std::cout << plot[i] << std::endl;
    }

    // Print the X axis
    std::cout << std::string(yLegendWidth + 7, ' ') << std::string(plotWidth, '-') << std::endl;
    std::cout << std::string(yLegendWidth + 7, ' ') << "0  1  2  3  4  5  6  7  8  9" << std::endl;
  }

  void drawCurve(const std::vector<long unsigned int>& data, double interval) {
    std::vector<double> newdata;
    for ( auto i:data ) {
      newdata.push_back(i/1000000.0 / interval);
    }
    return drawCurve(newdata);
  }
};

