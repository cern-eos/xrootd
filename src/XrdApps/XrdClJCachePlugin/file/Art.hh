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

namespace JCache {
  class Art {
  public:
    Art() {}
    virtual ~Art() {}

    void drawCurve(const std::vector<double>& dataPoints, double runtime) {
      if (dataPoints.size() != 40) {
        std::cerr << "Error: Exactly 40 data points are required." << std::endl;
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
	if (normalizedValue<0){
	  normalizedValue=0;
	}
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
        if (i==0) {
	  std::cerr << "# " << std::setw(yLegendWidth) << std::fixed << std::setprecision(2) << yValue << " MB/s | ";
        } else {
	  std::cerr << "# " << std::setw(yLegendWidth) << std::fixed << std::setprecision(2) << yValue << "      | ";
        }
        std::cerr << plot[i] << std::endl;
      }

      // Print the X axis
      std::cerr << "# " << std::string(yLegendWidth + 7, ' ') << std::string(plotWidth, '-') << std::endl;
      std::cerr << "# " << std::string(yLegendWidth + 7, ' ');
      for (size_t i = 0 ; i < dataPoints.size()/4; ++i) {
        std::cerr << std::fixed << std::setw(4) << std::left << (i*10);
      }
      std::cerr << "[ " << 100 << " % = " << std::fixed << std::setprecision(2) << runtime << "s ]"<< std::endl;
    }

    void drawCurve(const std::vector<long unsigned int>& data, double interval, double runtime) {
      std::vector<double> newdata;
      if (interval == 0) {
	interval = 0.00001;
      }
      if (runtime == 0) {
	runtime = 0.00001;
      }
      for ( auto i:data ) {
        newdata.push_back(i/1000000.0 / interval);
      }
      return drawCurve(newdata, runtime);
    }
  };
} // namespace JCache
