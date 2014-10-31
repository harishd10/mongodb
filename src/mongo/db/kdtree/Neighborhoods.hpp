#ifndef NEIGHBORHOODS_HPP
#define NEIGHBORHOODS_HPP

#include <stdio.h>
#include <vector>
#include <boost/unordered_map.hpp>

class Neighborhoods {
public:

	typedef std::vector<std::pair<float, float> > Geometry;
//  typedef boost::unordered_map<std::string, Geometry> GeometryMap;

	Neighborhoods() {
	}

	static void getBounds(const Geometry &geom, float bounds[4]) {
		if (geom.size() == 0) {
			bounds[0] = bounds[1] = -1e30;
			bounds[2] = bounds[3] = 1e30;
		} else {
			bounds[0] = bounds[1] = 1e30;
			bounds[2] = bounds[3] = -1e30;
			for (size_t i = 0; i < geom.size(); i++) {
				if (geom[i].first < bounds[0])
					bounds[0] = geom[i].first;
				if (geom[i].first > bounds[2])
					bounds[2] = geom[i].first;
				if (geom[i].second < bounds[1])
					bounds[1] = geom[i].second;
				if (geom[i].second > bounds[3])
					bounds[3] = geom[i].second;
			}
		}
	}

#ifdef __CUDACC__
	__host__ __device__
#endif
	static bool isInside(int nvert, float *vert, float testx, float testy) {
		if (nvert <= 0)
			return true;
		float firstX = vert[0];
		float firstY = vert[1];
		int i, j, c = 0;
		for (i = 1, j = 0; i < nvert; j = i++) {
			if (((vert[i * 2 + 1] > testy) != (vert[j * 2 + 1] > testy))
					&& (testx
							< (vert[j * 2] - vert[i * 2])
									* (testy - vert[i * 2 + 1])
									/ (vert[j * 2 + 1] - vert[i * 2 + 1])
									+ vert[i * 2]))
				c = !c;
			if (vert[i * 2] == firstX && vert[i * 2 + 1] == firstY) {
				if (++i < nvert) {
					firstX = vert[i * 2];
					firstY = vert[i * 2 + 1];
				}
			}
		}
		return c;
	}

//private:
//  GeometryMap geometries;
};

#endif
