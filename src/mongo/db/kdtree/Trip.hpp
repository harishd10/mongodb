#ifndef TRIPS_HPP
#define TRIPS_HPP

#include <vector>
#include <boost/unordered_set.hpp>


//struct Trip {
//	// last attribute is the index
//	// So should be size + 1
//	uint64_t * attributes;
//};

//struct TripKey {
//	uint64_t * attributes;
//};

typedef uint64_t Trip;
typedef uint64_t TripKey;

typedef std::vector<const Trip*> TripVector;
//typedef boost::unordered_set<const Trip*> TripSet;

#endif
