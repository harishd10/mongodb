#pragma once
#include <iostream>
#include <fstream>

#include "mongo/pch.h"

#include "mongo/db/kdtree/Trip.hpp"
#include "mongo/db/kdtree/KdBlock.hpp"

using namespace std;

namespace mongo {
	inline uint64_t getUKey(const Trip * trip, int keyIndex);
	void buildKdTree(KdBlock::KdNode *nodes, uint64_t *tmp, Trip *trips, uint64_t n, int depth, uint64_t thisNode, 
				uint64_t &freeNode, fstream& fo, std::vector<uint64_t> &blockRange, int size, uint64_t * range, TripKey * keys, uint64_t& offset);
	void createKdTree(std::string inputTrips, std::string keysFile, std::string nodeFile, std::string rangeFile, int size);
	inline void swap(Trip * trips, int currentId, int wantedId, int size);
}


