#include "KdIndex.hpp"

#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <string>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
// #include "mongo/db/kdtree/sort.h"
#include "radix_pair.h"

using namespace std;

namespace mongo {
	
	inline uint64_t getUKey(const Trip* trip, int keyIndex) {
		return trip[keyIndex];
	}
	
	inline void swap(Trip * trips, uint64_t currentId, uint64_t wantedId, int size, TripKey * keys) {
		uint64_t c = currentId * size;
		uint64_t w = wantedId * size;
		size_t s = sizeof(TripKey) * size;
		memcpy(keys, trips + c, s);
		memcpy(trips + c, trips + w, s);
		memcpy(trips + w, keys, s);
//		for(int i = 0;i < size;i ++) {
//			register uint64_t tmp = trips[c + i];
//			trips[c + i] = trips[w + i];
//			trips[w + i] = tmp;
//		}
	}
	
	void buildKdTree(KdBlock::KdNode *nodes, uint64_t *tmp, Trip *trips, uint64_t n, int depth, uint64_t thisNode, 
			uint64_t &freeNode, FILE *fo, std::vector<uint64_t> &blockRange, int size, uint64_t * range, TripKey * keys, uint64_t& offset) {
		int EXTRA_BLOCKS_PER_LEAF = size + 1; 
		KdBlock::KdNode *node = nodes + thisNode;
		
		if (n <= KdBlock::MAX_RECORDS_PER_BLOCK) {
			node->child_node = 0;
			node->median_value = n;
			// Gives offset
			*((uint64_t*) (node + 1)) = offset; //ftello(fo);
			for (int i = 0; i < size; i++) {
				range[i * 2 + 0] = ULONG_MAX;
				range[i * 2 + 1] = 0;
			}
			for (uint64_t i = 0; i < n; i++) {
				memcpy(keys, trips + i * EXTRA_BLOCKS_PER_LEAF, sizeof(TripKey) * EXTRA_BLOCKS_PER_LEAF);
				// index is size+1^th attribute
				offset ++;
				if(offset % 1000 == 0)
					fprintf(stderr, "\r%llu", offset);
				fwrite(keys, sizeof(TripKey), EXTRA_BLOCKS_PER_LEAF, fo);
				for (int j = 0; j < size; j++) {
					uint64_t k = getUKey(trips + i * EXTRA_BLOCKS_PER_LEAF, j);
					if (k < range[2 * j + 0]) {
						range[2 * j + 0] = k;
					}
					if (k > range[2 * j + 1]) {
						range[2 * j + 1] = k;
					}
				}
			}
			
			blockRange.insert(blockRange.end(), (uint64_t*) range, ((uint64_t*) range) + (size * 2));
			memcpy(node + 2, range, sizeof(uint64_t) * size * 2);
			if (n != KdBlock::MAX_RECORDS_PER_BLOCK) {
				hlog << "Padding a node with " << n << " to " <<  KdBlock::MAX_RECORDS_PER_BLOCK << endl;
				keys[size] = -1;
				for (uint32_t i = n; i < KdBlock::MAX_RECORDS_PER_BLOCK; i++) {
					offset++;
					fwrite(&keys, sizeof(TripKey), EXTRA_BLOCKS_PER_LEAF, fo);
				}
			}
			return;
		}
	
		int keyIndex = depth % size;
		
//		for (uint64_t i = 0; i < n; i++) {
//			tmp[i] = getUKey(trips + i * EXTRA_BLOCKS_PER_LEAF, keyIndex);
//			tmp[n + i] = i;
//		}
//		sort(tmp + n, tmp, n);
//		
//		size_t medianIndex = ceil(((float) (n / 2)) / KdBlock::MAX_RECORDS_PER_BLOCK) * KdBlock::MAX_RECORDS_PER_BLOCK - 1;
//		size_t index = tmp[n + medianIndex];
//		uint64_t median = tmp[index];
//		if (medianIndex == n - 1) {
//			hlog << "BINGO " << medianIndex << endl;
//			verify(0);
//		}		
//		for (uint64_t i = 0; i < n; i++) {
//			uint64_t currentId = i;
//			uint64_t wantedId = tmp[n + i];
//			while (wantedId != i) {
//				swap(trips, currentId, wantedId, EXTRA_BLOCKS_PER_LEAF);
//				tmp[n + currentId] = currentId;
//				currentId = wantedId;
//				wantedId = tmp[n + currentId];
//			}
//			tmp[n + currentId] = currentId;
//		}
		
//		if(depth < 5) {
//			hlog << "setting up sort " << depth << endl;
//		}
		for (size_t i = 0; i < n; i++) {
			tmp[i * 2] = getUKey(trips + i * EXTRA_BLOCKS_PER_LEAF, keyIndex);
			tmp[i * 2 + 1] = i;
		}
//		if(depth < 5) {
//			hlog << "Starting sorting " << depth << endl;
//		}
		sortArray((uint128_t*) tmp, n);
		size_t medianIndex = ceil( ((float) (n / 2)) / KdBlock::MAX_RECORDS_PER_BLOCK) * KdBlock::MAX_RECORDS_PER_BLOCK - 1;
		uint64_t median = tmp[medianIndex * 2];
//		if(depth < 5) {
//			hlog << "Finished sorting " << depth << endl;
//		}
		if (medianIndex == n - 1) {
			hlog << "BINGO " << medianIndex << endl;
			//verify(0);
			exit(0);
		}
		for (uint64_t i = 0; i < n; i++) {
//			if (i%10000UL==0)
//	  			fprintf(stderr, "%lu\n", i);
			uint64_t currentId = i;
			uint64_t wantedId = tmp[i * 2 + 1];
			while (wantedId != i) {
				swap(trips, currentId, wantedId, EXTRA_BLOCKS_PER_LEAF, keys);
//				SWAP(uint64_t, tripId[currentId], tripId[wantedId]);
				tmp[currentId * 2 + 1] = currentId;
				currentId = wantedId;
				wantedId = tmp[currentId * 2 + 1];
			}
			tmp[currentId * 2 + 1] = currentId;
		}
//		if(depth < 5) {
//			hlog << "Finished swapping " << depth << endl;
//		}
		node->median_value = median;
		node->child_node = freeNode;
		
		freeNode += 2 + ((uint64_t) (medianIndex + 1 <= KdBlock::MAX_RECORDS_PER_BLOCK)) * EXTRA_BLOCKS_PER_LEAF
				+ ((uint64_t) ((n - medianIndex - 1 <= KdBlock::MAX_RECORDS_PER_BLOCK) && (n - medianIndex - 1 > 0))) * EXTRA_BLOCKS_PER_LEAF;
		buildKdTree(nodes, tmp, trips, medianIndex + 1, depth + 1, node->child_node, freeNode, fo, blockRange, size, range, keys, offset);
		if (medianIndex < n - 1) {
			uint64_t off = (medianIndex + 1) * EXTRA_BLOCKS_PER_LEAF;
//			buildKdTree(nodes, tmp, trips + off, n - medianIndex - 1,
			buildKdTree(nodes, tmp, trips + off, n - medianIndex - 1,
					depth + 1, node->child_node + 1 + ((uint64_t) (medianIndex + 1 <= KdBlock::MAX_RECORDS_PER_BLOCK)) * EXTRA_BLOCKS_PER_LEAF, 
					freeNode, fo, blockRange, size, range, keys, offset);
		}
		else {
			nodes[node->child_node + 1].child_node = -1;
		}
	}
	
	void createKdTree(std::string inputTrips, std::string keysFile, std::string treeFile, std::string rangeFile, int size) {
		int EXTRA_BLOCKS_PER_LEAF = size + 1;
		hlog << "Creating KD tree" << endl;
		boost::iostreams::mapped_file mfile(inputTrips, boost::iostreams::mapped_file::priv);
		uint64_t n = mfile.size() / ((size + 1) * sizeof(uint64_t));
		
		hlog << "no. of records: " << n << endl;
		Trip *trips = (Trip*) mfile.const_data();
		// Ceil
		uint64_t blocks = (n / KdBlock::MAX_RECORDS_PER_BLOCK) + 1;
		
		// TODO 2 should be enough
		KdBlock::KdNode *nodes = (KdBlock::KdNode*) malloc(sizeof(KdBlock::KdNode) * (2 * blocks * EXTRA_BLOCKS_PER_LEAF));
		uint64_t *tmp = (uint64_t*) malloc(sizeof(uint64_t) * n * 2);
//		uint64_t *tripId = (uint64_t*) malloc(sizeof(uint64_t) * n);
		uint64_t *range = new uint64_t[size * 2];
		TripKey * keys =  new TripKey[size + 1];
		
//		for(uint64_t i = 0;i < n;i ++) {
//			tripId[i] = i;
//		}
		hlog << "Finished allocating memory" << endl;
		
		std::vector<uint64_t> blockRange;
		FILE *fdata = fopen(keysFile.c_str(), "wb");
		uint64_t freeNode = 1;
		uint64_t offset = 0;
		buildKdTree(nodes, tmp, trips, n, 0, 0, freeNode, fdata, blockRange,size, range, keys, offset);
		fclose(fdata);
		
		FILE *fblock = fopen(rangeFile.c_str(), "wb");
		fwrite(&blockRange[0], sizeof(uint64_t), blockRange.size(), fblock);
		fclose(fblock);
	
		// Writing new indices file
		hlog << "\rWriting " << freeNode << " nodes to " << treeFile << endl;
		FILE *fo = fopen(treeFile.c_str(), "wb");
		fwrite(nodes, sizeof(KdBlock::KdNode), freeNode, fo);
		fclose(fo);
	
		free(nodes);
		free(tmp);
		delete[] range;
		delete[] keys;
		mfile.close();
	}
	
}
