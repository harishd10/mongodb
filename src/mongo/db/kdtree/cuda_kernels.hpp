#ifndef CUDA_KERNELS_HPP
#define CUDA_KERNELS_HPP

#include <stdint.h>
#include <vector>
#include <cuda.h>
#include <cuda_runtime.h>
#include "KdQuery.hpp"

namespace mongo {

	void testBlock(int numBlocks, uint64_t *dBlocks, uint32_t *dBlockResults, uint64_t *dQueryRanges, cudaStream_t dStream, uint32_t size);
	int  compactBlocks(int numBlocks, uint32_t *dBlockResults, std::vector<uint32_t> &blockResults);

	void testKey(int numRecords, uint32_t *dBlockResults, TripKey *dKeys, uint32_t qsize, uint64_t * queryRange, int noRegions, uint32_t* dRegionSize, uint32_t* sRegionOffset, float *dRegions, int *dResults, cudaStream_t dStream);
	void getResults(int numRecords, long *dResults, std::vector<long> &results, cudaStream_t dStream);

//	void getResults(int numRecords, long *dResults, std::vector<long> &results, cudaStream_t dStream, long *dTmp);
	void getResults(int numRecords, int* dResults, std::vector<long> &results, cudaStream_t dStream, int* dTmp);

	void print_gpu(uint64_t * dBlocks);
//	void testBlockDP(int numBlocks, uint32_t *dBlocks, uint32_t *dBlockResults, TripKey *dKeys, uint32_t *dQueryRanges, const KdQuery &query, int nSrc, float *dSrcRegion, int nDst, float *dDstRegion, int *dResults, cudaStream_t dStream);
	
	void testKeyAll(int numRecords, TripKey *dKeys, uint32_t qsize, uint64_t *dQueryRange, int noRegions, uint32_t* dRegionSize, uint32_t* dRegionOffset, float* dRegions, long* dResults, cudaStream_t dStream);
//	void testKeyAll(int numRecords, TripKey *dKeys, const KdQuery &query, int nSrc, float *dSrcRegion, int nDst, float *dDstRegion, int *dResults, cudaStream_t dStream);
	
	void prefixSum();

	void testBlockDP(int numBlocks, uint64_t *dBlocks, uint32_t *dBlockResults, TripKey *dKeys, uint32_t qsize, uint64_t *dQueryRanges, int noRegions, uint32_t* dRegionSize, uint32_t* dRegionOffset, float* dRegions, int* dResults, cudaStream_t dStream);
}
  
#endif
