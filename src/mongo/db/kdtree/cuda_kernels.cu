#include <helper_cuda.h>
#include <helper_string.h>
#include "cuda_kernels.hpp"
#include <thrust/copy.h>
#include <thrust/device_ptr.h>
#include <thrust/remove.h>
#include "KdBlock.hpp"

#include <iostream>
#include <cstdio>
#include <cstdlib>

static const int THREAD_N = 128;
static const int RECORDS_PER_THREAD = 8;

namespace mongo {

	__global__ static void cudaprint_gpu(uint64_t *tmp) {
//		tmp[0] = 4321;
		printf("In gpu \n");
//		uint64_t *st = tmp + 4;
//		printf("%lf %lf %lu %lu\n", uint2double(st[0]), uint2double(st[1]), uint2long(st[2]), st[3]);
		double * dd = (double *)tmp;
		printf("%lf %lf\n", dd[0], dd[1]);
		printf("%lf %lf\n", dd[2], dd[3]);
		printf("%lf %lf\n", dd[4], dd[5]);
	}
	
	__global__ static void cudaTestBlock(uint64_t *blockRanges, int numBlocks, uint64_t *output, uint64_t *queryRanges, uint32_t size)
	{
		const uint32_t index = (blockIdx.x * blockDim.x + threadIdx.x);
		bool matched = (index < numBlocks) && KdQuery::rangeMatched(blockRanges + index * size * 2, queryRanges, size);
		output[index] = matched ? index : 0xFFFFFFFFFFFFFFFF;
	}
	
//	__global__ static void cudaTestKey(int numRecords, TripKey *keys, uint32_t *blockIndex, KdQuery query, int nSrc, float *srcRegion, int nDst, float *dstRegion, int *output)
	__global__ static void cudaTestKey(int numRecords, TripKey *keys, uint64_t *blockIndex, uint32_t qsize, uint64_t * queryRange, int noRegions, uint32_t* regionSize, 
						uint32_t* regionOffset, float *regions, long *output)
	{
		const int tid = (blockIdx.x * blockDim.x + threadIdx.x);
		const int stride = gridDim.x * blockDim.x;
		
		for (int i = tid; i < numRecords; i += stride) {
			int offset = KdBlock::MAX_RECORDS_PER_BLOCK
					* blockIndex[i / KdBlock::MAX_RECORDS_PER_BLOCK]
					+ i % KdBlock::MAX_RECORDS_PER_BLOCK;
			offset = offset * (qsize + 1);
			bool matched = KdQuery::queryMatched(queryRange,keys + offset, qsize);
			if(matched) {
				for(int j = 0;j < noRegions;j ++) {
					matched = Neighborhoods::isInside(regionSize[j], regions + regionOffset[j], uint2double(keys[offset + j * 2 + 0]), uint2double(keys[offset + j * 2 + 1]));
					if(!matched) {
						break;
					}
				}
			}
			output[i] = matched ? (long) keys[offset + qsize] : -1;
		}
	}

	
//	__global__ static void cudaTestKeyAll(int numRecords, TripKey *keys, KdQuery query, int nSrc, float *srcRegion, int nDst, float *dstRegion, int *output)
	__global__ static void cudaTestKeyAll(int numRecords, TripKey *keys, uint32_t qsize, uint64_t * queryRange, int noRegions, uint32_t* regionSize, 
						uint32_t* regionOffset, float *regions, long *output)
	{
		const int tid = (blockIdx.x * blockDim.x + threadIdx.x);
		const int stride = gridDim.x * blockDim.x;
		for (int i = tid; i < numRecords; i += stride) {
			int offset = i * (qsize + 1);
			bool matched = KdQuery::queryMatched(queryRange,keys + offset, qsize);
			if(matched) {
				for(int j = 0;j < noRegions;j ++) {
					matched = Neighborhoods::isInside(regionSize[j], regions + regionOffset[j], uint2double(keys[offset + j * 2 + 0]), uint2double(keys[offset + j * 2 + 1]));
					if(!matched) {
						break;
					}
				}
			}
			output[i] = matched ? (long) keys[offset + qsize] : -1;
		}
	}
//	
//	#if USE_DYNAMIC_PARALLELISM
//	
//	__global__ static void cudaTestKeyDP(TripKey *keys, KdQuery query, int nSrc, float *srcRegion, int nDst, float *dstRegion, int *output)
//	{
//	  const int index = (blockIdx.x * blockDim.x + threadIdx.x);
//	  bool matched = query.isTimeMatched(keys+index)
//	      && Neighborhoods::isInside(nSrc, srcRegion, keys[index].pickup_lat, keys[index].pickup_long)
//	      && Neighborhoods::isInside(nDst, dstRegion, keys[index].dropoff_lat, keys[index].dropoff_long);
//	  output[index] = matched?(int)keys[index].index:-1;
//	}
//	
//	__global__ static void cudaTestBlockDP(int numBlocks, uint32_t *blockRanges, TripKey *keys, uint32_t *queryRanges, KdQuery query, int nSrc, float *srcRegion, int nDst, float *dstRegion, int *output)
//	{
//	  const int index = (blockIdx.x * blockDim.x + threadIdx.x);
//	  bool matched = (index<numBlocks) && KdQuery::rangeMatched(blockRanges + index*12, queryRanges);
//	  if (matched) {
//	    cudaStream_t s;
//	    cudaStreamCreateWithFlags(&s, cudaStreamNonBlocking);
//	    int nThread = THREAD_N;
//	    cudaTestKeyDP<<<KdBlock::MAX_RECORDS_PER_BLOCK/nThread, nThread, 0, s>>>(keys+index*KdBlock::MAX_RECORDS_PER_BLOCK, query, nSrc, srcRegion, nDst, dstRegion, output+index*KdBlock::MAX_RECORDS_PER_BLOCK);
//	    cudaStreamDestroy(s);
//	  }
//	}
//	
//	__global__ static void cudaTestBlockBatchDP(int numBlocks, uint32_t *blockRanges, uint32_t *dBlockResults, TripKey *keys, uint32_t *queryRanges, KdQuery query, int nSrc, float *srcRegion, int nDst, float *dstRegion, int *output)
//	{
//	  __shared__  int sum[THREAD_N+1];
//	  const int index = (blockIdx.x * blockDim.x + threadIdx.x);
//	  const int tid = threadIdx.x;
//	  int offset, i;
//	  
//	  bool matched = (index<numBlocks) && KdQuery::rangeMatched(blockRanges + index*12, queryRanges);  
//	  sum[tid] = matched?1:0;
//	  for (offset=1, i=blockDim.x>>1; i>0; i>>=1) {
//	    __syncthreads();
//	    if(tid<i) {
//	      sum[offset*(2*tid+2)-1] += sum[offset*(2*tid+1)-1];
//	    }
//	    offset <<= 1;
//	  }
//	
//	  __syncthreads();
//	  if (tid==blockDim.x-1) {
//	    sum[blockDim.x] = sum[tid];
//	    sum[tid] = 0;
//	  }
//	
//	  for (i=1; i<blockDim.x; i<<=1) {  
//	    offset >>= 1;  
//	    __syncthreads();  
//	    if (tid<i) {  
//	        int ai = offset*(2*tid+1)-1;
//	        int bi = offset*(2*tid+2)-1;
//	        int temp = sum[ai];  
//	        sum[ai] = sum[bi];  
//	        sum[bi] += temp;
//	    }
//	  }
//	  
//	  __syncthreads();
//	  if (matched)
//	    dBlockResults[blockIdx.x*blockDim.x+sum[tid]] = index;
//	
//	  if (tid==blockDim.x-1) {
//	    int count = sum[blockDim.x]*KdBlock::MAX_RECORDS_PER_BLOCK;
//	    if (count>0) {
//	      cudaStream_t s;
//	      int nBlock = (count+THREAD_N*RECORDS_PER_THREAD-1)/(THREAD_N*RECORDS_PER_THREAD);
//	      cudaStreamCreateWithFlags(&s, cudaStreamNonBlocking);
//	      cudaTestKey<<<nBlock, THREAD_N, 0, s>>>(count, keys, dBlockResults+blockIdx.x*blockDim.x, query, nSrc, srcRegion, nDst, dstRegion,
//	                                              output+blockIdx.x*blockDim.x*KdBlock::MAX_RECORDS_PER_BLOCK);
//	      cudaStreamDestroy(s);
//	    }
//	  }
//	}
//	
//	#endif
	
	void testBlock(int numBlocks, uint64_t *dBlocks, uint64_t *dBlockResults, uint64_t *dQueryRanges, cudaStream_t dStream, uint32_t size)
	{
	  int nThread = THREAD_N;
	  int nBlock = (numBlocks+nThread-1)/(nThread);
	  cudaTestBlock<<<nBlock, nThread, 0, dStream>>>(dBlocks, numBlocks, dBlockResults, dQueryRanges, size);
	}

	void print_gpu(uint64_t * dBlocks) {
//		  uint64_t tmp=1234;
//		  cudaMemcpy(dBlocks, &tmp, sizeof(uint64_t), cudaMemcpyHostToDevice);
		  printf("before print\n");
		  cudaprint_gpu<<<1,1>>>(dBlocks);
		  cudaDeviceSynchronize();
//		  cudaMemcpy(&tmp, dBlocks, sizeof(uint64_t), cudaMemcpyDeviceToHost);
//		  printf("after execute %lu\n", tmp);
	}
	void testKey(int numRecords, uint64_t *dBlockResults, TripKey *dKeys, uint32_t qsize, uint64_t * dQueryRange, int noRegions, uint32_t* dRegionSize, 
					uint32_t* dRegionOffset, float *dRegions, long *dResults, cudaStream_t dStream) {
		int nThread = THREAD_N;
		int nBlock = (numRecords + nThread * RECORDS_PER_THREAD - 1) / (nThread * RECORDS_PER_THREAD);
//		printf("no blocks: %d %d\n",nBlock,nThread);
		cudaTestKey<<<nBlock, nThread, 0, dStream>>>(numRecords, dKeys, dBlockResults, qsize, dQueryRange, noRegions, dRegionSize, dRegionOffset, dRegions, dResults);
	}
	
	//	void testKeyAll(int numRecords, TripKey *dKeys, const KdQuery &query, int nSrc, float *dSrcRegion, int nDst, float *dDstRegion, int *dResults, cudaStream_t dStream)
	void testKeyAll(int numRecords, TripKey *dKeys, uint32_t qsize, uint64_t *dQueryRange, int noRegions, uint32_t* dRegionSize, uint32_t* dRegionOffset, float* dRegions, 
			long* dResults, cudaStream_t dStream) {
		int nThread = THREAD_N;
		int nBlock = (numRecords + nThread * RECORDS_PER_THREAD - 1) / (nThread * RECORDS_PER_THREAD);
		cudaTestKeyAll<<<nBlock, nThread, 0, dStream>>>(numRecords, dKeys, qsize, dQueryRange, noRegions, dRegionSize, dRegionOffset, dRegions, dResults);
	}
	
	struct is_valid {
		__host__  __device__
		bool operator()(const int x) {
			return x != -1;
		}
	};
	
	int compactBlocks(int numBlocks, uint64_t *dBlockResults, std::vector<uint64_t> &blockResults) {
		thrust::device_ptr<uint64_t> dpSrc(dBlockResults);
		size_t num = thrust::remove(dpSrc, dpSrc + numBlocks, -1) - dpSrc;
		size_t currentSize = blockResults.size();
		printf("num = %lu, curSize = %lu\n", num, currentSize);
		blockResults.resize(currentSize + num);
		thrust::copy(dpSrc, dpSrc + num, &blockResults[currentSize]);
		return num;
	}
	
	void getResults(int numRecords, long *dResults, std::vector<long> &results,
						cudaStream_t dStream) {
		thrust::device_ptr<long> dpResults(dResults);
		int numResults = thrust::remove(dpResults, dpResults + numRecords, -1) - dpResults;
		size_t currentSize = results.size();
		results.resize(currentSize + numResults);
		thrust::copy(dpResults, dpResults + numResults, &results[currentSize]);
	}
	
//	void testBlockDP(int numBlocks, uint32_t *dBlocks, uint32_t *dBlockResults, TripKey *dKeys, uint32_t *dQueryRanges, const KdQuery &query, int nSrc, float *dSrcRegion, int nDst, float *dDstRegion, int *dResults, cudaStream_t dStream)
//	{
//	#if USE_DYNAMIC_PARALLELISM
//	  cudaMemset(dResults, 0xFF, numBlocks*KdBlock::MAX_RECORDS_PER_BLOCK*sizeof(int));
//	  int nThread = THREAD_N;
//	  int nBlock = (numBlocks+nThread-1)/nThread;
//	  cudaTestBlockBatchDP<<<nBlock, nThread, 0, dStream>>>(numBlocks, dBlocks, dBlockResults, dKeys, dQueryRanges, query, nSrc, dSrcRegion, nDst, dDstRegion, dResults);
//	#else
//	  fprintf(stderr, "FATAL ERROR: USE_DYNAMIC_PARALLELISM is not enabled.\n");
//	  exit(1);
//	#endif
//	}
	
	__device__ int computePos(int size, volatile int *sum, int tid)
	{
	  int offset, i;
	  for (offset=1, i=size>>1; i>0; i>>=1) {
	    __syncthreads();
	    if(tid<i) {
	      sum[offset*(2*tid+2)-1] += sum[offset*(2*tid+1)-1];
	    }
	    offset <<= 1;
	  }
	
	  __syncthreads();
	  int count = sum[size-1];
	  if (tid==0)
	    sum[size-1] = 0;
	
	  for (i=1; i<size; i<<=1) {  
	    offset >>= 1;  
	    __syncthreads();  
	    if (tid<i) {  
	        int ai = offset*(2*tid+1)-1;
	        int bi = offset*(2*tid+2)-1;
	        int temp = sum[ai];  
	        sum[ai] = sum[bi];  
	        sum[bi] += temp;
	    }
	  }
	
	  return count;
	}


}
