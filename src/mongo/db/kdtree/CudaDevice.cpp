#include "CudaDevice.hpp"
#include <stdio.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <helper_cuda.h>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include "KdBlock.hpp"
#include "cuda_kernels.hpp"

namespace mongo {

	int CudaDevice::getNumberOfDevices() {
		int deviceCount = 0;
		cudaError_t error_id = cudaGetDeviceCount(&deviceCount);
		return deviceCount;
		if (error_id != cudaSuccess) {
			fprintf(stderr, "cudaGetDeviceCount returned %d\n-> %s\n",
					(int) error_id, cudaGetErrorString(error_id));
			exit(EXIT_FAILURE);
		}
	}
	
	CudaDevice::CudaDevice(int device) :
			memLimit(0), memory(NULL), lastKeys(NULL), lastRanges(NULL), started(
					false) {
		this->setDeviceId(device);
	}
	
	CudaDevice::~CudaDevice() {
	}
	
	void CudaDevice::setDeviceId(int device) {
		this->deviceId = device;
	}
	
	int CudaDevice::getDeviceId() const {
		return this->deviceId;
	}
	
	void CudaDevice::setMemoryLimit(size_t bytes) {
		this->memLimit = bytes;
	}
	
	size_t CudaDevice::getMemoryLimit() const {
		return this->memLimit;
	}
	
	void CudaDevice::start() {
		boost::mutex::scoped_lock lock(this->mutex);
		if (this->started)
			return;
		this->pThread = boost::shared_ptr<boost::thread>(
				new boost::thread(boost::bind(&CudaDevice::run, this)));
	}
	
	void CudaDevice::waitUntilStarted() {
		boost::mutex::scoped_lock lock(this->mutex);
		if (this->started)
			return;
		this->condStarted.wait(lock);
	}
	
	void CudaDevice::stop() {
		this->push(KdRequest(RT_STOP));
		this->pThread->join();
	}
	
	void CudaDevice::initialize() {
		int mydevice = this->deviceId % CudaDevice::getNumberOfDevices();
		cudaSetDevice(mydevice);
		cudaGetDevice(&mydevice);
		size_t memAvail(0), memTotal(0);
		cudaMemGetInfo(&memAvail, &memTotal);
		if (this->memLimit == 0)
			this->memLimit = memAvail;
		this->memReserved = std::min(0.9 * memAvail, 1.0 * this->memLimit);
		checkCudaErrors(cudaMalloc(&this->memory, this->memReserved));
//		fprintf(stderr, "GPU %d: reserved %g MB.\n", mydevice, this->memReserved / 1024.0 / 1024.0);
	
		boost::mutex::scoped_lock lock(this->mutex);
		this->started = true;
		lock.unlock();
		this->condStarted.notify_all();
	}
	
	void CudaDevice::finalize() {
		boost::mutex::scoped_lock lock(this->mutex);
		checkCudaErrors(cudaFree(this->memory));
		this->started = false;
	}
	
	void CudaDevice::run() {
		this->initialize();
		bool stop = false;
		while (!stop) {
			KdRequest request = this->queue.pop();
			switch (request.type) {
			case RT_CUDA:
			case RT_CUDA_DP:
				this->completeQuery(request, request.type == RT_CUDA_DP);
				break;
			case RT_CUDA_PARTIAL:
				this->partialQuery(request);
				break;
			case RT_STOP:
				stop = true;
				break;
			case RT_INVALID:
				this->queue.pop();
				break;
			default:
				break;
			};
		}
		this->finalize();
	}
	
	void CudaDevice::push(const KdRequest &request) {
		this->queue.push(request);
	}
	
	RequestResult CudaDevice::pop() {
		return this->results.popIf(RS_DONE).result;
	}
	
	void CudaDevice::pop(KdRequest &request) {
		this->results.popIf(RS_DONE, request);
	}
	
	// TODO
	void CudaDevice::completeQuery(KdRequest &request, bool dp) {
		uint32_t qsize = request.query->size;
		int tosub = sizeof(uint64_t) * 2 * qsize; // QueryRange
		int regionsSize = 0;
		for(int i = 0;i < request.noRegions;i ++) {
			regionsSize += request.regions[i].size() * 2; // Polygonal regions
		}
		tosub += regionsSize * sizeof(float);
		// TO pass region size and offset
		tosub += sizeof(uint32_t) * request.noRegions; // Polygonal regions offset
		tosub += sizeof(uint32_t) * request.noRegions; // Polygonal regions sizes
		
		
		size_t remMemory = this->memReserved - tosub;
		size_t den = sizeof(uint64_t) * qsize * 2 + sizeof(uint64_t);
		int maxBlocks = remMemory / den;
		
	//		int maxRecords = (this->memReserved - sizeof(uint32_t) * 12 - sizeof(float) * (request.srcRegion.size() + request.dstRegion.size()) * 2) / (sizeof(TripKey) + sizeof(int) + sizeof(uint32_t) * 13.0 / KdBlock::MAX_RECORDS_PER_BLOCK);
//		size_t maxRecords = this->memReserved - tosub;
//		float den = (sizeof(TripKey)* (qsize + 1) + sizeof(uint64_t) + sizeof(uint64_t) * ((float)qsize) * 2.0 / KdBlock::MAX_RECORDS_PER_BLOCK);
//		
//		maxRecords /= den;
//		int maxBlocks = maxRecords / KdBlock::MAX_RECORDS_PER_BLOCK;
		int totalBlocks = request.numBlocks;
//		printf("maxBlock: %d totalBlocks %d\n", maxBlocks, totalBlocks);
		
		TripKey *keys = request.keys;
		uint64_t *ranges = request.ranges;

		// HARISH qRange is the same as query for us
		uint64_t * qRange = new uint64_t[qsize * 2];
		request.query->toRange(qRange);
		
		cudaStream_t dStream;
		checkCudaErrors(cudaStreamCreate(&dStream));
		

		uint64_t *dQueryRange = (uint64_t*) this->memory;
		float *dRegions = (float*) (dQueryRange + qsize * 2);
		uint32_t *dRegionOffset = (uint32_t*)(dRegions + regionsSize); 
		uint32_t *dRegionSize = (uint32_t*)(dRegionOffset + request.noRegions);
		TripKey *dData = (TripKey*) (dRegionSize + request.noRegions);
	
		checkCudaErrors(cudaMemcpyAsync(dQueryRange, qRange, qsize * sizeof(uint64_t) * 2,cudaMemcpyHostToDevice, dStream));

		uint32_t offset = 0;
		for(int i = 0;i < request.noRegions;i ++) {
			checkCudaErrors(cudaMemcpyAsync(dRegions + offset, &request.regions[i].front(), sizeof(float) * request.regions[i].size() * 2,cudaMemcpyHostToDevice, dStream));
			checkCudaErrors(cudaMemcpyAsync(dRegionOffset + i, &offset, sizeof(uint32_t),cudaMemcpyHostToDevice, dStream));
			uint32_t rsize = request.regions[i].size(); 
			checkCudaErrors(cudaMemcpyAsync(dRegionSize + i, &rsize, sizeof(uint32_t),cudaMemcpyHostToDevice, dStream));
			offset += rsize * 2;
		}

//		checkCudaErrors(cudaMemcpyAsync(dDstRegion, &request.dstRegion[0],sizeof(float) * request.dstRegion.size() * 2,cudaMemcpyHostToDevice, dStream));

		request.result = RequestResult(new std::vector<long>());
		int iterations = 0;
		std::vector<uint64_t> blockResults;
		while (totalBlocks > 0) {
			int numBlocks = std::min(maxBlocks, totalBlocks);
			int numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
	
//			long *dResults = (long*) (dData + numRecords * (qsize + 1));
//			uint64_t *dBlocks = (uint64_t*) (dResults + numRecords);
			uint64_t *dBlocks = (uint64_t*) (dData);
//			// TODO check below
			uint64_t *dBlockResults = (uint64_t *)(dBlocks + numBlocks * qsize * 2);
//	
//			if (keys != this->lastKeys) {
//				checkCudaErrors(cudaMemcpyAsync(dData, keys, numRecords * sizeof(TripKey) * (qsize + 1),cudaMemcpyHostToDevice, dStream));
//				uint64_t *st = keys + 4;
//				this->lastKeys = keys;
//			}
			// if (ranges != this->lastRanges) 
			{
				checkCudaErrors(cudaMemcpyAsync(dBlocks, ranges,2 * qsize * sizeof(uint64_t) * numBlocks,cudaMemcpyHostToDevice, dStream));
				this->lastRanges = ranges;
			}
			totalBlocks -= numBlocks;
//			keys += numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
			ranges += numBlocks * 2 * qsize;
	
			if (dp) {
//				testBlockDP(numBlocks, dBlocks, dBlockResults, dData, dQueryRange,
//						request.query, request.srcRegion.size(), dSrcRegion,
//						request.dstRegion.size(), dDstRegion, dResults, dStream);
//				getResults(numRecords, dResults, *request.result, dStream);
			} else {
				testBlock(numBlocks, dBlocks, dBlockResults, dQueryRange, dStream, qsize);
				numBlocks = compactBlocks(numBlocks, dBlockResults, blockResults);
//				numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
//				testKey(numRecords, dBlockResults, dData, qsize, dQueryRange, request.noRegions, dRegionSize, dRegionOffset, dRegions, dResults, dStream);
//				getResults(numRecords, dResults, *request.result, dStream);
			}
			iterations++;
		}
//		printf("no. of blocks found: %lu\n", blockResults.size());
//		// fprintf(stderr, "(%d) TOTAL COUNT: %lu trips in %d iterations.\n", this->deviceId, request.result->size(), iterations);
		totalBlocks = blockResults.size();
		int maxRecords = (this->memReserved - tosub) / (sizeof(TripKey) * (qsize + 1) + sizeof(uint64_t));
		maxBlocks = maxRecords / KdBlock::MAX_RECORDS_PER_BLOCK;
		
		iterations = 0;
		uint64_t* blockIds = &blockResults[0];
		while (totalBlocks > 0) {
			int numBlocks = std::min(maxBlocks, totalBlocks);
			int numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
	
			for (int i = 0; i < numBlocks; i++) {
				checkCudaErrors(cudaMemcpyAsync(dData + KdBlock::MAX_RECORDS_PER_BLOCK * i * (qsize + 1), keys + blockIds[i] * (qsize + 1) * KdBlock::MAX_RECORDS_PER_BLOCK, 
						KdBlock::MAX_RECORDS_PER_BLOCK * sizeof(TripKey) * (qsize + 1), cudaMemcpyHostToDevice, dStream));
			}
	
			totalBlocks -= numBlocks;
			blockIds += numBlocks;
	
			long *dResults = (long*) (dData + numRecords * (qsize + 1));
			testKeyAll(numRecords, dData, qsize, dQueryRange, request.noRegions, dRegionSize, dRegionOffset, dRegions, dResults, dStream);
			getResults(numRecords, dResults, *request.result, dStream);
			iterations++;
		}
		
		checkCudaErrors(cudaStreamDestroy(dStream));
		request.state = RS_DONE;
		this->results.push(request);
	}
	
	void CudaDevice::partialQuery(KdRequest &request) {
//		printf("calling partial query\n");
		uint32_t qsize = request.query->size;
		int regionsSize = 0;
		int tosub = sizeof(uint64_t) * 2 * qsize;
		for(int i = 0;i < request.noRegions;i ++) {
			regionsSize += request.regions[i].size() * 2;
		}
		tosub += regionsSize * sizeof(float) + request.noRegions * sizeof(uint32_t) * 2;

		int maxRecords = (this->memReserved - tosub) / (sizeof(TripKey) * (qsize + 1) + sizeof(uint64_t));
		int maxBlocks = maxRecords / KdBlock::MAX_RECORDS_PER_BLOCK;
		int totalBlocks = request.numBlocks;
		uint64_t *blockIds = request.ranges;
	
		uint64_t qRange[qsize * 2];
		request.query->toRange(qRange);
		
		cudaStream_t dStream;
		checkCudaErrors(cudaStreamCreate(&dStream));
		
		uint64_t *dQueryRange = (uint64_t*) this->memory;
		float *dRegions = (float*) (dQueryRange + qsize * 2);
		
		uint32_t *dRegionOffset = (uint32_t*)(dRegions + regionsSize); 
		uint32_t *dRegionSize = (uint32_t*)(dRegionOffset + request.noRegions);
		TripKey *dData = (TripKey*) (dRegionSize + request.noRegions);
		
		checkCudaErrors(cudaMemcpyAsync(dQueryRange, qRange, qsize * sizeof(uint64_t) * 2,cudaMemcpyHostToDevice, dStream));
		
		uint32_t offset = 0;
		for(int i = 0;i < request.noRegions;i ++) {
			checkCudaErrors(cudaMemcpyAsync(dRegions + offset, &request.regions[i].front(), sizeof(float) * request.regions[i].size() * 2,cudaMemcpyHostToDevice, dStream));
			checkCudaErrors(cudaMemcpyAsync(dRegionOffset + i, &offset, sizeof(uint32_t),cudaMemcpyHostToDevice, dStream));
			uint32_t rsize = request.regions[i].size(); 
			checkCudaErrors(cudaMemcpyAsync(dRegionSize + i, &rsize, sizeof(uint32_t),cudaMemcpyHostToDevice, dStream));
			offset += rsize * 2;
		}
	
		request.result = RequestResult(new std::vector<long>());
		int iterations = 0;
		while (totalBlocks > 0) {
			int numBlocks = std::min(maxBlocks, totalBlocks);
			int numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
	
			for (int i = 0; i < numBlocks; i++) {
				checkCudaErrors(cudaMemcpyAsync(dData + KdBlock::MAX_RECORDS_PER_BLOCK * i * (qsize + 1), request.keys + blockIds[i] * (qsize + 1), 
						KdBlock::MAX_RECORDS_PER_BLOCK * sizeof(TripKey) * (qsize + 1), cudaMemcpyHostToDevice, dStream));
			}
	
			totalBlocks -= numBlocks;
			blockIds += numBlocks;
	
			long *dResults = (long*) (dData + numRecords * (qsize + 1));
			testKeyAll(numRecords, dData, qsize, dQueryRange, request.noRegions, dRegionSize, dRegionOffset, dRegions, dResults, dStream);
			getResults(numRecords, dResults, *request.result, dStream);
			iterations++;
		}
		// fprintf(stderr, "TOTAL COUNT: %lu trips in %d iterations.\n", request.result->size(), iterations);
		
		checkCudaErrors(cudaStreamDestroy(dStream));
		request.state = RS_DONE;
		this->results.push(request);
	}

}
