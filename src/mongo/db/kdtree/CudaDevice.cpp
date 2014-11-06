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
//		fprintf(stderr, "GPU %d: reserved %g MB.\n", mydevice,this->memReserved / 1024.0 / 1024.0);
	
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
				this->completeQuery(request, request.type == RT_CUDA_DP);
				break;
			case RT_CUDA_IM:
			case RT_CUDA_DP:
				this->completeQueryDP(request, request.type == RT_CUDA_DP);
				break;
			case RT_CUDA_PARTIAL:
				this->partialQuery(request);
				break;
			case RT_CUDA_PARTIAL_IM:
				this->partialQueryIM(request);
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
	void CudaDevice::completeQueryDP(KdRequest &request, bool dp) {
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

		int totalBlocks = request.numBlocks;
		tosub += sizeof(TripKey) * (qsize + 1) * totalBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;

		size_t remMemory = this->memReserved - tosub;
		size_t den = sizeof(uint64_t) * qsize * 2 + sizeof(uint64_t);
		den += (sizeof(TripKey) * (qsize + 1) + sizeof(uint64_t));
		int maxBlocks = remMemory / den;


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

		request.result = RequestResult(new std::vector<long>());
		int iterations = 0;
		std::vector<uint32_t> blockResults;
		while (totalBlocks > 0) {
			int numBlocks = std::min(maxBlocks, totalBlocks);
			int numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;

			int *dTmp = (int*) (dData + numRecords * (qsize + 1));
			int *dResults = (int*) (dTmp + numRecords);
			uint64_t *dBlocks = (uint64_t*) (dResults + numRecords);
			uint32_t *dBlockResults = (uint32_t *)(dBlocks + numBlocks * qsize * 2);

			// copy data
			if(keys != this->lastKeys)
			{
				checkCudaErrors(cudaMemcpyAsync(dData, keys, KdBlock::MAX_RECORDS_PER_BLOCK * sizeof(TripKey) * (qsize + 1) * numBlocks, cudaMemcpyHostToDevice, dStream));
				this->lastKeys = keys;
			}

			// copy block ranges
			if (ranges != this->lastRanges) 
			{
				checkCudaErrors(cudaMemcpyAsync(dBlocks, ranges,2 * qsize * sizeof(uint64_t) * numBlocks,cudaMemcpyHostToDevice, dStream));
				this->lastRanges = ranges;
			}
			totalBlocks -= numBlocks;
			ranges += numBlocks * 2 * qsize;

			if(dp) {
				testBlockDP(numBlocks, dBlocks, dBlockResults, dData, qsize, dQueryRange, request.noRegions, dRegionSize, dRegionOffset, dRegions, dResults, dStream);
				getResults(numRecords, dResults, *request.result, dStream, dTmp);
			} else {
				testBlock(numBlocks, dBlocks, dBlockResults, dQueryRange, dStream, qsize);
				numBlocks = compactBlocks(numBlocks, dBlockResults, blockResults);
				numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
				testKey(numRecords, dBlockResults, dData, qsize, dQueryRange, request.noRegions, dRegionSize, dRegionOffset, dRegions, dResults, dStream);
				getResults(numRecords, dResults, *request.result, dStream, dTmp);
			}
			iterations++;
		}
		checkCudaErrors(cudaStreamDestroy(dStream));
		request.state = RS_DONE;
		this->results.push(request);
	}



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
		
		int totalBlocks = request.numBlocks;
		
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

		request.result = RequestResult(new std::vector<long>());
		int iterations = 0;
		std::vector<uint32_t> blockResults;
		while (totalBlocks > 0) {
			int numBlocks = std::min(maxBlocks, totalBlocks);
			int numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;
	
			uint64_t *dBlocks = (uint64_t*) (dData);
			uint32_t *dBlockResults = (uint32_t *)(dBlocks + numBlocks * qsize * 2);

			{
				checkCudaErrors(cudaMemcpyAsync(dBlocks, ranges,2 * qsize * sizeof(uint64_t) * numBlocks,cudaMemcpyHostToDevice, dStream));
				this->lastRanges = ranges;
			}
			totalBlocks -= numBlocks;
			ranges += numBlocks * 2 * qsize;
	
			if (dp) {
				// Do nothing
			} else {
				testBlock(numBlocks, dBlocks, dBlockResults, dQueryRange, dStream, qsize);
				numBlocks = compactBlocks(numBlocks, dBlockResults, blockResults);
			}
			iterations++;
		}
//		// fprintf(stderr, "(%d) TOTAL COUNT: %lu trips in %d iterations.\n", this->deviceId, request.result->size(), iterations);
		totalBlocks = blockResults.size();
		int maxRecords = (this->memReserved - tosub) / (sizeof(TripKey) * (qsize + 1) + sizeof(uint64_t));
		maxBlocks = maxRecords / KdBlock::MAX_RECORDS_PER_BLOCK;
		
		iterations = 0;
		uint32_t* blockIds = &blockResults[0];
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
//		printf("Total blocks in hybrid: %d \n",totalBlocks);	
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

	void CudaDevice::partialQueryIM(KdRequest &request) {
		uint32_t qsize = request.query->size;
		int regionsSize = 0;
		int tosub = sizeof(uint64_t) * 2 * qsize;
		for(int i = 0;i < request.noRegions;i ++) {
			regionsSize += request.regions[i].size() * 2;
		}
		tosub += regionsSize * sizeof(float) + request.noRegions * sizeof(uint32_t) * 2;

		int maxRecords = (this->memReserved - tosub) / (sizeof(TripKey) * (qsize + 1) + sizeof(uint64_t));
		int maxBlocks = maxRecords / KdBlock::MAX_RECORDS_PER_BLOCK;
		TripKey *keys = request.keys;

		int totalBlocks = request.totalBlocks;
		int numBlocks = request.numBlocks;
		uint64_t *blockIds = request.ranges;
		uint32_t *blockResults = (uint32_t*)malloc(numBlocks*sizeof(uint32_t));
		for(int i = 0;i < request.numBlocks;i ++) {
			blockResults[i] = (uint32_t)(blockIds[i]);
		}
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
		if(totalBlocks > maxBlocks) {
			fprintf(stderr, "Data does not fit in memory!\n");
			return;
		}

		int totRecords = totalBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;

		int numRecords = numBlocks * KdBlock::MAX_RECORDS_PER_BLOCK;

		int *dTmp = (int*) (dData + totRecords * (qsize + 1));
		int *dResults = (int*) (dTmp + numRecords);
		uint32_t *dBlockResults = (uint32_t *)(dResults + numRecords);

			// copy data
		if(keys != this->lastKeys)
		{
			checkCudaErrors(cudaMemcpyAsync(dData, keys, KdBlock::MAX_RECORDS_PER_BLOCK * sizeof(TripKey) * (qsize + 1) * totalBlocks, cudaMemcpyHostToDevice, dStream));
			checkCudaErrors(cudaDeviceSynchronize());
			this->lastKeys = keys;
		}
		checkCudaErrors(cudaMemcpyAsync(dBlockResults, blockResults, sizeof(uint32_t) * numBlocks,cudaMemcpyHostToDevice, dStream));
		checkCudaErrors(cudaDeviceSynchronize());

		testKey(numRecords, dBlockResults, dData, qsize, dQueryRange, request.noRegions, dRegionSize, dRegionOffset, dRegions, dResults, dStream);
		getResults(numRecords, dResults, *request.result, dStream, dTmp);
		iterations++;

		checkCudaErrors(cudaStreamDestroy(dStream));
		request.state = RS_DONE;
		free(blockResults);
		this->results.push(request);
	}
}
