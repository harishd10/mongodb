#include "CudaDb.hpp"
#include "CudaDevice.hpp"
#include "CudaHandler.hpp"
#include "KdBlock.hpp"
#include <boost/filesystem.hpp>
#include <omp.h>

#include "mongo/pch.h"

namespace mongo {

	CudaDb::CudaDb() :
			numRecords(0), numBlocks(0), keySize(0), kdb(0) {
		this->deviceHandler = CudaHandler::getInstance();
	}
	
	CudaDb::CudaDb(const char *binFile, const char *rangeFile, const char *treeFile, int keySize) :
			numRecords(0), numBlocks(0), kdb(0) {
		this->keySize = keySize;
		this->setBinFile(binFile);
		this->setRangeFile(rangeFile);
		this->setTreeFile(treeFile);
		this->deviceHandler = CudaHandler::getInstance();
	}
	
	CudaDb::~CudaDb() {
		if (this->kdb)
			delete this->kdb;
	}
	
	void CudaDb::setBinFile(const char *binFile) {
		// bin is keys
		if (binFile) {
			this->numRecords = boost::filesystem::file_size(binFile) / (sizeof(TripKey) * keySize);
			this->numBlocks = this->numRecords / KdBlock::MAX_RECORDS_PER_BLOCK;
			this->fBin.open(binFile);
		}
	}
	
	void CudaDb::setRangeFile(const char *rangeFile) {
		if (rangeFile)
			this->fRange.open(rangeFile);
	}
	
	void CudaDb::setTreeFile(const char *treeFile) {
		if (treeFile) {
			if (this->kdb)
				delete this->kdb;
			this->kdb = new KdBlock(treeFile);
		}
	}
	
//	void CudaDb::start(int nGpus, size_t gpuMemLimit) {
//		int maxDevices = CudaDevice::getNumberOfDevices();
//		if (nGpus < 0)
//			nGpus = maxDevices;
//		int nDev = std::min(nGpus, maxDevices);
//		this->devices.clear();
//		for (int i = 0; i < nDev; i++) {
//			boost::shared_ptr<CudaDevice> dev(new CudaDevice(i));
//			if (gpuMemLimit > 0)
//				dev->setMemoryLimit(gpuMemLimit);
//			dev->start();
//			this->devices.push_back(dev);
//		}
//		for (size_t i = 0; i < this->devices.size(); i++)
//			this->devices[i]->waitUntilStarted();
//	}
//
//	void CudaDb::stop() {
//		for (size_t i = 0; i < this->devices.size(); i++)
//			this->devices[i]->stop();
//	}
	
//	void CudaDb::start(int nGpus, size_t gpuMemLimit) {
//		this->deviceHandler.start(nGpus, gpuMemLimit);
//	}

//	void CudaDb::stop() {
//		this->deviceHandler.stop();
//	}

	void CudaDb::requestQuery(const KdRequest &r) {
		
		KdRequest request = r;
		
		if (request.type == RT_CPU) {
			request.result = RequestResult(new std::vector<long>());
		}
		this->queue.push(request);
	
		switch (request.type) {
		case RT_CUDA:
		case RT_CUDA_DP:
			hlog << "GPU execution" << endl;
			hlog << "No. of devices: " << this->deviceHandler->devices.size() << endl;
			request.numBlocks = this->numBlocks / this->deviceHandler->devices.size();
			for (size_t i = 0; i < this->deviceHandler->devices.size(); i++) {
				request.ranges = (uint64_t*) this->fRange.data() + (request.numBlocks) * i * 2 * r.query->size;
				// TODO  change this when devices > 1
				request.keys = (TripKey*) this->fBin.data() + (request.numBlocks) * i * KdBlock::MAX_RECORDS_PER_BLOCK;
				this->deviceHandler->devices[i]->push(request);
			}
			break;
	
		case RT_CUDA_PARTIAL: {
			request.keys = (TripKey*) this->fBin.data();
			KdBlock::QueryResult result = this->kdb->execute(*request.query);
			int numBlocks = result.blocks->size();
			for (size_t i = 0; i < this->deviceHandler->devices.size(); i++) {
				request.numBlocks = numBlocks / this->deviceHandler->devices.size();
				request.ranges = new uint64_t[request.numBlocks];
				for (int k = 0; k < request.numBlocks; k++) {
					request.ranges[k] = result.blocks->at(request.numBlocks * i + k).second;
				}
				this->deviceHandler->devices[i]->push(request);
			}
		}
			break;
	
		case RT_CPU: {
			hlog << "CPU execution" << endl;
			size_t EXTRA_BLOCKS_PER_LEAF = this->keySize; 
			int noKeys = this->keySize - 1;
			int gsize = request.noRegions;
			
			KdBlock::QueryResult result = this->kdb->execute(*request.query);
			TripKey *keys = (TripKey*) fBin.data();
			printf("No. of blocks %zu \n", result.blocks->size());

			uint64_t noBlocks = result.blocks->size();
//			for (size_t i = 0; i < noBlocks; i++) {
//				uint32_t count = result.blocks->at(i).first;
//				uint64_t offset = result.blocks->at(i).second;
//				for (uint32_t j = 0; j < count; j++) {
//					uint64_t pos = (offset + j) * EXTRA_BLOCKS_PER_LEAF;
//					TripKey * curKey = keys + pos;
//					uint64_t index = * (curKey + noKeys);
//					bool match = true;
//					for(int k = 0;k < noKeys;k ++) {
//						if(!request.query->isMatched(curKey,k)) {
//							match = false;
//							break;
//						}
//					}
//					if(match) {
//						for(int k = 0;k < gsize;k ++) {
//							double x = uint2double(curKey[k * 2]);
//							double y = uint2double(curKey[k * 2 + 1]);
//							if(!Neighborhoods::isInside(request.regions[k].size(),&request.regions[k][0].first,x,y)) {
//								match = false;
//								break;
//							}
//						}
//					}
//					if(match) {
//						request.result->push_back(index);
//					}
//				}
//			}

			ResultVec res[noBlocks];
#pragma omp parallel for
			for (size_t i = 0; i < noBlocks; i++) {
				uint32_t count = result.blocks->at(i).first;
				uint64_t offset = result.blocks->at(i).second;
				for (uint32_t j = 0; j < count; j++) {
					uint64_t pos = (offset + j) * EXTRA_BLOCKS_PER_LEAF;
					TripKey * curKey = keys + pos;
					uint64_t index = * (curKey + noKeys);
					bool match = true;
					for(int k = 0;k < noKeys;k ++) {
						if(!request.query->isMatched(curKey,k)) {
							match = false;
							break;
						}
					}
					if(match) {
						for(int k = 0;k < gsize;k ++) {
							double x = uint2double(curKey[k * 2]);
							double y = uint2double(curKey[k * 2 + 1]);
							if(!Neighborhoods::isInside(request.regions[k].size(),&request.regions[k][0].first,x,y)) {
								match = false;
								break;
							}
						}
					}
					if(match) {
						res[i].push_back(index);
					}
				}
			}
			for (size_t i = 0; i < noBlocks; i++) {
				request.result->insert(request.result->end(),res[i].begin(),res[i].end());
			}
		}
			break;
	
		default:
			fprintf(stderr, "Unhandled request type %d\n", request.state);
			break;
		}
	}
	
	void CudaDb::getResult(RequestResult &result) {
		KdRequest request = this->queue.pop();
		if (request.type == RT_CPU) {
			result->insert(result->end(), request.result->begin(),
					request.result->end());
			return;
		}
		for (size_t i = 0; i < this->deviceHandler->devices.size(); i++) {
			this->deviceHandler->devices[i]->pop(request);
			result->insert(result->end(), request.result->begin(),
					request.result->end());
			if (request.type == RT_CUDA_PARTIAL)
				delete[] request.ranges;
		}
	}
	
	RequestResult CudaDb::getResult() {
		RequestResult result(new std::vector<long>());
		this->getResult(result);
		return result;
	}
	
	size_t CudaDb::getNumberOfRecords() {
		return this->numRecords;
	}
	
	size_t CudaDb::getNumberOfBlocks() {
		return this->numBlocks;
	}

	size_t CudaDb::getKeySize() {
		return this->keySize;
	}
}
