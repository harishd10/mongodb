#ifndef CUDA_DB_HPP
#define CUDA_DB_HPP
#include <stdio.h>
#include <stdint.h>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <vector>
#include "RequestQueue.hpp"

#define hlog cerr

namespace mongo {

	class CudaDevice;
	class KdBlock;

	typedef std::vector<long> ResultVec;

	class CudaDb {
	public:
		CudaDb();
		CudaDb(const char *binFile, const char *rangeFile, const char *treeFile, int keySize);
		~CudaDb();
	
		void setBinFile(const char *binFile);
		void setRangeFile(const char *rangeFile);
		void setTreeFile(const char *treeFile);
	
		void start(int nGpus = -1, size_t gpuMemLimit = 0);
		void stop();
	
		void requestQuery(const KdRequest &request);
		void getResult(RequestResult &result);
		RequestResult getResult();
	
		size_t getNumberOfRecords();
		size_t getNumberOfBlocks();
		size_t getKeySize();
	
	private:
		boost::iostreams::mapped_file_source fBin;
		boost::iostreams::mapped_file_source fRange;
	
		std::vector<boost::shared_ptr<CudaDevice> > devices;
	
		size_t numRecords;
		size_t numBlocks;
		size_t keySize;
		
		KdBlock *kdb;
		RequestQueue queue;
	};

}

#endif
