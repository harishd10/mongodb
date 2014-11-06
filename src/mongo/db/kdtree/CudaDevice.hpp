#ifndef CUDA_DEVICE_HPP
#define CUDA_DEVICE_HPP
#include <queue>
#include <vector>
#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include "RequestQueue.hpp"

namespace mongo {

	class CudaDevice {
	public:
		CudaDevice(int device = 0);
		~CudaDevice();
	
		void setKdFile(const std::string &fn);
	
		void setDeviceId(int device);
		int getDeviceId() const;
	
		void setMemoryLimit(size_t bytes);
		size_t getMemoryLimit() const;
	
		void start();
		void waitUntilStarted();
		void stop();
	
		void push(const KdRequest &request);
		RequestResult pop();
		void pop(KdRequest &request);
	
		static int getNumberOfDevices();
	
	protected:
		void initialize();
		void finalize();
		void run();
	
		void completeQuery(KdRequest &request, bool dp);
		void completeQueryDP(KdRequest &request, bool dp);
		void partialQuery(KdRequest &request);
		void partialQueryIM(KdRequest &request);
	
	private:
		int deviceId;
		size_t memLimit;
		size_t memReserved;
		void *memory;
		void *lastKeys;
		void *lastRanges;
	
		boost::shared_ptr<boost::thread> pThread;
		RequestQueue queue;
		RequestQueue results;
	
		bool started;
		mutable boost::mutex mutex;
		boost::condition_variable condStarted;
	};

}

#endif
