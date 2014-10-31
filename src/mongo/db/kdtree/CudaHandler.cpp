#include "CudaHandler.hpp"
#include "CudaDevice.hpp"

#include "mongo/pch.h"


namespace mongo {

	CudaHandler* CudaHandler::instance = NULL;

	CudaHandler::CudaHandler() {
	}

	CudaHandler::~CudaHandler() {
		this->stop();
	}

	CudaHandler *CudaHandler::getInstance(int nGpus,size_t gpuMemLimit){
	    if(instance == NULL) {
	        instance = new CudaHandler();
	        instance->start(nGpus,gpuMemLimit);
	    }

	    return instance;
	}
	void CudaHandler::start(int nGpus, size_t gpuMemLimit) {
		int maxDevices = CudaDevice::getNumberOfDevices();
		if (nGpus < 0)
			nGpus = maxDevices;
		int nDev = std::min(nGpus, maxDevices);
		this->devices.clear();
		for (int i = 0; i < nDev; i++) {
			boost::shared_ptr<CudaDevice> dev(new CudaDevice(i));
			if (gpuMemLimit > 0)
				dev->setMemoryLimit(gpuMemLimit);
			dev->start();
			this->devices.push_back(dev);
		}
		for (size_t i = 0; i < this->devices.size(); i++)
			this->devices[i]->waitUntilStarted();
	}

	void CudaHandler::stop() {
		for (size_t i = 0; i < this->devices.size(); i++)
			this->devices[i]->stop();
	}

}



