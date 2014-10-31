#ifndef CUDAHANDLER_HPP_
#define CUDAHANDLER_HPP_

#include <stdio.h>
#include <stdint.h>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <vector>



namespace mongo {

	class CudaDevice;

	class CudaHandler {
	private:
		CudaHandler();
		~CudaHandler();
		static CudaHandler* instance;
		void start(int nGpus = -1, size_t gpuMemLimit = 0);
		void stop();

	public:
		static CudaHandler* getInstance(int nGpus = -1, size_t gpuMemLimit = 0);
		std::vector<boost::shared_ptr<CudaDevice> > devices;
	};

}


#endif /* CUDAHANDLER_HPP_ */
