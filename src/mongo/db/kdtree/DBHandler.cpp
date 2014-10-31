
#include "DBHandler.hpp"
#include "CudaDb.hpp"

namespace mongo {

	DBHandler* DBHandler::instance = NULL;

	DBHandler::DBHandler() { }
	DBHandler::~DBHandler() {
		for(boost::unordered_map<string, CudaDb*>::iterator it = devices.begin();it != devices.end(); ++it) {
			CudaDb* db = it->second;
			delete db;
		}
		devices.clear();
	}

	DBHandler* DBHandler::getInstance() {
		if(instance == NULL) {
			instance = new DBHandler();
		}
		return instance;
	}

	CudaDb* DBHandler::getCudaDb(const char *binFile, const char *rangeFile, const char *treeFile, int keySize) {
		string bin = string(binFile);
		if(devices.find(bin) == devices.end()) {
			CudaDb* db = new CudaDb(binFile, rangeFile, treeFile, keySize);
			devices[bin] = db;
		}
		return devices[bin];
	}
}
