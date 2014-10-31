#ifndef DBHANDLER_HPP_
#define DBHANDLER_HPP_

#include <string>

#include "CudaDb.hpp"

using namespace std;

namespace mongo {

class DBHandler {


private:
	DBHandler();
	~DBHandler();
	static DBHandler* instance;
	boost::unordered_map<string, CudaDb* > devices;

public:
	static DBHandler* getInstance();
	CudaDb* getCudaDb(const char *binFile, const char *rangeFile, const char *treeFile, int keySize);
};


}



#endif /* DBHANDLER_HPP_ */
