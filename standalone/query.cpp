#include <iostream>
#include <string>

#include "MongoPerf.h"

using namespace std;
using namespace mongo;

/**
    the complete list of valid BSON types
    see also bsonspec.org
*/

int main(int argc, char *argv[]) {
	if(argc < 2) {
		cerr << "Enter sample count" << endl;
		exit(0);
	}
//	string _indexFile = "/local_scratch/harish/inmemory/test.trips.kdtest." + string(argv[1]);
	string _indexFile = string(argv[1]);
	//cout << _indexFile << endl;
	if(argv[1][0] == '0') {
		_indexFile = "/local_scratch/harish/mongo-data/test.trips.kdtest";
	}
	//string _indexFile = "/local_scratch/harish/mongo-data/test.trips.kdtest";
	string keysFile = _indexFile + ".keys";
	string treeFile = _indexFile + ".tree";
	string rangeFile = _indexFile + ".range";
	
	testPerformance(keysFile, treeFile, rangeFile, argc, argv);
	return 0;
}
