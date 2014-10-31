#include <iostream>
#include "KdIndex.hpp"
#include <cstring>
#include <string>

using namespace std;
using namespace mongo;

int main(int argc, char* argv[]) {
	//string _indexFile = "/local_scratch/harish/inmemory/smallblock/test.trips.kdtest." + string(argv[1]);
//	_indexFile = _indexFile + ".1";
	string _indexFile = string(argv[1]);
	string datafile = _indexFile + ".data";
	string keysFile = _indexFile + ".keys";
	string treeFile = _indexFile + ".tree";
	string rangeFile = _indexFile + ".range";
	int size = 6;
	createKdTree(datafile,keysFile,treeFile,rangeFile,size);
}



