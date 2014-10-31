#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <string>
#include "Trip.hpp"

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include "radix_pair.h"

using namespace std;

uint64_t * ptime;
bool sortFn(uint64_t i, uint64_t j) {
	return ptime[i] < ptime[j];
}

int main(int argc, char * argv[]) {
	string _indexFile = "test.trips.kdtest.sample_45";
	string datafile = "../../inmemory/" + _indexFile + ".data";

	int p = atoi(argv[1]);
	string samplefile = _indexFile + ".sample_" + string(argv[1]) + ".data";
	uint64_t size = 6;

	int EXTRA_BLOCKS_PER_LEAF = size + 1;
	cout << "Creating KD tree, size = " << size << endl;
	boost::iostreams::mapped_file mfile(datafile, boost::iostreams::mapped_file::priv);
	uint64_t fsize = mfile.size();
	uint64_t n = mfile.size() / ((size + 1) * sizeof(uint64_t));
	cout << "no. of records: " << n << endl;
	uint64_t nBlocks = 16384 * p / 15;
	Trip *trips = (Trip*) mfile.const_data();
	uint64_t *index;
	ptime = (uint64_t *) malloc(sizeof(uint64_t) * n);
	index = (uint64_t *) malloc(sizeof(uint64_t) * n);
	cout << "before sort" << endl;
	for(uint64_t i = 0;i < n;i ++) {
		if(i % 1000000 == 0) {
			printf("\r %ld",i);
			fflush(stdout);
		}
		uint64_t tin = i * (size + 1) + 4;
		ptime[i] = trips[tin];
		index[i] = i;
	}
	cout << endl;
	cout << "Finished allocating memory" << endl;
	std::sort(index, index + n,sortFn);
	cout << "Finished sorting" << endl;
	
	for(uint64_t i = 1;i < n;i ++) {
		if(ptime[index[i]] < ptime[index[i-1]]) {
			cerr << "didn't sort properly" << endl;
			exit(0);
		}
	}
	cout << "sampling data" << endl;

	FILE *fo = fopen(samplefile.c_str(), "wb");
	uint64_t ct = 0;
	uint64_t rct = nBlocks * 4096;
	int ad = floor(n / rct);
	cout << "no. of blocks: " << nBlocks <<  " no. of records: " << rct << endl;
	for(uint64_t i = 0;i < n;i += ad) {
		uint64_t in = index[i] * (size + 1);
		fwrite(trips+in, sizeof(Trip), size+1, fo);
		ct ++;
		if(ct == rct) {
			break;
		}
	}
	fclose(fo);
	cout << "no. of records written: " << ct << endl;
	return 0;
}

