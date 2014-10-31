#ifndef MONGOPERF_H_
#define MONGOPERF_H_

#include <stdio.h>
#include <stdint.h>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <vector>
#include "timing.hpp"
#include "CudaDb.hpp"
#include "KdQuery.hpp"
#include "Neighborhoods.hpp"

using namespace std;
using namespace mongo;

typedef std::vector<std::pair<uint64_t, uint64_t> > TimePeriod;
const int size = 6;

enum QueryType {
	Equal, Lt, Gt
};

extern KdRequest * queryRequest;
extern REQUEST_TYPE requestType;

void updateQuery(KdRequest *queryRequest, int index, int64_t lval, double dval, QueryType qtype, bool isdouble);
void addPoly(KdRequest *queryRequest, int index, const Neighborhoods::Geometry &poly);
int createQuery(const TimePeriod &pickup, const TimePeriod &dropoff, const Neighborhoods::Geometry &src, const Neighborhoods::Geometry &dst);
void testPerformance(string keysFile, string treeFile, string rangeFile, int argc, char* argv[]);


#endif /* MONGOPERF_H_ */
