#include "MongoPerf.h"
#include <stdio.h>
#include <stdint.h>
#include <omp.h>

using namespace std;
using namespace mongo;

KdRequest * queryRequest;
REQUEST_TYPE requestType;

inline static uint64_t createTime(int year, int month, int day, int hour, int min, int sec) {
	struct tm timeinfo;
	memset(&timeinfo, 0, sizeof(timeinfo));
	timeinfo.tm_year = year - 1900;
	timeinfo.tm_mon = month - 1;
	timeinfo.tm_mday = day;
	timeinfo.tm_hour = hour;
	timeinfo.tm_min = min;
	timeinfo.tm_sec = sec;
	timeinfo.tm_isdst = -1;
	return mktime(&timeinfo);
}

void updateQuery(KdRequest *queryRequest, int index, int64_t lval, double dval, QueryType qtype, bool isdouble) {
    KdQuery* query = queryRequest->query;
    if(index >= 0 && index < size) {
        uint64_t val;
		if(isdouble) {
			val = double2uint(dval);
		} else {
			val = long2uint(lval);
		}
    	if(qtype == Equal) {
			query->setInterval(index,val,val);
    	} else if(qtype == Lt) {
    		query->setUpperBound(index, val);
    	} else {
    		query->setLowerBound(index, val);
    	}
    } else {
    	hlog << "additional keys not supported" << endl;
    	exit(0);
    }
}


void addPoly(KdRequest *queryRequest, int index, const Neighborhoods::Geometry &poly) {
    int gindex = index / 2;
    // TODO for now assuming double
    double minx = 0, maxx = 0, miny = 0,maxy = 0;
    bool first = true;
    for(size_t i = 0;i < poly.size();i ++) {
    	double x = poly[i].first;
    	double y = poly[i].second;
    	
    	if(first) {
    		first = false;
    		minx = x;
    		maxx = x;
    		miny = y;
    		maxy = y;
    	}
    	if(minx > x) {
    		minx = x;
    	}
    	if(miny > y) {
    		miny = y;
    	}
    	if(maxx < x) {
    		maxx = x;
    	}
    	if(maxy < y) {
    		maxy = y;
    	}
		
    	// TODO add (x,y) to polygon
    	std::pair<float, float> point(x,y);
    	queryRequest->regions[gindex].push_back(point);
    }
    // add bounding box to constraints
	updateQuery(queryRequest,index,maxx,maxx,Lt,true);
	updateQuery(queryRequest,index,minx,minx,Gt,true);

	index = index + 1;
	updateQuery(queryRequest,index,maxy,maxy,Lt,true);
	updateQuery(queryRequest,index,miny,miny,Gt,true);
}

int createQuery(const TimePeriod &pickup, const TimePeriod &dropoff, const Neighborhoods::Geometry &src, const Neighborhoods::Geometry &dst) {
	// assuming pickup.size() == dropoff.size(), or one of them is 0
	int no = pickup.size();
	if(no == 0) {
		no = dropoff.size();
	}
	queryRequest = new KdRequest[no];
	int geomSize = 2;
	for(int i = 0;i < no;i ++) {
		queryRequest[i].type = requestType;
		queryRequest[i].query = new KdQuery(size);
		queryRequest[i].regions = new Neighborhoods::Geometry[geomSize];
		queryRequest[i].noRegions = geomSize;
		
		if(pickup.size() != 0) {
			int64_t st = pickup[i].first;
			int64_t en = pickup[i].second;
			updateQuery(&queryRequest[i],4, st, 0, Gt, false);
			updateQuery(&queryRequest[i],4, en, 0, Lt, false);
		}
		if(dropoff.size() != 0) {
			uint64_t st = dropoff[i].first;
			uint64_t en = dropoff[i].second;
			updateQuery(&queryRequest[i],5, st, 0, Gt, false);
			updateQuery(&queryRequest[i],5, en, 0, Lt, false);
		}
	
		if(src.size() > 0) {
			addPoly(&queryRequest[i], 0, src);
		}
	
		if(dst.size() > 0) {
			addPoly(&queryRequest[i], 2, dst);
		}

	}
	return no;
}

void testPerformance(string keysFile, string treeFile, string rangeFile, int argc, char* argv[]) {
	int           nGpu        = -1;
	size_t        gpuMemLimit = 0;
	std::string   rType       = "hybrid";
	int           nRequest    = 1;
	int           nIteration  = 1;
	int           duration    = 1;
	bool          report      = true;
	
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--help") == 0) {
			fprintf(stderr,
					"Usage: cuda_query <input.data> <input.range> <input.keys> [options]\n");
			fprintf(stderr, "Options:\n");
			fprintf(stderr,
					"   --ngpu <number of GPUs>          [default: %d]\n",
					nGpu);
			fprintf(stderr,
					"   --mem  <GPU mem limit>           [default: %lu]\n",
					gpuMemLimit);
			fprintf(stderr,
					"   --type <cuda|cuda_im|cuda_dp|hybrid|hybrid_im|cpu|cpup> [default: %s]\n",
					rType.c_str());
			fprintf(stderr,
					"   --iter <number of iterations>    [default: %d]\n",
					nIteration);
			fprintf(stderr,
					"   --nreq <number of requests>      [default: %d]\n",
					nRequest);
			fprintf(stderr,
					"   --len <length of each request>   [default: %d]\n",
					duration);
			fprintf(stderr,
					"   --report                         [default: OFF]\n");
			exit(0);
		} else if (strcmp(argv[i], "--ngpu") == 0) {
			nGpu = atoi(argv[++i]);
		} else if (strcmp(argv[i], "--mem") == 0) {
			gpuMemLimit = atoll(argv[++i]);
		} else if (strcmp(argv[i], "--type") == 0) {
			rType = argv[++i];
		} else if (strcmp(argv[i], "--nreq") == 0) {
			nRequest = atoi(argv[++i]);
		} else if (strcmp(argv[i], "--iter") == 0) {
			nIteration = atoi(argv[++i]);
		} else if (strcmp(argv[i], "--len") == 0) {
			duration = atoi(argv[++i]);
		} else if (strcmp(argv[i], "--report") == 0) {
			report = true;
		}
	}
	
	Neighborhoods neighbors("neighborhoods.txt");
	  
	Neighborhoods::Geometry srcRegion;
	neighbors.append("Chelsea", srcRegion);
	neighbors.append("Gramercy", srcRegion);
	neighbors.append("West Village", srcRegion);
	  
	Neighborhoods::Geometry dstRegion;
	neighbors.append("Midtown", dstRegion);
	neighbors.append("Financial District", dstRegion);
	  
	requestType = RT_CUDA_PARTIAL;
	if (rType == "cuda")
		requestType = RT_CUDA;
	else if (rType == "cuda_dp")
		requestType = RT_CUDA_DP;
	else if (rType == "cpu") {
		requestType = RT_CPU;
		omp_set_num_threads(1);
	}
	else if (rType == "cpup") {
		requestType = RT_CPU;
		omp_set_num_threads(omp_get_max_threads());
	} else if (rType == "cuda_im")
		requestType = RT_CUDA_IM;
	else if (rType == "hybrid_im")
		requestType = RT_CUDA_PARTIAL_IM;

	  
	  
	int year[5] = {2009, 2010, 2011, 2012, 2013};
	int stride = (int)(ceil((60.0-duration+1)/nRequest));
	TimePeriod ptime;
	TimePeriod dtime;
	
//	cout << "before db start" << endl;
	CudaDb db(keysFile.c_str(), rangeFile.c_str(), treeFile.c_str(), size + 1);
    db.start(nGpu, gpuMemLimit);
//  cout << "after db start" << endl;

	for (int k = 0; k < nIteration; k++) {
//		double start = WALLCLOCK();
		int reqCnt = 0;
		ptime.clear();
		dtime.clear();
		for (int m = 0; m < (60 - duration + 1); m += stride, reqCnt++) {
			uint64_t st = createTime(year[m / 12], 1 + m % 12, 1, 0, 0, 0);
			uint64_t en = createTime(year[(m + duration - 1) / 12], 1 + (m + duration - 1) % 12, 27, 23, 59, 59);
			ptime.push_back(std::make_pair(st, en));
//			dtime.push_back(std::make_pair(st, en));
		}
		int no = createQuery(ptime, dtime, srcRegion, dstRegion);
//		cout << "no. of queries: " << no << endl;

		double start = WALLCLOCK();
	    for(int i = 0;i < no;i ++) {
		    db.requestQuery(queryRequest[i]);
	    }
	    RequestResult result = KdRequest::emptyResult();
	    for (int m=0; m<no; m++) {
	    	db.getResult(result);
	    }
	    
		double end = WALLCLOCK();
//		fprintf(stderr, "TOTAL COUNT: %lu trips in %d requests\n", result->size(), reqCnt);
//		fprintf(stderr, "TOTAL TIME: %.2f ms\n", (end - start) * 1000);
		if (report && k == nIteration - 1) {
			fprintf(stdout, "%s\t%d\t%lu\t%d\t%d\t%lu\t%lu\t%.2f\n", rType.c_str(), nGpu, gpuMemLimit, nRequest, duration, db.getNumberOfRecords(), result->size(), (end-start)*1000);
		}
	}
	
    db.stop();

}
