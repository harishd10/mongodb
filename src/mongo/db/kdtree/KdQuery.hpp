#ifndef QUERY_HPP
#define QUERY_HPP

#include <stdint.h>
#include <time.h>
#include <limits.h>
#include <float.h>
#include "Trip.hpp"
#include "Neighborhoods.hpp"
#include <boost/shared_ptr.hpp>
#include <iostream>

using namespace std;
namespace mongo {

#ifdef __CUDACC__
	__host__ __device__
#endif
	inline uint64_t double2uint(double f) {
		register uint64_t t(*((uint64_t*) &f));
		return t ^ ((-(t >> 63UL)) | 0x8000000000000000UL);
	}
	
#ifdef __CUDACC__
	__host__ __device__
#endif
	inline double uint2double(uint64_t f) {
		register uint64_t u = f ^ (((f >> 63UL) - 1UL) | 0x8000000000000000UL);
		return *((double*) &u);
	}
	
#ifdef __CUDACC__
	__host__ __device__
#endif
	inline int64_t uint2long(uint64_t f) {
		register uint64_t u = (f ^ 0x8000000000000000UL);
		return *((int64_t*) &u);
	}
	
#ifdef __CUDACC__
	__host__ __device__
#endif
	inline uint64_t long2uint(int64_t f) {
		register uint64_t t(*((uint64_t*) &f));
		return (t ^ 0x8000000000000000UL);
	}
	
	
	struct KdQuery {
		KdQuery(int size) {
			this->size = size;
			lbQuery = new uint64_t[size];
			ubQuery = new uint64_t[size];
			for(int i = 0;i < size;i ++) {
				lbQuery[i] = 0;
				ubQuery[i] = ULONG_MAX;
			}
		}
		
		~KdQuery() {
			delete[] lbQuery;
			delete[] ubQuery;
		}
		
		void setInterval(int index, uint64_t t0, uint64_t t1) {
			this->lbQuery[index] = t0;
			this->ubQuery[index] = t1;
		}
	
		void setUpperBound(int index, uint64_t t) {
			this->ubQuery[index] = t;
		}
		
		void setLowerBound(int index, uint64_t t) {
			this->lbQuery[index] = t;
		}
		
		bool isMatched(const Trip *trip) const {
			for(int i = 0;i < size;i ++) {
				if(this->lbQuery[i] > trip[i] || trip[i] > this->ubQuery[i]) {
					return false;
				}
			}
			return true;
		}
	
	#ifdef __CUDACC__
		__host__ __device__
	#endif
		bool isMatched(const Trip *trip, int index) const {
			return (this->lbQuery[index] <= trip[index]
					&& trip[index] <= this->ubQuery[index]);
		}
		
	#ifdef __CUDACC__
		__host__ __device__
	#endif
		static bool rangeMatched(const uint64_t *range1, const uint64_t *range2, int size) {
			for (int i = 0; i < size; i++) {
				if (range1[i * 2] > range2[i * 2 + 1] || range2[i * 2] > range1[i * 2 + 1]) {
					return false;
				}
			}
			return true;
		}
	
	#ifdef __CUDACC__
		__host__ __device__
	#endif
		static bool queryMatched(const uint64_t *range, const uint64_t *key, int size) {
			for (int i = 0; i < size; i++) {
				if (range[i * 2] > key[i] || range[i * 2 + 1] < key[i]) {
					return false;
				}
			}
			return true;
		}
		
		void toRange(uint64_t *range) const {
			for(int i = 0;i < size;i ++) {
				range[2 * i + 0] = lbQuery[i];
				range[2 * i + 1] = ubQuery[i];
			}
		}
		  
		int size;
		uint64_t * lbQuery;
		uint64_t * ubQuery;
		
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
	
	};
	
	enum REQUEST_TYPE {
		RT_CUDA = 0,
		RT_CUDA_DP = 1,
		RT_CUDA_PARTIAL = 2,
		RT_CPU = 3,
		RT_STOP = 4,
		RT_INVALID
	};
	
	enum REQUEST_STATE {
		RS_NEW = 0, RS_DONE = 1, RS_INVALID
	};
	
	typedef boost::shared_ptr<std::vector<long> > RequestResult;
	
	struct KdRequest {
		KdRequest() :
				type(RT_INVALID), state(RS_NEW) {
			numBlocks = 0;
			keys = 0;
			query = 0;
			regions = 0;
			noRegions = 0;
			ranges = 0;
		}
		KdRequest(REQUEST_TYPE t) :
				type(t), state(RS_NEW) {
			numBlocks = 0;
			keys = 0;
			query = 0;
			regions = 0;
			noRegions = 0;
			ranges = 0;
		}
	
		REQUEST_TYPE type;
		REQUEST_STATE state;
		int numBlocks;
		TripKey *keys;
		uint64_t *ranges;
		KdQuery * query;
	
		Neighborhoods::Geometry * regions;
		int noRegions;
		
		RequestResult result;
	
		static RequestResult emptyResult() {
			return RequestResult(new std::vector<long>);
		}
	};

}

#endif
