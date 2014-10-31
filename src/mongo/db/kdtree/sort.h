#pragma once

#include <algorithm>

namespace mongo {

	struct compare {
		uint64_t * data;
	
		compare(uint64_t * fn) {
			data = fn;
		}
	
		bool operator()(int in1, int in2) {
			uint64_t v1 = data[in1];
			uint64_t v2 = data[in2];
			if(v1 < v2) {
				return true;
			} 
			if(v1 == v2 && in1 < in2) {
				return true;
			}
			return false;
		}
	};
	
	void sort(uint64_t* in, uint64_t* data, int length) {
		std::sort(in, in + length, compare(data));
	}
}



