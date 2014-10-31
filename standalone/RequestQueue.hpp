#ifndef REQUEST_QUEUE_H
#define REQUEST_QUEUE_H

#include <string.h>
#include <deque>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include "KdQuery.hpp"

namespace mongo {

	class RequestQueue {
	public:
	
		RequestQueue(size_t _maxSize = 128) {
			this->maxSize = _maxSize;
		}
	
		size_t size() const {
			boost::mutex::scoped_lock lock(this->mutex);
			return this->queue.size();
		}
	
		void push(const KdRequest &request) {
			boost::mutex::scoped_lock lock(this->mutex);
			while (this->maxSize > 0 && this->queue.size() >= this->maxSize)
				this->condPush.wait(lock);
			this->queue.push_back(request);
			lock.unlock();
			this->condPop.notify_one();
		}
	
		void waitToPush() {
			boost::mutex::scoped_lock lock(this->mutex);
			while (this->maxSize > 0 && this->queue.size() >= this->maxSize)
				this->condPush.wait(lock);
			lock.unlock();
		}
	
		KdRequest pop() {
			boost::mutex::scoped_lock lock(this->mutex);
			while (this->queue.empty())
				this->condPop.wait(lock);
			KdRequest request = this->queue.front();
			this->queue.pop_front();
			lock.unlock();
			this->condPush.notify_one();
			return request;
		}
	
		bool checkState(REQUEST_STATE state) {
			boost::mutex::scoped_lock lock(this->mutex);
			return (!this->queue.empty() && this->queue.front().state == state);
		}
	
		void popIf(REQUEST_STATE state, KdRequest &request) {
			boost::mutex::scoped_lock lock(this->mutex);
			while (this->queue.empty() || this->queue.front().state != state)
				this->condPop.wait(lock);
			request = this->queue.front();
			this->queue.pop_front();
			lock.unlock();
			this->condPush.notify_one();
		}
	
		KdRequest popIf(REQUEST_STATE state) {
			KdRequest request;
			this->popIf(state, request);
			return request;
		}
	
		void notifyProcessed() {
			this->condPop.notify_one();
		}
	
		KdRequest & front() {
			return this->queue.front();
		}
	
	private:
		size_t maxSize;
		std::deque<KdRequest> queue;
		mutable boost::mutex mutex;
		boost::condition_variable condPush;
		boost::condition_variable condPop;
	};

}

#endif
