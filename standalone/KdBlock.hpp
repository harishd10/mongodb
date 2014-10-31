#ifndef KD_BLOCK_QUERY_HPP
#define KD_BLOCK_QUERY_HPP

#include <iostream>

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/shared_ptr.hpp>

#include "KdQuery.hpp"

namespace mongo {

	class KdBlock {
	public:
		static const uint32_t MAX_RECORDS_PER_BLOCK = 4096; //4;
	//	static const int EXTRA_BLOCKS_PER_LEAF = 7;
	
		struct QueryIterator {
			QueryIterator() :
					iter(0) {
			}
			QueryIterator(TripVector::const_iterator it) :
					iter(it) {
			}
			inline const Trip & operator *() {
				return *(*this->iter);
			}
			inline const Trip * operator->() {
				return *(this->iter);
			}
			inline QueryIterator operator++() {
				this->iter++;
				return *this;
			}
			inline QueryIterator operator--() {
				this->iter--;
				return *this;
			}
			inline QueryIterator operator+=(int dif) {
				this->iter += dif;
				return *this;
			}
			inline QueryIterator operator-=(int dif) {
				this->iter -= dif;
				return *this;
			}
			inline QueryIterator operator++(int) {
				this->iter++;
				return *this;
			}
			inline QueryIterator operator--(int) {
				this->iter--;
				return *this;
			}
			inline ptrdiff_t operator-(const QueryIterator &it) {
				return this->iter - it.iter;
			}
			inline bool operator==(const QueryIterator &it) const {
				return this->iter == it.iter;
			}
			inline bool operator!=(const QueryIterator &it) const {
				return this->iter != it.iter;
			}
			inline bool operator<=(const QueryIterator &it) const {
				return this->iter <= it.iter;
			}
			inline bool operator<(const QueryIterator &it) const {
				return this->iter < it.iter;
			}
			inline bool operator>=(const QueryIterator &it) const {
				return this->iter >= it.iter;
			}
			inline bool operator >(const QueryIterator &it) const {
				return this->iter > it.iter;
			}
			TripVector::const_iterator iter;
		};
	
		typedef std::pair<uint64_t, uint64_t> ResultBlock;
		typedef std::vector<ResultBlock> BlockVector;
		typedef boost::shared_ptr<BlockVector> PBlockVector;
	
		struct QueryResult {
			// TODO Change to uint64_t?
	//		size_t count;
			uint64_t count;
			PBlockVector blocks;
		};
	
	#pragma pack(push, 1)
		struct KdNode {
			uint64_t child_node;
			uint64_t median_value;
		};
	#pragma pack(pop)
	
		struct Iterator {
			Iterator() {
			}
			Iterator(const Trip*t, const KdNode *e) :
					trip(t), end(e) {
			}
	
			const Trip & operator *() {
				return *this->trip;
			}
			const Trip * operator->() {
				return this->trip;
			}
			bool operator==(const Iterator &it) const {
				return this->trip == it.trip;
			}
			bool operator!=(const Iterator &it) const {
				return this->trip != it.trip;
			}
			Iterator operator++(int) {
				const KdNode *node = reinterpret_cast<const KdNode*>(this->trip + 1);
				while (node < this->end && node->child_node != 0)
					node++;
				if (node < this->end) {
					this->trip =
							reinterpret_cast<const Trip*>(&(node->median_value));
				} else
					this->trip = reinterpret_cast<const Trip*>(this->end);
				return *this;
			}
		private:
			const Trip *trip;
			const KdNode *end;
		};
	
	public:
	
		KdBlock() :
				nodes(0), endNode(0) {
		}
	
		KdBlock(const std::string & treeFileName) {
			this->open(treeFileName);
		}
	
		void open(const std::string & treeFileName) {
			this->fTree.open(treeFileName);
			this->nodes = reinterpret_cast<const KdNode*>(fTree.data());
			size_t nodeCount = this->fTree.size() / sizeof(KdNode);
			this->endNode = this->nodes + nodeCount;
		}
	
		Iterator begin() {
			const KdNode *node = this->nodes;
			while (node < this->endNode && node->child_node != 0)
				node++;
			return Iterator(reinterpret_cast<const Trip*>(&(node->median_value)),
					this->endNode);
		}
	
		Iterator end() {
			return Iterator(reinterpret_cast<const Trip*>(this->endNode),
					this->endNode);
		}
	
		QueryResult execute(const KdQuery &q) {
			uint64_t * range = new uint64_t[q.size * 2];
			for(int i = 0;i < q.size;i ++) {
				range[i * 2] = q.lbQuery[i];
				range[i * 2 + 1] = q.ubQuery[i];
			}
			QueryResult result;
			result.count = 0;
			result.blocks = boost::shared_ptr<BlockVector>(new BlockVector());
			searchKdTree(nodes, 0, range, 0, q, result);
			delete[] range;
			return result;
		}
	
		typedef Iterator iterator;
		typedef Iterator const_iterator;
	
	private:
		boost::iostreams::mapped_file_source fTree;
		const KdNode* nodes;
		const KdNode *endNode;
	
		inline bool inRange(uint64_t value, uint64_t range[2]) {
			return (range[0] <= value) && (value <= range[1]);
		}
	
		void searchKdTree(const KdNode *nodes, uint64_t root, uint64_t* range, int depth, const KdQuery &query, QueryResult &result) {
			uint32_t EXTRA_BLOCKS_PER_LEAF = query.size + 1;
			const KdNode *node = nodes + root;
			if (node->child_node == 0xFFFFFFFFFFFFFFFF)
				return;
			if (node->child_node == 0) {
				if (KdQuery::rangeMatched((uint64_t*) (node + 2), (uint64_t*) range, query.size)) {
					uint64_t count = node->median_value;
					uint64_t offset = *((uint64_t*) (node + 1));
					result.blocks->push_back(std::make_pair(count, offset));
					result.count += count;
				}
				return;
			}
			int rangeIndex = depth % query.size;
			uint64_t median = node->median_value;
			if (range[rangeIndex * 2 + 0] <= median) {
				searchKdTree(nodes, node->child_node, range, depth + 1, query,result);
			}
			if (range[rangeIndex * 2 + 1] > median) {
				uint64_t nextNode = node->child_node + 1;
				if (nodes[node->child_node].child_node == 0) {
					nextNode += EXTRA_BLOCKS_PER_LEAF;
				}
				searchKdTree(nodes, nextNode, range, depth + 1, query, result);
			}
		}
	
	};

}

#endif
