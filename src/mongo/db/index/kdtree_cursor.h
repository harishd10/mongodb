
#pragma once


#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pdfile.h"
#include "mongo/platform/unordered_map.h"

#include "mongo/db/index/kdtree_access_method.h"
#include "mongo/db/kdtree/KdQuery.hpp"

#include <boost/iostreams/device/mapped_file.hpp>

namespace mongo {
	
	enum QueryType {
		Equal, Lt, Gt
	};
	
	class KdtreeCursor : public IndexCursor {
	public:
		KdtreeCursor(KdtreeAccessMethod* accessMethod);
		~KdtreeCursor() {
			if(disk.is_open()) {
				disk.close();
			}
		}
        virtual Status seek(const BSONObj& position);
        virtual bool isEOF() const;
        virtual void next();
        virtual BSONObj getKey() const;
        virtual DiskLoc getValue() const;

        virtual Status savePosition();
        virtual Status restorePosition();

        virtual string toString();


        // Deprecated. not implemented
        virtual Status seek(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive) { return Status::OK(); }
        virtual Status skip(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive) { return Status::OK(); }
        
        // Not required. so not implements
        virtual Status setOptions(const CursorOptions& options) { return Status::OK(); }

    private:
        KdtreeAccessMethod* _accessMethod;
        vector<long> sel;
        boost::iostreams::mapped_file_source disk;
        DiskLoc* result;
        uint32_t noQueries;
        
        unsigned long pos;
//        double * lbQuery;
//        double * ubQuery;
//        KdQuery * query;
        KdRequest * queryRequest;
        const static REQUEST_TYPE requestType = RT_CUDA_PARTIAL;
        
        void parseQuery(const BSONObj& position, vector<BSONObj>& queries);
        void getQuery(const BSONObj& position);
        void updateQuery(KdRequest* request,string name, BSONElement e, QueryType qtype);
        void updateQuery(KdRequest* request,int index, int64_t lval, double dval, QueryType qtype);
        void initQuery(int noQueries);
        void addPoly(KdRequest* request,string name, BSONObj& poly);
        void addBox(KdRequest* request,string name, BSONObj& box);
        void generateQuery(KdRequest* request,const BSONObj& position);
 	};
	

} // namespace mongo
