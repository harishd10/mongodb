#include "mongo/db/index/kdtree_cursor.h"

#include <fstream>
#include <vector>
#include <climits>

#include "mongo/db/index/kdtree_access_method.h"
#include "mongo/db/index/catalog_hack.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/queryutil.h"

#include "mongo/db/kdtree/KdBlock.hpp"
#include "mongo/db/kdtree/CudaDb.hpp"
#include "mongo/db/kdtree/CudaHandler.hpp"
#include "mongo/db/kdtree/DBHandler.hpp"
#include <boost/foreach.hpp>

namespace mongo {

	KdtreeCursor::KdtreeCursor(KdtreeAccessMethod* accessMethod): _accessMethod(accessMethod) {
		pos = 0;
		result = 0;
		noQueries = 0;
		queryRequest = 0;
	}
	
	void KdtreeCursor::initQuery(int noQueries) {
		int size = _accessMethod->keys.size();
		queryRequest = new KdRequest[noQueries];
		for(int i = 0;i < noQueries;i ++) {
			queryRequest[i].type = requestType;
			queryRequest[i].query = new KdQuery(size);
			
			int geomSize = _accessMethod->geoKeys.size();
			queryRequest[i].regions = new Neighborhoods::Geometry[geomSize];
			queryRequest[i].noRegions = geomSize;
		}
	}
	
	void KdtreeCursor::updateQuery(KdRequest *queryRequest, int index, int64_t lval, double dval, QueryType qtype) {
        int size = _accessMethod->keys.size();
        KdQuery* query = queryRequest->query;
        if(index >= 0 && index < size) {
            uint64_t val;
			if(_accessMethod->type[index] == NumberDouble) {
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
        	verify(0);
        }
	}
	
	void KdtreeCursor::updateQuery(KdRequest *queryRequest, string name, BSONElement e, QueryType qtype) {
		int64_t lval;
		double dval;
    	int index = _accessMethod->keyIndex[name];
        if (e.type() == NumberInt) {
        	lval = e.numberInt();
        	dval = lval;
        } else if(e.type() == NumberLong) {
        	lval = e.numberLong();
        	dval = lval;
        } else if(e.type() == NumberDouble) {
        	dval = e.numberDouble();
        	lval = dval;
        } else {
        	hlog << "unsupported type" << endl;
        	verify(0);
        }
        updateQuery(queryRequest,index, lval, dval, qtype);
	}
	
    void KdtreeCursor::addPoly(KdRequest *queryRequest, string name, BSONObj& poly) {
        BSONObjIterator i(poly);
        
        int index = _accessMethod->keyIndex[name+".x"];
        int gindex = index / 2;
        // TODO for now assuming double
        double minx = 0, maxx = 0, miny = 0,maxy = 0;
        bool first = true;
        while(i.more()) {
        	BSONElement pt = i.next();
        	BSONObjIterator i1(pt.Obj());
        	double x = i1.next().number();
        	double y = i1.next().number();
        	
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
        	std::pair<double, double> point(x,y);
        	queryRequest->regions[gindex].push_back(point);
        }
        // add bounding box to constraints
		updateQuery(queryRequest,index,maxx,maxx,Lt);
		updateQuery(queryRequest,index,minx,minx,Gt);

		index = _accessMethod->keyIndex[name+".y"];
		updateQuery(queryRequest,index,maxy,maxy,Lt);
		updateQuery(queryRequest,index,miny,miny,Gt);
    }
    
    void KdtreeCursor::addBox(KdRequest *queryRequest, string name, BSONObj& box) {
    	BSONObjIterator i(box);
    	
    	BSONElement ep1 = i.next();
    	BSONElement ep2 = i.next();
    	// TODO for now assuming double
    	
    	BSONObjIterator i1(ep1.Obj());
    	double x1 = i1.next().number();
    	double y1 = i1.next().number();
    	
    	BSONObjIterator i2(ep2.Obj());
    	double x2 = i2.next().number();
    	double y2 = i2.next().number();
    	
    	int index = _accessMethod->keyIndex[name+".x"];
    	if(x1 < x2) {
    		updateQuery(queryRequest,index,x2,x2,Lt);
    		updateQuery(queryRequest,index,x1,x1,Gt);
    	} else {
    		updateQuery(queryRequest,index,x2,x2,Gt);
    		updateQuery(queryRequest,index,x1,x1,Lt);
    	}
    	
    	index = _accessMethod->keyIndex[name+".y"];
    	if(x1 < x2) {
    		updateQuery(queryRequest,index,y2,y2,Lt);
    		updateQuery(queryRequest,index,y1,y1,Gt);
    	} else {
    		updateQuery(queryRequest,index,y2,y2,Gt);
    	    updateQuery(queryRequest,index,y1,y1,Lt);
    	}
    }

    void KdtreeCursor::parseQuery(const BSONObj& position, vector<BSONObj>& queries) {
    	BSONObjIterator i(position);
    	vector<BSONElement> orConditions;
    	vector<BSONElement> normalConditions;
    	bool _or = false;
    	while (i.more()) {
            BSONElement e = i.next();
            string attrName = e.fieldName();
            if (attrName == "$or" && e.type() == Array) {
            	if(_or) {
            		hlog << "two ors not supported as of now" << endl;
            		verify(0);
            	}
            	_or = true;
//            	hlog << "or found" << endl;
                BSONObj obj = e.embeddedObject();
				BSONObjIterator j(obj);
				while(j.more()) {
					BSONElement ee = j.next();
					orConditions.push_back(ee);
				}
            } else {
            	normalConditions.push_back(e);
            }
    	}
    	int ns = normalConditions.size();
    	int os = orConditions.size();
    	for(int i = 0;i < os;i ++) {
    		BSONObjBuilder b;
    		BSONObj cond = orConditions[i].embeddedObject();
    		vector<BSONElement> v;
    		cond.elems(v);
    		for(size_t k = 0;k < v.size();k ++) {
    			b.append(v[k]);
    		}
        	for(int j = 0;j < ns;j ++) {
        		b.append(normalConditions[j]);
        	}
        	queries.push_back(b.obj());
    	}
    	if(os == 0) {
    		BSONObjBuilder b;
        	for(int j = 0;j < ns;j ++) {
        		b.append(normalConditions[j]);
        	}
        	queries.push_back(b.obj());
    	}
    }
    
    void KdtreeCursor::generateQuery(KdRequest* request,const BSONObj& position) {
        BSONObjIterator i(position);
//        hlog << "query: " << position << endl;
        
        while (i.more()) {
            BSONElement e = i.next();
            string attrName = e.fieldName();
            if (e.type() == Array) {
            	hlog << "array not yet supported" << endl;
            	verify(0);
            } else if (e.type() == Object) {
            	BSONObjIterator j(e.embeddedObject());
            	while(j.more()) {
                	e = j.next();
                    switch (e.getGtLtOp()) {
                    case BSONObj::opNEAR: {
                    	hlog << "near functionality not yet supported" << endl;
                    	verify(0);
                    } break;
                    
                    case BSONObj::opWITHIN: {
                        uassert(25000, "$within has to take an object or array", e.isABSONObj());
                        KeySet::iterator it = _accessMethod->geoKeys.find(attrName);
                        if(it == _accessMethod->geoKeys.end()) {
                            throw UserException(25001, str::stream() << attrName
                                                                     << "not indexed as 2d");
                        }
                        BSONObj context = e.embeddedObject();
                        e = e.embeddedObject().firstElement();
                        string type = e.fieldName();

                        if (type == "$box") {
                            uassert(25002,"$box has to take an object or array", e.isABSONObj());
                            BSONObj box = e.embeddedObject();
                            addBox(request,attrName, box);
                        } else if (startsWith(type, "$poly")) {
                            uassert(25003,"$polygon has to take an object or array", e.isABSONObj());
                            BSONObj poly = e.embeddedObject();
                            addPoly(request,attrName, poly);
                        } else {
                            throw UserException(25004, str::stream() << "unknown $within information : "
                                                                     << context
                                                                     << ", a shape must be specified.");
                        }
                    } break;
                    
                    case BSONObj::LT:
                    case BSONObj::LTE: {
                    	updateQuery(request,attrName,e, Lt);
                    } break;
                    
                    case BSONObj::GT:
                    case BSONObj::GTE: {
                    	updateQuery(request,attrName, e , Gt);
                    } break;

                    default:
                        // Otherwise... assume the object defines a point, and we want to do a
                        // zero-radius $within $center
                        break;
                    }
            	}
            } else  {
            	updateQuery(request,attrName,e, Equal);
            }
        }
    }
	void KdtreeCursor::getQuery(const BSONObj& position) {
		vector<BSONObj> queries;
		parseQuery(position, queries);
		noQueries = queries.size();
//		hlog << "No. of queries: " << noQueries <<  endl;
		
		initQuery(queries.size());
		for(size_t s = 0;s < noQueries;s ++) {
			generateQuery(&queryRequest[s], queries[s]);
		}
	}
	
    /**
     * A cursor doesn't point anywhere by default.  You must seek to the start position.
     * The provided position must be a predicate that the index understands.  The
     * predicate must describe one value, though there may be several instances
     *
     * Possible return values:
     * 1. Success: seeked to the position.
     * 2. Success: seeked to 'closest' key oriented according to the cursor's direction.
     * 3. Error: can't seek to the position.
     */
	Status KdtreeCursor::seek(const BSONObj& position) {
		// TODO this is where the index is searched to get the required results.
		_accessMethod->readMetaData();
		// TODO use variable position to initialize search parameters
		getQuery(position);
		sel.clear();
		
	    int           nGpu        = 3;
	    size_t        gpuMemLimit = 0;
	    CudaHandler::getInstance(nGpu, gpuMemLimit);


		string dataFile = _accessMethod->_indexFile + ".data";
		string diskFile = _accessMethod->_indexFile + ".disk";
		
		int noKeys = _accessMethod->keys.size();
//		int keysize = (noKeys + 1) * sizeof(uint64_t);
//		hlog << "key size: " << keysize << endl;
		pos = 0;
		
		// TODO do search. 
		ifstream data(dataFile.c_str(), ios::binary);
		disk.open(diskFile);
		result = (DiskLoc *) disk.data();
		
	    data.seekg (0, data.end);
//	    long length = data.tellg();
	    data.seekg (0, data.beg);
//	    long noRecs = length / keysize;
	    data.close();
	    
	    //TODO do index search, storing result in vector sel. for now storing all records as result
	    
		string keysFile = _accessMethod->_indexFile + ".keys";
		string treeFile = _accessMethod->_indexFile + ".tree";
		string rangeFile = _accessMethod->_indexFile + ".range";

//	    CudaDb db(keysFile.c_str(), rangeFile.c_str(), treeFile.c_str(), noKeys + 1);
		CudaDb* db = DBHandler::getInstance()->getCudaDb(keysFile.c_str(), rangeFile.c_str(), treeFile.c_str(), noKeys + 1);
	    
//	    db.start(nGpu, gpuMemLimit);

	    for(uint32_t i = 0;i < noQueries;i ++) {
		    db->requestQuery(queryRequest[i]);
	    }
	    RequestResult result = KdRequest::emptyResult();
	    for(uint32_t i = 0;i < noQueries;i ++) {
	    	db->getResult(result);
	    }
//	    db.stop();
		
		// Fastest way to access data is in sorted order. So this additional step.
		sel.insert(sel.end(), result->begin(), result->end());
		std::sort(sel.begin(), sel.end());
		
	    for(uint32_t i = 0;i < noQueries;i ++) {
	    	delete queryRequest[i].query;
	    	delete[] queryRequest[i].regions;
	    }
		delete[] queryRequest;

		return Status::OK();
	}

	// Are we out of documents?
	bool KdtreeCursor::isEOF() const {
		if (pos == sel.size()) {
			return true;
		}
		return false;
	}
	
	// Move to the next key/value pair.  Assumes !isEOF().
	void KdtreeCursor::next() {
		pos ++;
	}
	
    //
    // Accessors
    //

    // Current key we point at.  Assumes !isEOF().	
	BSONObj KdtreeCursor::getKey() const {
		return BSONObj();
	}
	
	// Current value we point at.  Assumes !isEOF().
	DiskLoc KdtreeCursor::getValue() const {
		return result[sel[pos]];
	}
	
    //
    // Yielding support
    //

    /**
     * Yielding semantics:
     * If the entry that a cursor points at is not deleted during a yield, the cursor will
     * point at that entry after a restore.
     * An entry inserted during a yield may or may not be returned by an in-progress scan.
     * An entry deleted during a yield may or may not be returned by an in-progress scan.
     * An entry modified during a yield may or may not be returned by an in-progress scan.
     * An entry that is not inserted or deleted during a yield will be returned, and only once.
     * If the index returns entries in a given order (Btree), this order will be mantained even
     * if the entry corresponding to a saved position is deleted during a yield.
     */

    /**
     * Save our current position in the index.  Assumes that we are currently pointing to a
     * valid position in the index.
     * If not, we error.  Otherwise, succeed.
     */
	Status KdtreeCursor::savePosition() {
//		hlog << "savePosition called" << endl;
		return Status::OK(); 
	}
	
    /**
     * Restore the saved position.  Errors if there is no saved position.
     * The cursor may be EOF after a restore.
     */
	Status KdtreeCursor::restorePosition() {
//		hlog << "restorePosition called" << endl;
		return Status::OK();
	}

	// Return a string describing the cursor.
	string KdtreeCursor::toString() {
		hlog << "toString called" << endl;
		return string("Kdtree Cursor");
	}

} // namespace mongo
