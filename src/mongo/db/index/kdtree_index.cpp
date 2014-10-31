#include "mongo/db/index/kdtree_index.h"

#include "mongo/db/index/catalog_hack.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/repl/is_master.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/sort_phase_one.h"
#include "mongo/util/processinfo.h"
#include "mongo/db/pdfile_private.h"
#include "mongo/bson/bsontypes.h"

#include "mongo/db/index/kdtree_access_method.h"

#include <boost/foreach.hpp>

namespace mongo {
	
	uint64_t KdtreeBuilder::addKeys(NamespaceDetails* d, const char* ns,
						   const IndexDetails& idx,
						   const BSONObj& order,
						   int64_t nrecords,
						   ProgressMeter* progressMeter,
						   bool mayInterrupt, int idxNo) {

		// First get the required keys
		BSONObjIterator k(order);
		BSONElement e = k.next();
		vector<string> keys;
		
		KeyMap kmap;
		if(!e.eoo()) {
			verify(e.fieldName() == string("type"));
			while (1) {
				BSONElement e = k.next();
				if (e.eoo())
					break;
				BSONType type = e.type();

				switch(type) {
				case String:
					if(e.String() == IndexNames::GEO_2D) {
//						string s = e.fieldName() + string(".x");
//						keys.push_back(s);
//						s = e.fieldName() + string(".y");
//						keys.push_back(s);
						kmap[e.fieldName()] = TWOD;
					} else {
						verify(0);
					}
					break;
				case NumberDouble:
					// TODO for now ignoring ascending / descending ordering
//					keys.push_back(e.fieldName());
					kmap[e.fieldName()] = COMP;
					break;
				default:
					verify(0);
				}
			}
		}
		BOOST_FOREACH(KeyMap::value_type i, kmap) {
		    std::cout<<i.first<<":"<<i.second<<"\n";
		}

		// index location
		// Once we have the keys, read the keys from all the records to create the require index block files
		auto_ptr<IndexDescriptor> desc(CatalogHack::getDescriptor(d, idxNo));
		KdtreeAccessMethod kdm(desc.get());


        kdm.writeMetaData(kmap);
        Status status = kdm.buildIndex(d, ns, idx,order,nrecords,progressMeter,mayInterrupt, idxNo);
        if(status != Status::OK()) {
        	hlog << status.codeString() << endl;
        	verify(0);
        }
        return kdm.noRecords;
	}

	uint64_t KdtreeBuilder::fastBuildIndex(const char* ns, NamespaceDetails* d,
											   IndexDetails& idx, bool mayInterrupt,
											   int idxNo) {
		CurOp * op = cc().curop();

		Timer t;

		tlog(1) << "building kdtree based index " << ns << ' ' << idx.info.obj().toString() << endl;

		BSONObj order = idx.keyPattern();

	    ProgressMeterHolder pm(op->setMessage("Getting keys", "Getting keys Progress",d->stats.nrecords,10));
		uint64_t nrecs = addKeys(d, ns, idx, order, d->stats.nrecords, pm.get(), mayInterrupt, idxNo );
		pm.finished();

		return nrecs;
	}

}

