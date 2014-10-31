#pragma once

#include <set>

#include "mongo/db/jsobj.h"
#include "mongo/db/pdfile.h"

//namespace IndexUpdateTests {
//    class AddKeysToPhaseOne;
//    class InterruptAddKeysToPhaseOne;
//    class DoDropDups;
//    class InterruptDoDropDups;
//}

namespace mongo {

	// TODO for later use if required
//    class BSONObjExternalSorter;
//    class ExternalSortComparison;
//    class IndexDetails;
//    class NamespaceDetails;
    class ProgressMeter;
    class ProgressMeterHolder;
//    struct SortPhaseOne;

    class KdtreeBuilder {
    public:
        /**
         * Want to build an index?  Call this.  Throws DBException.
         */
        static uint64_t fastBuildIndex(const char* ns, NamespaceDetails* d, IndexDetails& idx,
                                       bool mayInterrupt, int idxNo);

    private:

        static uint64_t addKeys(NamespaceDetails* d, const char* ns, const IndexDetails& idx,
        						   const BSONObj& order,
        						   int64_t nrecords,
        						   ProgressMeter* progressMeter,
        						   bool mayInterrupt, int idxNo);
        // TODO Nothing for now. Don't care for now if index creation is interrupted.
//        friend class IndexUpdateTests::AddKeysToPhaseOne;
//        friend class IndexUpdateTests::InterruptAddKeysToPhaseOne;
//        friend class IndexUpdateTests::DoDropDups;
//        friend class IndexUpdateTests::InterruptDoDropDups;
//
//
//        static void addKeysToPhaseOne(NamespaceDetails* d, const char* ns, const IndexDetails& idx,
//                                      const BSONObj& order, SortPhaseOne* phaseOne,
//                                      int64_t nrecords, ProgressMeter* progressMeter,
//                                      bool mayInterrupt,
//                                      int idxNo);
//
//        static void doDropDups(const char* ns, NamespaceDetails* d, const set<DiskLoc>& dupsToDrop,
//                               bool mayInterrupt );
    };

}  // namespace mongo
