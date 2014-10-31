#pragma once

#include <vector>
#include <fstream>

#include "mongo/db/diskloc.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index/btree_interface.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/util/progress_meter.h"

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#define TWOD 0
#define COMP 1

#define LONG 0
#define DOUBLE 1

namespace mongo {

	typedef boost::unordered_map<string, int> KeyMap;
	typedef boost::unordered_set<string> KeySet;
	
	class KdtreeAccessMethod : public IndexAccessMethod {
    public:
		
        static string getIndexFileName(const char* ns, const string& idx);
		
    	KdtreeAccessMethod(IndexDescriptor* descriptor);
        virtual ~KdtreeAccessMethod() { }

        virtual Status buildIndex(NamespaceDetails* d, const char* ns,
				   const IndexDetails& idx,
				   const BSONObj& order,
				   int64_t nrecords,
				   ProgressMeter* progressMeter,
				   bool mayInterrupt, int idxNo);
        virtual Status writeMetaData(KeyMap kmap);
        virtual Status updateMetaData();
        virtual Status readMetaData();
        virtual Status dropIndex();
        
        /**
         * Inherited methods
         */
        
        /**
         * Internally generate the keys {k1, ..., kn} for 'obj'.  For each key k, insert (k ->
         * 'loc') into the index.  'obj' is the object at the location 'loc'.  If not NULL,
         * 'numInserted' will be set to the number of keys added to the index for the document.  If
         * there is more than one key for 'obj', either all keys will be inserted or none will.
         * 
         * The behavior of the insertion can be specified through 'options'.  
         */
        virtual Status insert(const BSONObj& obj,
                              const DiskLoc& loc,
                              const InsertDeleteOptions& options,
                              int64_t* numInserted);

        /** 
         * Analogous to above, but remove the records instead of inserting them.  If not NULL,
         * numDeleted will be set to the number of keys removed from the index for the document.
         */
        virtual Status remove(const BSONObj& obj,
                              const DiskLoc& loc,
                              const InsertDeleteOptions& options,
                              int64_t* numDeleted);

        /**
         * Checks whether the index entries for the document 'from', which is placed at location
         * 'loc' on disk, can be changed to the index entries for the doc 'to'. Provides a ticket
         * for actually performing the update.
         *
         * Returns an error if the update is invalid.  The ticket will also be marked as invalid.
         * Returns OK if the update should proceed without error.  The ticket is marked as valid.
         *
         * There is no obligation to perform the update after performing validation.
         */
        virtual Status validateUpdate(const BSONObj& from,
                                      const BSONObj& to,
                                      const DiskLoc& loc,
                                      const InsertDeleteOptions& options,
                                      UpdateTicket* ticket);

        /**
         * Perform a validated update.  The keys for the 'from' object will be removed, and the keys
         * for the object 'to' will be added.  Returns OK if the update succeeded, failure if it did
         * not.  If an update does not succeed, the index will be unmodified, and the keys for
         * 'from' will remain.  Assumes that the index has not changed since validateUpdate was
         * called.  If the index was changed, we may return an error, as our ticket may have been
         * invalidated.
         */
        virtual Status update(const UpdateTicket& ticket, int64_t* numUpdated);

        /**
         * Fills in '*out' with an IndexCursor.  Return a status indicating success or reason of
         * failure. If the latter, '*out' contains NULL.  See index_cursor.h for IndexCursor usage.
         */
        virtual Status newCursor(IndexCursor **out);

        /**
         * Try to page-in the pages that contain the keys generated from 'obj'.
         * This can be used to speed up future accesses to an index by trying to ensure the
         * appropriate pages are not swapped out.
         * See prefetch.cpp.
         */
        virtual Status touch(const BSONObj& obj);

        /**
         * Walk the entire index, checking the internal structure for consistency.
         * Set numKeys to the number of keys in the index.
         *
         * Return OK if the index is valid.
         *
         * Currently wasserts that the index is invalid.  This could/should be changed in
         * the future to return a Status.
         */
        virtual Status validate(int64_t* numKeys);

    protected:
        friend class KdtreeBuilder;
        friend class KdtreeCursor;
        
        IndexDescriptor* _descriptor;
        string _indexFile;

        KeySet allKeys;
        KeySet geoKeys;
        KeySet compKeys;
        KeyMap keyIndex;
        
        vector<BSONType> type;
        vector<string> keys;
        long noRecords;
    };

}  // namespace mongo
