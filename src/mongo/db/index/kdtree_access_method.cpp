#include "mongo/db/index/kdtree_access_method.h"

#include <vector>
#include <cstdio>

#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/pdfile_private.h"
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

#include "mongo/db/index/kdtree_cursor.h"
#include "mongo/db/kdtree/KdIndex.hpp"

#include <boost/foreach.hpp>


namespace mongo {

	string KdtreeAccessMethod::getIndexFileName(const char* ns, const string &indexName) {
		return (dbpath + string("/") + string(ns) + string(".") + indexName);
	}

	KdtreeAccessMethod::KdtreeAccessMethod(IndexDescriptor* descriptor): _descriptor(descriptor) {
		_indexFile = getIndexFileName(descriptor->parentNS().c_str(), descriptor->indexName());
		noRecords = 0;
//		hlog << "Index Name: " << _indexFile << endl;
	}

    Status KdtreeAccessMethod::buildIndex(NamespaceDetails* d, const char* ns,
			   const IndexDetails& idx,
			   const BSONObj& order,
			   int64_t nrecords,
			   ProgressMeter* progressMeter,
			   bool mayInterrupt, int idxNo) {
    	hlog << "building index " << endl;
    	hlog << "no. of records: " << nrecords << endl;
    	Status ret = Status::OK();
		unsigned int size = keys.size();
		uint64_t * vals = new uint64_t [size];
		type.clear();
		for(unsigned int i = 0;i < size;i ++) {
			type.push_back(NumberLong);
		}
		hlog << "finished initing type" << endl;
    	try {
    		fstream data, disk;
    		string datafile = _indexFile + string(".data");
    		string diskfile = _indexFile + string(".disk");
    		
// TODO set this variable if you want to create a dummy index

#ifndef DUMMY_INDEX
    		
    		data.open(datafile.c_str(),ios::out | ios::binary);
    		disk.open(diskfile.c_str(),ios::out | ios::binary);
    		shared_ptr<Cursor> cursor = theDataFileMgr.findAll( ns );
    		uint64_t ct = 0;
    		
            while ( cursor->ok() ) {
                RARELY killCurrentOp.checkForInterrupt( !mayInterrupt );
                BSONObj o = cursor->current();
                DiskLoc loc = cursor->currLoc();

                // get key values from o, and store it along with loc
                // TODO write only if all keys are present for the tuple
                
                bool present = true;
                
                for(unsigned int i = 0;i < size;i ++) {
                	string key = keys[i];
                	unsigned int pos = key.find(".");
                	BSONElement e;
                	double floatVal;
                	long intVal;
                	long longVal;

                	if(pos > 0 && pos < key.length()) {
                		string skey = key.substr(0,pos);
                		e = o.getField(skey);
                		BSONObj oo = e.Obj();
                		skey = key.substr(pos + 1);
                		e = oo.getField(skey);
                	} else {
                		e = o.getField(key);
                	}
                	if(e.eoo()) {
                		present = false;
                		break;
                	}
                	switch(e.type()) {
                	case NumberDouble:
                		floatVal = e.Double();
                		vals[i] = double2uint(floatVal);
                		type[i] = NumberDouble;
                		break;
                	case NumberInt:
                		intVal = e.Int();
                		vals[i] = long2uint(intVal);
                		break;
                	case NumberLong:
                		longVal = e.Long();
                		vals[i] = long2uint(longVal);
                		break;
                	default:
                		hlog << "unknown type!!!" << endl;
                		ret = Status(ErrorCodes::InternalError, "unknown type to index!!", 0);
                		break;
                	}
                }
                if(present) {
                	data.write((char *)vals, sizeof(uint64_t) * size);
                	data.write((char *)&ct, sizeof(uint64_t));
                	
                	disk.write((char *)&loc, sizeof(DiskLoc));
                	ct ++;
                }
                cursor->advance();
                progressMeter->hit();
            }
    		data.close();
    		disk.close();
    		noRecords = ct;

    		string keysFile = _indexFile + ".keys";
    		string treeFile = _indexFile + ".tree";
    		string rangeFile = _indexFile + ".range";
    		createKdTree(datafile,keysFile,treeFile,rangeFile,size);
#endif        	
    		delete vals;
    		return updateMetaData();
    	} catch (int e) {
            problem() << "could not write index file"
                      << _descriptor->indexNamespace()
                      << endl;
            ret = Status(ErrorCodes::InternalError, "could not open index file for write", e);
    	}
    	delete vals;
    	return ret;
    }

    Status KdtreeAccessMethod::updateMetaData() {
    	Status ret = Status::OK();
    	try {
    		string metafile = _indexFile + string(".meta");
        	hlog << "updating metadata:" << metafile.c_str() << endl;
        	ofstream metadata(metafile.c_str(), ios_base::app);
        	unsigned int size = keys.size();
        	for(unsigned int i = 0;i < size;i ++) {
        		if(type[i] == NumberLong) {
        			metadata << LONG << endl;
        		} else {
        			metadata << DOUBLE << endl;
        		}
        	}
        	hlog << "finished updating metadata" << endl;
        	metadata.close();
    	} catch (int e) {
            problem() << "could not read meta data for index"
                      << _descriptor->indexNamespace()
                      << endl;
            ret = Status(ErrorCodes::InternalError, "could not write meta data for index", e);
    	}
    	return ret;
    }
    
    Status KdtreeAccessMethod::writeMetaData(KeyMap kmap) {
    	Status ret = Status::OK();
    	
		BOOST_FOREACH(KeyMap::value_type i, kmap) {
		    if(i.second == TWOD) {
		    	geoKeys.insert(i.first);
		    } else if(i.second == COMP) {
		    	compKeys.insert(i.first);
		    }
		}
		
    	try {
    		string metafile = _indexFile + string(".meta");
        	hlog << "writing metadata:" << metafile.c_str() << endl;
        	ofstream metadata(metafile.c_str());
        	unsigned int size = geoKeys.size();
        	metadata << size << endl;
        	int in = 0;
        	BOOST_FOREACH(KeySet::value_type i, geoKeys) {
        		metadata << i << endl;
        		string s = i;
        		allKeys.insert(s + ".x");
        		allKeys.insert(s + ".y");
        		keyIndex[s + ".x"] = in;
        		keyIndex[s + ".y"] = in + 1;
        		keys.push_back(s + ".x");
        		keys.push_back(s + ".y");
        		in += 2;
        	}
        	size = compKeys.size();
    	    metadata << size << endl;
        	BOOST_FOREACH(KeySet::value_type i, compKeys) {
        		metadata << i << endl;
        		string s = i;
        		allKeys.insert(s);
        		keys.push_back(s);
        		keyIndex[i] = in ++;
        	}
        	hlog << "Split of keys : " <<  geoKeys.size() << " " << compKeys.size()<< endl;
        	hlog << "No. of keys : " << in << endl;
        	hlog << "finished writing metadata" << endl;
        	metadata.close();
    	} catch (int e) {
            problem() << "could not read meta data for index"
                      << _descriptor->indexNamespace()
                      << endl;
            ret = Status(ErrorCodes::InternalError, "could not write meta data for index", e);
    	}
    	return ret;
    }

    Status KdtreeAccessMethod::readMetaData() {
    	Status ret = Status::OK();
    	try {
    		string metafile = _indexFile + string(".meta");
//        	hlog << "reading metadata:" << metafile.c_str() << endl;
        	ifstream metadata(metafile.c_str());
        	unsigned int size;
        	int in = 0;
        	
        	metadata >> size;
        	for(unsigned int i = 0;i < size;i ++) {
        		char fname[100];
        		metadata >> fname;
        		
        		string s(fname);
        		geoKeys.insert(s);
        		allKeys.insert(s + ".x");
        		allKeys.insert(s + ".y");
        		keyIndex[s + ".x"] = in;
        		keyIndex[s + ".y"] = in + 1;
        		in += 2;
        		keys.push_back(s + ".x");
        		keys.push_back(s + ".y");
        	}
        	metadata >> size;
        	for(unsigned int i = 0;i < size;i ++) {
        		char fname[100];
        		metadata >> fname;
        		
        		string s(fname);
        		compKeys.insert(s);
        		allKeys.insert(s);
        		keyIndex[s] = in ++;
        		keys.push_back(s);
        	}
        	size = keys.size();
        	for(unsigned int i = 0;i < size;i ++) {
        		int typ;
        		metadata >> typ;
        		if(typ == LONG) {
        			type.push_back(NumberLong);
        		} else {
        			type.push_back(NumberDouble);
        		}
        	}
    	} catch (int e){
            problem() << "could not read meta data for index"
                      << _descriptor->indexNamespace()
                      << endl;
            ret = Status(ErrorCodes::InternalError, "could not read meta data for index", e);
    	}
    	return ret;
    }

    Status KdtreeAccessMethod::dropIndex() {
    	// remove the created files
    	hlog << "dropping..." << endl;
    	Status ret = Status::OK();
    	try {
    		string datafile = _indexFile + string(".data");
    		string diskfile = _indexFile + string(".disk");
    		string metafile = _indexFile + string(".meta");
    		string keysFile = _indexFile + ".keys";
    		string nodeFile = _indexFile + ".node";
    		string rangeFile = _indexFile + ".range";

    		std::remove(datafile.c_str());
    		std::remove(diskfile.c_str());
    		std::remove(metafile.c_str());
    		std::remove(keysFile.c_str());
    		std::remove(nodeFile.c_str());
    		std::remove(rangeFile.c_str());
    	} catch (int e) {
            problem() << "could not delete index files"
                      << _descriptor->indexNamespace()
                      << endl;
            ret = Status(ErrorCodes::InternalError, "could not open index file for write", e);
    	}
    	return ret;
    }
    
    // TODO Inherited methods
    
    /**
     * Internally generate the keys {k1, ..., kn} for 'obj'.  For each key k, insert (k ->
     * 'loc') into the index.  'obj' is the object at the location 'loc'.  If not NULL,
     * 'numInserted' will be set to the number of keys added to the index for the document.  If
     * there is more than one key for 'obj', either all keys will be inserted or none will.
     * 
     * The behavior of the insertion can be specified through 'options'.  
     */
    Status KdtreeAccessMethod::insert(const BSONObj& obj,
                          const DiskLoc& loc,
                          const InsertDeleteOptions& options,
                          int64_t* numInserted) {
    	Status ret = Status(ErrorCodes::InternalError, "insert not yet implemented", 0);
    	verify(0);
    	return ret;
    }

    /** 
     * Analogous to above, but remove the records instead of inserting them.  If not NULL,
     * numDeleted will be set to the number of keys removed from the index for the document.
     */
    Status KdtreeAccessMethod::remove(const BSONObj& obj,
                          const DiskLoc& loc,
                          const InsertDeleteOptions& options,
                          int64_t* numDeleted){
    	Status ret = Status(ErrorCodes::InternalError, "remove not yet implemented", 0);
    	verify(0);
    	return ret;
    }

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
    Status KdtreeAccessMethod::validateUpdate(const BSONObj& from,
                                  const BSONObj& to,
                                  const DiskLoc& loc,
                                  const InsertDeleteOptions& options,
                                  UpdateTicket* ticket){
    	Status ret = Status(ErrorCodes::InternalError, "validateUpdate not yet implemented", 0);
    	verify(0);
    	return ret;
    }

    /**
     * Perform a validated update.  The keys for the 'from' object will be removed, and the keys
     * for the object 'to' will be added.  Returns OK if the update succeeded, failure if it did
     * not.  If an update does not succeed, the index will be unmodified, and the keys for
     * 'from' will remain.  Assumes that the index has not changed since validateUpdate was
     * called.  If the index was changed, we may return an error, as our ticket may have been
     * invalidated.
     */
    Status KdtreeAccessMethod::update(const UpdateTicket& ticket, int64_t* numUpdated){
    	Status ret = Status(ErrorCodes::InternalError, "update not yet implemented", 0);
    	verify(0);
    	return ret;
    }

    /**
     * Fills in '*out' with an IndexCursor.  Return a status indicating success or reason of
     * failure. If the latter, '*out' contains NULL.  See index_cursor.h for IndexCursor usage.
     */
    Status KdtreeAccessMethod::newCursor(IndexCursor **out){
    	*out = new KdtreeCursor(this);
    	return Status::OK();
    }

    /**
     * Try to page-in the pages that contain the keys generated from 'obj'.
     * This can be used to speed up future accesses to an index by trying to ensure the
     * appropriate pages are not swapped out.
     * See prefetch.cpp.
     */
    Status KdtreeAccessMethod::touch(const BSONObj& obj){
    	Status ret = Status(ErrorCodes::InternalError, "touch not yet implemented", 0);
    	verify(0);
    	return ret;
    }

    /**
     * Walk the entire index, checking the internal structure for consistency.
     * Set numKeys to the number of keys in the index.
     *
     * Return OK if the index is valid.
     *
     * Currently wasserts that the index is invalid.  This could/should be changed in
     * the future to return a Status.
     */
    Status KdtreeAccessMethod::validate(int64_t* numKeys){
    	Status ret = Status(ErrorCodes::InternalError, "validate not yet implemented", 0);
    	verify(0);
    	return ret;
    }

    
    
} // namespace mongo
