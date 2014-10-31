/** @file index.cpp */

/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "mongo/pch.h"

#include "mongo/db/index.h"

#include <boost/checked_delete.hpp>

#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/background.h"
#include "mongo/db/btree.h"
#include "mongo/db/index_legacy.h"
#include "mongo/db/index_names.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index_update.h"
#include "mongo/db/namespace-inl.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/repl/rs.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    // What's the default version of our indices?
    const int DefaultIndexVersionNumber = 1;

    int removeFromSysIndexes(const char *ns, const char *idxName) {
        string system_indexes = cc().database()->name + ".system.indexes";
        BSONObjBuilder b;
        b.append("ns", ns);
        b.append("name", idxName); // e.g.: { name: "ts_1", ns: "foo.coll" }
        BSONObj cond = b.done();
        return (int) deleteObjects(system_indexes.c_str(), cond, false, false, true);
    }

    /* this is just an attempt to clean up old orphaned stuff on a delete all indexes
       call. repair database is the clean solution, but this gives one a lighter weight
       partial option.  see dropIndexes()
    */
    void assureSysIndexesEmptied(const char *ns, IndexDetails *idIndex) {
        string system_indexes = cc().database()->name + ".system.indexes";
        BSONObjBuilder b;
        b.append("ns", ns);
        if( idIndex ) {
            b.append("name", BSON( "$ne" << idIndex->indexName().c_str() ));
        }
        BSONObj cond = b.done();
        int n = (int) deleteObjects(system_indexes.c_str(), cond, false, false, true);
        if( n ) {
            log() << "info: assureSysIndexesEmptied cleaned up " << n << " entries" << endl;
        }
    }

    int IndexDetails::keyPatternOffset( const string& key ) const {
        BSONObjIterator i( keyPattern() );
        int n = 0;
        while ( i.more() ) {
            BSONElement e = i.next();
            if ( key == e.fieldName() )
                return n;
            n++;
        }
        return -1;
    }

    /* delete this index.  does NOT clean up the system catalog
       (system.indexes or system.namespaces) -- only NamespaceIndex.
    */
    void IndexDetails::kill_idx() {
        string ns = indexNamespace(); // e.g. foo.coll.$ts_1
        try {

            string pns = parentNS(); // note we need a copy, as parentNS() won't work after the drop() below

            // clean up parent namespace index cache
            NamespaceDetailsTransient::get( pns.c_str() ).deletedIndex();

            string name = indexName();

            /* important to catch exception here so we can finish cleanup below. */
            try {
                dropNS(ns.c_str());
            }
            catch(DBException& ) {
                LOG(2) << "IndexDetails::kill(): couldn't drop ns " << ns << endl;
            }
            head.setInvalid();
            info.setInvalid();

            // clean up in system.indexes.  we do this last on purpose.
            int n = removeFromSysIndexes(pns.c_str(), name.c_str());
            wassert( n == 1 );

        }
        catch ( DBException &e ) {
            log() << "exception in kill_idx: " << e << ", ns: " << ns << endl;
        }
    }

    // should be { <something> : <simpletype[1|-1]>, .keyp.. }
    static bool validKeyPattern(BSONObj kp) {
        BSONObjIterator i(kp);
        while( i.moreWithEOO() ) {
            BSONElement e = i.next();
            if( e.type() == Object || e.type() == Array )
                return false;
        }
        return true;
    }

    static bool needToUpgradeMinorVersion(const string& newPluginName) {
        if (IndexNames::existedBefore24(newPluginName))
            return false;

        DataFileHeader* dfh = cc().database()->getFile(0)->getHeader();
        if (dfh->versionMinor == PDFILE_VERSION_MINOR_24_AND_NEWER)
            return false; // these checks have already been done

        fassert(16737, dfh->versionMinor == PDFILE_VERSION_MINOR_22_AND_OLDER);

        return true;
    }

    static void upgradeMinorVersionOrAssert(const string& newPluginName) {
        const string systemIndexes = cc().database()->name + ".system.indexes";
        shared_ptr<Cursor> cursor(theDataFileMgr.findAll(systemIndexes));
        for ( ; cursor && cursor->ok(); cursor->advance()) {
            const BSONObj index = cursor->current();
            const BSONObj key = index.getObjectField("key");
            const string plugin = IndexNames::findPluginName(key);
            if (IndexNames::existedBefore24(plugin))
                continue;

            const string errmsg = str::stream()
                << "Found pre-existing index " << index << " with invalid type '" << plugin << "'. "
                << "Disallowing creation of new index type '" << newPluginName << "'. See "
                << "http://dochub.mongodb.org/core/index-type-changes"
                ;

            error() << errmsg << endl;
            uasserted(16738, errmsg);
        }

        DataFileHeader* dfh = cc().database()->getFile(0)->getHeader();
        getDur().writingInt(dfh->versionMinor) = PDFILE_VERSION_MINOR_24_AND_NEWER;
    }

    bool prepareToBuildIndex(const BSONObj& io,
                             bool mayInterrupt,
                             bool god,
                             string& sourceNS,
                             NamespaceDetails*& sourceCollection,
                             BSONObj& fixedIndexObject) {
        sourceCollection = 0;

        // the collection for which we are building an index
        sourceNS = io.getStringField("ns");
        uassert(10096, "invalid ns to index", sourceNS.find( '.' ) != string::npos);
        massert(10097, str::stream() << "bad table to index name on add index attempt current db: " << cc().database()->name << "  source: " << sourceNS ,
                cc().database()->name == nsToDatabase(sourceNS));

        // logical name of the index.  todo: get rid of the name, we don't need it!
        const char *name = io.getStringField("name");
        uassert(12523, "no index name specified", *name);

        BSONObj key = io.getObjectField("key");
        uassert(12524, "index key pattern too large", key.objsize() <= 2048);
        if( !validKeyPattern(key) ) {
            string s = string("bad index key pattern ") + key.toString();
            uasserted(10098 , s.c_str());
        }

        if ( sourceNS.empty() || key.isEmpty() ) {
            LOG(2) << "bad add index attempt name:" << (name?name:"") << "\n  ns:" <<
                   sourceNS << "\n  idxobj:" << io.toString() << endl;
            string s = "bad add index attempt " + sourceNS + " key:" + key.toString();
            uasserted(12504, s);
        }

        sourceCollection = nsdetails(sourceNS);
        if( sourceCollection == 0 ) {
            // try to create it
            string err;
            if ( !userCreateNS(sourceNS.c_str(), BSONObj(), err, false) ) {
                problem() << "ERROR: failed to create collection while adding its index. " << sourceNS << endl;
                return false;
            }
            sourceCollection = nsdetails(sourceNS);
            tlog() << "info: creating collection " << sourceNS << " on add index" << endl;
            verify( sourceCollection );
        }

        if ( sourceCollection->findIndexByName(name) >= 0 ) {
            // index already exists.
            return false;
        }
        if( sourceCollection->findIndexByKeyPattern(key) >= 0 ) {
            LOG(2) << "index already exists with diff name " << name << ' ' << key.toString() << endl;
            return false;
        }

        if ( sourceCollection->nIndexes >= NamespaceDetails::NIndexesMax ) {
            stringstream ss;
            ss << "add index fails, too many indexes for " << sourceNS << " key:" << key.toString();
            string s = ss.str();
            log() << s << endl;
            uasserted(12505,s);
        }

        /* this is because we want key patterns like { _id : 1 } and { _id : <someobjid> } to
           all be treated as the same pattern.
        */
        if ( IndexDetails::isIdIndexPattern(key) ) {
            if( !god ) {
                ensureHaveIdIndex( sourceNS.c_str(), mayInterrupt );
                return false;
            }
        }
        else {
            /* is buildIndexes:false set for this replica set member?
               if so we don't build any indexes except _id
            */
            if( theReplSet && !theReplSet->buildIndexes() )
                return false;
        }

        string pluginName = IndexNames::findPluginName( key );
        if (pluginName.size()) {
            uassert(16734, str::stream() << "Unknown index plugin '" << pluginName << "' "
                                         << "in index "<< key,
                    IndexNames::isKnownName(pluginName));

            if (needToUpgradeMinorVersion(pluginName))
                upgradeMinorVersionOrAssert(pluginName);
        }

        { 
            BSONObj o = io;
            o = IndexLegacy::adjustIndexSpecObject(o);
            BSONObjBuilder b;
            int v = DefaultIndexVersionNumber;
            if( !o["v"].eoo() ) {
                double vv = o["v"].Number();
                // note (one day) we may be able to fresh build less versions than we can use
                // isASupportedIndexVersionNumber() is what we can use
                uassert(14803, str::stream() << "this version of mongod cannot build new indexes of version number " << vv, 
                    vv == 0 || vv == 1);
                v = (int) vv;
            }
            // idea is to put things we use a lot earlier
            b.append("v", v);
            b.append(o["key"]);
            if( o["unique"].trueValue() )
                b.appendBool("unique", true); // normalize to bool true in case was int 1 or something...
            b.append(o["ns"]);

            {
                // stripping _id
                BSONObjIterator i(o);
                while ( i.more() ) {
                    BSONElement e = i.next();
                    string s = e.fieldName();
                    if( s != "_id" && s != "v" && s != "ns" && s != "unique" && s != "key" )
                        b.append(e);
                }
            }
        
            fixedIndexObject = b.obj();
        }

        return true;
    }
}
