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

#include "mongo/db/index_update.h"

#include "mongo/client/dbclientinterface.h"
#include "mongo/db/background.h"
#include "mongo/db/btreebuilder.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/extsort.h"
#include "mongo/db/index.h"
#include "mongo/db/index/btree_based_builder.h"
#include "mongo/db/index/catalog_hack.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/pdfile_private.h"
#include "mongo/db/repl/is_master.h"
#include "mongo/db/repl/rs.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/startup_test.h"

#include "mongo/db/index/kdtree_index.h"
#include "mongo/db/index/kdtree_access_method.h"

namespace mongo {
    
    /**
     * Remove the provided (obj, dl) pair from the provided index.
     */
    static void _unindexRecord(NamespaceDetails *d, int idxNo, const BSONObj& obj,
                               const DiskLoc& dl, bool logIfError = true) {
        auto_ptr<IndexDescriptor> desc(CatalogHack::getDescriptor(d, idxNo));
        auto_ptr<IndexAccessMethod> iam(CatalogHack::getIndex(desc.get()));
        InsertDeleteOptions options;
        options.logIfError = logIfError;

        int64_t removed;
        Status ret = iam->remove(obj, dl, options, &removed);
        if (Status::OK() != ret) {
            problem() << "Couldn't unindex record " << obj.toString() << " status: "
                << ret.toString() << endl;
        }
    }

    /**
     * Remove the provided (obj, dl) pair from all indices.
     */
    void unindexRecord(NamespaceDetails* nsd, Record* todelete, const DiskLoc& dl,
                       bool noWarn /* = false */) {

        BSONObj obj = BSONObj::make(todelete);
        int numIndices = nsd->getTotalIndexCount();

        for (int i = 0; i < numIndices; i++) {
            // If i >= d->nIndexes, it's a background index, and we DO NOT want to log anything.
            bool logIfError = (i < nsd->nIndexes) ? !noWarn : false;
            _unindexRecord(nsd, i, obj, dl, logIfError);
        }
    }

    /**
     * Add the provided (obj, dl) pair to the provided index.
     */
    static void addKeysToIndex(const char *ns, NamespaceDetails *d, int idxNo, const BSONObj& obj,
                               const DiskLoc &recordLoc, bool dupsAllowed) {
        IndexDetails& id = d->idx(idxNo);
        auto_ptr<IndexDescriptor> desc(CatalogHack::getDescriptor(d, idxNo));
        auto_ptr<IndexAccessMethod> iam(CatalogHack::getIndex(desc.get()));
        InsertDeleteOptions options;
        options.logIfError = false;
        options.dupsAllowed = (!KeyPattern::isIdKeyPattern(id.keyPattern()) && !id.unique())
            || ignoreUniqueIndex(id);

        int64_t inserted;
        Status ret = iam->insert(obj, recordLoc, options, &inserted);
        if (Status::OK() != ret) {
            uasserted(ret.location(), ret.reason());
        }
    }

    /**
     * Add the provided (obj, loc) pair to all indices.
     */
    void indexRecord(const char *ns, NamespaceDetails *d, const BSONObj &obj, const DiskLoc &loc) {
        int numIndices = d->getTotalIndexCount();

        for (int i = 0; i < numIndices; ++i) {
            IndexDetails &id = d->idx(i);

            try {
                addKeysToIndex(ns, d, i, obj, loc, !id.unique() || ignoreUniqueIndex(id));
            }
            catch (AssertionException&) {
                // TODO: the new index layer indexes either all or no keys, so j <= i can be j < i.
                for (int j = 0; j <= i; j++) {
                    try {
                        _unindexRecord(d, j, obj, loc, false);
                    }
                    catch(...) {
                        LOG(3) << "unindex fails on rollback after unique "
                                  "key constraint prevented insert" << std::endl;
                    }
                }
                throw;
            }
        }
    }

    //
    // Bulk index building
    //

    class BackgroundIndexBuildJob : public BackgroundOperation {

        unsigned long long addExistingToIndex(const char *ns, NamespaceDetails *d,
                                              IndexDetails& idx) {
            bool dupsAllowed = !idx.unique();
            bool dropDups = idx.dropDups();

            ProgressMeter& progress = cc().curop()->setMessage("bg index build",
                                                               "Background Index Build Progress",
                                                               d->stats.nrecords);

            unsigned long long n = 0;
            unsigned long long numDropped = 0;
            auto_ptr<ClientCursor> cc;
            {
                shared_ptr<Cursor> c = theDataFileMgr.findAll(ns);
                cc.reset( new ClientCursor(QueryOption_NoCursorTimeout, c, ns) );
            }

            std::string idxName = idx.indexName();
            int idxNo = IndexBuildsInProgress::get(ns, idxName);

            // After this yields in the loop, idx may point at a different index (if indexes get
            // flipped, see insert_makeIndex) or even an empty IndexDetails, so nothing below should
            // depend on idx. idxNo should be recalculated after each yield.

            while ( cc->ok() ) {
                BSONObj js = cc->current();
                try {
                    {
                        if ( !dupsAllowed && dropDups ) {
                            LastError::Disabled led( lastError.get() );
                            addKeysToIndex(ns, d, idxNo, js, cc->currLoc(), dupsAllowed);
                        }
                        else {
                            addKeysToIndex(ns, d, idxNo, js, cc->currLoc(), dupsAllowed);
                        }
                    }
                    cc->advance();
                }
                catch( AssertionException& e ) {
                    if( e.interrupted() ) {
                        killCurrentOp.checkForInterrupt();
                    }

                    if ( dropDups ) {
                        DiskLoc toDelete = cc->currLoc();
                        bool ok = cc->advance();
                        ClientCursor::YieldData yieldData;
                        massert( 16093, "after yield cursor deleted" , cc->prepareToYield( yieldData ) );
                        theDataFileMgr.deleteRecord( d, ns, toDelete.rec(), toDelete, false, true , true );
                        if( !cc->recoverFromYield( yieldData ) ) {
                            cc.release();
                            if( !ok ) {
                                /* we were already at the end. normal. */
                            }
                            else {
                                uasserted(12585, "cursor gone during bg index; dropDups");
                            }
                            break;
                        }

                        // Recalculate idxNo if we yielded
                        idxNo = IndexBuildsInProgress::get(ns, idxName);
                        // This index must still be around, because this is thread that would clean
                        // it up
                        numDropped++;
                    }
                    else {
                        log() << "background addExistingToIndex exception " << e.what() << endl;
                        throw;
                    }
                }
                n++;
                progress.hit();

                getDur().commitIfNeeded();

                if ( cc->yieldSometimes( ClientCursor::WillNeed ) ) {
                    progress.setTotalWhileRunning( d->stats.nrecords );

                    // Recalculate idxNo if we yielded
                    idxNo = IndexBuildsInProgress::get(ns, idxName);
                }
                else {
                    idxNo = -1;
                    cc.release();
                    uasserted(12584, "cursor gone during bg index");
                    break;
                }
            }
            progress.finished();
            if ( dropDups )
                log() << "\t backgroundIndexBuild dupsToDrop: " << numDropped << endl;
            return n;
        }

        /* we do set a flag in the namespace for quick checking, but this is our authoritative info -
           that way on a crash/restart, we don't think we are still building one. */
        set<NamespaceDetails*> bgJobsInProgress;

        void prep(const char *ns, NamespaceDetails *d) {
            Lock::assertWriteLocked(ns);
            uassert( 13130 , "can't start bg index b/c in recursive lock (db.eval?)" , !Lock::nested() );
            bgJobsInProgress.insert(d);
        }
        void done(const char *ns) {
            NamespaceDetailsTransient::get(ns).addedIndex(); // clear query optimizer cache
            Lock::assertWriteLocked(ns);
        }

    public:
        BackgroundIndexBuildJob(const char *ns) : BackgroundOperation(ns) { }

        unsigned long long go(string ns, NamespaceDetails *d, IndexDetails& idx) {

            // clear cached things since we are changing state
            // namely what fields are indexed
            NamespaceDetailsTransient::get(ns.c_str()).addedIndex();

            unsigned long long n = 0;

            prep(ns.c_str(), d);
            try {
                idx.head.writing() = BtreeBasedBuilder::makeEmptyIndex(idx);
                n = addExistingToIndex(ns.c_str(), d, idx);
                // idx may point at an invalid index entry at this point
            }
            catch(...) {
                if( cc().database() && nsdetails(ns) == d ) {
                    done(ns.c_str());
                }
                else {
                    log() << "ERROR: db gone during bg index?" << endl;
                }
                throw;
            }
            done(ns.c_str());
            return n;
        }
    };

    // throws DBException
    void buildAnIndex(const std::string& ns,
                      NamespaceDetails* d,
                      IndexDetails& idx,
                      bool mayInterrupt) {

        bool background = idx.info.obj()["background"].trueValue();
        hlog << "Before building index" << endl;
        tlog() << "build index " << ns << ' ' << idx.keyPattern() << ( background ? " background" : "" ) << endl;
        Timer t;
        unsigned long long n;

        verify( Lock::isWriteLocked(ns) );


        if( inDBRepair || !background ) {
            int idxNo = IndexBuildsInProgress::get(ns.c_str(), idx.info.obj()["name"].valuestr());

            // Harish St
            // TODO to disable background creation
            auto_ptr<IndexDescriptor> desc(CatalogHack::getDescriptor(d, idxNo));
            string type = CatalogHack::getAccessMethodName(desc->keyPattern());

            if(type == IndexNames::KDTREE) {
            	hlog << "building kdtree index" << endl;
            	n = KdtreeBuilder::fastBuildIndex(ns.c_str(), d, idx, mayInterrupt, idxNo);
            } else {
                n = BtreeBasedBuilder::fastBuildIndex(ns.c_str(), d, idx, mayInterrupt, idxNo);
                verify( !idx.head.isNull() );
            }
            // TODO uncomment once index is done
//            verify( !idx.head.isNull() );
            // Harish End
        }
        else {
            BackgroundIndexBuildJob j(ns.c_str());
            n = j.go(ns, d, idx);
        }
        tlog() << "build index done.  scanned " << n << " total records. " << t.millis() / 1000.0 << " secs" << endl;
    }

    extern BSONObj id_obj;  // { _id : 1 }

    void ensureHaveIdIndex(const char* ns, bool mayInterrupt) {
        NamespaceDetails *d = nsdetails(ns);
        if ( d == 0 || d->isSystemFlagSet(NamespaceDetails::Flag_HaveIdIndex) )
            return;

        d->setSystemFlag( NamespaceDetails::Flag_HaveIdIndex );

        {
            NamespaceDetails::IndexIterator i = d->ii();
            while( i.more() ) {
                if( i.next().isIdIndex() )
                    return;
            }
        }

        string system_indexes = cc().database()->name + ".system.indexes";

        BSONObjBuilder b;
        b.append("name", "_id_");
        b.append("ns", ns);
        b.append("key", id_obj);
        BSONObj o = b.done();

        /* edge case: note the insert could fail if we have hit maxindexes already */
        theDataFileMgr.insert(system_indexes.c_str(), o.objdata(), o.objsize(), mayInterrupt, true);
    }

    /* remove bit from a bit array - actually remove its slot, not a clear
       note: this function does not work with x == 63 -- that is ok
             but keep in mind in the future if max indexes were extended to
             exactly 64 it would be a problem
    */
    unsigned long long removeBit(unsigned long long b, int x) {
        unsigned long long tmp = b;
        return
            (tmp & ((((unsigned long long) 1) << x)-1)) |
            ((tmp >> (x+1)) << x);
    }

    bool dropIndexes(NamespaceDetails *d, const char *ns, const char *name, string &errmsg, BSONObjBuilder &anObjBuilder, bool mayDeleteIdIndex) {
        BackgroundOperation::assertNoBgOpInProgForNs(ns);

        d = d->writingWithExtra();
        d->aboutToDeleteAnIndex();

        /* there may be pointers pointing at keys in the btree(s).  kill them. */
        ClientCursor::invalidate(ns);

        // delete a specific index or all?
        if ( *name == '*' && name[1] == 0 ) {
            LOG(4) << "  d->nIndexes was " << d->nIndexes << std::endl;
            anObjBuilder.append("nIndexesWas", (double)d->nIndexes);
            IndexDetails *idIndex = 0;
            if( d->nIndexes ) {
                for ( int i = 0; i < d->nIndexes; i++ ) {
                    if ( !mayDeleteIdIndex && d->idx(i).isIdIndex() ) {
                        idIndex = &d->idx(i);
                    }
                    else {
                        d->idx(i).kill_idx();
                    }
                }
                d->nIndexes = 0;
            }
            if ( idIndex ) {
                d->getNextIndexDetails(ns) = *idIndex;
                d->addIndex(ns);
                wassert( d->nIndexes == 1 );
            }
            /* assuming here that id index is not multikey: */
            d->multiKeyIndexBits = 0;
            assureSysIndexesEmptied(ns, idIndex);
            anObjBuilder.append("msg", mayDeleteIdIndex ?
                                "indexes dropped for collection" :
                                "non-_id indexes dropped for collection");
        }
        else {
            // delete just one index
            int x = d->findIndexByName(name);
            if ( x >= 0 ) {
                LOG(4) << "  d->nIndexes was " << d->nIndexes << endl;
                anObjBuilder.append("nIndexesWas", (double)d->nIndexes);

                /* note it is  important we remove the IndexDetails with this
                 call, otherwise, on recreate, the old one would be reused, and its
                 IndexDetails::info ptr would be bad info.
                 */
                IndexDetails *id = &d->idx(x);

                // Harish start
                auto_ptr<IndexDescriptor> desc(CatalogHack::getDescriptor(d, x));
                string type = CatalogHack::getAccessMethodName(desc->keyPattern());
                if(type == IndexNames::KDTREE) {
                	hlog << "dropping kdtree index" << endl;
                	KdtreeAccessMethod kdm(desc.get());
                	kdm.dropIndex();
                }
                // Harish end

                if ( !mayDeleteIdIndex && id->isIdIndex() ) {
                    errmsg = "may not delete _id index";
                    return false;
                }
                id->kill_idx();
                d->multiKeyIndexBits = removeBit(d->multiKeyIndexBits, x);
                d->nIndexes--;
                for ( int i = x; i < d->nIndexes; i++ )
                    d->idx(i) = d->idx(i+1);
            }
            else {
                int n = removeFromSysIndexes(ns, name); // just in case an orphaned listing there - i.e. should have been repaired but wasn't
                if( n ) {
                    log() << "info: removeFromSysIndexes cleaned up " << n << " entries" << endl;
                }
                log() << "dropIndexes: " << name << " not found" << endl;
                errmsg = "index not found";
                return false;
            }
        }
        return true;
    }

    class IndexUpdateTest : public StartupTest {
    public:
        void run() {
            verify( removeBit(1, 0) == 0 );
            verify( removeBit(2, 0) == 1 );
            verify( removeBit(2, 1) == 0 );
            verify( removeBit(255, 1) == 127 );
            verify( removeBit(21, 2) == 9 );
            verify( removeBit(0x4000000000000001ULL, 62) == 1 );
        }
    } iu_unittest;

}  // namespace mongo
