/**
*    Copyright (C) 2013 10gen Inc.
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

#include "hash_index_cursor.h"

#include <boost/scoped_ptr.hpp>
#include <vector>

#include "mongo/db/btreecursor.h"
#include "mongo/db/hasher.h"
#include "mongo/db/index/hash_access_method.h"  // for HashAccessMethod::makeSingleKey
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/queryutil.h"

namespace mongo {

    HashIndexCursor::HashIndexCursor(const string& hashedField, HashSeed seed, int hashVersion,
                                     IndexDescriptor* descriptor)
        : _hashedField(hashedField), _seed(seed), _hashVersion(hashVersion),
          _descriptor(descriptor) { }

    Status HashIndexCursor::setOptions(const CursorOptions& options) {
        return Status::OK();
    }

    Status HashIndexCursor::seek(const BSONObj& position) {
        //Use FieldRangeSet to parse the query into a vector of intervals
        //These should be point-intervals if this cursor is ever used
        //So the FieldInterval vector will be, e.g. <[1,1], [3,3], [6,6]>
        FieldRangeSet frs( "" , position, true, true );
        const vector<FieldInterval>& intervals = frs.range( _hashedField.c_str() ).intervals();

        //Construct a new query based on the hashes of the previous point-intervals
        //e.g. {a : {$in : [ hash(1) , hash(3) , hash(6) ]}}
        BSONObjBuilder newQueryBuilder;
        BSONObjBuilder inObj( newQueryBuilder.subobjStart( _hashedField ) );
        BSONArrayBuilder inArray( inObj.subarrayStart("$in") );
        vector<FieldInterval>::const_iterator i;
        for( i = intervals.begin(); i != intervals.end(); ++i ){
            if ( ! i->equality() ){
                _oldCursor.reset(
                        BtreeCursor::make( nsdetails( _descriptor->parentNS()),
                            _descriptor->getOnDisk(),
                            BSON( "" << MINKEY ) ,
                            BSON( "" << MAXKEY ) ,
                            true ,
                            1 ) );
                return Status::OK();
            }
            inArray.append(HashAccessMethod::makeSingleKey(i->_lower._bound, _seed, _hashVersion));
        }
        inArray.done();
        inObj.done();
        BSONObj newQuery = newQueryBuilder.obj();

        BSONObjBuilder specBuilder;
        BSONObjIterator it(_descriptor->keyPattern());
        while (it.more()) {
            BSONElement e = it.next();
            specBuilder.append(e.fieldName(), 1);
        }
        BSONObj spec = specBuilder.obj();

        //Use the point-intervals of the new query to create a Btree cursor
        FieldRangeSet newfrs( "" , newQuery , true, true );
        shared_ptr<FieldRangeVector> newVector(
                new FieldRangeVector( newfrs , spec, 1 ) );

        _oldCursor.reset(
                BtreeCursor::make(nsdetails(_descriptor->parentNS()),
                    _descriptor->getOnDisk(),
                    newVector,
                    0,
                    1));

        return Status::OK();
    }

    Status HashIndexCursor::seek(const vector<const BSONElement*>& position,
                                 const vector<bool>& inclusive) {
        return Status(ErrorCodes::IllegalOperation, "Unimplemented seek called on Hash index");
    }

    Status HashIndexCursor::skip(const vector<const BSONElement*>& position,
                                 const vector<bool>& inclusive) {
        return Status(ErrorCodes::IllegalOperation, "Unimplemented seek called on Hash index");
    }

    bool HashIndexCursor::isEOF() const { return _oldCursor->eof(); }
    BSONObj HashIndexCursor::getKey() const { return _oldCursor->currKey(); }
    DiskLoc HashIndexCursor::getValue() const { return _oldCursor->currLoc(); }
    void HashIndexCursor::next() { _oldCursor->advance(); }
    string HashIndexCursor::toString() { return _oldCursor->toString(); }

    Status HashIndexCursor::savePosition() { _oldCursor->noteLocation(); return Status::OK(); }
    Status HashIndexCursor::restorePosition() { _oldCursor->checkLocation(); return Status::OK(); }

    void HashIndexCursor::aboutToDeleteBucket(const DiskLoc& bucket) {
        _oldCursor->aboutToDeleteBucket(bucket);
    }

}  // namespace mongo
