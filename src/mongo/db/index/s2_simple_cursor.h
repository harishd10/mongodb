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

#pragma once

#include <vector>

#include "mongo/db/btreecursor.h"
#include "mongo/db/geo/geoquery.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/s2_common.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pdfile.h"
#include "mongo/platform/unordered_set.h"
#include "third_party/s2/s2cap.h"
#include "third_party/s2/s2regionintersection.h"

namespace mongo {

    class S2SimpleCursor : public IndexCursor {
    public:
        S2SimpleCursor(IndexDescriptor* descriptor, const S2IndexingParams& params);

        virtual ~S2SimpleCursor() { }

        // Not implemented
        virtual Status seek(const BSONObj& position);
        virtual Status seek(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive);
        virtual Status skip(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive);
        Status setOptions(const CursorOptions& options);

        // Implemented:
        // Not part of the IndexCursor spec.
        void seek(const BSONObj& query, const vector<GeoQuery>& regions);

        bool isEOF() const;
        BSONObj getKey() const;
        DiskLoc getValue() const;
        void next();

        virtual string toString();

        virtual Status savePosition();
        virtual Status restorePosition();

        virtual void aboutToDeleteBucket(const DiskLoc& bucket);
    private:
        IndexDescriptor* _descriptor;

        // The query with the geo stuff taken out.  We use this with a matcher.
        BSONObj _filteredQuery;

        // What geo regions are we looking for?
        vector<GeoQuery> _fields;

        // How were the keys created?  We need this to search for the right stuff.
        S2IndexingParams _params;

        // What have we checked so we don't repeat it and waste time?
        unordered_set<DiskLoc, DiskLoc::Hasher> _seen;

        // This really does all the work/points into the btree.
        scoped_ptr<BtreeCursor> _btreeCursor;

        // Stat counters/debug information goes below:
        // How many items did we look at in the btree?
        long long _nscanned;

        // How many did we try to match?
        long long _matchTested;

        // How many did we geo-test?
        long long _geoTested;

        // How many cells were in our cover?
        long long _cellsInCover;
    };

}  // namespace mongo
