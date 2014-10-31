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

    /**
     * This is the cursor that the caller of S2AccessMethod::newCursor actually gets.  When you call
     * seek on a S2IndexCursor, it creates another cursor depending on the predicate in the query.
     * The behavior for $near is so different than the behavior for the other geo predicates that
     * it's best to separate the cursors.
     */
    class S2IndexCursor : public IndexCursor {
    public:
        S2IndexCursor(const S2IndexingParams& params, IndexDescriptor* descriptor);
        virtual ~S2IndexCursor() { }

        // Parse the query, figure out if it's a near or a non-near predicate, and create the
        // appropriate sub-cursor.
        virtual Status seek(const BSONObj& position);

        // Not implemented:
        virtual Status seek(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive);
        virtual Status skip(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive);
        virtual Status setOptions(const CursorOptions& options);

        // Implemented:
        virtual bool isEOF() const;
        virtual BSONObj getKey() const;
        virtual DiskLoc getValue() const;
        virtual void next();

        virtual string toString();

        virtual Status savePosition();
        virtual Status restorePosition();

        virtual void aboutToDeleteBucket(const DiskLoc& bucket);

    private:
        S2IndexingParams _params;
        IndexDescriptor *_descriptor;
        scoped_ptr<IndexCursor> _underlyingCursor;
    };
}  // namespace mongo
