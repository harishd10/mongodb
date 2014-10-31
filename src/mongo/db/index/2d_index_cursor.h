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

#include "mongo/base/status.h"
#include "mongo/db/geo/geoquery.h"
#include "mongo/db/index/2d_common.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pdfile.h"
#include "mongo/platform/unordered_map.h"

namespace mongo {

    class TwoDAccessMethod;
    class GeoNearArguments;

    namespace twod_internal {
        class GeoCursorBase;

        class TwoDGeoNearRunner {
        public:
            static bool run2DGeoNear(NamespaceDetails* nsd, int idxNo, const BSONObj& cmdObj,
                                     const GeoNearArguments &parsedArgs, string& errmsg,
                                     BSONObjBuilder& result, unordered_map<string, double>* stats);
        };
    }

    class TwoDIndexCursor : public IndexCursor {
    public:
        TwoDIndexCursor(TwoDAccessMethod* accessMethod);

        /**
         *  Parse the query, figure out if it's a near or a non-near predicate, and create the
         * appropriate sub-cursor.
         */
        virtual Status seek(const BSONObj& position);

        /**
         * We pay attention to the numWanted option.
         */
        virtual Status setOptions(const CursorOptions& options);

        // Not implemented:
        virtual Status seek(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive) { return Status::OK(); }
        virtual Status skip(const vector<const BSONElement*>& position,
                            const vector<bool>& inclusive) { return Status::OK(); }

        // Implemented:
        virtual bool isEOF() const;
        virtual BSONObj getKey() const;
        virtual DiskLoc getValue() const;
        virtual void next();

        virtual string toString();

        virtual Status savePosition();
        virtual Status restorePosition();

        virtual void aboutToDeleteBucket(const DiskLoc& bucket);
        virtual void explainDetails(BSONObjBuilder* b);

    private:
        TwoDAccessMethod* _accessMethod;
        int _numWanted;

        scoped_ptr<twod_internal::GeoCursorBase> _underlyingCursor;
    };
}  // namespace mongo
