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

#include "mongo/bson/bson_field.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/concurrency/mutex.h"

namespace mongo {
    /**
     * Simple class for storing statistics for the RangeDeleter.
     */
    class RangeDeleterStats {
    public:
        //
        // BSON representation top level fields.
        //

        // Total number of deletes, including deletes that are pending and in progress.
        static const BSONField<int> TotalDeletesField;

        // Total number of deletes that are yet to be worked on.
        static const BSONField<int> PendingDeletesField;

        // Total number of deletes that are currently in progress.
        static const BSONField<int> InProgressDeletesField;

        /**
         * Creates a stat object given the mutex from the RangeDeleter object
         * that this instance is keeping track of.
         */
        RangeDeleterStats(mutex* lockPtr):
            _lockPtr(lockPtr),
            _totalDeletes(0),
            _pendingDeletes(0),
            _inProgressDeletes(0) {
        }

        /**
         * Returns the BSON representation of this stat object.
         */
        BSONObj toBSON() const;

        //
        // Setters - Should be holding mutex passed to
        // the constructor when calling these methods.
        //

        void incTotalDeletes_inlock() {
            _totalDeletes++;
        }

        void decTotalDeletes_inlock() {
            _totalDeletes--;
        }

        void incPendingDeletes_inlock() {
            _pendingDeletes++;
        }

        void decPendingDeletes_inlock() {
            _pendingDeletes--;
        }

        void incInProgressDeletes_inlock() {
            _inProgressDeletes++;
        }

        void decInProgressDeletes_inlock() {
            _inProgressDeletes--;
        }

        bool hasInProgress_inlock() {
            return _inProgressDeletes > 0;
        }

    private:
        // Protects all data structures below this. Not owned here.
        mutable mutex* _lockPtr;

        int _totalDeletes;
        int _pendingDeletes;
        int _inProgressDeletes;
    };
}
