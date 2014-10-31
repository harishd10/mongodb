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

#include <string>

namespace mongo {

    using std::string;

    class BSONObj;

    /**
     * We use the string representation of index names all over the place, so we declare them all
     * once here.
     */
    class IndexNames {
    public:
        static const string GEO_2D;
        static const string GEO_HAYSTACK;
        static const string GEO_2DSPHERE;
        static const string TEXT;
        static const string TEXT_INTERNAL;
        static const string HASHED;

        //Harish
        static const string KDTREE;

        /**
         * True if is a regular (non-plugin) index or uses a plugin that existed before 2.4.
         * These plugins are grandfathered in and allowed to exist in DBs with
         * PDFILE_MINOR_VERSION_22_AND_OLDER
         */
        static bool existedBefore24(const string& name) {
            return name.empty()
                || name == IndexNames::GEO_2D
                || name == IndexNames::GEO_HAYSTACK
                || name == IndexNames::HASHED;
        }

        /**
         * Return the first string value in the provided object.  For an index key pattern,
         * a field with a non-string value indicates a "special" (not straight Btree) index.
         */
        static string findPluginName(const BSONObj& keyPattern);

        static bool isKnownName(const string& name) {
            return name.empty()
                   || name == IndexNames::GEO_2D
                   || name == IndexNames::GEO_2DSPHERE
                   || name == IndexNames::GEO_HAYSTACK
                   || name == IndexNames::TEXT
                   || name == IndexNames::TEXT_INTERNAL
                   || name == IndexNames::HASHED
                   || name == IndexNames:: KDTREE;
        }
    };

}  // namespace mongo
