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

#include "mongo/db/index/s2_access_method.h"

#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/geo/geoparser.h"
#include "mongo/db/index_names.h"
#include "mongo/db/index/s2_index_cursor.h"
#include "mongo/db/jsobj.h"
#include "third_party/s2/s2.h"
#include "third_party/s2/s2cell.h"
#include "third_party/s2/s2polygon.h"
#include "third_party/s2/s2polyline.h"
#include "third_party/s2/s2regioncoverer.h"

namespace {

    static void keysFromRegion(S2RegionCoverer *coverer, const S2Region &region,
                               vector<string> *out) {
        vector<S2CellId> covering;
        coverer->GetCovering(region, &covering);
        for (size_t i = 0; i < covering.size(); ++i) {
            out->push_back(covering[i].toString());
        }
    }

}  // namespace

namespace mongo {

    // Thanks, Wikipedia.
    const double S2IndexingParams::kRadiusOfEarthInMeters = (6378.1 * 1000);

    static int configValueWithDefault(IndexDescriptor *desc, const string& name, int def) {
        BSONElement e = desc->getInfoElement(name);
        if (e.isNumber()) { return e.numberInt(); }
        return def;
    }

    S2AccessMethod::S2AccessMethod(IndexDescriptor *descriptor)
        : BtreeBasedAccessMethod(descriptor) {

        // Set up basic params.
        _params.maxKeysPerInsert = 200;
        // This is advisory.
        _params.maxCellsInCovering = 50;
        // Near distances are specified in meters...sometimes.
        _params.radius = S2IndexingParams::kRadiusOfEarthInMeters;
        // These are not advisory.
        _params.finestIndexedLevel = configValueWithDefault(descriptor, "finestIndexedLevel",
            S2::kAvgEdge.GetClosestLevel(500.0 / _params.radius));
        _params.coarsestIndexedLevel = configValueWithDefault(descriptor, "coarsestIndexedLevel",
            S2::kAvgEdge.GetClosestLevel(100 * 1000.0 / _params.radius));
        uassert(16747, "coarsestIndexedLevel must be >= 0", _params.coarsestIndexedLevel >= 0);
        uassert(16748, "finestIndexedLevel must be <= 30", _params.finestIndexedLevel <= 30);
        uassert(16749, "finestIndexedLevel must be >= coarsestIndexedLevel",
                _params.finestIndexedLevel >= _params.coarsestIndexedLevel);

        int geoFields = 0;

        // Categorize the fields we're indexing and make sure we have a geo field.
        BSONObjIterator i(descriptor->keyPattern());
        while (i.more()) {
            BSONElement e = i.next();
            if (e.type() == String && IndexNames::GEO_2DSPHERE == e.String() ) {
                ++geoFields;
            }
            else {
                // We check for numeric in 2d, so that's the check here
                uassert( 16823, (string)"Cannot use " + IndexNames::GEO_2DSPHERE +
                                    " index with other special index types: " + e.toString(),
                         e.isNumber() );
            }
        }
        uassert(16750, "Expect at least one geo field, spec=" + descriptor->keyPattern().toString(),
                geoFields >= 1);
    }

    Status S2AccessMethod::newCursor(IndexCursor** out) {
        *out = new S2IndexCursor(_params, _descriptor);
        return Status::OK();
    }

    void S2AccessMethod::getKeys(const BSONObj& obj, BSONObjSet* keys) {
        BSONObjSet keysToAdd;
        // We output keys in the same order as the fields we index.
        BSONObjIterator i(_descriptor->keyPattern());
        while (i.more()) {
            BSONElement e = i.next();

            // First, we get the keys that this field adds.  Either they're added literally from
            // the value of the field, or they're transformed if the field is geo.
            BSONElementSet fieldElements;
            // false means Don't expand the last array, duh.
            obj.getFieldsDotted(e.fieldName(), fieldElements, false);

            BSONObjSet keysForThisField;
            if (IndexNames::GEO_2DSPHERE == e.valuestr()) {
                getGeoKeys(fieldElements, &keysForThisField);
            } else {
                getLiteralKeys(fieldElements, &keysForThisField);
            }

            // We expect there to be the missing field element present in the keys if data is
            // missing.  So, this should be non-empty.
            verify(!keysForThisField.empty());

            // We take the Cartesian product of all of the keys.  This requires that we have
            // some keys to take the Cartesian product with.  If keysToAdd.empty(), we
            // initialize it.  
            if (keysToAdd.empty()) {
                keysToAdd = keysForThisField;
                continue;
            }

            BSONObjSet updatedKeysToAdd;
            for (BSONObjSet::const_iterator it = keysToAdd.begin(); it != keysToAdd.end();
                    ++it) {
                for (BSONObjSet::const_iterator newIt = keysForThisField.begin();
                        newIt!= keysForThisField.end(); ++newIt) {
                    BSONObjBuilder b;
                    b.appendElements(*it);
                    b.append(newIt->firstElement());
                    updatedKeysToAdd.insert(b.obj());
                }
            }
            keysToAdd = updatedKeysToAdd;
        }

        if (keysToAdd.size() > _params.maxKeysPerInsert) {
            warning() << "insert of geo object generated lots of keys (" << keysToAdd.size()
                << ") consider creating larger buckets. obj="
                << obj;
        }

        *keys = keysToAdd;
    }

    // Get the index keys for elements that are GeoJSON.
    void S2AccessMethod::getGeoKeys(const BSONElementSet& elements, BSONObjSet* out) const {
        S2RegionCoverer coverer;
        _params.configureCoverer(&coverer);

        // See here for GeoJSON format: geojson.org/geojson-spec.html
        for (BSONElementSet::iterator i = elements.begin(); i != elements.end(); ++i) {
            uassert(16754, "Can't parse geometry from element: " + i->toString(),
                    i->isABSONObj());
            const BSONObj &obj = i->Obj();

            vector<string> cells;
            S2Polyline line;
            S2Cell point;
            // We only support GeoJSON polygons.  Why?:
            // 1. we don't automagically do WGS84/flat -> WGS84, and
            // 2. the old polygon format must die.
            if (GeoParser::isGeoJSONPolygon(obj)) {
                S2Polygon polygon;
                GeoParser::parseGeoJSONPolygon(obj, &polygon);
                keysFromRegion(&coverer, polygon, &cells);
            } else if (GeoParser::parseLineString(obj, &line)) {
                keysFromRegion(&coverer, line, &cells);
            } else if (GeoParser::parsePoint(obj, &point)) {
                S2CellId parent(point.id().parent(_params.finestIndexedLevel));
                cells.push_back(parent.toString());
            } else {
                uasserted(16755, "Can't extract geo keys from object, malformed geometry?:"
                        + obj.toString());
            }
            uassert(16756, "Unable to generate keys for (likely malformed) geometry: "
                    + obj.toString(),
                    cells.size() > 0);

            for (vector<string>::const_iterator it = cells.begin(); it != cells.end(); ++it) {
                BSONObjBuilder b;
                b.append("", *it);
                out->insert(b.obj());
            }
        }

        if (0 == out->size()) {
            BSONObjBuilder b;
            b.appendNull("");
            out->insert(b.obj());
        }
    }

    void S2AccessMethod::getLiteralKeysArray(const BSONObj& obj, BSONObjSet* out) const {
        BSONObjIterator objIt(obj);
        if (!objIt.more()) {
            // Empty arrays are indexed as undefined.
            BSONObjBuilder b;
            b.appendUndefined("");
            out->insert(b.obj());
        } else {
            // Non-empty arrays are exploded.
            while (objIt.more()) {
                BSONObjBuilder b;
                b.appendAs(objIt.next(), "");
                out->insert(b.obj());
            }
        }
    }

    void S2AccessMethod::getOneLiteralKey(const BSONElement& elt, BSONObjSet* out) const {
        if (Array == elt.type()) {
            getLiteralKeysArray(elt.Obj(), out);
        } else {
            // One thing, not an array, index as-is.
            BSONObjBuilder b;
            b.appendAs(elt, "");
            out->insert(b.obj());
        }
    }

    // elements is a non-geo field.  Add the values literally, expanding arrays.
    void S2AccessMethod::getLiteralKeys(const BSONElementSet& elements,
                                        BSONObjSet* out) const {
        if (0 == elements.size()) {
            // Missing fields are indexed as null.
            BSONObjBuilder b;
            b.appendNull("");
            out->insert(b.obj());
        } else {
            for (BSONElementSet::iterator i = elements.begin(); i != elements.end(); ++i) {
                getOneLiteralKey(*i, out);
            }
        }
    }

}  // namespace mongo
