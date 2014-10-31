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

#include "mongo/db/btree.h"
#include "mongo/db/btreecursor.h"
#include "mongo/db/hasher.h"
#include "mongo/db/index/hash_access_method.h"
#include "mongo/db/index/hash_index_cursor.h"
#include "mongo/db/queryutil.h"

namespace mongo {

    long long int HashAccessMethod::makeSingleKey(const BSONElement& e, HashSeed seed, int v) {
        massert(16767, "Only HashVersion 0 has been defined" , v == 0 );
        return BSONElementHasher::hash64(e, seed);
    }

    HashAccessMethod::HashAccessMethod(IndexDescriptor* descriptor)
        : BtreeBasedAccessMethod(descriptor) {

        const string HASHED_INDEX_TYPE_IDENTIFIER = "hashed";

        //change these if single-field limitation lifted later
        uassert(16763, "Currently only single field hashed index supported." ,
                1 == descriptor->getNumFields());
        uassert(16764, "Currently hashed indexes cannot guarantee uniqueness. Use a regular index.",
                !descriptor->unique());

        //Default _seed to 0 if "seed" is not included in the index spec
        //or if the value of "seed" is not a number
        _seed = descriptor->getInfoElement("seed").numberInt();

        //In case we have hashed indexes based on other hash functions in
        //the future, we store a hashVersion number. If hashVersion changes,
        // "makeSingleKey" will need to change accordingly.
        //Defaults to 0 if "hashVersion" is not included in the index spec
        //or if the value of "hashversion" is not a number
        _hashVersion = descriptor->getInfoElement("hashVersion").numberInt();

        //Get the hashfield name
        BSONElement firstElt = descriptor->keyPattern().firstElement();
        massert(16765, "error: no hashed index field",
                firstElt.str().compare(HASHED_INDEX_TYPE_IDENTIFIER) == 0);
        _hashedField = firstElt.fieldName();
    }

    Status HashAccessMethod::newCursor(IndexCursor** out) {
        *out = new HashIndexCursor(_hashedField, _seed, _hashVersion, _descriptor);
        return Status::OK();
    }

    void HashAccessMethod::getKeys(const BSONObj& obj, BSONObjSet* keys) {
        getKeysImpl(obj, _hashedField, _seed, _hashVersion, _descriptor->isSparse(), keys);
    }

    // static
    void HashAccessMethod::getKeysImpl(const BSONObj& obj, const string& hashedField, HashSeed seed,
                                       int hashVersion, bool isSparse, BSONObjSet* keys) {
        const char* cstr = hashedField.c_str();
        BSONElement fieldVal = obj.getFieldDottedOrArray(cstr);
        uassert(16766, "Error: hashed indexes do not currently support array values",
                fieldVal.type() != Array );

        if (!fieldVal.eoo()) {
            BSONObj key = BSON( "" << makeSingleKey(fieldVal, seed, hashVersion));
            keys->insert(key);
        }
        else if (!isSparse) {
            BSONObj nullObj = BSON("" << BSONNULL);
            keys->insert(BSON("" << makeSingleKey(nullObj.firstElement(), seed, hashVersion)));
        }
    }

}  // namespace mongo
