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

#include "mongo/db/ops/modifier_bit.h"

#include "mongo/base/error_codes.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/db/ops/field_checker.h"
#include "mongo/db/ops/path_support.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    using mongoutils::str::stream;

    struct ModifierBit::PreparedState {

        PreparedState(mutablebson::Document& doc)
            : doc(doc)
            , idxFound(0)
            , elemFound(doc.end())
            , boundDollar("")
            , inPlace(false)
            , noOp(false) {
        }

        // Document that is going to be changed.
        mutablebson::Document& doc;

        // Index in _fieldRef for which an Element exist in the document.
        int32_t idxFound;

        // Element corresponding to _fieldRef[0.._idxFound].
        mutablebson::Element elemFound;

        // Value to bind to a $-positional field, if one is provided.
        std::string boundDollar;

        // Value to be applied.
        SafeNum newValue;

        // True if this update can be applied in place.
        bool inPlace;

        // True if this update is a no-op
        bool noOp;
    };

    ModifierBit::ModifierBit()
        : ModifierInterface ()
        , _fieldRef()
        , _posDollar(0)
        , _val()
        , _op(NULL) {
    }

    ModifierBit::~ModifierBit() {
    }

    Status ModifierBit::init(const BSONElement& modExpr) {

        // Perform standard field name and updateable checks.
        _fieldRef.parse(modExpr.fieldName());
        Status status = fieldchecker::isUpdatable(_fieldRef);
        if (! status.isOK()) {
            return status;
        }

        // If a $-positional operator was used, get the index in which it occurred.
        fieldchecker::isPositional(_fieldRef, &_posDollar);

        if (modExpr.type() != mongo::Object)
            return Status(ErrorCodes::BadValue,
                          "Value following $bit must be an Object");

        BSONElement payloadElt = modExpr.embeddedObject().firstElement();
        dassert(!payloadElt.eoo());

        const StringData payloadFieldName = payloadElt.fieldName();

        // TODO: If this becomes three items, we should consider how to provide "subclasses" of
        // this mod. This would probably involve makign 'init' a static factory.
        const bool isAnd = (payloadFieldName == "and");
        const bool isOr = (payloadFieldName == "or");

        if (!(isAnd || isOr))
            return Status(
                ErrorCodes::BadValue,
                "Only 'and' and 'or' are supported $bit sub-operators");

        if ((payloadElt.type() != mongo::NumberInt) &&
            (payloadElt.type() != mongo::NumberLong))
            return Status(
                ErrorCodes::BadValue,
                "Argument to $bit operation must be a NumberInt or NumberLong");

        _val = SafeNum(payloadElt);
        _op = isAnd ?
            &SafeNum::bitAnd :
            &SafeNum::bitOr;

        return Status::OK();
    }

    Status ModifierBit::prepare(mutablebson::Element root,
                                const StringData& matchedField,
                                ExecInfo* execInfo) {

        _preparedState.reset(new PreparedState(root.getDocument()));

        // If we have a $-positional field, it is time to bind it to an actual field part.
        if (_posDollar) {
            if (matchedField.empty()) {
                return Status(ErrorCodes::BadValue, "matched field not provided");
            }
            _preparedState->boundDollar = matchedField.toString();
            _fieldRef.setPart(_posDollar, _preparedState->boundDollar);
        }

        // Locate the field name in 'root'.
        Status status = pathsupport::findLongestPrefix(_fieldRef,
                                                       root,
                                                       &_preparedState->idxFound,
                                                       &_preparedState->elemFound);


        // FindLongestPrefix may say the path does not exist at all, which is fine here, or
        // that the path was not viable or otherwise wrong, in which case, the mod cannot
        // proceed.
        if (status.code() == ErrorCodes::NonExistentPath) {
            _preparedState->elemFound = root.getDocument().end();
        }
        else if (!status.isOK()) {
            return status;
        }

        // We register interest in the field name. The driver needs this info to sort out if
        // there is any conflict among mods.
        execInfo->fieldRef[0] = &_fieldRef;

        //
        // in-place and no-op logic
        //

        // If the field path is not fully present, then this mod cannot be in place, nor is a
        // noOp.
        if (!_preparedState->elemFound.ok() ||
            _preparedState->idxFound < static_cast<int32_t>(_fieldRef.numParts() - 1)) {
            // If no target element exists, the value we will write is the result of applying
            // the operation to a zero-initialized integer element.
            _preparedState->newValue = (SafeNum(static_cast<int>(0)).*_op)(_val);
            return Status::OK();
        }

        if (!_preparedState->elemFound.isIntegral())
            return Status(
                ErrorCodes::BadValue,
                "Cannot apply $bit to a value of non-integral type");

        const SafeNum currentValue = _preparedState->elemFound.getValueSafeNum();

        // Apply the op over the existing value and the mod value, and capture the result.
        _preparedState->newValue = (currentValue.*_op)(_val);

        if (!_preparedState->newValue.isValid())
            return Status(ErrorCodes::BadValue,
                          "Failed to apply $bit to current value");

        // If the values are identical (same type, same value), then this is a no-op, and
        // therefore in-place as well.
        if (_preparedState->newValue.isIdentical(currentValue)) {
            _preparedState->noOp = execInfo->noOp = true;
            _preparedState->inPlace = execInfo->inPlace = true;
            return Status::OK();
        }

        // TODO: Cases where the type changes but size is the same.
        if (currentValue.type() == _preparedState->newValue.type()) {
            _preparedState->inPlace = execInfo->inPlace = true;
        }

        return Status::OK();
    }

    Status ModifierBit::apply() const {
        dassert(_preparedState->noOp == false);

        // If there's no need to create any further field part, the $bit is simply a value
        // assignment.
        if (_preparedState->elemFound.ok() &&
            _preparedState->idxFound == static_cast<int32_t>(_fieldRef.numParts() - 1)) {
            return _preparedState->elemFound.setValueSafeNum(_preparedState->newValue);
        }

        dassert(_preparedState->inPlace == false);

        //
        // Complete document path logic
        //

        // Creates the final element that's going to be $set in 'doc'.
        mutablebson::Document& doc = _preparedState->doc;
        StringData lastPart = _fieldRef.getPart(_fieldRef.numParts() - 1);
        mutablebson::Element elemToSet = doc.makeElementSafeNum(lastPart, _preparedState->newValue);
        if (!elemToSet.ok()) {
            return Status(ErrorCodes::InternalError, "can't create new element");
        }

        // Now, we can be in two cases here, as far as attaching the element being set goes:
        // (a) none of the parts in the element's path exist, or (b) some parts of the path
        // exist but not all.
        if (!_preparedState->elemFound.ok()) {
            _preparedState->elemFound = doc.root();
            _preparedState->idxFound = 0;
        }
        else {
            _preparedState->idxFound++;
        }

        // createPathAt() will complete the path and attach 'elemToSet' at the end of it.
        return pathsupport::createPathAt(_fieldRef,
                                         _preparedState->idxFound,
                                         _preparedState->elemFound,
                                         elemToSet);
    }

    Status ModifierBit::log(mutablebson::Element logRoot) const {

        // We'd like to create an entry such as {$set: {<fieldname>: <value>}} under 'logRoot'.
        // We start by creating the {$set: ...} Element.
        mutablebson::Document& doc = logRoot.getDocument();
        mutablebson::Element setElement = doc.makeElementObject("$set");
        if (!setElement.ok()) {
            return Status(ErrorCodes::InternalError, "cannot append log entry for $set mod");
        }

        // Then we create the {<fieldname>: <value>} Element.
        mutablebson::Element logElement = doc.makeElementSafeNum(_fieldRef.dottedField(),
                                                                 _preparedState->newValue);
        if (!logElement.ok()) {
            return Status(ErrorCodes::InternalError, "cannot append details for $set mod");
        }

        // Now, we attach the {<fieldname>: <value>} Element under the {$set: ...} one.
        Status status = setElement.pushBack(logElement);
        if (!status.isOK()) {
            return status;
        }

        // And attach the result under the 'logRoot' Element provided.
        return logRoot.pushBack(setElement);
    }

} // namespace mongo
