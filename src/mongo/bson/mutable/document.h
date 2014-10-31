/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <boost/scoped_ptr.hpp>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/mutable/const_element.h"
#include "mongo/bson/mutable/element.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/safe_num.h"

namespace mongo {
namespace mutablebson {

    /** Mutable BSON Overview
     *
     *  Mutable BSON provides classes to facilitate the manipulation of existing BSON objects
     *  or the construction of new BSON objects from scratch in an incremental fashion. The
     *  operations (including additions, deletions, renamings, type changes and value
     *  modification) that are to be performed do not need to be known ahead of time, and do
     *  not need to occur in any particular order. This is in contrast to BSONObjBuilder and
     *  BSONArrayBuilder which offer only serialization and cannot revise already serialized
     *  data. If you need to build a BSONObj but you know upfront what you need to build then
     *  you should use BSONObjBuilder and BSONArrayBuilder directly as they will be faster and
     *  less resource intensive.
     *
     *  The classes in this library (Document, Element, and ConstElement) present a tree-like
     *  (or DOM like) interface. Elements are logically equivalent to BSONElements: they carry
     *  a type, a field name, and a value. Every Element belongs to a Document, which roots the
     *  tree, and Elements of proper type (mongo::Object or mongo::Array) may have child
     *  Elements of their own. Given an Element, you may navigate to the Element's parent, to
     *  its siblings to the left or right of the Element in the tree, and to the leftmost or
     *  rightmost children of the Element. Note that some Elements may not offer all of these
     *  relationships: An Element that represents a terminal BSON value (like an integer) will
     *  not have children (though it may well have siblings). Similarly, an Element that is an
     *  'only child' will not have any left or right siblings. Given a Document, you may begin
     *  navigating by obtaining the root Element of the tree by calling Document::root. See the
     *  documentation for the Element class for the specific navigation methods that will be
     *  available from the root Element.
     *
     *  Elements within the Document may be modified in various ways: the value of the Element
     *  may be changed, the Element may be removed, it may be renamed, and if it is eligible
     *  for children (i.e. it represents a mongo::Array or mongo::Object) it may have child
     *  Elements added to it. Once you have completed building or modifying the Document, you
     *  may write it back out to a BSONObjBuilder by calling Document::writeTo. You may also
     *  serialize individual Elements within the Document to BSONObjBuilder or BSONArrayBuilder
     *  objects by calling Element::writeTo or Element::writeArrayTo.
     *
     *  In addition to the above capabilities, there are algorithms provided in 'algorithm.h'
     *  to help with tasks like searching for Elements that match a predicate or for sorting
     *  the children of an Object Element.
     *
     *  Example 1: Building up a document from scratch, reworking it, and then serializing it:

         namespace mmb = mongo::mutablebson;
         // Create a new document
         mmb::Document doc;
         // doc contents: '{}'

         // Get the root of the document.
         mmb::Element root = doc.root();

         // Create a new mongo::NumberInt typed Element to represent life, the universe, and
         // everything, then push that Element into the root object, making it a child of root.
         mmb::Element e0 = doc.makeElementInt("ltuae", 42);
         root.pushBack(e0);
         // doc contents: '{ ltuae : 42 }'

         // Create a new empty mongo::Object-typed Element named 'magic', and push it back as a
         // child of the root, making it a sibling of e0.
         mmb::Element e1 = doc.makeElementObject("magic");
         root.pushBack(e1);
         // doc contents: '{ ltuae : 42, magic : {} }'

         // Create a new mongo::NumberDouble typed Element to represent Pi, and insert it as child
         // of the new object we just created.
         mmb::Element e3 = doc.makeElementDouble("pi", 3.14);
         e1.pushBack(e3);
         // doc contents: '{ ltuae : 42, magic : { pi : 3.14 } }'

         // Create a new mongo::NumberDouble to represent Plancks constant in electrovolt
         // micrometers, and add it as a child of the 'magic' object.
         mmb::Element e4 = doc.makeElementDouble("hbar", 1.239);
         e1.pushBack(e4);
         // doc contents: '{ ltuae : 42, magic : { pi : 3.14, hbar : 1.239 } }'

         // Rename the parent element of 'hbar' to be 'constants'.
         e4.parent().rename("constants");
         // doc contents: '{ ltuae : 42, constants : { pi : 3.14, hbar : 1.239 } }'

         // Rename 'ltuae' to 'answer' by accessing it as the root objects left child.
         doc.root().leftChild().rename("answer");
         // doc contents: '{ answer : 42, constants : { pi : 3.14, hbar : 1.239 } }'

         // Sort the constants by name.
         mmb::sortChildren(doc.root().rightChild(), mmb::FieldNameLessThan());
         // doc contents: '{ answer : 42, constants : { hbar : 1.239, pi : 3.14 } }'

         mongo::BSONObjBuilder builder;
         doc.writeTo(&builder);
         mongo::BSONObj result = builder.obj();
         // result contents: '{ answer : 42, constants : { hbar : 1.239, pi : 3.14 } }'

     *  While you can use this library to build Documents from scratch, its real purpose is to
     *  manipulate existing BSONObjs. A BSONObj may be passed to the Document constructor or to
     *  Document::make[Object|Array]Element, in which case the Document or Element will reflect
     *  the values contained within the provided BSONObj. Modifications will not alter the
     *  underlying BSONObj: they are held off to the side within the Document. However, when
     *  the Document is subsequently written back out to a BSONObjBuilder, the modifications
     *  applied to the Document will be reflected in the serialized version.
     *
     *  Example 2: Modifying an existing BSONObj (some error handling removed for length)

         namespace mmb = mongo::mutablebson;

         static const char inJson[] =
             "{"
             "  'whale': { 'alive': true, 'dv': -9.8, 'height': 50, attrs : [ 'big' ] },"
             "  'petunias': { 'alive': true, 'dv': -9.8, 'height': 50 } "
             "}";
         mongo::BSONObj obj = mongo::fromjson(inJson);

         // Create a new document representing the BSONObj with the above contents.
         mmb::Document doc(obj);

         // The whale hits the planet and dies.
         mmb::Element whale = mmb::findFirstChildNamed(doc.root(), "whale");
         // Find the 'dv' field in the whale.
         mmb::Element whale_deltav = mmb::findFirstChildNamed(whale, "dv");
         // Set the dv field to zero.
         whale_deltav.setValueDouble(0.0);
         // Find the 'height' field in the whale.
         mmb::Element whale_height = mmb::findFirstChildNamed(whale, "height");
         // Set the height field to zero.
         whale_deltav.setValueDouble(0);
         // Find the 'alive' field, and set it to false.
         mmb::Element whale_alive = mmb::findFirstChildNamed(whale, "alive");
         whale_alive.setValueBool(false);

         // The petunias survive, update its fields much like we did above.
         mmb::Element petunias = mmb::findFirstChildNamed(doc.root(), "petunias");
         mmb::Element petunias_deltav = mmb::findFirstChildNamed(petunias, "dv");
         petunias_deltav.setValueDouble(0.0);
         mmb::Element petunias_height = mmb::findFirstChildNamed(petunias, "height");
         petunias_deltav.setValueDouble(0);

         // Replace the whale by its wreckage, saving only its attributes:
         // Construct a new mongo::Object element for the ex-whale.
         mmb::Element ex_whale = doc.makeElementObject("ex-whale");
         doc.root().pushBack(ex_whale);
         // Find the attributes of the old 'whale' element.
         mmb::Element whale_attrs = mmb::findFirstChildNamed(whale, "attrs");
         // Remove the attributes from the whale (they remain valid, but detached).
         whale_attrs.remove();
         // Add the attributes into the ex-whale.
         ex_whale.pushBack(whale_attrs);
         // Remove the whale object.
         whale.remove();

         // Current state of document:
         "{"
         "    'petunias': { 'alive': true, 'dv': 0.0, 'height': 50 },"
         "    'ex-whale': { 'attrs': [ 'big' ] } })"
         "}";

     * Both of the above examples are derived from tests in mutable_bson_test.cpp, see the
     * tests Example1 and Example2 if you would like to play with the code.
     *
     * Additional details on Element and Document are available in their class and member
     * comments.
     */


    /** Document is the entry point into the mutable BSON system. It has a fairly simple
     *  API. It acts as an owner for the Element resources of the document, provides a
     *  pre-constructed designated root Object Element, and acts as a factory for new Elements,
     *  which may then be attached to the root or to other Elements by calling the appropriate
     *  topology mutation methods in Element.
     *
     *  The default constructor builds an empty Document which you may then extend by creating
     *  new Elements and manipulating the tree topology. It is also possible to build a
     *  Document that derives its initial values from a BSONObj. The given BSONObj will not be
     *  modified, but it also must not be modified elsewhere while Document is using it. Unlike
     *  all other calls in this library where a BSONObj is passed in, the one argument Document
     *  constructor *does not copy* the BSONObj's contents, so they must remain valid for the
     *  duration of Documents lifetime. Document does hold a copy of the BSONObj itself, so it
     *  will up the refcount if the BSONObj internals are counted.
     *
     *  Newly constructed Elements formed by calls to 'makeElement[Type]' methods are not
     *  attached to the root of the document. You must explicitly attach them somewhere. If you
     *  lose the Element value that is returned to you from a 'makeElement' call before you
     *  attach it to the tree then the value will be unreachable. Elements in a document do not
     *  outlive the Document.
     *
     *  Document provides a convenience method to serialize all of the Elements in the tree
     *  that are reachable from the root element to a BSONObjBuilder. In general you should use
     *  this in preference to root().writeTo() if you mean to write the entire
     *  Document. Similarly, Document provides wrappers for comparisons that simply delegate to
     *  comparison operations on the root Element.
     *
     *  A 'const Document' is very limited: you may only write its contents out or obtain a
     *  ConstElement for the root. ConstElement is much like Element, but does not permit
     *  mutations. See the class comment for ConstElement for more information.
     */
    class Document {

        // TODO: In principle there is nothing that prevents implementing a deep copy for
        // Document, but for now it is not permitted.
        MONGO_DISALLOW_COPYING(Document);

    public:

        //
        // Lifecycle
        //

        /** Construct a new empty document. */
        Document();

        /** Construct new document for the given BSONObj. The data in 'value' is NOT copied. */
        explicit Document(const BSONObj& value);

        ~Document();


        //
        // Comparison API
        //

        /** Compare this Document to 'other' with the semantics of BSONObj::woCompare. */
        inline int compareWith(const Document& other, bool considerFieldName = true) const;

        /** Compare this Document to 'other' with the semantics of BSONObj::woCompare. */
        inline int compareWithBSONObj(const BSONObj& other, bool considerFieldName = true) const;


        //
        // Serialization API
        //

        /** Serialize the Elements reachable from the root Element of this Document to the
         *  provided builder.
         */
        inline void writeTo(BSONObjBuilder* builder) const;

        /** Serialize the Elements reachable from the root Element of this Document and return
         *  the result as a BSONObj. */
        inline BSONObj getObject() const;


        //
        // Element creation API.
        //
        // Newly created elements are not attached to the tree (effectively, they are
        // 'roots'). You must call one of the topology management methods in 'Element' to
        // connect the newly created Element to another Element in the Document, possibly the
        // Element referenced by Document::root. Elements do not outlive the Document.
        //

        /** Create a new double Element with the given value and field name. */
        Element makeElementDouble(const StringData& fieldName, double value);

        /** Create a new string Element with the given value and field name. */
        Element makeElementString(const StringData& fieldName, const StringData& value);

        /** Create a new empty object Element with the given field name. */
        Element makeElementObject(const StringData& fieldName);

        /** Create a new object Element with the given field name. The data in 'value' is
         *  copied.
         */
        Element makeElementObject(const StringData& fieldName, const BSONObj& value);

        /** Create a new empty array Element with the given field name. */
        Element makeElementArray(const StringData& fieldName);

        /** Create a new array Element with the given field name. The data in 'value' is
         *  copied.
         */
        Element makeElementArray(const StringData& fieldName, const BSONObj& value);

        /** Create a new binary Element with the given data and field name. */
        Element makeElementBinary(
            const StringData& fieldName, uint32_t len, BinDataType binType, const void* data);

        /** Create a new undefined Element with the given field name. */
        Element makeElementUndefined(const StringData& fieldName);

        /** Create a new OID Element with the given value and field name. */
        Element makeElementOID(const StringData& fieldName, const mongo::OID& value);

        /** Create a new bool Element with the given value and field name. */
        Element makeElementBool(const StringData& fieldName, bool value);

        /** Create a new date Element with the given value and field name. */
        Element makeElementDate(const StringData& fieldName, Date_t value);

        /** Create a new null Element with the given field name. */
        Element makeElementNull(const StringData& fieldName);

        /** Create a new regex Element with the given data and field name. */
        Element makeElementRegex(
            const StringData& fieldName, const StringData& regex, const StringData& flags);

        /** Create a new DBRef Element with the given data and field name. */
        Element makeElementDBRef(
            const StringData& fieldName, const StringData& ns, const mongo::OID& oid);

        /** Create a new code Element with the given value and field name. */
        Element makeElementCode(const StringData& fieldName, const StringData& value);

        /** Create a new symbol Element with the given value and field name. */
        Element makeElementSymbol(const StringData& fieldName, const StringData& value);

        /** Create a new scoped code Element with the given data and field name. */
        Element makeElementCodeWithScope(
            const StringData& fieldName, const StringData& code, const BSONObj& scope);

        /** Create a new integer Element with the given value and field name. */
        Element makeElementInt(const StringData& fieldName, int32_t value);

        /** Create a new timetamp Element with the given value and field name. */
        Element makeElementTimestamp(const StringData& fieldName, OpTime value);

        /** Create a new long integer Element with the given value and field name. */
        Element makeElementLong(const StringData& fieldName, int64_t value);

        /** Create a new min key Element with the given field name. */
        Element makeElementMinKey(const StringData& fieldName);

        /** Create a new max key Element with the given field name. */
        Element makeElementMaxKey(const StringData& fieldName);


        //
        // Element creation methods from variant types
        //

        /** Construct a new Element with the same name, type, and value as the provided
         *  BSONElement. The value is copied.
         */
        Element makeElement(const BSONElement& elt);

        /** Construct a new Element with the same type and value as the provided BSONElement,
         *  but with a new name. The value is copied.
         */
        Element makeElementWithNewFieldName(const StringData& fieldName, const BSONElement& elt);

        /** Create a new element of the appopriate type to hold the given value, with the given
         *  field name.
         */
        Element makeElementSafeNum(const StringData& fieldName, const SafeNum& value);


        //
        // Accessors
        //

        /** Returns the root element for this document. */
        inline Element root();

        /** Returns the root element for this document. */
        inline ConstElement root() const;

        /** Returns an element that will compare equal to a non-ok element. */
        Element end();

        /** Returns an element that will compare equal to a non-ok element. */
        ConstElement end() const;

    private:
        friend class Element;

        // For now, the implementation of Document is firewalled.
        class Impl;
        inline Impl& getImpl();
        inline const Impl& getImpl() const;
        const boost::scoped_ptr<Impl> _impl;

        // The root element of this document.
        const Element _root;
    };

} // namespace mutablebson
} // namespace mongo

#include "mongo/bson/mutable/document-inl.h"
