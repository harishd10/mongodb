MongoDB with (a) STING
======================

This is the MongoDB database that has been  modified to support our GPU based spatio-temporal index (STING). STING has the following properties:

1. It supports an index to be created on multiple spatial attributes.
   (current implementation assumed 2D coordinates.)
2. It can also be used as a multi-attribute index.
3. There is no limit on the number of attributes.
4. Uses the GPU to support interactive queries.

The MongoDB developer release 2.50 was used as the base code.

Compiling
---------
Ensure CUDA is installed in standard directories. Else specify the directory in cuda.py. Then use scons similar to commpiling standard MongoDB code.

GPU Support
-----------
The following query execution implementations are present inside MongoDB: CPU, GPU and Hybrid. This is specified using the constants CPU, CUDA, and CUDA_PARTIAL respectively (append _IM to the constants to use in-memory variant, and CUDA_DP to use dynamic parallelism). To select the required type, change the value of variable requestType in the file "src/mongo/db/index/kdtree_cursor.h". 

The GPU based querying implementation details can be found in the standalone code as well (in the ./standalone directory). This uses the index files created in MongoDB (which can also be created by the standalone code by providing the appropriate binary file format).

Creating a STING index
------------------------
A STING index can be created in MongoDB using a command similar to:

db.trips.ensureIndex({type: "sting", pickup_time: 1, dropoff_time: 1, pickup: "2d", dropoff: "2d"},{name: "taxi_index"})

In the above example, trips is the name of the collection.
The first attribute should be - type: "sting"
A "2d" attribute should be stored in the collection as {pickup: {x , y}}

Query syntax
------------
Uses standard MongoDB syntax for spatial queries. A typical query will look like:

db.trips.find({pickup_time: {$gt: 0, $lt: 50}, "pickup": {$geoWithin: {$polygon: [[7,7],[7,15],[15,7]]}}})


LICENSE
--------

  The source files originally from MongoDB are made available partially under the terms of the
  GNU Affero General Public License (AGPL), and partially under the terms of the 
  Apache License, version 2.0.  MONGO-README for more details.
  
  The files pertaining to STING -- src/mongo/db/kdtree/\*, src/mongo/db/index/kdtree\*, and standalone/\* -- are 
  made available under the terms of the Apache License, version 2.0.

