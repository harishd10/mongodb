MongoDB with STING
==================

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
Currently the following 3 query execution implementations are present inside MongoDB: CPU, GPU and Hybrid. This is specified using the constants CPU, CUDA, and CUDA_PARTIAL respectively. To select the required type, change the value of variable requestType in the file "src/mongo/db/index/kdtree_cursor.h" and recompile the code. The standalone code (in the ./standalone directory) supports both in-memory as well as out-of-core execution of the different strategies which can be specified in the command-line.

Creating a STING index
------------------------
An index can be created using a command similar to:

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
  
  The files pertaining to STING -- src/mongo/db/kdtree/*, src/mongo/db/index/kdtree* -- are 
  made available under the terms of the Apache License, version 2.0.

