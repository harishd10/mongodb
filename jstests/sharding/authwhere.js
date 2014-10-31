// Verify that a user with read access to database "test" cannot access database "test2" via a where
// clause.

//
// User document declarations.  All users in this test are added to the admin database.
//

var adminUser = {
    user: "admin",
    pwd: "a",
    roles: [ "readWriteAnyDatabase",
             "dbAdminAnyDatabase",
             "userAdminAnyDatabase",
             "clusterAdmin" ]
}

var test1Reader = {
    user: "test",
    pwd: "a",
    roles: [],
    otherDBRoles: { test1: [ "read" ] }
};

function assertGLEOK(status) {
    assert(status.ok && status.err === null,
           "Expected OK status object; found " + tojson(status));
}

function assertRemove(collection, pattern) {
    collection.remove(pattern);
    assertGLEOK(collection.getDB().getLastErrorObj());
}

function assertInsert(collection, obj) {
    collection.insert(obj);
    assertGLEOK(collection.getDB().getLastErrorObj());
}

var cluster = new ShardingTest("authwhere", 1, 0, 1,
                               { extraOptions: { keyFile: "jstests/libs/key1" } });

// Set up the test data.
(function() {
    var adminDB = cluster.getDB('admin');
    var test1DB = adminDB.getSiblingDB('test1');
    var test2DB = adminDB.getSiblingDB('test2');
    var ex;
    try {
        adminDB.addUser(adminUser)
        assert(adminDB.auth(adminUser.user, adminUser.pwd));

        assertRemove(adminDB.system.users, { user: test1Reader.user, userSource: null });
        adminDB.addUser(test1Reader);

        assertInsert(test1DB.foo, { a: 1 });
        assertInsert(test2DB.foo, { x: 1 });
    }
    finally {
        adminDB.logout();
    }
}());

(function() {
    var adminDB = cluster.getDB('admin');
    var test1DB;
    var test2DB;
    assert(adminDB.auth(test1Reader.user, test1Reader.pwd));
    try {
        test1DB = adminDB.getSiblingDB("test1");
        test2DB = adminDB.getSiblingDB("test2");

        // Sanity check.  test1Reader can count (read) test1, but not test2.
        assert.eq(test1DB.foo.count(), 1);
        assert.throws(test2DB.foo.count);

        // Cannot examine second database from a where clause.
        assert.throws(test1DB.foo.count, ["db.getSiblingDB('test2').foo.count() == 1"]);

        // Cannot write test1 via tricky where clause.
        assert.throws(test1DB.foo.count, ["db.foo.insert({b: 1})"]);
        assert.eq(test1DB.foo.count(), 1);
    }
    finally {
        adminDB.logout();
    }
}());
