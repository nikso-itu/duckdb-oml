var duckdb = require('../../duckdb/tools/nodejs');
var assert = require('assert');

describe(`oml extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"});
        conn = new duckdb.Connection(db);
        conn.exec(`LOAD '${process.env.OML_EXTENSION_BINARY_PATH}';`, function (err) {
            if (err) throw err;
            done();
        });
    });

    it('oml function should return expected amount of rows', function (done) {
        db.all("SELECT * from OmlGen('data/oml_testing/st_lrwan1_11.oml');", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{value: 67725}]);
            done();
        });
    });
});