'use strict';

var expect = require('chai').expect;
var store = require('../index');

describe('#store', function() {

    let table = 'TEST';

    before(function(done) {
        store.init({ apiVersion: '2012-08-10' , region: 'eu-west-1' , endpoint: "http://localhost:8000"
            , accessKeyId: process.env.ACCESS_KEY_ID , secretAccessKey: process.env.ACCESS_KEY });
        done(null);
    });

    it('no tables in the beginning', function(done) {

        store.findTable(table, (e,r) => {
            if(e)
                done(e);
            else {
                expect(r).to.equal(false);
                done(null);
            }
        });

    });

    it('1 table after creating one', function(done) {

        store.createTable({table: table, rangeKey: 'dataspec', numKey: 'id'}, (e) => {
            if(e)
                done(e);
            else {
                store.findTable(table, (e,r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(true);
                        done(null);
                    }
                });
            }
        });

    });

    it('no objects there in the beginning', function(done) {

        store.getObjsCount(table, (e, r) => {
            if(e)
                done(e);
            else {
                expect(r).to.equal(0);
                done(null);
            }
        });

    });

    it('1 object after adding one', function(done) {

        store.putObj(table,  {dataspec: "test-obj", id: 1, "field0": 23 } ,(e) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(1);
                        done(null);
                    }
                });
            }
        });

    });

    it('2 objects after adding another one', function(done) {

        store.putObj(table,  {dataspec: "test-obj", id: 2, "field0": 27 } ,(e) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(2);
                        done(null);
                    }
                });
            }
        });

    });

    it('finding objects', function(done) {

        store.findObj(table,  { id: 2 , "field0": 27} ,(e,r) => {
            if(e)
                done(e);
            else {
                expect(r[0]).to.eql({dataspec: "test-obj", id: 2, "field0": 27 });
                done(null);
            }
        });

    });

    it('2 objects after updating one', function(done) {

        store.putObj(table,  {dataspec: 'test-obj', id: 1 } ,(e,o) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(2);
                        store.getObj(table, {dataspec: 'test-obj', id: 1 }, (e, r) => {
                            if(e)
                                done(e);
                            else {
                                expect(r).to.eql({dataspec: 'test-obj', id: 1 });
                                done(null);
                            }
                        });
                    }
                });

            }
        });

    });

    it('1 objects after deleting one', function(done) {

        store.delObj(table,  {dataspec: "test-obj", id: 2 } ,(e) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(1);
                        done(null);
                    }
                });
            }
        });

    });

    it('0 table after dropping one', function(done) {

        store.dropTable(table, (e) => {
            if(e)
                done(e);
            else {
                store.findTable(table, (e,r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(false);
                        done(null);
                    }
                });
            }
        });

    });

});