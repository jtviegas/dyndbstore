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

    it('01 no tables in the beginning', function(done) {

        store.findTable(table, (e,r) => {
            if(e)
                done(e);
            else {
                expect(r).to.equal(false);
                done(null);
            }
        });

    });

    it('02 1 table after creating one', function(done) {

        store.createTable(table,(e) => {
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

    it('03 no objects there in the beginning', function(done) {

        store.getObjsCount(table, (e, r) => {
            if(e)
                done(e);
            else {
                expect(r).to.equal(0);
                done(null);
            }
        });

    });

    it('04 1 object after adding one', function(done) {

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

    it('#05 2 objects after adding another one', function(done) {

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

    it('#06 finding objects', function(done) {

        store.findObj(table,  { id: 2 , "field0": 27} ,(e,r) => {
            if(e)
                done(e);
            else {
                expect(r[0]).to.eql({dataspec: "test-obj", id: 2, "field0": 27 });
                done(null);
            }
        });

    });

    it('#07 2 objects after updating one', function(done) {

        store.putObj(table,  {dataspec: 'test-obj', id: 1 } ,(e,o) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(2);
                        store.getObj(table,  1 , (e, r) => {
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

    it('#071 6 objects after adding 4', function(done) {

        store.putObjs(table,  [{dataspec: 'test-obj3', id: 3 }, {dataspec: 'test-obj4', id: 4 }, {dataspec: 'test-obj5', id: 5 }, {dataspec: 'test-obj6', id: 6 }] ,(e,o) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(6);
                        store.getObj(table,  1 , (e, r) => {
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

    it('#08 search for 2 objects by id range', function(done) {

        store.findObjsByIdRange(table, 3, 4,(e,r) => {
            if(e)
                done(e);
            else {
                expect(r.length).to.equal(2);
                done(null);
            }
        });

    });

    it('#081 search for 2 objects by attribute range', function(done) {

        store.findObjsByCriteria(table, { 'id': [2,6], 'dataspec': ['test-obj','test-obj3'] }, true,(e,r) => {
            if(e)
                done(e);
            else {
                expect(r.length).to.equal(2);
                console.log(r);
                done(null);
            }
        });

    });

    it('#082 search for 2 objects by attribute range no join', function(done) {

        store.findObjsByCriteria(table, { 'id': 6, 'dataspec': 'test-obj4' }, false,(e,r) => {
            if(e)
                done(e);
            else {
                expect(r.length).to.equal(2);
                console.log(r);
                done(null);
            }
        });

    });

    it('#083 5 objects after deleting one', function(done) {

        store.delObj(table,  2  ,(e) => {
            if(e)
                done(e);
            else {
                store.getObjsCount(table, (e, r) => {
                    if(e)
                        done(e);
                    else {
                        expect(r).to.equal(5);
                        done(null);
                    }
                });
            }
        });

    });



    it('#09 0 table after dropping one', function(done) {

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