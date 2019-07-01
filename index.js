'use strict';




const AWS = require("aws-sdk");

const dyndbstore = function () {

    let initiated = false;
    let db;
    let doc;

    const verify = () => {
        if(!initiated)
            throw new Error('!!! module not initiated !!!');
    };

    const init = (config) => {
        console.log("[init|in] config:", config);
        db = new AWS.DynamoDB(config);
        doc = new AWS.DynamoDB.DocumentClient(config);
        initiated = true;
        console.log("[init|out]");
    };

    const dropTable = (table, callback) => {
        console.log("[dropTable|in] table:", table);

        try {
            verify();

            let params = {
                TableName: table
            };
            db.deleteTable(params, function (err) {
                if (err) {
                    if (err.code === 'ResourceNotFoundException')
                        callback(null, 'no table there');
                    else
                        callback(err);
                } else
                    callback(null);
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[dropTable|out]");
    };

    const createTable = (parameters, callback) => {
        console.log("[createTable|in] parameters:", parameters);

        try {
            verify();

            if( (! parameters.table) || (! parameters.rangeKey) || (! parameters.numKey) )
                throw new Error('!!! must provide parameters ( table, rangeKey, numKey ) !!!');

            let params = {
                AttributeDefinitions: [
                    {
                        AttributeName: parameters.rangeKey,
                        AttributeType: 'S'
                    },
                    {
                        AttributeName: parameters.numKey,
                        AttributeType: 'N'
                    }
                ],
                KeySchema: [
                    {
                        AttributeName: parameters.rangeKey,
                        KeyType: 'HASH'
                    },
                    {
                        AttributeName: parameters.numKey,
                        KeyType: 'RANGE'
                    }
                ],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 16,
                    WriteCapacityUnits: 5
                },
                TableName: parameters.table,
                StreamSpecification: {
                    StreamEnabled: false
                }
            };

            db.createTable(params, function (err, data) {
                if (err)
                    callback(err);
                else
                    callback(null, data);
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[createTable|out]");
    };

    const findTable = (table, callback) => {
        console.log("[findTable|in] table:", table);
        try{
            verify();

            let params = {"Limit": 100};
            db.listTables(params, function (err, data) {
                if (err)
                    callback(err);
                else
                    callback(null, data.TableNames && 0 < data.TableNames.length && -1 < data.TableNames.indexOf(table));
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[findTable|out]");
    };

    const getObjsCount = (table, callback) => {
        console.log("[getObjsCount|in] table:", table);
        try{
            verify();
            let params = {TableName: table};
            doc.scan(params, (e, d) => {
                if (e)
                    callback(e);
                else {
                    callback(null, d.Items.length);
                }
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[getObjsCount|out]");
    };

    const putObj = (table, obj, callback) => {
        console.log("[putObj|in] table:", table, "obj:", obj);
        try{
            verify();
            let params = {
                TableName: table,
                Item: obj
            };

            doc.put(params, function (err, data) {
                if (err)
                    callback(err);
                else
                    callback(null, data);

            });
        }
        catch(e){
            callback(e);
        }
        console.log("[putObj|out]");
    };

    const getObj = (table, key, callback) => {
        console.log("[getObj|in] table:", table, " key:", key);
        try{
            verify();

            let expAttrValues = {};
            let expAttrNames = {};
            let expression = '';

            let i=0;
            for (let prop in key) {
                if (key.hasOwnProperty(prop)) {
                    expAttrValues[':' + prop] = key[prop];
                    expAttrNames['#' + prop] = prop;
                    expression +=  ( 0 < i ? ' and ' : '' ) + '#' + prop + ' = ' + ':' + prop
                    i++;
                }
            }

            let params = {
                ExpressionAttributeValues: expAttrValues
                , ExpressionAttributeNames: expAttrNames
                , KeyConditionExpression: expression
                , TableName: table
            };
            doc.query(params, (e, d) => {
                console.log('[getObj|doc.query|cb] e:', e, 'd:', d);
                if (e)
                    callback(e);
                else {
                    let out = null;
                    if (Array.isArray(d.Items) && 0 < d.Items.length)
                        out = d.Items[0]
                    callback(null, out);
                }

            });
        }
        catch(e){
            callback(e);
        }
        console.log("[getObj|out]");
    };

    const delObj = (table, key, callback) => {
        console.log("[delObj|in] table:", table, "key:", key);
        try {
            verify();

            var params = {
                Key: key,
                TableName: table
            };

            doc.delete(params, function (err, data) {
                if (err)
                    callback(err);
                else
                    callback(null);
            });

        }
        catch(e){
                callback(e);
            }

        console.log("[delObj|out]");
    };

    const findObj = (table, filter, callback) => {
        console.log("[findObj|in] table:", table, "filter:", filter);
        try {
            verify();

            let filterExpression = '';
            let expressionValues = {};

            let i=0;
            for (let prop in filter) {
                if (filter.hasOwnProperty(prop)) {
                    filterExpression +=  ( 0 < i ? ' and ' : '' ) + prop + ' = ' + ':' + prop
                    expressionValues[':' + prop] = filter[prop];
                    i++;
                }
            }

            var params = {
                TableName : table,
                FilterExpression : filterExpression,
                ExpressionAttributeValues : expressionValues
            };

            doc.scan(params, function(err, data) {
                if (err)
                    callback(err);
                else {
                    callback(null, data.Items);
                }
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[findObj|out]");
    };




    return {
        createTable: createTable
        , init: init
        , findTable: findTable
        , dropTable: dropTable
        , getObjsCount: getObjsCount
        , putObj: putObj
        , getObj: getObj
        , delObj: delObj
        , findObj: findObj
    };

}();


/**
 * store facade to DynamoDb
 * @return {object}
 */
module.exports = dyndbstore;