'use strict';

const AWS = require("aws-sdk");
const commons = require('@jtviegas/jscommons').commons;

const dyndbstore = function () {

    const AWS_API_VERSION = '2012-08-10';

    const CONFIGURATION_SPEC = {
        region: 'DYNDBSTORE_AWS_REGION'
        , endpoint: 'DYNDBSTORE_AWS_DB_ENDPOINT'
        , accessKeyId: 'DYNDBSTORE_AWS_ACCESS_KEY_ID'
        , secretAccessKey: 'DYNDBSTORE_AWS_ACCESS_KEY'
    };

    let initiated = false;
    let db;
    let doc;

    const verify = () => {
        if(!initiated)
            throw new Error('!!! module not initiated !!!');
    };

    const init = (config) => {
        console.log("[init|in] config:", config);

        let configuration = commons.getConfiguration(CONFIGURATION_SPEC, config);
        configuration.apiVersion = AWS_API_VERSION;

        if(!initiated){
            db = new AWS.DynamoDB(configuration);
            doc = new AWS.DynamoDB.DocumentClient(configuration);
            initiated = true;
        }
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

    const createTableWithNumericId = (name, callback) => {
        console.log("[createTableWithNumericId|in] name:", name);

        try {
            verify();

            if( ! name )
                throw new Error('!!! must provide table name !!!');

            let params = {
                AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'N' }],
                KeySchema: [ { AttributeName: 'id', KeyType: 'HASH' } ],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 16,
                    WriteCapacityUnits: 5
                },
                TableName: name,
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
        console.log("[createTableWithNumericId|out]");
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

    const getObjs = (table, callback) => {
        console.log("[getObjs|in] table:", table);
        try{
            verify();
            let params = {TableName: table};
            console.log("[getObjs] going for scan doc:", doc);
            doc.scan(params, (e, d) => {
                console.log("[getObjs] e:", e);
                console.log("[getObjs] d:", d);
                if (e)
                    callback(e);
                else {
                    callback(null, d.Items);
                }
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[getObjs|out]");
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

    const putObjs = (table, objArray, callback) => {
        console.log("[putObjs|in] table:", table, "objArray:", objArray);
        try{
            verify();
            let params = {};
            params['RequestItems'] = {};
            params.RequestItems[table] = [];

            for (let obj of objArray)
                params.RequestItems[table].push( {PutRequest: { Item: obj }} );

            doc.batchWrite(params, function (err, data) {
                if (err)
                    callback(err);
                else
                    callback(null, data);

            });
        }
        catch(e){
            callback(e);
        }
        console.log("[putObjs|out]");
    };

    const getObj = (table, key, callback) => {
        console.log("[getObj|in] table:", table, " key:", key);
        try{
            verify();

            let expAttrValues = {};
            let expAttrNames = {};
            let expression = '#id = :id';

            expAttrValues[':id'] = key;
            expAttrNames['#id'] = 'id';

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
                Key: {id: key},
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

            let params = {
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

    const findObjsByIdRange = (table, startId, endId, callback) => {
        console.log("[findObjsByIdRange|in] table:", table, "startId:", startId, "endId:", endId);
        try {
            verify();

            let expressionValues = {':id_1': startId, ':id_2': endId};
            let expAttrNames = {'#id': 'id'};
            let filterExpression = ':id_1 <= #id and :id_2 >= #id';

            let params = {
                TableName : table,
                FilterExpression : filterExpression,
                ExpressionAttributeValues : expressionValues,
                ExpressionAttributeNames: expAttrNames
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
        console.log("[findObjsByIdRange|out]");
    };

    const findObjsByCriteria = (table, criteria, join, callback) => {
        console.log("[findObjsByCriteria|in] table:", table, "criteria:", criteria, "join:", join);
        try {
            verify();

            let joinExpression = ( join ? ' and ' : ' or ' );

            let expressionValues = {};
            let expAttrNames = {};
            let filterExpression = '';

            let index = 0;
            for (let att in criteria){

                if (criteria.hasOwnProperty(att)) {
                    if( Array.isArray(criteria[att]) ){
                        let expValue1  = ':' + att + '_a_' + index;
                        let expValue2  = ':' + att + '_b_' + index;
                        expressionValues[expValue1] = criteria[att][0];
                        expressionValues[expValue2] = criteria[att][1];
                        let expAttName = '#' + att;
                        expAttrNames[expAttName] = att;
                        filterExpression += (0 === filterExpression.length ? '' : joinExpression) + expValue1 + ' <= ' + expAttName + ' and ' + expValue2 +  ' >= ' + expAttName;
                    }
                    else {
                        let expValue1  = ':' + att + '_a_' + index;
                        expressionValues[expValue1] = criteria[att];
                        let expAttName = '#' + att;
                        expAttrNames[expAttName] = att;
                        filterExpression += (0 === filterExpression.length ? '' : joinExpression) + expValue1 + ' = ' + expAttName;
                    }

                    index++;
                }
            }

            let params = {
                TableName : table,
                FilterExpression : filterExpression,
                ExpressionAttributeValues : expressionValues,
                ExpressionAttributeNames: expAttrNames
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
        console.log("[findObjsByIdRange|out]");
    };

    const delObjs = (table, idArray, callback) => {
        console.log("[delObjs|in] table:", table, "idArray:", idArray);
        try{
            verify();
            let params = {};
            params['RequestItems'] = {};
            params.RequestItems[table] = [];

            for (let _id of idArray)
                params.RequestItems[table].push( {DeleteRequest: { Key: { id: _id } }} );

            doc.batchWrite(params, function (err, data) {
                if (err)
                    callback(err);
                else
                    callback(null, data);

            });
        }
        catch(e){
            callback(e);
        }
        console.log("[delObjs|out]");
    };

    const findObjIds = (table, callback) => {
        console.log("[findObjIds|in] table:", table);
        try {
            verify();
            let params = {
                TableName : table
                , AttributesToGet: [ 'id' ]
            };
            doc.scan(params, function(err, data) {
                if (err)
                    callback(err);
                else {
                    let result = [];
                    for( let i=0; i < data.Items.length; i++ ){
                        let item = data.Items[i];
                        result.push(item.id);
                    }
                    callback(null, result);
                }
            });
        }
        catch(e){
            callback(e);
        }
        console.log("[findObjIds|out]");
    };


    return {
        createTable: createTableWithNumericId
        , init: init
        , findTable: findTable
        , dropTable: dropTable
        , getObjsCount: getObjsCount
        , putObj: putObj
        , getObj: getObj
        , delObj: delObj
        , findObj: findObj
        , putObjs: putObjs
        , findObjsByIdRange: findObjsByIdRange
        , findObjsByCriteria: findObjsByCriteria
        , getObjs: getObjs
        , delObjs: delObjs
        , findObjIds: findObjIds
    };

}();


/**
 * store facade to DynamoDb
 * @return {object}
 */
module.exports = dyndbstore;