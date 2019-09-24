'use strict';

const winston = require('winston');
const AWS = require("aws-sdk");
const commons = require('@jtviegas/jscommons').commons;
const logger = winston.createLogger(commons.getDefaultWinstonConfig());

const dyndbstore = function () {

    const AWS_API_VERSION = '2012-08-10';

    const CONFIGURATION_SPEC = {
        region: 'DYNDBSTORE_AWS_REGION'
        , accessKeyId: 'DYNDBSTORE_AWS_ACCESS_KEY_ID'
        , secretAccessKey: 'DYNDBSTORE_AWS_ACCESS_KEY'
        , test: 'DYNDBSTORE_TEST'
    };

    let initiated = false;
    let db;
    let doc;

    const verify = () => {
        if(!initiated)
            throw new Error('!!! module not initiated !!!');
    };

    const init = (config) => {
        logger.info("[dyndbstore|init|in] (%o)", config);

        let configuration = commons.getConfiguration(CONFIGURATION_SPEC, config);
        configuration.apiVersion = AWS_API_VERSION;
        if( configuration.test && configuration.test.store_endpoint )
            configuration.endpoint = configuration.test.store_endpoint;

        if(!initiated){
            db = new AWS.DynamoDB(configuration);
            doc = new AWS.DynamoDB.DocumentClient(configuration);
            initiated = true;
        }
        logger.info("[dyndbstore|init|out]");
    };

    const dropTable = (table, callback) => {
        logger.debug("[dyndbstore|dropTable|in] (%s)", table);

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
        logger.debug("[dyndbstore|dropTable|out]");
    };

    const createTableWithNumericId = (name, callback) => {
        logger.debug("[dyndbstore|createTableWithNumericId|in] (%s)", name);

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
        logger.debug("[dyndbstore|createTableWithNumericId|out]");
    };


    const findTable = (table, callback) => {
        logger.debug("[dyndbstore|findTable|in] (%s)", table);
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
        logger.debug("[dyndbstore|findTable|out]");
    };

    const getObjsCount = (table, callback) => {
        logger.debug("[dyndbstore|getObjsCount|in] (%s)", table);
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
        logger.debug("[dyndbstore|getObjsCount|out]");
    };

    const getObjs = (table, callback) => {
        logger.debug("[dyndbstore|getObjs|in] (%s)", table);
        try{
            verify();
            let params = {TableName: table};
            //logger.debug("[dyndbstore|getObjs] going for scan %o", doc);
            doc.scan(params, (e, d) => {
                logger.debug("[dyndbstore|getObjs] e:%o d%o", e, d);
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
        logger.debug("[dyndbstore|getObjs|out]");
    };


    const putObj = (table, obj, callback) => {
        logger.debug("[dyndbstore|putObj|in] (%s,%o)", table, obj);
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
        logger.debug("[dyndbstore|putObj|out]");
    };

    const putObjs = (table, objArray, callback) => {
        logger.debug("[dyndbstore|putObjs|in] (%s,%o)", table, objArray);
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
        logger.debug("[dyndbstore|putObjs|out]");
    };

    const getObj = (table, key, callback) => {
        logger.debug("[dyndbstore|getObj|in] (%s,%s)", table, key);
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
                logger.debug('[dyndbstore|getObj|doc.query|cb] (%o,%o)', e, d);
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
        logger.debug("[dyndbstore|getObj|out]");
    };

    const delObj = (table, key, callback) => {
        logger.debug("[dyndbstore|delObj|in] (%s,%s)", table, key);
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

        logger.debug("[dyndbstore|delObj|out]");
    };

    const findObj = (table, filter, callback) => {
        logger.debug("[dyndbstore|findObj|in] (%s,%o)", table, filter);
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
        logger.debug("[dyndbstore|findObj|out]");
    };

    const findObjsByIdRange = (table, startId, endId, callback) => {
        logger.debug("[dyndbstore|findObjsByIdRange|in] (%s,%s,%s)", table, startId, endId);
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
        logger.debug("[dyndbstore|findObjsByIdRange|out]");
    };

    const findObjsByCriteria = (table, criteria, join, callback) => {
        logger.debug("[dyndbstore|findObjsByCriteria|in] (%s,%o,%o)", table, criteria, join);
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
        logger.debug("[dyndbstore|findObjsByIdRange|out]");
    };

    const delObjs = (table, idArray, callback) => {
        logger.debug("[dyndbstore|delObjs|in] (%s,%o)", table, idArray);
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
        logger.debug("[dyndbstore|delObjs|out]");
    };

    const findObjIds = (table, callback) => {
        logger.debug("[dyndbstore|findObjIds|in] (%s)", table);
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
        logger.debug("[dyndbstore|findObjIds|out]");
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