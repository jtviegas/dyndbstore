const winston = require('winston');
const { 
    DynamoDBClient, DeleteTableCommand, ListTablesCommand, CreateTableCommand,
    PutItemCommand, GetItemCommand, ScanCommand, DeleteItemCommand, DescribeTableCommand
} = require("@aws-sdk/client-dynamodb");

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.splat(),
        winston.format.timestamp(),
        winston.format.printf(info => {
                return `${info.timestamp} ${info.level}: ${info.message}`;
            })
        ),
    transports: [new winston.transports.Console()],
    exitOnError: false
});





class DynamoDbStore {

    constructor(config) {
        this.client = new DynamoDBClient(config);
        this.scanLimit = config.scanLimit || 12 
    }

    async dropTable(table) {
        logger.info("[DynamoDbStore|dropTable|in] (%s)", table);
        const input = { TableName: table};
        const command = new DeleteTableCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|dropTable] response: %o", response);
        logger.info("[DynamoDbStore|dropTable|out]");
    };

    async isTable(table) {
        logger.info("[DynamoDbStore|isTable|in] (%s)", table);
        const command = new ListTablesCommand({});
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|isTable] response: %o", response)
        const result = 'TableNames' in response ? response["TableNames"].includes(table) : false;
        logger.info("[DynamoDbStore|isTable|out] => %s", result);
        return result
    };

    async getTableStatus(name) {
        logger.info("[DynamoDbStore|getTableStatus|in] (%s)", name);
        const input = {
            "TableName": name
          };
        const command = new DescribeTableCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|getTableStatus] response: %o", response)
        const result = response["Table"]["TableStatus"]
        logger.info("[DynamoDbStore|getTableStatus|out] => %s", result);
        return result
    };

    async createTableWithId(table) {
        logger.info("[DynamoDbStore|createTableWithId|in] (%s)", table);

        const input = {
            AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
            TableName: table,
            KeySchema: [ { AttributeName: 'id', KeyType: 'HASH' } ],
            BillingMode: "PAY_PER_REQUEST",
            DeletionProtectionEnabled: false,
            SSESpecification: {
                Enabled: false,
            },
            StreamSpecification: {
                StreamEnabled: false
            },
            TableClass: "STANDARD"
        };
        const command = new CreateTableCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|createTableWithId] response: %o", response);

        logger.info("[DynamoDbStore|createTableWithId|out]");
    };

    async putObj (table, obj) {
        logger.info("[DynamoDbStore|putObj|in] (%s, %o)", table, obj);
        const input= { 
            "Item": obj,
            "TableName": table, 
            "ReturnConsumedCapacity": "TOTAL",
          };
        const command = new PutItemCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|putObj] (%o)", response)
        const result = response["ConsumedCapacity"]["CapacityUnits"] === 1 ? obj : undefined;
        logger.info("[DynamoDbStore|putObj|out] (%o)", result)
        return result
    };

    async getObj (table, key) {
        logger.info("[DynamoDbStore|getObj|in] (%s, %o)", table, key);
        const input= { 
            "TableName": table, 
            "Key": key
          };
        const command = new GetItemCommand(input);
        const response = await this.client.send(command);

        logger.info("[DynamoDbStore|getObj|out] => %o", response);
        return response["Item"]
    };

    async getObjs (table, key) {
        logger.info("[DynamoDbStore|getObjs|in] (%s)", table);
        const input= { 
            "TableName": table,
            "Limit": this.scanLimit
        };
        input.ExclusiveStartKey = key || undefined

        const command = new ScanCommand(input);
        const response = await this.client.send(command);
        const result = { 
            "items": response.Items,
            "lastKey": response.LastEvaluatedKey || undefined
         }
        logger.info("[DynamoDbStore|getObjs|out] => %o", result);
        return result
    };

    async delObj (table, key) {
        logger.info("[DynamoDbStore|delObj|in] (%s, %o)", table, key);
        const input= { 
            "TableName": table, 
            "Key": key
          };
        const command = new DeleteItemCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|delObj|out] => %o", response);
        return response
    };

}

class AbstractSchema {

    toEntity(obj){
        throw new Error("toEntity() must be implemented by subclasses");
    }

    fromEntity(entity){
        throw new Error("fromEntity() must be implemented by subclasses");
    }
}

class DynamoDbStoreWrapper {

    constructor(config, schemas) {
        this.store = new DynamoDbStore(config);
        this.schemas = schemas;
    }

    getSchema(table){
        
        let result = undefined;
        if (Object.hasOwn(this.schemas, table)){
            result = this.schemas[table]
        }
        else if (Object.hasOwn(this.schemas, "*")){
            result = this.schemas["*"]
        } 
        else {
            throw new Error(`[getSchema] no schema for table: ${table}`);
        }
        return result;
    }

    getTableSuffix(table){
        const parts = table.split(".")
        return parts[(parts.length - 1) ] 
    }

    async getTableStatus(name) {
        logger.info("[DynamoDbStoreWrapper|getTableStatus|in] (%s)", name);
        const result = await this.store.getTableStatus(name);
        logger.info("[DynamoDbStoreWrapper|getTableStatus|out] => %s", result);
        return result
    }

    async createTable(name) {
        logger.info("[DynamoDbStoreWrapper|createTable|in] (%s)", name);
        await this.store.createTableWithId(name);
        logger.info("[DynamoDbStoreWrapper|createTable|out]");
    }

    async dropTable(name) {
        logger.info("[DynamoDbStoreWrapper|dropTable|in] (%s)", name);
        await this.store.dropTable(name);
        logger.info("[DynamoDbStoreWrapper|dropTable|out]");
    }

    async isTable(name) {
        logger.info("[DynamoDbStoreWrapper|isTable|in] (%s)", name);
        const result = await this.store.isTable(name);
        logger.info("[DynamoDbStoreWrapper|isTable|out] => %s", result);
        return result
    }

    async putObj (table, obj) {
        logger.info("[DynamoDbStoreWrapper|putObj|in] (%s, %o)", table, obj);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.putObj(table, schema.toEntity(obj));
        const result = response ? schema.fromEntity(response) : undefined
        logger.info("[DynamoDbStoreWrapper|putObj|out] (%o)", result)
        return result
    };

    async getObj (table, id) {
        logger.info("[DynamoDbStoreWrapper|getObj|in] (%s, %s)", table, id);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.getObj(table, {"id": {"S": id}});
        const result = schema.fromEntity(response)
        logger.info("[DynamoDbStoreWrapper|getObj|out] => %o", result);
        return result
    };

    async getObjs (table, lastKey) {
        logger.info("[DynamoDbStoreWrapper|getObjs|in] (%s, %s)", table, lastKey);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.getObjs(table, lastKey);
        const result = {
            "lastKey": response.lastKey,
            "items": []
        }
        for(const item of response.items){
            result.items.push(schema.fromEntity(item))
        }
        logger.info("[DynamoDbStoreWrapper|getObjs|out] => %o", result);
        return result
    };

    async delObj (table, id) {
        logger.info("[DynamoDbStoreWrapper|delObj|in] (%s, %s)", table, id);
        const response = await this.store.delObj(table, {"id": {"S": id}});
        logger.info("[DynamoDbStoreWrapper|delObj|out] => %o", response);
    };

}

module.exports = {};
module.exports.DynamoDbStore = DynamoDbStore;
module.exports.DynamoDbStoreWrapper = DynamoDbStoreWrapper;
module.exports.AbstractSchema = AbstractSchema;